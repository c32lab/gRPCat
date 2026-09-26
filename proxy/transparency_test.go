package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/c32lab/gRPCat/middleware"
)

// These tests pin behaviors a client must not be able to tell apart from
// talking to the backend directly: when the RPC completes, which status code
// it ends with, when headers arrive, and what the backend sees on the wire.

var bidiDesc = &grpc.StreamDesc{StreamName: "Bidi", ServerStreams: true, ClientStreams: true}

// startBidiBackend starts a backend whose /test.Echo/Bidi handler is h.
func startBidiBackend(t *testing.T, h func(grpc.ServerStream) error) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.ForceServerCodecV2(&ProxyCodec{}))
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{{
			StreamName: "Bidi", ServerStreams: true, ClientStreams: true,
			Handler: func(_ any, s grpc.ServerStream) error { return h(s) },
		}},
	}, nil)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func dialFrameClient(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(&ProxyCodec{})))
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func openBidi(t *testing.T, conn *grpc.ClientConn) grpc.ClientStream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	stream, err := grpc.NewClientStream(ctx, bidiDesc, conn, "/test.Echo/Bidi")
	if err != nil {
		t.Fatalf("new stream: %v", err)
	}
	return stream
}

// recvUntilEnd drains the stream and returns the error that ended it.
func recvUntilEnd(stream grpc.ClientStream) error {
	for {
		if err := stream.RecvMsg(&Frame{}); err != nil {
			return err
		}
	}
}

// TestForwarder_BackendCompletesBeforeClientHalfCloses: a backend may finish
// the RPC while the client still has its send side open. Directly connected,
// the client gets the OK status at once; the proxy must not hold it back until
// the client half-closes, and a message the client sends in that window must
// not turn the OK into an error.
func TestForwarder_BackendCompletesBeforeClientHalfCloses(t *testing.T) {
	backendAddr := startBidiBackend(t, func(s grpc.ServerStream) error {
		f := &Frame{}
		if err := s.RecvMsg(f); err != nil {
			return err
		}
		f.Free()
		return nil
	})
	conn := dialFrameClient(t, startProxyServer(t, backendAddr))

	t.Run("status arrives without half-close", func(t *testing.T) {
		stream := openBidi(t, conn)
		if err := stream.SendMsg(frameFromBytes([]byte("m1"))); err != nil {
			t.Fatalf("send: %v", err)
		}
		done := make(chan error, 1)
		go func() { done <- stream.RecvMsg(&Frame{}) }()
		select {
		case err := <-done:
			if !errors.Is(err, io.EOF) {
				t.Fatalf("want io.EOF (backend completed OK), got %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("client never received the backend's completion: proxy waited for the client to half-close")
		}
	})

	t.Run("send after backend completion keeps OK", func(t *testing.T) {
		stream := openBidi(t, conn)
		if err := stream.SendMsg(frameFromBytes([]byte("m1"))); err != nil {
			t.Fatalf("send: %v", err)
		}
		// Give the backend time to complete and the proxy time to react.
		time.Sleep(300 * time.Millisecond)
		_ = stream.SendMsg(frameFromBytes([]byte("m2"))) // may or may not fail; the status below is what matters
		stream.CloseSend()
		if err := recvUntilEnd(stream); !errors.Is(err, io.EOF) {
			t.Fatalf("backend completed with OK but client saw %v", err)
		}
	})
}

// TestForwarder_BackendErrorForwardsHeaders: headers a backend set before
// failing reach the client, whether it flushed them (SendHeader) or left grpc
// to send them with the status (SetHeader).
func TestForwarder_BackendErrorForwardsHeaders(t *testing.T) {
	for _, flush := range []bool{true, false} {
		name := "SetHeader"
		if flush {
			name = "SendHeader"
		}
		t.Run(name, func(t *testing.T) {
			backendAddr := startBidiBackend(t, func(s grpc.ServerStream) error {
				md := metadata.Pairs("x-backend", "yes")
				var err error
				if flush {
					err = s.SendHeader(md)
				} else {
					err = s.SetHeader(md)
				}
				if err != nil {
					return err
				}
				return status.Error(codes.NotFound, "nope")
			})
			stream := openBidi(t, dialFrameClient(t, startProxyServer(t, backendAddr)))
			_ = stream.SendMsg(frameFromBytes([]byte("m1")))
			stream.CloseSend()
			hdr, err := stream.Header()
			if err != nil {
				t.Fatalf("header: %v", err)
			}
			if got := hdr.Get("x-backend"); len(got) != 1 || got[0] != "yes" {
				t.Errorf("backend header lost on the error path: x-backend=%v", got)
			}
			if err := recvUntilEnd(stream); status.Code(err) != codes.NotFound {
				t.Errorf("want NotFound from backend, got %v", err)
			}
		})
	}
}

// TestForwarder_HeadersArriveBeforeFirstMessage: a backend that sends headers
// and then waits must not leave the client's Header() blocked until the first
// response message.
func TestForwarder_HeadersArriveBeforeFirstMessage(t *testing.T) {
	backendAddr := startBidiBackend(t, func(s grpc.ServerStream) error {
		f := &Frame{}
		if err := s.RecvMsg(f); err != nil { // m1
			return err
		}
		f.Free()
		if err := s.SendHeader(metadata.Pairs("x-early", "yes")); err != nil {
			return err
		}
		if err := s.RecvMsg(f); err != nil { // m2: only sent once the client has seen the headers
			return err
		}
		f.Free()
		return s.SendMsg(frameFromBytes([]byte("r1")))
	})
	stream := openBidi(t, dialFrameClient(t, startProxyServer(t, backendAddr)))
	if err := stream.SendMsg(frameFromBytes([]byte("m1"))); err != nil {
		t.Fatalf("send m1: %v", err)
	}

	type hdrResult struct {
		md  metadata.MD
		err error
	}
	got := make(chan hdrResult, 1)
	go func() {
		md, err := stream.Header()
		got <- hdrResult{md, err}
	}()
	select {
	case r := <-got:
		if r.err != nil {
			t.Fatalf("header: %v", r.err)
		}
		if v := r.md.Get("x-early"); len(v) != 1 || v[0] != "yes" {
			t.Fatalf("want x-early=yes, got %v", v)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Header() blocked: proxy only forwards headers together with the first response message")
	}

	if err := stream.SendMsg(frameFromBytes([]byte("m2"))); err != nil {
		t.Fatalf("send m2: %v", err)
	}
	stream.CloseSend()
	if err := stream.RecvMsg(&Frame{}); err != nil {
		t.Fatalf("recv r1: %v", err)
	}
	if err := recvUntilEnd(stream); !errors.Is(err, io.EOF) {
		t.Fatalf("want clean end, got %v", err)
	}
}

// TestForwarder_UnreachableBackendIsUnavailable: a backend the proxy cannot
// connect to must surface as Unavailable, the code retry policies key on, not
// as Internal.
func TestForwarder_UnreachableBackendIsUnavailable(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	deadAddr := lis.Addr().String()
	lis.Close() // nothing listens here any more

	stream := openBidi(t, dialFrameClient(t, startProxyServer(t, deadAddr)))
	_ = stream.SendMsg(frameFromBytes([]byte("m1")))
	stream.CloseSend()
	if err := recvUntilEnd(stream); status.Code(err) != codes.Unavailable {
		t.Fatalf("want Unavailable for an unreachable backend, got %v", err)
	}
}

// TestForwarder_BackendRejectsBeforeFirstFrame: a backend that fails the RPC
// without reading anything ends the stream around the time the proxy writes
// the first frame. Whichever side wins, the client must see the backend's
// status. This is an end-to-end smoke check only: the timing that makes the
// first SendMsg return io.EOF is rare, so the decision itself is pinned by
// TestSendFirstFrame_StatusFidelity.
func TestForwarder_BackendRejectsBeforeFirstFrame(t *testing.T) {
	backendAddr := startImmediateErrorStreamBackend(t, codes.Unauthenticated, "who are you")
	conn := dialFrameClient(t, startProxyServer(t, backendAddr))
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		stream, err := grpc.NewClientStream(ctx, clientStreamDesc, conn, "/test.Echo/ServerStream")
		if err != nil {
			cancel()
			t.Fatalf("new stream: %v", err)
		}
		_ = stream.SendMsg(frameFromBytes(buildGRPCMessage(make([]byte, 64<<10))))
		err = recvUntilEnd(stream)
		cancel()
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("iteration %d: want the backend's Unauthenticated, got %v", i, err)
		}
	}
}

// TestServer_GetGRPCServerServesProtoServices: services registered on the
// proxy's grpc.Server (the documented way to add health/reflection) use
// protobuf messages, which the forced ProxyCodec must hand to the proto codec
// instead of rejecting.
func TestServer_GetGRPCServerServesProtoServices(t *testing.T) {
	srv, err := NewServer(&Config{DefaultBackend: startProtoEchoBackend(t)})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	healthpb.RegisterHealthServer(srv.GetGRPCServer(), health.NewServer())
	conn := dialProtoProxy(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health check through the proxy's server failed: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("health status: want SERVING, got %v", resp.GetStatus())
	}

	// Proxying on the same server must be unaffected.
	out := &wrapperspb.StringValue{}
	if err := conn.Invoke(ctx, "/test.ProtoEcho/Echo", wrapperspb.String("hi"), out); err != nil {
		t.Fatalf("proxied invoke: %v", err)
	}
	if out.GetValue() != "echo:hi" {
		t.Errorf("proxied response: want echo:hi, got %q", out.GetValue())
	}
}

// TestForwarder_BackendSeesClientContentType: the backend must see the
// content-type the client sent (bare application/grpc for a stock client, the
// client's subtype otherwise), not one derived from ProxyCodec's name, and
// only the proxy's own grpc-accept-encoding.
func TestForwarder_BackendSeesClientContentType(t *testing.T) {
	seen := make(chan metadata.MD, 1)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	backend := grpc.NewServer()
	backend.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.ProtoEcho",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Echo",
			Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				md, _ := metadata.FromIncomingContext(ctx)
				select {
				case seen <- md:
				default:
				}
				in := &wrapperspb.StringValue{}
				if err := dec(in); err != nil {
					return nil, err
				}
				return in, nil
			},
		}},
	}, nil)
	go backend.Serve(lis)
	t.Cleanup(backend.Stop)

	srv, err := NewServer(&Config{DefaultBackend: lis.Addr().String()})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	conn := dialProtoProxy(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Invoke(ctx, "/test.ProtoEcho/Echo", wrapperspb.String("hi"), &wrapperspb.StringValue{}); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	md := <-seen
	if got := md.Get("content-type"); len(got) != 1 || got[0] != "application/grpc" {
		t.Errorf("backend saw content-type %v, want [application/grpc]", got)
	}
	if got := md.Get("grpc-accept-encoding"); len(got) > 1 {
		t.Errorf("client's grpc-accept-encoding forwarded alongside the proxy's own: %v", got)
	}

	// A client that names a subtype must have it forwarded unchanged.
	if err := conn.Invoke(ctx, "/test.ProtoEcho/Echo", wrapperspb.String("hi"), &wrapperspb.StringValue{},
		grpc.CallContentSubtype("proto")); err != nil {
		t.Fatalf("invoke with subtype: %v", err)
	}
	md = <-seen
	if got := md.Get("content-type"); len(got) != 1 || got[0] != "application/grpc+proto" {
		t.Errorf("backend saw content-type %v, want [application/grpc+proto]", got)
	}
}

// TestMiddleware_RequestMetadataEditsReachBackend: Request.Metadata is the
// metadata the backend receives, so Set/Delete/Append by middleware take
// effect (gin-style) while untouched entries pass through unchanged.
func TestMiddleware_RequestMetadataEditsReachBackend(t *testing.T) {
	seen := make(chan metadata.MD, 1)
	backendAddr := startMetadataCapturingBackend(t, seen)

	srv, err := NewServer(&Config{DefaultBackend: backendAddr})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv.Use(middleware.MiddlewareFunc(func(ctx *middleware.Context) {
		ctx.Request.Metadata.Set("authorization", "Bearer service-token")
		ctx.Request.Metadata.Delete("x-secret")
		ctx.AddMetadata("x-routed-by", "grpcat")
	}))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.grpcServer.Serve(lis)
	t.Cleanup(srv.Stop)
	conn := dialFrameClient(t, lis.Addr().String())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"authorization", "Bearer client-token",
		"x-secret", "1",
		"x-keep", "1",
	))
	if err := conn.Invoke(ctx, "/test.Echo/Echo", frameFromBytes([]byte("x")), &Frame{}); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	md := <-seen
	if got := md.Get("authorization"); len(got) != 1 || got[0] != "Bearer service-token" {
		t.Errorf("Set did not replace the client's value: authorization=%v", got)
	}
	if got := md.Get("x-secret"); len(got) != 0 {
		t.Errorf("Delete did not remove the entry: x-secret=%v", got)
	}
	if got := md.Get("x-routed-by"); len(got) != 1 || got[0] != "grpcat" {
		t.Errorf("AddMetadata entry missing: x-routed-by=%v", got)
	}
	if got := md.Get("x-keep"); len(got) != 1 || got[0] != "1" {
		t.Errorf("untouched entry did not pass through: x-keep=%v", got)
	}
}

// sendErrClientStream is a grpc.ClientStream whose SendMsg returns sendErr;
// nothing else is implemented.
type sendErrClientStream struct {
	grpc.ClientStream
	sendErr error
}

func (s *sendErrClientStream) SendMsg(any) error { return s.sendErr }

// TestSendFirstFrame_StatusFidelity pins how the first frame's send result is
// reported: io.EOF (the backend already ended the stream) defers to the status
// RecvMsg will deliver, status errors pass through, anything else is Internal.
func TestSendFirstFrame_StatusFidelity(t *testing.T) {
	cases := []struct {
		name    string
		sendErr error
		wantNil bool
		want    codes.Code
	}{
		{"ok", nil, true, codes.OK},
		{"eof defers to RecvMsg", io.EOF, true, codes.OK},
		{"status passes through", status.Error(codes.ResourceExhausted, "too big"), false, codes.ResourceExhausted},
		{"other error is Internal", errors.New("boom"), false, codes.Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := sendFirstFrame(&sendErrClientStream{sendErr: tc.sendErr}, frameFromBytes([]byte("x")))
			if tc.wantNil {
				if err != nil {
					t.Fatalf("want nil, got %v", err)
				}
				return
			}
			if status.Code(err) != tc.want {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
}

// oneMessageClientStream is a grpc.ClientStream that delivers one message and
// then io.EOF, with no headers.
type oneMessageClientStream struct {
	delivered bool
}

func (s *oneMessageClientStream) Header() (metadata.MD, error) { return nil, nil }
func (s *oneMessageClientStream) Trailer() metadata.MD         { return nil }
func (s *oneMessageClientStream) CloseSend() error             { return nil }
func (s *oneMessageClientStream) Context() context.Context     { return context.Background() }
func (s *oneMessageClientStream) SendMsg(any) error            { return nil }
func (s *oneMessageClientStream) RecvMsg(m any) error {
	if s.delivered {
		return io.EOF
	}
	s.delivered = true
	f := m.(*Frame)
	f.Free()
	*f = *frameFromBytes([]byte("m"))
	return nil
}

// sendErrServerStream is a grpc.ServerStream whose SendMsg fails with sendErr.
type sendErrServerStream struct {
	recvErrServerStream
	sendErr error
}

func (s *sendErrServerStream) SendMsg(any) error { return s.sendErr }

// TestForwardBackendToClient_ClientWriteFailureIsNotBackendDone pins that a
// failed write to the client is reported with backendDone=false: the backend
// stream is still live, so Forward must not read its trailers (a data race
// against grpc-go's transport) and cancels it instead.
func TestForwardBackendToClient_ClientWriteFailureIsNotBackendDone(t *testing.T) {
	f := NewForwarder(nil, nil, nil)
	defer f.Close()

	want := errors.New("client went away")
	res := <-f.forwardBackendToClient(&oneMessageClientStream{}, &sendErrServerStream{sendErr: want})
	if !errors.Is(res.err, want) {
		t.Fatalf("want the client write error, got %v", res.err)
	}
	if res.backendDone {
		t.Fatal("a client write failure must not be reported as the backend being done")
	}
}

// TestForwarder_ClientKeepsSendingAfterBackendCompletes: a client that is
// still streaming when the backend completes must get the backend's OK, with
// many such streams in flight at once. The CloseSend-vs-SendMsg invariant this
// shape used to violate is pinned deterministically by
// TestForward_NoCloseSendWhileClientStillSending.
func TestForwarder_ClientKeepsSendingAfterBackendCompletes(t *testing.T) {
	backendAddr := startBidiBackend(t, func(s grpc.ServerStream) error {
		f := &Frame{}
		if err := s.RecvMsg(f); err != nil {
			return err
		}
		f.Free()
		return nil
	})
	conn := dialFrameClient(t, startProxyServer(t, backendAddr))

	const streams = 20
	errs := make(chan error, streams)
	for i := 0; i < streams; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			stream, err := grpc.NewClientStream(ctx, bidiDesc, conn, "/test.Echo/Bidi")
			if err != nil {
				errs <- err
				return
			}
			sendDone := make(chan struct{})
			go func() {
				defer close(sendDone)
				for stream.SendMsg(frameFromBytes([]byte("m"))) == nil {
				}
			}()
			err = recvUntilEnd(stream)
			<-sendDone
			errs <- err
		}()
	}
	for i := 0; i < streams; i++ {
		if err := <-errs; !errors.Is(err, io.EOF) {
			t.Errorf("stream ended with %v, want io.EOF", err)
		}
	}
}

// TestForwarder_TrailersOnlyStaysTrailersOnly: a backend that fails without
// sending headers produces a trailers-only response; the proxy must not turn
// it into headers plus trailers by sending an empty header block.
func TestForwarder_TrailersOnlyStaysTrailersOnly(t *testing.T) {
	backendAddr := startImmediateErrorStreamBackend(t, codes.PermissionDenied, "nope")
	conn := dialFrameClient(t, startProxyServer(t, backendAddr))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := grpc.NewClientStream(ctx, clientStreamDesc, conn, "/test.Echo/ServerStream")
	if err != nil {
		t.Fatalf("new stream: %v", err)
	}
	_ = stream.SendMsg(frameFromBytes(buildGRPCMessage([]byte("x"))))
	if err := recvUntilEnd(stream); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("want PermissionDenied, got %v", err)
	}
	hdr, err := stream.Header()
	if err != nil {
		t.Fatalf("header: %v", err)
	}
	if len(hdr) != 0 {
		t.Fatalf("trailers-only response grew a header block: %v", hdr)
	}
}
