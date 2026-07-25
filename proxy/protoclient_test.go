package proxy

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/c32lab/gRPCat/middleware"
)

// These tests use a STOCK protobuf client (no grpc.ForceCodec) and a STOCK
// protobuf backend (no grpc.ForceServerCodec) — the configuration a real
// deployment uses. The other end-to-end tests in this package put ProxyCodec
// on the client too and hand-frame the body with buildGRPCMessage, which
// double-frames the message and masks header-handling bugs in the proxy.

// TestProxy_StockProtoClient_FirstPayload verifies that RequestInfo.FirstPayload
// is the exact protobuf wire encoding of the client's request, and that
// Hooks.OnFirstFrameError does not fire on a normal request.
func TestProxy_StockProtoClient_FirstPayload(t *testing.T) {
	backendAddr := startProtoEchoBackend(t)

	req := wrapperspb.String("hello-world-payload")
	wantPayload, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	var hookFired atomic.Bool
	srv, err := NewServer(&Config{
		DefaultBackend: backendAddr,
		Hooks: &Hooks{
			OnFirstFrameError: func(_ *middleware.RequestInfo, _ error) error {
				hookFired.Store(true)
				return nil
			},
		},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	gotPayload := make(chan []byte, 1)
	srv.Use(middleware.MiddlewareFunc(func(ctx *middleware.Context) {
		// FirstPayload is already a private copy, but this test outlives the
		// middleware chain, so snapshot it into the channel's own slice.
		buf := make([]byte, len(ctx.Request.FirstPayload))
		copy(buf, ctx.Request.FirstPayload)
		select {
		case gotPayload <- buf:
		default:
		}
	}))

	conn := dialProtoProxy(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := &wrapperspb.StringValue{}
	if err := conn.Invoke(ctx, "/test.ProtoEcho/Echo", req, resp); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	// Plain forwarding must still work end to end.
	if resp.GetValue() != "echo:hello-world-payload" {
		t.Errorf("backend response: want %q got %q", "echo:hello-world-payload", resp.GetValue())
	}

	select {
	case got := <-gotPayload:
		if !bytes.Equal(got, wantPayload) {
			t.Errorf("FirstPayload mismatch:\n want % x (len %d)\n  got % x (len %d)",
				wantPayload, len(wantPayload), got, len(got))
		}
	case <-time.After(time.Second):
		t.Fatal("middleware did not run")
	}

	if hookFired.Load() {
		t.Error("Hooks.OnFirstFrameError fired on a normal request")
	}
}

// TestFrame_ParseAs_StockProtoClient verifies Frame.ParseAs on a frame that
// came off the real codec path. gRPC strips the 5-byte message header before
// the codec runs, so the frame holds a bare protobuf payload and ParseAs must
// unmarshal it directly; treating it as a framed gRPC message reads the length
// prefix out of protobuf field bytes and fails.
func TestFrame_ParseAs_StockProtoClient(t *testing.T) {
	backendAddr := startProtoEchoBackend(t)

	srv, err := NewServer(&Config{DefaultBackend: backendAddr})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	type parsed struct {
		value string
		err   error
	}
	got := make(chan parsed, 1)
	srv.Use(middleware.MiddlewareFunc(func(ctx *middleware.Context) {
		// Same bytes the codec handed the proxy for the first frame.
		frame := frameFromBytes(ctx.Request.FirstPayload)
		out := &wrapperspb.StringValue{}
		err := frame.ParseAs(out)
		select {
		case got <- parsed{value: out.GetValue(), err: err}:
		default:
		}
	}))

	conn := dialProtoProxy(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := &wrapperspb.StringValue{}
	if err := conn.Invoke(ctx, "/test.ProtoEcho/Echo", wrapperspb.String("parse-me"), resp); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	select {
	case p := <-got:
		if p.err != nil {
			t.Fatalf("ParseAs failed on a real frame: %v", p.err)
		}
		if p.value != "parse-me" {
			t.Errorf("ParseAs decoded value: want %q got %q", "parse-me", p.value)
		}
	case <-time.After(time.Second):
		t.Fatal("middleware did not run")
	}
}

// TestProxy_FirstFrameReadError covers the path where the first frame cannot be
// read at all: a stock client sends a message past the proxy's configured
// receive limit, so RecvMsg fails before any payload exists. The hook must fire
// and the middleware chain must be skipped.
func TestProxy_FirstFrameReadError(t *testing.T) {
	type hookCall struct {
		req *middleware.RequestInfo
		err error
	}
	calls := make(chan hookCall, 1)

	srv, err := NewServer(&Config{
		DefaultBackend: startProtoEchoBackend(t),
		// The default is "no limit", so pin a small one to make the read fail.
		MaxRecvMsgSize: 1024,
		Hooks: &Hooks{
			OnFirstFrameError: func(req *middleware.RequestInfo, err error) error {
				select {
				case calls <- hookCall{req: req, err: err}:
				default:
				}
				return nil
			},
		},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	var mwRan atomic.Bool
	srv.Use(middleware.MiddlewareFunc(func(ctx *middleware.Context) {
		mwRan.Store(true)
	}))

	conn := dialProtoProxy(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Larger than the MaxRecvMsgSize configured above.
	oversized := wrapperspb.Bytes(make([]byte, 4096))
	invokeErr := conn.Invoke(ctx, "/test.ProtoEcho/Echo", oversized, &wrapperspb.BytesValue{})
	// gRPC's own serverStream.RecvMsg writes this status to the client before
	// returning to the handler, so what the handler returns cannot change it.
	// See TestTransparentHandler_FirstFrameReadError for the returned error.
	if got := status.Code(invokeErr); got != codes.ResourceExhausted {
		t.Fatalf("client status: want ResourceExhausted, got %v (%v)", got, invokeErr)
	}

	select {
	case c := <-calls:
		if c.err == nil {
			t.Error("hook received a nil error")
		}
		if c.req == nil {
			t.Fatal("hook received a nil RequestInfo")
		}
		if c.req.FirstPayload != nil {
			t.Errorf("FirstPayload: want nil, got %d bytes", len(c.req.FirstPayload))
		}
		if c.req.Service != "test.ProtoEcho" || c.req.Method != "Echo" {
			t.Errorf("RequestInfo route: got %s/%s", c.req.Service, c.req.Method)
		}
	case <-time.After(time.Second):
		t.Fatal("Hooks.OnFirstFrameError did not fire")
	}

	if mwRan.Load() {
		t.Error("middleware chain ran despite the first-frame read failure")
	}
}

// TestProxy_StockProtoClient_SendResponse verifies that a response injected by
// middleware via ctx.SendResponse decodes correctly in a stock protobuf client.
func TestProxy_StockProtoClient_SendResponse(t *testing.T) {
	backendAddr := startProtoEchoBackend(t)

	injected, err := proto.Marshal(wrapperspb.String("injected-by-middleware"))
	if err != nil {
		t.Fatalf("marshal injected response: %v", err)
	}

	srv, err := NewServer(&Config{DefaultBackend: backendAddr})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv.Use(middleware.MiddlewareFunc(func(ctx *middleware.Context) {
		ctx.SendResponse(injected)
	}))

	conn := dialProtoProxy(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := &wrapperspb.StringValue{}
	if err := conn.Invoke(ctx, "/test.ProtoEcho/Echo", wrapperspb.String("ignored"), resp); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if resp.GetValue() != "injected-by-middleware" {
		t.Errorf("injected response: want %q got %q", "injected-by-middleware", resp.GetValue())
	}
}

// dialProtoProxy serves srv on a random port and returns a stock protobuf
// client connected to it.
func dialProtoProxy(t *testing.T, srv *Server) *grpc.ClientConn {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.grpcServer.Serve(lis)
	t.Cleanup(func() { srv.Stop() })

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// startProtoEchoBackend starts a backend using gRPC's default protobuf codec
// (no ProxyCodec) that echoes "echo:"+value.
func startProtoEchoBackend(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.ProtoEcho",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Echo",
				Handler: func(_ any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
					in := &wrapperspb.StringValue{}
					if err := dec(in); err != nil {
						return nil, err
					}
					return wrapperspb.String("echo:" + in.GetValue()), nil
				},
			},
		},
	}, nil)

	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })

	return lis.Addr().String()
}
