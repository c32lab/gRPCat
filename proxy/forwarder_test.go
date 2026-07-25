package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/c32lab/gRPCat/middleware"
)

func TestForwarder_UnaryRPC(t *testing.T) {
	// Start backend server
	backendAddr := startEchoBackend(t)

	// Start proxy server
	proxyAddr := startProxyServer(t, backendAddr)

	// Create client connection to proxy
	conn, err := grpc.NewClient(proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("failed to dial proxy: %v", err)
	}
	defer conn.Close()

	// Make unary call through proxy
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send request
	reqFrame := frameFromBytes(buildGRPCMessage([]byte("hello")))
	respFrame := &Frame{}

	err = conn.Invoke(ctx, "/test.Echo/Echo", reqFrame, respFrame)
	if err != nil {
		t.Fatalf("invoke failed: %v", err)
	}

	// Verify response
	payload := extractPayload(respFrame.Data())
	if string(payload) != "hello" {
		t.Errorf("expected 'hello', got '%s'", string(payload))
	}
}

func TestForwarder_ServerStreaming(t *testing.T) {
	// Start backend that sends 3 messages
	backendAddr := startStreamingBackend(t, 3)

	// Start proxy server
	proxyAddr := startProxyServer(t, backendAddr)

	// Create client connection to proxy
	conn, err := grpc.NewClient(proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("failed to dial proxy: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create stream
	streamDesc := &grpc.StreamDesc{ServerStreams: true}
	stream, err := conn.NewStream(ctx, streamDesc, "/test.Echo/ServerStream")
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Send request
	reqFrame := frameFromBytes(buildGRPCMessage([]byte("stream")))
	if err := stream.SendMsg(reqFrame); err != nil {
		t.Fatalf("failed to send: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("failed to close send: %v", err)
	}

	// Receive responses
	count := 0
	for {
		respFrame := &Frame{}
		if err := stream.RecvMsg(respFrame); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("recv failed: %v", err)
		}
		count++
	}

	if count != 3 {
		t.Errorf("expected 3 messages, got %d", count)
	}
}

func TestForwarder_MetadataForwarding(t *testing.T) {
	// Channel to capture received metadata
	receivedMD := make(chan metadata.MD, 1)

	// Start backend that captures metadata
	backendAddr := startMetadataCapturingBackend(t, receivedMD)

	// Start proxy server
	proxyAddr := startProxyServer(t, backendAddr)

	// Create client connection to proxy
	conn, err := grpc.NewClient(proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("failed to dial proxy: %v", err)
	}
	defer conn.Close()

	// Make call with custom metadata
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	md := metadata.Pairs("x-custom-header", "test-value")
	ctx = metadata.NewOutgoingContext(ctx, md)

	reqFrame := frameFromBytes(buildGRPCMessage([]byte("meta")))
	respFrame := &Frame{}

	err = conn.Invoke(ctx, "/test.Echo/Echo", reqFrame, respFrame)
	if err != nil {
		t.Fatalf("invoke failed: %v", err)
	}

	// Verify metadata was forwarded
	select {
	case gotMD := <-receivedMD:
		if vals := gotMD.Get("x-custom-header"); len(vals) == 0 || vals[0] != "test-value" {
			t.Errorf("expected x-custom-header=test-value, got %v", vals)
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for metadata")
	}
}

// Helper functions

func startEchoBackend(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer(grpc.ForceServerCodecV2(&ProxyCodec{}))
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Echo",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					frame := &Frame{}
					if err := dec(frame); err != nil {
						return nil, err
					}
					// Echo back the same data
					return frame, nil
				},
			},
		},
		Streams: []grpc.StreamDesc{},
	}, nil)

	go server.Serve(listener)
	t.Cleanup(func() { server.Stop() })

	return listener.Addr().String()
}

func startStreamingBackend(t *testing.T, msgCount int) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer(grpc.ForceServerCodecV2(&ProxyCodec{}))
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Methods:     []grpc.MethodDesc{},
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "ServerStream",
				ServerStreams: true,
				Handler: func(srv any, stream grpc.ServerStream) error {
					// Receive request
					frame := &Frame{}
					if err := stream.RecvMsg(frame); err != nil {
						return err
					}

					// Send multiple responses
					for i := 0; i < msgCount; i++ {
						resp := frameFromBytes(buildGRPCMessage([]byte("response")))
						if err := stream.SendMsg(resp); err != nil {
							return err
						}
					}
					return nil
				},
			},
		},
	}, nil)

	go server.Serve(listener)
	t.Cleanup(func() { server.Stop() })

	return listener.Addr().String()
}

func startMetadataCapturingBackend(t *testing.T, mdChan chan<- metadata.MD) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer(grpc.ForceServerCodecV2(&ProxyCodec{}))
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Echo",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					// Capture incoming metadata
					if md, ok := metadata.FromIncomingContext(ctx); ok {
						mdChan <- md
					}

					frame := &Frame{}
					if err := dec(frame); err != nil {
						return nil, err
					}
					return frame, nil
				},
			},
		},
		Streams: []grpc.StreamDesc{},
	}, nil)

	go server.Serve(listener)
	t.Cleanup(func() { server.Stop() })

	return listener.Addr().String()
}

func startProxyServer(t *testing.T, backend string) string {
	t.Helper()

	config := &Config{DefaultBackend: backend}
	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("failed to create proxy server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	go server.grpcServer.Serve(listener)
	t.Cleanup(func() { server.Stop() })

	return listener.Addr().String()
}

// buildGRPCMessage creates a gRPC message frame from payload
// Format: [compressed(1)] [length(4)] [payload]
func buildGRPCMessage(payload []byte) []byte {
	msg := make([]byte, 5+len(payload))
	msg[0] = 0 // not compressed
	msg[1] = byte(len(payload) >> 24)
	msg[2] = byte(len(payload) >> 16)
	msg[3] = byte(len(payload) >> 8)
	msg[4] = byte(len(payload))
	copy(msg[5:], payload)
	return msg
}

// extractPayload extracts payload from gRPC message frame
func extractPayload(data []byte) []byte {
	if len(data) < 5 {
		return nil
	}
	return data[5:]
}

// TestForwarder_BackendStatusPropagation verifies that when the backend
// returns a non-OK status, the proxy preserves its code/message instead of
// collapsing to codes.Internal.
func TestForwarder_BackendStatusPropagation(t *testing.T) {
	backendAddr := startStatusBackend(t, codes.PermissionDenied, "nope")
	proxyAddr := startProxyServer(t, backendAddr)

	conn, err := grpc.NewClient(proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = conn.Invoke(ctx, "/test.Echo/Echo",
		frameFromBytes(buildGRPCMessage([]byte("x"))),
		&Frame{},
	)
	if err == nil {
		t.Fatalf("expected error from backend, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected status error, got %T: %v", err, err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Errorf("code: want %v got %v (msg=%q)", codes.PermissionDenied, st.Code(), st.Message())
	}
	if st.Message() != "nope" {
		t.Errorf("message: want %q got %q", "nope", st.Message())
	}
}

// startStatusBackend starts a backend that fails every request with the
// given status code and message.
func startStatusBackend(t *testing.T, code codes.Code, msg string) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.ForceServerCodecV2(&ProxyCodec{}))
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Echo",
				Handler: func(_ any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
					frame := &Frame{}
					if err := dec(frame); err != nil {
						return nil, err
					}
					return nil, status.Error(code, msg)
				},
			},
		},
	}, nil)
	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })
	return lis.Addr().String()
}

// TestForwarder_ClientStreaming: client sends N frames, backend replies
// once with the count. Exercises the c2s goroutine finishing after
// multiple frames before backend responds.
func TestForwarder_ClientStreaming(t *testing.T) {
	backendAddr := startClientStreamingBackend(t)
	proxyAddr := startProxyServer(t, backendAddr)

	conn, err := grpc.NewClient(proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := conn.NewStream(ctx,
		&grpc.StreamDesc{ClientStreams: true},
		"/test.Echo/ClientStream",
	)
	if err != nil {
		t.Fatalf("new stream: %v", err)
	}

	const n = 4
	for i := 0; i < n; i++ {
		if err := stream.SendMsg(frameFromBytes(buildGRPCMessage([]byte("x")))); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}

	resp := &Frame{}
	if err := stream.RecvMsg(resp); err != nil {
		t.Fatalf("recv: %v", err)
	}
	got := string(extractPayload(resp.Data()))
	if got != "4" {
		t.Errorf("expected count=4, got %q", got)
	}

	// Trailer / EOF.
	if err := stream.RecvMsg(&Frame{}); err != io.EOF {
		t.Errorf("expected EOF after single response, got %v", err)
	}
}

// TestForwarder_BidiStreaming: client and backend exchange messages
// concurrently. Exercises c2s and s2c pumping in parallel.
func TestForwarder_BidiStreaming(t *testing.T) {
	backendAddr := startBidiEchoBackend(t)
	proxyAddr := startProxyServer(t, backendAddr)

	conn, err := grpc.NewClient(proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := conn.NewStream(ctx,
		&grpc.StreamDesc{ClientStreams: true, ServerStreams: true},
		"/test.Echo/Bidi",
	)
	if err != nil {
		t.Fatalf("new stream: %v", err)
	}

	const n = 3
	want := []string{"a", "bb", "ccc"}
	for _, msg := range want {
		if err := stream.SendMsg(frameFromBytes(buildGRPCMessage([]byte(msg)))); err != nil {
			t.Fatalf("send %q: %v", msg, err)
		}
		resp := &Frame{}
		if err := stream.RecvMsg(resp); err != nil {
			t.Fatalf("recv for %q: %v", msg, err)
		}
		got := string(extractPayload(resp.Data()))
		if got != msg {
			t.Errorf("echo mismatch: sent %q got %q", msg, got)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}
	if err := stream.RecvMsg(&Frame{}); err != io.EOF {
		t.Errorf("expected EOF after %d echoes, got %v", n, err)
	}
}

func startClientStreamingBackend(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.ForceServerCodecV2(&ProxyCodec{}))
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "ClientStream",
				ClientStreams: true,
				Handler: func(_ any, stream grpc.ServerStream) error {
					count := 0
					for {
						if err := stream.RecvMsg(&Frame{}); err != nil {
							if err == io.EOF {
								break
							}
							return err
						}
						count++
					}
					return stream.SendMsg(frameFromBytes(buildGRPCMessage([]byte(strconv.Itoa(count)))))
				},
			},
		},
	}, nil)
	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })
	return lis.Addr().String()
}

func startBidiEchoBackend(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.ForceServerCodecV2(&ProxyCodec{}))
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "Bidi",
				ClientStreams: true,
				ServerStreams: true,
				Handler: func(_ any, stream grpc.ServerStream) error {
					for {
						in := &Frame{}
						if err := stream.RecvMsg(in); err != nil {
							if err == io.EOF {
								return nil
							}
							return err
						}
						payload := extractPayload(in.Data())
						out := frameFromBytes(buildGRPCMessage(payload))
						if err := stream.SendMsg(out); err != nil {
							return err
						}
					}
				},
			},
		},
	}, nil)
	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })
	return lis.Addr().String()
}

// TestForwarder_MetadataNotDoubled verifies that metadata keys are forwarded
// exactly once to the backend, not doubled by the Join(incoming, incoming)
// bug that existed when mwCtx.Metadata aliased the incoming MD.
func TestForwarder_MetadataNotDoubled(t *testing.T) {
	receivedMD := make(chan metadata.MD, 1)
	backendAddr := startMetadataCapturingBackend(t, receivedMD)
	proxyAddr := startProxyServer(t, backendAddr)

	conn, err := grpc.NewClient(proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	md := metadata.Pairs("x-unique-key", "single-value")
	ctx = metadata.NewOutgoingContext(ctx, md)

	err = conn.Invoke(ctx, "/test.Echo/Echo",
		frameFromBytes(buildGRPCMessage([]byte("md"))),
		&Frame{},
	)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	select {
	case gotMD := <-receivedMD:
		vals := gotMD.Get("x-unique-key")
		if len(vals) != 1 {
			t.Errorf("expected x-unique-key to appear exactly once, got %d times: %v", len(vals), vals)
		}
		if len(vals) > 0 && vals[0] != "single-value" {
			t.Errorf("expected 'single-value', got %q", vals[0])
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for metadata")
	}
}

// TestForwarder_MiddlewareAbortStatus verifies that when a middleware calls
// AbortWithError, the client receives the exact status code and message.
func TestForwarder_MiddlewareAbortStatus(t *testing.T) {
	backendAddr := startEchoBackend(t)

	srv, err := NewServer(&Config{DefaultBackend: backendAddr})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	srv.Use(middleware.MiddlewareFunc(func(ctx *middleware.Context) {
		ctx.AbortWithError(codes.NotFound, "gone")
	}))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.grpcServer.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = conn.Invoke(ctx, "/test.Echo/Echo",
		frameFromBytes(buildGRPCMessage([]byte("x"))),
		&Frame{},
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected status error, got %T: %v", err, err)
	}
	if st.Code() != codes.NotFound {
		t.Errorf("code: want NotFound, got %v", st.Code())
	}
	if st.Message() != "gone" {
		t.Errorf("message: want %q, got %q", "gone", st.Message())
	}
}

// TestForwarder_MiddlewareSendEmptyResponse verifies that SendResponse([]byte{})
// returns a successful empty response rather than falling through to the error branch.
func TestForwarder_MiddlewareSendEmptyResponse(t *testing.T) {
	backendAddr := startEchoBackend(t)

	srv, err := NewServer(&Config{DefaultBackend: backendAddr})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	srv.Use(middleware.MiddlewareFunc(func(ctx *middleware.Context) {
		ctx.SendResponse([]byte{})
	}))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.grpcServer.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := &Frame{}
	err = conn.Invoke(ctx, "/test.Echo/Echo",
		frameFromBytes(buildGRPCMessage([]byte("x"))),
		resp,
	)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
}

// TestForwarder_EmptyStreamForwardsHeaders verifies that when the backend
// returns EOF with no messages, the proxy still forwards response headers.
func TestForwarder_EmptyStreamForwardsHeaders(t *testing.T) {
	backendAddr := startEmptyStreamBackend(t)
	proxyAddr := startProxyServer(t, backendAddr)

	conn, err := grpc.NewClient(proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := conn.NewStream(ctx, clientStreamDesc, "/test.Echo/ServerStream")
	if err != nil {
		t.Fatalf("new stream: %v", err)
	}
	if err := stream.SendMsg(frameFromBytes(buildGRPCMessage([]byte("x")))); err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}

	// Backend returns EOF immediately; headers should still be forwarded.
	md, err := stream.Header()
	if err != nil {
		t.Fatalf("header: %v", err)
	}
	vals := md.Get("x-empty-stream")
	if len(vals) == 0 || vals[0] != "true" {
		t.Errorf("expected header x-empty-stream=true, got %v", vals)
	}
}

// startEmptyStreamBackend starts a backend that sends a header then returns
// EOF with no response messages.
func startEmptyStreamBackend(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer(grpc.ForceServerCodecV2(&ProxyCodec{}))
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "ServerStream",
				ServerStreams: true,
				Handler: func(srv any, stream grpc.ServerStream) error {
					// Consume the client message
					frame := &Frame{}
					stream.RecvMsg(frame)
					// Send header but no messages
					stream.SendHeader(metadata.Pairs("x-empty-stream", "true"))
					return nil
				},
			},
		},
	}, nil)
	go server.Serve(lis)
	t.Cleanup(func() { server.Stop() })
	return lis.Addr().String()
}

// eofClientStream is a grpc.ClientStream whose RecvMsg always fails with
// recvErr, used to drive forwardBackendToClient's empty-stream branch with an
// error a real backend stream never produces.
type eofClientStream struct {
	header  metadata.MD
	recvErr error
}

func (s *eofClientStream) Header() (metadata.MD, error) { return s.header, nil }
func (s *eofClientStream) Trailer() metadata.MD         { return nil }
func (s *eofClientStream) CloseSend() error             { return nil }
func (s *eofClientStream) Context() context.Context     { return context.Background() }
func (s *eofClientStream) SendMsg(any) error            { return nil }
func (s *eofClientStream) RecvMsg(any) error            { return s.recvErr }

// recvErrServerStream is a grpc.ServerStream that fails RecvMsg with recvErr
// and records the headers forwarded to it.
type recvErrServerStream struct {
	recvErr    error
	sentHeader metadata.MD
}

func (s *recvErrServerStream) SetHeader(metadata.MD) error { return nil }
func (s *recvErrServerStream) SendHeader(md metadata.MD) error {
	s.sentHeader = md
	return nil
}
func (s *recvErrServerStream) SetTrailer(metadata.MD)   {}
func (s *recvErrServerStream) Context() context.Context { return context.Background() }
func (s *recvErrServerStream) SendMsg(any) error        { return nil }
func (s *recvErrServerStream) RecvMsg(any) error        { return s.recvErr }

// TestForwardBackendToClient_WrappedEOFForwardsHeaders pins that a backend
// read error wrapping io.EOF still counts as an empty stream, so response
// headers reach the client. A bare `err == io.EOF` check drops them.
func TestForwardBackendToClient_WrappedEOFForwardsHeaders(t *testing.T) {
	f := NewForwarder(nil, nil, nil)
	defer f.Close()

	src := &eofClientStream{
		header:  metadata.Pairs("x-empty-stream", "true"),
		recvErr: fmt.Errorf("clean end: %w", io.EOF),
	}
	dst := &recvErrServerStream{}

	if err := <-f.forwardBackendToClient(src, dst); !errors.Is(err, io.EOF) {
		t.Fatalf("expected wrapped io.EOF, got %v", err)
	}
	if vals := dst.sentHeader.Get("x-empty-stream"); len(vals) == 0 || vals[0] != "true" {
		t.Errorf("expected header x-empty-stream=true forwarded, got %v", dst.sentHeader)
	}
}

// TestForwarder_WrappedEOFFromClient pins that a client read error wrapping
// io.EOF ends the request stream normally instead of aborting the RPC.
func TestForwarder_WrappedEOFFromClient(t *testing.T) {
	backendAddr := startEmptyStreamBackend(t)

	f := NewForwarder(nil, nil, nil)
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream := &recvErrServerStream{recvErr: fmt.Errorf("clean end: %w", io.EOF)}
	// The backend method is not client-streaming, so it needs the one request
	// message the proxy already read; the client's stream then ends.
	firstFrame := frameFromBytes([]byte("x"))
	if err := f.Forward(ctx, "/test.Echo/ServerStream", stream, backendAddr, nil, firstFrame); err != nil {
		t.Fatalf("expected clean completion, got %v", err)
	}
}
