package proxy

import (
	"context"
	"math"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// startLargeEchoBackend starts an echo backend with its own message limits
// raised, so the only 4MB cap left on a large round trip is the proxy's.
func startLargeEchoBackend(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.ForceServerCodec(&ProxyCodec{}),
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
	)
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
					return frame, nil
				},
			},
		},
	}, nil)

	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })
	return lis.Addr().String()
}

// startFixedSizeBackend starts a backend that answers every request with a
// response of respSize bytes, regardless of the request size.
func startFixedSizeBackend(t *testing.T, respSize int) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.ForceServerCodec(&ProxyCodec{}),
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
	)
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Echo",
				Handler: func(_ any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
					if err := dec(&Frame{}); err != nil {
						return nil, err
					}
					return &Frame{data: make([]byte, respSize)}, nil
				},
			},
		},
	}, nil)

	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })
	return lis.Addr().String()
}

// startSizedProxy starts a proxy carrying the given size limits and returns
// a client connection to it whose own limits are unbounded.
func startSizedProxy(t *testing.T, backend string, maxRecv, maxSend int) *grpc.ClientConn {
	t.Helper()

	srv, err := NewServer(&Config{
		DefaultBackend: backend,
		MaxRecvMsgSize: maxRecv,
		MaxSendMsgSize: maxSend,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.grpcServer.Serve(lis)
	t.Cleanup(func() { srv.Stop() })

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.ForceCodec(&ProxyCodec{}),
			grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32),
		),
	)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return conn
}

// TestProxy_LargeMessageRoundTrip verifies that a message over grpc-go's 4MB
// default survives the round trip when Config leaves the limits unset. It
// covers both receive limits at once: the request trips the proxy's server
// side, the echoed response trips its backend-client side.
func TestProxy_LargeMessageRoundTrip(t *testing.T) {
	const payloadSize = 6 << 20 // 6MB, over grpc-go's 4MB default

	conn := startSizedProxy(t, startLargeEchoBackend(t), 0, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	resp := &Frame{}
	if err := conn.Invoke(ctx, "/test.Echo/Echo", &Frame{data: payload}, resp); err != nil {
		t.Fatalf("invoke with %d-byte payload: %v", payloadSize, err)
	}

	if len(resp.data) != payloadSize {
		t.Fatalf("response size: want %d got %d", payloadSize, len(resp.data))
	}
	if resp.data[0] != payload[0] || resp.data[payloadSize-1] != payload[payloadSize-1] {
		t.Error("response payload does not match request")
	}
}

// TestProxy_MaxRecvMsgSizeRejects verifies an explicitly configured receive
// limit is still enforced by the proxy's server side.
func TestProxy_MaxRecvMsgSizeRejects(t *testing.T) {
	conn := startSizedProxy(t, startLargeEchoBackend(t), 1024, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := conn.Invoke(ctx, "/test.Echo/Echo", &Frame{data: make([]byte, 4096)}, &Frame{})
	if err == nil {
		t.Fatal("expected the 4096-byte request to be rejected, got nil")
	}
	if st, _ := status.FromError(err); st.Code() != codes.ResourceExhausted {
		t.Errorf("code: want ResourceExhausted got %v (%v)", st.Code(), err)
	}
}

// TestProxy_MaxSendMsgSizeRejects verifies an explicitly configured send limit
// reaches the backend connection: receiving is unbounded here, so the failure
// can only come from the proxy's send to the backend. The code must be the
// same ResourceExhausted a later message in the stream would produce.
func TestProxy_MaxSendMsgSizeRejects(t *testing.T) {
	conn := startSizedProxy(t, startLargeEchoBackend(t), 0, 1024)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := conn.Invoke(ctx, "/test.Echo/Echo", &Frame{data: make([]byte, 4096)}, &Frame{})
	if err == nil {
		t.Fatal("expected the 4096-byte request to be rejected, got nil")
	}
	if st, _ := status.FromError(err); st.Code() != codes.ResourceExhausted {
		t.Errorf("code: want ResourceExhausted got %v (%v)", st.Code(), err)
	}
}

// TestProxy_MaxSendMsgSizeRejectsResponse verifies the send limit also reaches
// the proxy's server side. The request is small enough to reach the backend,
// so only the oversized response can trip the limit.
func TestProxy_MaxSendMsgSizeRejectsResponse(t *testing.T) {
	conn := startSizedProxy(t, startFixedSizeBackend(t, 4096), 0, 1024)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := conn.Invoke(ctx, "/test.Echo/Echo", &Frame{data: []byte("x")}, &Frame{})
	if err == nil {
		t.Fatal("expected the 4096-byte response to be rejected, got nil")
	}
	if st, _ := status.FromError(err); st.Code() != codes.ResourceExhausted {
		t.Errorf("code: want ResourceExhausted got %v (%v)", st.Code(), err)
	}
}

// TestServer_BackendDialOptionsWinOnConflict pins the append ORDER on the
// backend leg, the counterpart of TestServer_ServerOptionsWinOnConflict for the
// listening side. Config.BackendDialOptions are appended after the proxy's own
// grpc.WithDefaultCallOptions, and gRPC applies call options sequentially to a
// single callInfo, so a user-supplied grpc.MaxCallRecvMsgSize overrides the one
// derived from Config.MaxRecvMsgSize. Appending the other way round would
// silently ignore the user's limit and reject the 4096-byte response below with
// ResourceExhausted. The request is 1 byte, so the proxy's server-side receive
// limit (also 1024) stays out of the picture.
func TestServer_BackendDialOptionsWinOnConflict(t *testing.T) {
	srv, err := NewServer(&Config{
		DefaultBackend: startFixedSizeBackend(t, 4096),
		MaxRecvMsgSize: 1024,
		BackendDialOptions: []grpc.DialOption{
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1 << 20)),
		},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.grpcServer.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.ForceCodec(&ProxyCodec{}),
			grpc.MaxCallRecvMsgSize(math.MaxInt32),
		),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := &Frame{}
	if err := conn.Invoke(ctx, "/test.Echo/Echo", &Frame{data: []byte("x")}, resp); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if len(resp.data) != 4096 {
		t.Errorf("response size: want 4096 got %d", len(resp.data))
	}
}

// TestServer_MaxMsgSizeWiredToBackend verifies Config.MaxRecvMsgSize /
// MaxSendMsgSize reach the connection cache, with zero meaning "no limit".
func TestServer_MaxMsgSizeWiredToBackend(t *testing.T) {
	srv, err := NewServer(&Config{
		DefaultBackend: "127.0.0.1:1",
		MaxRecvMsgSize: 1 << 20,
		MaxSendMsgSize: 2 << 20,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if got := srv.forwarder.cache.maxRecvMsgSize; got != 1<<20 {
		t.Errorf("cache.maxRecvMsgSize: want %d got %d", 1<<20, got)
	}
	if got := srv.forwarder.cache.maxSendMsgSize; got != 2<<20 {
		t.Errorf("cache.maxSendMsgSize: want %d got %d", 2<<20, got)
	}

	srv2, err := NewServer(&Config{DefaultBackend: "127.0.0.1:1"})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if got := srv2.forwarder.cache.maxRecvMsgSize; got != math.MaxInt32 {
		t.Errorf("default cache.maxRecvMsgSize: want %d got %d", math.MaxInt32, got)
	}
	if got := srv2.forwarder.cache.maxSendMsgSize; got != math.MaxInt32 {
		t.Errorf("default cache.maxSendMsgSize: want %d got %d", math.MaxInt32, got)
	}

	// A cache built directly keeps gRPC's defaults rather than a zero limit.
	direct := NewConnectionCache(nil, nil, nil)
	if direct.maxRecvMsgSize != 0 || direct.maxSendMsgSize != 0 {
		t.Errorf("NewConnectionCache should leave sizes unset, got %d/%d",
			direct.maxRecvMsgSize, direct.maxSendMsgSize)
	}
}

// TestConnectionCache_UnsetSizesStillCarryTraffic pins the "zero means leave
// gRPC's defaults alone" guards in Get. A cache built through the exported
// constructor has both sizes unset, so passing them to grpc.MaxCallRecvMsgSize
// / MaxCallSendMsgSize unguarded would cap every RPC at 0 bytes.
func TestConnectionCache_UnsetSizesStillCarryTraffic(t *testing.T) {
	cache := NewConnectionCache(nil, nil, nil)
	t.Cleanup(cache.Close)

	conn, err := cache.Get(startLargeEchoBackend(t))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := &Frame{}
	if err := conn.Invoke(ctx, "/test.Echo/Echo", &Frame{data: []byte("hi")}, resp); err != nil {
		t.Fatalf("invoke through an unset-size cache: %v", err)
	}
	if string(resp.data) != "hi" {
		t.Errorf("echo payload: want %q got %q", "hi", resp.data)
	}
}
