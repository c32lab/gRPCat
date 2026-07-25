package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/c32lab/gRPCat/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestServer_UseNilPanics(t *testing.T) {
	server, err := NewServer(&Config{DefaultBackend: "localhost:50051"})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when adding nil middleware")
		}
	}()

	server.Use(nil)
}

func TestServer_UseValidMiddleware(t *testing.T) {
	server, err := NewServer(&Config{DefaultBackend: "localhost:50051"})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	mw := middleware.MiddlewareFunc(func(ctx *middleware.Context) {
		ctx.Next()
	})

	// Should not panic
	server.Use(mw)
}

func TestNewServer_RequiresBackend(t *testing.T) {
	_, err := NewServer(&Config{DefaultBackend: ""})
	if err == nil {
		t.Error("expected error when DefaultBackend is empty")
	}
}

func TestNewServer_NilConfig(t *testing.T) {
	_, err := NewServer(nil)
	if err == nil {
		t.Error("expected error when config is nil")
	}
}

// TestServer_StopClosesBackendConnections verifies that Stop tears down
// pooled backend connections.
func TestServer_StopClosesBackendConnections(t *testing.T) {
	backendAddr := startEchoBackend(t)

	srv, err := NewServer(&Config{DefaultBackend: backendAddr})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.grpcServer.Serve(lis)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Invoke(ctx,
		"/test.Echo/Echo",
		&Frame{data: buildGRPCMessage([]byte("hi"))},
		&Frame{},
	); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	if got := len(srv.forwarder.cache.conns); got == 0 {
		t.Fatalf("expected cached backend connection before Stop, got 0")
	}

	srv.Stop()

	if got := len(srv.forwarder.cache.conns); got != 0 {
		t.Errorf("expected cache empty after Stop, got %d entries", got)
	}

	// Idempotent.
	srv.Stop()
}

// TestServer_StartStopGoroutine verifies Stop unblocks the ctx-waiter
// goroutine launched by Start.
func TestServer_StartStopGoroutine(t *testing.T) {
	backendAddr := startEchoBackend(t)
	srv, err := NewServer(&Config{DefaultBackend: backendAddr})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ready := make(chan error, 1)
	go func() {
		ready <- srv.Start(context.Background(), "127.0.0.1:0")
	}()

	time.Sleep(50 * time.Millisecond)
	srv.Stop()

	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return after Stop; ctx-waiter goroutine likely leaked")
	}

	select {
	case <-srv.stopped:
	default:
		t.Error("stopped channel not closed")
	}
}

// TestServer_KeepaliveConfigWired verifies Config.KeepaliveParams reaches
// the connection cache.
func TestServer_KeepaliveConfigWired(t *testing.T) {
	ka := &keepalive.ClientParameters{
		Time:                10 * time.Minute,
		Timeout:             30 * time.Second,
		PermitWithoutStream: false,
	}
	srv, err := NewServer(&Config{
		DefaultBackend:  "127.0.0.1:1",
		KeepaliveParams: ka,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if srv.forwarder.cache.keepalive != ka {
		t.Errorf("cache.keepalive not wired: got %+v want %+v", srv.forwarder.cache.keepalive, ka)
	}

	srv2, err := NewServer(&Config{DefaultBackend: "127.0.0.1:1"})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if srv2.forwarder.cache.keepalive != nil {
		t.Errorf("expected nil keepalive by default, got %+v", srv2.forwarder.cache.keepalive)
	}
}

// TestServer_BackendDialOptionsWired verifies Config.BackendTransportCreds
// and Config.BackendDialOptions reach the connection cache.
func TestServer_BackendDialOptionsWired(t *testing.T) {
	creds := insecure.NewCredentials()
	customOpt := grpc.WithAuthority("custom")
	srv, err := NewServer(&Config{
		DefaultBackend:        "127.0.0.1:1",
		BackendTransportCreds: creds,
		BackendDialOptions:    []grpc.DialOption{customOpt},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if srv.forwarder.cache.transportCreds != creds {
		t.Error("transportCreds not wired")
	}
	if len(srv.forwarder.cache.dialOpts) != 1 {
		t.Errorf("expected 1 dial option, got %d", len(srv.forwarder.cache.dialOpts))
	}

	// Default: nil creds → insecure fallback, no extra dial options.
	srv2, err := NewServer(&Config{DefaultBackend: "127.0.0.1:1"})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if srv2.forwarder.cache.transportCreds != nil {
		t.Errorf("expected nil transportCreds by default, got %+v", srv2.forwarder.cache.transportCreds)
	}
	if len(srv2.forwarder.cache.dialOpts) != 0 {
		t.Errorf("expected 0 dial options by default, got %d", len(srv2.forwarder.cache.dialOpts))
	}
}

// errRecvStream is a grpc.ServerStream whose RecvMsg always fails, used to
// drive TransparentHandler's first-frame read-error path directly. Driving it
// over a real connection cannot show the handler's return value: gRPC's own
// serverStream.RecvMsg writes the read status to the client before the handler
// ever sees the error (stream.go, deferred WriteStatus).
type errRecvStream struct {
	ctx     context.Context
	recvErr error
}

func (s *errRecvStream) SetHeader(metadata.MD) error  { return nil }
func (s *errRecvStream) SendHeader(metadata.MD) error { return nil }
func (s *errRecvStream) SetTrailer(metadata.MD)       {}
func (s *errRecvStream) Context() context.Context     { return s.ctx }
func (s *errRecvStream) SendMsg(any) error            { return nil }
func (s *errRecvStream) RecvMsg(any) error            { return s.recvErr }

// methodTransportStream supplies the full method name that
// grpc.MethodFromServerStream reads out of the stream context.
type methodTransportStream struct{ method string }

func (m *methodTransportStream) Method() string               { return m.method }
func (m *methodTransportStream) SetHeader(metadata.MD) error  { return nil }
func (m *methodTransportStream) SendHeader(metadata.MD) error { return nil }
func (m *methodTransportStream) SetTrailer(metadata.MD) error { return nil }

// TestTransparentHandler_FirstFrameReadError pins what the handler returns when
// the first frame cannot be read, for each Hooks.OnFirstFrameError outcome.
func TestTransparentHandler_FirstFrameReadError(t *testing.T) {
	statusErr := status.Error(codes.Unavailable, "transport closed")
	plainErr := errors.New("boom")
	hookErr := status.Error(codes.FailedPrecondition, "hook-abort")

	tests := []struct {
		name    string
		recvErr error
		hook    func(*middleware.RequestInfo, error) error
		want    error  // exact error, when non-nil
		wantMsg string // otherwise match code+message
	}{
		{
			name:    "hook error is returned verbatim",
			recvErr: statusErr,
			hook:    func(*middleware.RequestInfo, error) error { return hookErr },
			want:    hookErr,
		},
		{
			name:    "hook returning nil falls back to the read status",
			recvErr: statusErr,
			hook:    func(*middleware.RequestInfo, error) error { return nil },
			want:    statusErr,
		},
		{
			name:    "no hook keeps the read status",
			recvErr: statusErr,
			want:    statusErr,
		},
		{
			name:    "non-status read error becomes Internal",
			recvErr: plainErr,
			wantMsg: "failed to read first frame: boom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{DefaultBackend: "127.0.0.1:1"}
			if tt.hook != nil {
				cfg.Hooks = &Hooks{OnFirstFrameError: tt.hook}
			}
			srv, err := NewServer(cfg)
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}
			var mwRan bool
			srv.Use(middleware.MiddlewareFunc(func(ctx *middleware.Context) { mwRan = true }))

			ctx := grpc.NewContextWithServerTransportStream(context.Background(),
				&methodTransportStream{method: "/test.Echo/Echo"})
			got := srv.TransparentHandler()(nil, &errRecvStream{ctx: ctx, recvErr: tt.recvErr})

			if tt.want != nil {
				if got != tt.want {
					t.Errorf("handler error: want %v got %v", tt.want, got)
				}
			} else {
				st, ok := status.FromError(got)
				if !ok || st.Code() != codes.Internal || st.Message() != tt.wantMsg {
					t.Errorf("handler error: want Internal/%q got %v", tt.wantMsg, got)
				}
			}
			if mwRan {
				t.Error("middleware chain ran despite the first-frame read failure")
			}
		})
	}
}

// TestTransparentHandler_WrappedEOFFirstFrame pins that a first-frame error
// wrapping io.EOF is treated as a client that sent no message, not as a read
// failure: the middleware chain still runs.
func TestTransparentHandler_WrappedEOFFirstFrame(t *testing.T) {
	srv, err := NewServer(&Config{DefaultBackend: "127.0.0.1:1"})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	var mwRan bool
	// Abort so the request never reaches the (unreachable) backend.
	srv.Use(middleware.MiddlewareFunc(func(ctx *middleware.Context) {
		mwRan = true
		ctx.AbortWithError(codes.FailedPrecondition, "stop")
	}))

	ctx := grpc.NewContextWithServerTransportStream(context.Background(),
		&methodTransportStream{method: "/test.Echo/Echo"})
	got := srv.TransparentHandler()(nil, &errRecvStream{
		ctx:     ctx,
		recvErr: fmt.Errorf("clean end: %w", io.EOF),
	})

	if !mwRan {
		t.Fatalf("middleware chain did not run: wrapped io.EOF misread as read error (got %v)", got)
	}
	if st, ok := status.FromError(got); !ok || st.Code() != codes.FailedPrecondition {
		t.Errorf("handler error: want FailedPrecondition got %v", got)
	}
}

// TestServer_ConcurrentUse guards against races between Use() and request
// handling. Run with -race.
func TestServer_ConcurrentUse(t *testing.T) {
	backendAddr := startEchoBackend(t)

	srv, err := NewServer(&Config{DefaultBackend: backendAddr})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.grpcServer.Serve(lis)
	defer srv.Stop()

	cc, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cc.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				srv.Use(middleware.MiddlewareFunc(func(ctx *middleware.Context) { ctx.Next() }))
			}
		}
	}()

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = cc.Invoke(ctx, "/test.Echo/Echo",
					&Frame{data: buildGRPCMessage([]byte("c"))},
					&Frame{},
				)
				cancel()
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}
