package proxy

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/c32lab/gRPCat/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
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

	conn, err := grpc.Dial(lis.Addr().String(),
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

	cc, err := grpc.Dial(lis.Addr().String(),
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
