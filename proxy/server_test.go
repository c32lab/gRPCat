package proxy

import (
	"testing"

	"github.com/c32lab/gRPCat/middleware"
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
