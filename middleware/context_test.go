package middleware

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestContext_MiddlewareChain(t *testing.T) {
	order := []int{}

	mw1 := MiddlewareFunc(func(ctx *Context) {
		order = append(order, 1)
		ctx.Next()
		order = append(order, 4)
	})

	mw2 := MiddlewareFunc(func(ctx *Context) {
		order = append(order, 2)
		ctx.Next()
		order = append(order, 3)
	})

	req := &RequestInfo{Service: "test", Method: "Test"}
	ctx := NewContext(req, []Middleware{mw1, mw2})
	ctx.Next()

	expected := []int{1, 2, 3, 4}
	if len(order) != len(expected) {
		t.Fatalf("expected %d calls, got %d", len(expected), len(order))
	}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("position %d: expected %d, got %d", i, v, order[i])
		}
	}
}

func TestContext_Abort(t *testing.T) {
	executed := []int{}

	mw1 := MiddlewareFunc(func(ctx *Context) {
		executed = append(executed, 1)
		ctx.Abort()
		ctx.Next() // should be no-op after abort
	})

	mw2 := MiddlewareFunc(func(ctx *Context) {
		executed = append(executed, 2) // should not execute
	})

	req := &RequestInfo{Service: "test", Method: "Test"}
	ctx := NewContext(req, []Middleware{mw1, mw2})
	ctx.Next()

	if len(executed) != 1 || executed[0] != 1 {
		t.Errorf("expected only mw1 to execute, got %v", executed)
	}

	if !ctx.IsAborted() {
		t.Error("expected context to be aborted")
	}
}

func TestContext_AbortWithError(t *testing.T) {
	mw := MiddlewareFunc(func(ctx *Context) {
		ctx.AbortWithError(codes.PermissionDenied, "access denied")
	})

	req := &RequestInfo{Service: "test", Method: "Test"}
	ctx := NewContext(req, []Middleware{mw})
	ctx.Next()

	if !ctx.IsAborted() {
		t.Error("expected context to be aborted")
	}
	if ctx.Response == nil {
		t.Fatal("expected response to be set")
	}
	if ctx.Response.Code != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", ctx.Response.Code)
	}
	if ctx.Response.Msg != "access denied" {
		t.Errorf("expected 'access denied', got %s", ctx.Response.Msg)
	}
}

func TestContext_SetGet(t *testing.T) {
	req := &RequestInfo{Service: "test", Method: "Test"}
	ctx := NewContext(req, nil)

	ctx.Set("key1", "value1")
	ctx.Set("key2", 42)

	if v, ok := ctx.Get("key1"); !ok || v != "value1" {
		t.Errorf("expected 'value1', got %v", v)
	}

	if v := ctx.GetString("key1"); v != "value1" {
		t.Errorf("expected 'value1', got %s", v)
	}

	if v := ctx.GetString("key2"); v != "" {
		t.Errorf("expected empty string for non-string, got %s", v)
	}

	if v := ctx.GetString("nonexistent"); v != "" {
		t.Errorf("expected empty string for nonexistent, got %s", v)
	}
}

func TestContext_SetBackend(t *testing.T) {
	req := &RequestInfo{Service: "test", Method: "Test"}
	ctx := NewContext(req, nil)

	ctx.SetBackend("localhost:50051")

	if ctx.Backend != "localhost:50051" {
		t.Errorf("expected 'localhost:50051', got %s", ctx.Backend)
	}
}

func TestContext_AddMetadata(t *testing.T) {
	req := &RequestInfo{
		Service:  "test",
		Method:   "Test",
		Metadata: metadata.Pairs("existing", "value"),
	}
	ctx := NewContext(req, nil)

	ctx.AddMetadata("new-key", "new-value")

	if vals := ctx.Metadata.Get("new-key"); len(vals) == 0 || vals[0] != "new-value" {
		t.Errorf("expected new-key=new-value, got %v", vals)
	}
}

func TestContext_Reset(t *testing.T) {
	req := &RequestInfo{Service: "test", Method: "Test"}
	ctx := NewContext(req, []Middleware{})
	ctx.Set("key", "value")
	ctx.Backend = "backend"
	ctx.Response = &ResponseInfo{Code: codes.OK}

	ctx.Reset()

	if ctx.Request != nil {
		t.Error("expected Request to be nil")
	}
	if ctx.Response != nil {
		t.Error("expected Response to be nil")
	}
	if ctx.Backend != "" {
		t.Error("expected Backend to be empty")
	}
	if ctx.values != nil {
		t.Error("expected values to be nil")
	}
}

func TestContext_SendResponse(t *testing.T) {
	mw := MiddlewareFunc(func(ctx *Context) {
		ctx.SendResponse([]byte("mock response"))
	})

	req := &RequestInfo{Service: "test", Method: "Test"}
	ctx := NewContext(req, []Middleware{mw})
	ctx.Next()

	if !ctx.IsAborted() {
		t.Error("expected context to be aborted")
	}
	if ctx.Response == nil {
		t.Fatal("expected response to be set")
	}
	if string(ctx.Response.Data) != "mock response" {
		t.Errorf("expected 'mock response', got %s", string(ctx.Response.Data))
	}
	if ctx.Response.Code != codes.OK {
		t.Errorf("expected OK, got %v", ctx.Response.Code)
	}
}
