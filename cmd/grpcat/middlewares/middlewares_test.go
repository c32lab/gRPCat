package middlewares

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"github.com/c32lab/gRPCat/middleware"
)

func TestRouteMiddleware(t *testing.T) {
	tests := []struct {
		name        string
		routes      map[string]string
		service     string
		wantBackend string
	}{
		{
			name:        "matching service is routed",
			routes:      map[string]string{"user.Service": "localhost:50052"},
			service:     "user.Service",
			wantBackend: "localhost:50052",
		},
		{
			name:        "unmatched service falls through to the default backend",
			routes:      map[string]string{"user.Service": "localhost:50052"},
			service:     "order.Service",
			wantBackend: "", // empty means Config.DefaultBackend is used
		},
		{
			name: "each service picks its own backend",
			routes: map[string]string{
				"user.Service":  "localhost:50052",
				"order.Service": "localhost:50053",
			},
			service:     "order.Service",
			wantBackend: "localhost:50053",
		},
		{
			name:        "no routes configured leaves the backend alone",
			routes:      nil,
			service:     "any.Service",
			wantBackend: "",
		},
		{
			name:        "matching is exact, not prefix",
			routes:      map[string]string{"user.Service": "localhost:50052"},
			service:     "user.ServiceV2",
			wantBackend: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewRouteMiddleware()
			for svc, backend := range tt.routes {
				m.AddRoute(svc, backend)
			}

			ctx := middleware.NewContext(
				&middleware.RequestInfo{Service: tt.service, Method: "Any"},
				[]middleware.Middleware{m},
			)
			ctx.Next()

			if ctx.Backend != tt.wantBackend {
				t.Errorf("Backend: want %q, got %q", tt.wantBackend, ctx.Backend)
			}
			if ctx.IsAborted() {
				t.Error("routing must not abort the chain")
			}
		})
	}
}

// TestRouteMiddleware_LastAddWins pins that re-adding a service replaces its
// backend, which is what makes AddRoute usable for reconfiguration.
func TestRouteMiddleware_LastAddWins(t *testing.T) {
	m := NewRouteMiddleware()
	m.AddRoute("svc", "first:1")
	m.AddRoute("svc", "second:2")

	ctx := middleware.NewContext(
		&middleware.RequestInfo{Service: "svc", Method: "M"},
		[]middleware.Middleware{m},
	)
	ctx.Next()

	if ctx.Backend != "second:2" {
		t.Errorf("Backend: want %q, got %q", "second:2", ctx.Backend)
	}
}

// TestRouteMiddleware_ContinuesChain pins that routing does not swallow the
// rest of the chain — a router that terminated it would silently disable every
// middleware registered after it.
func TestRouteMiddleware_ContinuesChain(t *testing.T) {
	m := NewRouteMiddleware()
	m.AddRoute("svc", "backend:1")

	var ran bool
	next := middleware.MiddlewareFunc(func(*middleware.Context) { ran = true })

	ctx := middleware.NewContext(
		&middleware.RequestInfo{Service: "svc", Method: "M"},
		[]middleware.Middleware{m, next},
	)
	ctx.Next()

	if !ran {
		t.Error("the middleware after the router never ran")
	}
}

func TestLoggingMiddleware(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(nil)

	var ran bool
	next := middleware.MiddlewareFunc(func(*middleware.Context) {
		ran = true
		log.Printf("[Inner]")
	})

	ctx := middleware.NewContext(
		&middleware.RequestInfo{Service: "pkg.Service", Method: "Method"},
		[]middleware.Middleware{NewLoggingMiddleware(), next},
	)
	ctx.Next()

	if !ran {
		t.Error("the middleware after the logger never ran")
	}

	out := buf.String()
	for _, want := range []string{"[Request]", "[Response]", "pkg.Service", "Method", "Duration:"} {
		if !strings.Contains(out, want) {
			t.Errorf("log output missing %q:\n%s", want, out)
		}
	}

	// Order, not just presence: the logger calls Next() in the middle, so the
	// downstream middleware must run between the two log lines. Asserting only
	// presence would still pass if Next() were dropped, since the chain runs to
	// completion either way — that pre/post bracketing is the whole point of
	// this example.
	req := strings.Index(out, "[Request]")
	inner := strings.Index(out, "[Inner]")
	resp := strings.Index(out, "[Response]")
	if !(req < inner && inner < resp) {
		t.Errorf("want [Request] < [Inner] < [Response], got offsets %d/%d/%d:\n%s",
			req, inner, resp, out)
	}
}
