package middlewares

import (
	"github.com/c32lab/gRPCat/middleware"
)

type RouteMiddleware struct {
	routes map[string]string
}

func NewRouteMiddleware() *RouteMiddleware {
	return &RouteMiddleware{
		routes: make(map[string]string),
	}
}

func (m *RouteMiddleware) AddRoute(service, backend string) {
	m.routes[service] = backend
}

func (m *RouteMiddleware) Handle(ctx *middleware.Context) {
	if backend, ok := m.routes[ctx.Request.Service]; ok {
		ctx.SetBackend(backend)
	}
	ctx.Next()
}
