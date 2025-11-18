package middleware

// Middleware processes requests in a chain
type Middleware interface {
	Handle(ctx *Context)
}

// MiddlewareFunc is a function adapter for Middleware interface
type MiddlewareFunc func(ctx *Context)

func (f MiddlewareFunc) Handle(ctx *Context) {
	f(ctx)
}
