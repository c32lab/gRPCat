package middleware

import (
	"sync"

	"google.golang.org/grpc/metadata"
)

var contextPool = sync.Pool{
	New: func() any { return &Context{} },
}

// AcquireContext returns a Context from the pool, initialized with the given
// request and middleware chain. Pair every call with ReleaseContext once the
// request is done. For non-pooled use (tests, one-shots), use NewContext.
func AcquireContext(req *RequestInfo, middlewares []Middleware) *Context {
	c := contextPool.Get().(*Context)
	c.Request = req
	c.Metadata = metadata.MD{}
	c.middlewares = middlewares
	c.index = -1
	return c
}

// ReleaseContext clears c and returns it to the pool. Do not use c afterward.
func ReleaseContext(c *Context) {
	c.reset()
	contextPool.Put(c)
}
