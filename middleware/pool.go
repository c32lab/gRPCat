package middleware

import "sync"

var contextPool = sync.Pool{
	New: func() any { return &Context{} },
}

// AcquireContext returns a Context from the pool, initialized with the given
// request and middleware chain. Pair every call with ReleaseContext once the
// request is done. For non-pooled use (tests, one-shots), use NewContext.
// Like NewContext it guarantees a non-nil Request with a non-nil Metadata map.
func AcquireContext(req *RequestInfo, middlewares []Middleware) *Context {
	c := contextPool.Get().(*Context)
	c.Request = normalizeRequest(req)
	c.middlewares = middlewares
	c.index = -1
	return c
}

// ReleaseContext clears c and returns it to the pool. Do not use c afterward.
func ReleaseContext(c *Context) {
	c.reset()
	contextPool.Put(c)
}
