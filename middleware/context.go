package middleware

import (
	"math"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

const (
	// abortIndex represents a typical value used in abort functions.
	abortIndex = math.MaxInt >> 1
)

// RequestInfo contains information about the incoming gRPC request
type RequestInfo struct {
	Service string
	Method  string
	// Metadata is the metadata the backend will receive. It starts as a
	// copy of the client's request metadata, and edits made by middleware
	// (Set, Delete, Append, or Context.AddMetadata) take effect on the
	// forwarded request, gin-style. Left untouched, the backend gets exactly
	// what the client sent. It is never nil on a Context obtained from
	// NewContext or AcquireContext.
	Metadata metadata.MD
	// FirstPayload is the decoded protobuf payload of the FIRST client
	// message on the stream. It is populated for routing/inspection by
	// middlewares before forwarding begins. Semantics by RPC type:
	//   - Unary: the full request body.
	//   - Server-streaming: the single client request.
	//   - Client-streaming / Bidirectional: only the first client message.
	//     Subsequent messages are not buffered here; middleware cannot
	//     inspect them without additional machinery.
	// May be nil if the client closed the stream without sending a message.
	//
	// The slice is a private copy of the message, not the buffers the proxy
	// forwards: it is safe to read, to proto.Unmarshal, and to hold past the
	// middleware chain, and it stays valid after the proxy has released the
	// message. Writing to it is harmless but pointless - it does not change
	// the bytes the backend receives.
	FirstPayload []byte
}

// ResponseInfo contains the response to be sent back
type ResponseInfo struct {
	Data []byte
	Code codes.Code
	Msg  string
}

// Context is passed through the middleware chain.
//
// The proxy pools Contexts: one is recycled as soon as its RPC completes, so
// middleware must not keep a reference to it or to its Request past that
// point, including from goroutines it starts.
type Context struct {
	Request  *RequestInfo
	Response *ResponseInfo
	Backend  string

	// Shared data between middlewares (protected by mu)
	mu     sync.RWMutex
	values map[string]any

	// Internal state
	index       int
	middlewares []Middleware
}

// NewContext creates a new middleware context. req may be nil; the returned
// Context always has a non-nil Request with a non-nil Metadata map, so
// middleware can call Set or Append on it without checks.
func NewContext(req *RequestInfo, middlewares []Middleware) *Context {
	return &Context{
		Request:     normalizeRequest(req),
		values:      make(map[string]any),
		middlewares: middlewares,
		index:       -1,
	}
}

// normalizeRequest gives every Context a Request whose Metadata map can be
// written to: metadata.MD.Set and Append panic on a nil map, and callers
// building a RequestInfo by hand (tests, library users) rarely set one.
func normalizeRequest(req *RequestInfo) *RequestInfo {
	if req == nil {
		req = &RequestInfo{}
	}
	if req.Metadata == nil {
		req.Metadata = metadata.MD{}
	}
	return req
}

// Set stores a value in the context (thread-safe)
func (c *Context) Set(key string, value any) {
	c.mu.Lock()
	if c.values == nil {
		c.values = make(map[string]any)
	}
	c.values[key] = value
	c.mu.Unlock()
}

// Get retrieves a value from the context (thread-safe)
func (c *Context) Get(key string) (any, bool) {
	c.mu.RLock()
	val, ok := c.values[key]
	c.mu.RUnlock()
	return val, ok
}

// GetString retrieves a string value from the context (thread-safe)
func (c *Context) GetString(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if val, ok := c.values[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

// Next executes the next middleware in the chain.
//
// Semantics: Next() advances the chain index and loops through all
// remaining middlewares (matching gin's behavior). A middleware that
// does NOT call Next() is therefore equivalent to one that does — the
// rest of the chain still runs after this middleware returns. The only
// way to stop the chain is Abort() / AbortWithError() / SendResponse().
//
// Calling Next() explicitly is useful when a middleware wants to run
// logic after downstream middlewares complete (pre/post pattern).
func (c *Context) Next() {
	c.index++
	for c.index < len(c.middlewares) {
		c.middlewares[c.index].Handle(c)
		c.index++
	}
}

// Abort stops the middleware chain execution
func (c *Context) Abort() {
	c.index = abortIndex
}

// AbortWithError stops execution and returns an error to the client.
//
// code must not be codes.OK: a status built from OK is nil, which would turn
// the abort into a successful RPC with an empty body. codes.OK is therefore
// normalized to codes.Internal. Use SendResponse to abort with a successful
// response.
func (c *Context) AbortWithError(code codes.Code, msg string) {
	if code == codes.OK {
		code = codes.Internal
	}
	c.Response = &ResponseInfo{
		Code: code,
		Msg:  msg,
	}
	c.Abort()
}

// SendResponse sends a custom response (raw protobuf bytes) and stops execution
func (c *Context) SendResponse(data []byte) {
	if data == nil {
		data = []byte{}
	}
	c.Response = &ResponseInfo{
		Data: data,
		Code: codes.OK,
	}
	c.Abort()
}

// IsAborted returns whether the context is aborted
func (c *Context) IsAborted() bool {
	return c.index >= abortIndex
}

// SetBackend sets the backend address for forwarding
func (c *Context) SetBackend(backend string) {
	c.Backend = backend
}

// AddMetadata appends a metadata entry to the backend request; shorthand for
// c.Request.Metadata.Append. Use Set or Delete on Request.Metadata directly
// to replace or drop an entry.
func (c *Context) AddMetadata(key, value string) {
	if c.Request.Metadata == nil {
		c.Request.Metadata = metadata.MD{}
	}
	c.Request.Metadata.Append(key, value)
}

// reset clears the context for reuse via the pool. Unexported because pool
// lifecycle is owned by AcquireContext/ReleaseContext.
func (c *Context) reset() {
	c.Request = nil
	c.Response = nil
	c.Backend = ""
	c.values = nil
	c.index = -1
	c.middlewares = nil
}
