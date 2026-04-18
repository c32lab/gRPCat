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
	Service  string
	Method   string
	Metadata metadata.MD
	// FirstPayload is the decoded protobuf payload of the FIRST client
	// message on the stream. It is populated for routing/inspection by
	// middlewares before forwarding begins. Semantics by RPC type:
	//   - Unary: the full request body.
	//   - Server-streaming: the single client request.
	//   - Client-streaming / Bidirectional: only the first client message.
	//     Subsequent messages are not buffered here; middleware cannot
	//     inspect them without additional machinery.
	// May be nil if the payload could not be parsed as a gRPC message.
	FirstPayload []byte
}

// ResponseInfo contains the response to be sent back
type ResponseInfo struct {
	Data []byte
	Code codes.Code
	Msg  string
}

// Context is passed through the middleware chain
type Context struct {
	Request  *RequestInfo
	Response *ResponseInfo
	Backend  string
	Metadata metadata.MD

	// Shared data between middlewares (protected by mu)
	mu     sync.RWMutex
	values map[string]any

	// Internal state
	index       int
	middlewares []Middleware
}

// NewContext creates a new middleware context
func NewContext(req *RequestInfo, middlewares []Middleware) *Context {
	return &Context{
		Request:     req,
		Metadata:    metadata.MD{},
		values:      make(map[string]any),
		middlewares: middlewares,
		index:       -1,
	}
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

// AbortWithError stops execution and returns an error to the client
func (c *Context) AbortWithError(code codes.Code, msg string) {
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

// AddMetadata adds metadata to be sent to backend
func (c *Context) AddMetadata(key, value string) {
	if c.Metadata == nil {
		c.Metadata = metadata.MD{}
	}
	c.Metadata.Append(key, value)
}

// Init initializes the context with request info and middlewares
func (c *Context) Init(req *RequestInfo, middlewares []Middleware) {
	c.Request = req
	c.Metadata = metadata.MD{}
	c.middlewares = middlewares
	c.index = -1
}

// Reset resets the context for reuse (called by sync.Pool)
func (c *Context) Reset() {
	c.Request = nil
	c.Response = nil
	c.Backend = ""
	c.Metadata = nil
	c.values = nil
	c.index = -1
	c.middlewares = nil
}
