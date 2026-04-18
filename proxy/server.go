package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/c32lab/gRPCat/middleware"
	"github.com/c32lab/gRPCat/parser"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Config struct {
	DefaultBackend string
	// KeepaliveParams, if set, controls client-side keepalive for backend
	// connections. Leave nil to use gRPC defaults — setting an aggressive
	// Time (e.g. 10s) can violate a backend's EnforcementPolicy.MinTime
	// (vtctld defaults to 5m) and trigger GOAWAY ENHANCE_YOUR_CALM.
	KeepaliveParams *keepalive.ClientParameters
	// BackendTransportCreds sets the transport credentials for backend
	// connections. When nil, insecure credentials are used.
	BackendTransportCreds credentials.TransportCredentials
	// BackendDialOptions supplies additional grpc.DialOption values
	// (stream interceptors, stats handlers, etc.) for backend connections.
	// These are appended after credentials and keepalive options.
	// Note: unary interceptors have no effect because all RPCs are
	// proxied as bidirectional streams via grpc.NewClientStream.
	BackendDialOptions []grpc.DialOption
}

type Server struct {
	config      *Config
	mu          sync.RWMutex
	middlewares []middleware.Middleware
	grpcServer  *grpc.Server
	contextPool sync.Pool
	forwarder   *Forwarder

	stopOnce sync.Once
	stopped  chan struct{}
}

func NewServer(config *Config) (*Server, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if config.DefaultBackend == "" {
		return nil, fmt.Errorf("must specify DefaultBackend")
	}

	server := &Server{
		config:      config,
		middlewares: []middleware.Middleware{},
		forwarder:   NewForwarder(config.KeepaliveParams, config.BackendTransportCreds, config.BackendDialOptions),
		stopped:     make(chan struct{}),
	}

	// Initialize context pool for reusing middleware contexts
	server.contextPool.New = func() any {
		return &middleware.Context{}
	}

	server.grpcServer = grpc.NewServer(
		grpc.ForceServerCodec(&ProxyCodec{}),
		grpc.UnknownServiceHandler(server.TransparentHandler()),
	)

	return server, nil
}

func (s *Server) Start(ctx context.Context, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	// Watch for ctx cancel OR an explicit Stop. Either triggers graceful
	// shutdown; the goroutine exits on whichever fires first so Stop() alone
	// doesn't leave this goroutine parked on ctx.Done().
	go func() {
		select {
		case <-ctx.Done():
			s.Stop()
		case <-s.stopped:
		}
	}()

	return s.grpcServer.Serve(listener)
}

func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopped)
		s.grpcServer.GracefulStop()
		// Release pooled backend connections. GracefulStop has returned, so
		// no forwarder is still writing to the cache.
		s.forwarder.Close()
	})
}

// GetGRPCServer exposes the underlying *grpc.Server so library users can
// register additional services (health, reflection, their own handlers)
// alongside the proxy on the same listener.
func (s *Server) GetGRPCServer() *grpc.Server {
	return s.grpcServer
}

func (s *Server) Use(mw middleware.Middleware) {
	if mw == nil {
		panic("middleware cannot be nil")
	}
	s.mu.Lock()
	s.middlewares = append(s.middlewares, mw)
	s.mu.Unlock()
}

// snapshotMiddlewares returns a stable view of the middleware slice. The
// returned slice is safe to hand to a middleware.Context even if Use is
// called concurrently, because we never mutate an already-published slice.
func (s *Server) snapshotMiddlewares() []middleware.Middleware {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.middlewares
}

// acquireContext gets a context from the pool and initializes it
func (s *Server) acquireContext(req *middleware.RequestInfo) *middleware.Context {
	ctx := s.contextPool.Get().(*middleware.Context)
	ctx.Init(req, s.snapshotMiddlewares())
	return ctx
}

// releaseContext resets and returns a context to the pool
func (s *Server) releaseContext(ctx *middleware.Context) {
	ctx.Reset()
	s.contextPool.Put(ctx)
}

// TransparentHandler creates a grpc.StreamHandler that proxies all requests.
// This is registered as grpc.UnknownServiceHandler to handle any unregistered service.
func (s *Server) TransparentHandler() grpc.StreamHandler {
	// Return a closure that captures the server reference
	// This ensures middlewares are read at request time, not at initialization time
	// and uses the context pool for better performance
	return func(srv any, serverStream grpc.ServerStream) error {
		ctx := serverStream.Context()
		md, _ := metadata.FromIncomingContext(ctx)

		// Extract gRPC method name (e.g., "/helloworld.Greeter/SayHello")
		fullMethodName, ok := grpc.MethodFromServerStream(serverStream)
		if !ok {
			return status.Errorf(codes.Internal, "failed to get method name from stream")
		}

		// Parse method name into service and method components
		service, method, err := parser.ParseGRPCPath(fullMethodName)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid method name: %v", err)
		}

		// Read first message frame
		firstFrame := &Frame{}
		firstFrameErr := serverStream.RecvMsg(firstFrame)

		// gRPC's codec path (ProxyCodec.Unmarshal) hands us the transport's
		// internal buffer; it is only valid until the next RecvMsg. We keep
		// this frame alive past middleware execution and pass it through to
		// Forward, so copy it up front. See proxy/codec.go:73.
		if firstFrameErr == nil && len(firstFrame.data) > 0 {
			buf := make([]byte, len(firstFrame.data))
			copy(buf, firstFrame.data)
			firstFrame.data = buf
		}

		// Try to parse the first frame's payload for middleware inspection
		var firstPayload []byte
		if firstFrameErr == nil && len(firstFrame.Data()) > 0 {
			if msg, parseErr := parser.ParseGRPCMessage(firstFrame.Data()); parseErr == nil {
				// Copy payload to avoid referencing gRPC's internal buffer
				firstPayload = make([]byte, len(msg.Payload))
				copy(firstPayload, msg.Payload)
			}
		} else if firstFrameErr != nil && firstFrameErr != io.EOF {
			if _, ok := status.FromError(firstFrameErr); ok {
				return firstFrameErr
			}
			return status.Errorf(codes.Internal, "failed to read first frame: %v", firstFrameErr)
		}

		// Prepare request info for middleware chain
		requestInfo := &middleware.RequestInfo{
			Service:      service,
			Method:       method,
			Metadata:     md,
			FirstPayload: firstPayload,
		}

		// Acquire context from pool and initialize it
		mwCtx := s.acquireContext(requestInfo)
		defer s.releaseContext(mwCtx)

		// Execute middleware chain
		mwCtx.Next()

		// Check if any middleware aborted the request
		if mwCtx.IsAborted() {
			return handleAborted(serverStream, mwCtx)
		}

		// Determine backend server
		backend := mwCtx.Backend
		if backend == "" {
			backend = s.config.DefaultBackend
		}

		// Forward the stream to backend
		var frameToForward *Frame
		if firstFrameErr == nil {
			frameToForward = firstFrame
		}

		return s.forwarder.Forward(
			serverStream.Context(),
			fullMethodName,
			serverStream,
			backend,
			mwCtx.Metadata,
			frameToForward,
		)
	}
}

// handleAborted processes requests that were aborted by middleware
func handleAborted(serverStream grpc.ServerStream, mwCtx *middleware.Context) error {
	if mwCtx.Response == nil {
		return status.Errorf(codes.Internal, "middleware aborted without response")
	}

	// If middleware provided custom response data, send it to client
	if mwCtx.Response.Data != nil {
		grpcMsg := &parser.GRPCMessage{Payload: mwCtx.Response.Data}
		mockFrame := &Frame{data: parser.EncodeGRPCMessage(grpcMsg)}
		if err := serverStream.SendMsg(mockFrame); err != nil {
			return status.Errorf(codes.Internal, "failed to send response: %v", err)
		}
		return nil
	}

	// Otherwise, return error to client
	code := mwCtx.Response.Code
	msg := mwCtx.Response.Msg
	if msg == "" {
		msg = "request aborted"
	}

	return status.Errorf(code, "%s", msg)
}
