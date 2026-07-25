package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
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
	// They are also appended after the proxy's own default call options, so
	// a grpc.WithDefaultCallOptions carrying grpc.MaxCallRecvMsgSize /
	// grpc.MaxCallSendMsgSize overrides MaxRecvMsgSize / MaxSendMsgSize on
	// the backend leg.
	// Note: unary interceptors have no effect because all RPCs are
	// proxied as bidirectional streams via grpc.NewClientStream.
	BackendDialOptions []grpc.DialOption
	// ServerOptions supplies additional grpc.ServerOption values for the
	// listening side (TLS credentials, keepalive enforcement, max concurrent
	// streams, stats handlers, etc.). These are appended after the proxy's
	// own codec, unknown-service-handler and message size options, so they
	// win on conflict.
	// Note: unary interceptors have no effect because all RPCs are handled
	// as streams via grpc.UnknownServiceHandler.
	ServerOptions []grpc.ServerOption
	// Hooks supplies optional callbacks for proxy lifecycle events. Leave
	// nil to disable all hooks.
	Hooks *Hooks
	// MaxRecvMsgSize and MaxSendMsgSize bound message sizes in both
	// directions: they apply to the proxy's server side (client-facing) and
	// to its backend connections alike, unless overridden — a size option
	// inside ServerOptions wins on the listening side, and one inside
	// BackendDialOptions wins on the backend side, because both are applied
	// after these. Zero means "no limit" (math.MaxInt32), so the proxy stays
	// transparent and the backend keeps ownership of the policy; grpc-go's
	// own 4MB receive default would otherwise cap throughput invisibly. Set
	// them explicitly to re-impose a bound — an unbounded public-facing
	// proxy will buffer whatever a peer sends, which is a DoS surface.
	MaxRecvMsgSize int
	MaxSendMsgSize int
}

// Hooks groups optional callbacks the proxy invokes at well-defined points.
// All fields are optional; a nil field means "no callback".
type Hooks struct {
	// OnFirstFrameError is called when the first message frame cannot be
	// read from the client stream (transport error, cancellation, or a
	// message over the server's receive limit; a clean EOF is not an
	// error). Returning a non-nil error makes the handler abort with that
	// error; returning nil falls back to aborting with the underlying read
	// error. RequestInfo.FirstPayload is nil when this fires.
	//
	// gRPC writes the read status to the client itself before the handler
	// regains control, so the returned error sets the server-side result
	// (logs, stats handlers) and not the status the client observes.
	//
	// It is never called for a well-formed request: gRPC strips the
	// message header before the codec runs, so the proxy does no framing
	// of its own and has nothing left to fail at.
	OnFirstFrameError func(req *middleware.RequestInfo, err error) error
}

type Server struct {
	config      *Config
	mu          sync.RWMutex
	middlewares []middleware.Middleware
	grpcServer  *grpc.Server
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

	maxRecvMsgSize := resolveMsgSize(config.MaxRecvMsgSize)
	maxSendMsgSize := resolveMsgSize(config.MaxSendMsgSize)

	server := &Server{
		config:      config,
		middlewares: []middleware.Middleware{},
		forwarder:   NewForwarder(config.KeepaliveParams, config.BackendTransportCreds, config.BackendDialOptions),
		stopped:     make(chan struct{}),
	}
	server.forwarder.cache.maxRecvMsgSize = maxRecvMsgSize
	server.forwarder.cache.maxSendMsgSize = maxSendMsgSize

	serverOpts := []grpc.ServerOption{
		grpc.ForceServerCodec(&ProxyCodec{}),
		grpc.UnknownServiceHandler(server.TransparentHandler()),
		grpc.MaxRecvMsgSize(maxRecvMsgSize),
		grpc.MaxSendMsgSize(maxSendMsgSize),
	}
	serverOpts = append(serverOpts, config.ServerOptions...)
	server.grpcServer = grpc.NewServer(serverOpts...)

	return server, nil
}

// resolveMsgSize maps Config's "zero means no limit" convention onto the byte
// count gRPC expects.
func resolveMsgSize(n int) int {
	if n <= 0 {
		return math.MaxInt32
	}
	return n
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
// called concurrently: Use only appends, so it writes at indices at or above
// the length of every previously returned slice, and a reader only indexes
// below its own snapshot's length. No reader can observe a slot being
// written, and a reallocating append leaves the old array untouched.
func (s *Server) snapshotMiddlewares() []middleware.Middleware {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.middlewares
}

// acquireContext gets a context from the pool and initializes it
func (s *Server) acquireContext(req *middleware.RequestInfo) *middleware.Context {
	return middleware.AcquireContext(req, s.snapshotMiddlewares())
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

		// gRPC strips the 5-byte message header before invoking the codec
		// (rpc_util.go recv -> recvAndDecompress), so the frame already holds
		// the bare payload. It was copied above, so middleware may keep it.
		var firstPayload []byte
		if firstFrameErr == nil {
			firstPayload = firstFrame.Data()
		}

		// Prepare request info for middleware chain
		requestInfo := &middleware.RequestInfo{
			Service:      service,
			Method:       method,
			Metadata:     md,
			FirstPayload: firstPayload,
		}

		// The first frame could not be read at all (transport error, not a
		// clean EOF). Invoke the OnFirstFrameError hook if one is set; a
		// non-nil return aborts the request with that error.
		if firstFrameErr != nil && !errors.Is(firstFrameErr, io.EOF) {
			if s.config.Hooks != nil && s.config.Hooks.OnFirstFrameError != nil {
				if hookErr := s.config.Hooks.OnFirstFrameError(requestInfo, firstFrameErr); hookErr != nil {
					return hookErr
				}
			}
			if _, ok := status.FromError(firstFrameErr); ok {
				return firstFrameErr
			}
			return status.Errorf(codes.Internal, "failed to read first frame: %v", firstFrameErr)
		}

		// Acquire context from pool and initialize it
		mwCtx := s.acquireContext(requestInfo)
		defer middleware.ReleaseContext(mwCtx)

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

	// If middleware provided custom response data, send it to client.
	// gRPC prepends the 5-byte message header itself (rpc_util.go msgHeader),
	// so the frame must carry the bare payload.
	if mwCtx.Response.Data != nil {
		mockFrame := &Frame{data: mwCtx.Response.Data}
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
