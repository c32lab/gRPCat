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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Config struct {
	DefaultBackend string
}

type Server struct {
	config      *Config
	middlewares []middleware.Middleware
	grpcServer  *grpc.Server
	contextPool sync.Pool
	forwarder   *Forwarder
}

func NewServer(config *Config) (*Server, error) {
	if config.DefaultBackend == "" {
		return nil, fmt.Errorf("must specify DefaultBackend")
	}

	server := &Server{
		config:      config,
		middlewares: []middleware.Middleware{},
		forwarder:   NewForwarder(),
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

	go func() {
		<-ctx.Done()
		s.grpcServer.GracefulStop()
	}()

	return s.grpcServer.Serve(listener)
}

func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
}

func (s *Server) GetGRPCServer() *grpc.Server {
	return s.grpcServer
}

func (s *Server) Use(mw middleware.Middleware) {
	if mw == nil {
		panic("middleware cannot be nil")
	}
	s.middlewares = append(s.middlewares, mw)
}

// acquireContext gets a context from the pool and initializes it
func (s *Server) acquireContext(req *middleware.RequestInfo) *middleware.Context {
	ctx := s.contextPool.Get().(*middleware.Context)
	ctx.Init(req, s.middlewares)
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

		// Try to parse the first frame's payload for middleware inspection
		var firstPayload []byte
		if firstFrameErr == nil && len(firstFrame.Data()) > 0 {
			if msg, parseErr := parser.ParseGRPCMessage(firstFrame.Data()); parseErr == nil {
				// Copy payload to avoid referencing gRPC's internal buffer
				firstPayload = make([]byte, len(msg.Payload))
				copy(firstPayload, msg.Payload)
			}
		} else if firstFrameErr != nil && firstFrameErr != io.EOF {
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
	if len(mwCtx.Response.Data) > 0 {
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
