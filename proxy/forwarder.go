// Package proxy implements transparent gRPC message forwarding.
// It forwards raw gRPC frames without deserializing protobuf messages.
package proxy

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	// clientStreamDesc is used to create bidirectional streams to backend servers.
	// Both ServerStreams and ClientStreams must be true to support all gRPC streaming modes.
	clientStreamDesc = &grpc.StreamDesc{
		ServerStreams: true,
		ClientStreams: true,
	}
)

// Forwarder handles bidirectional gRPC stream proxying between client and backend server.
// It maintains a connection cache to reuse backend connections.
type Forwarder struct {
	cache *ConnectionCache
}

// NewForwarder creates a new Forwarder with an empty connection cache.
// ka is optional keepalive; creds overrides default insecure transport
// credentials (nil = insecure); dialOpts are additional backend dial options.
func NewForwarder(ka *keepalive.ClientParameters, creds credentials.TransportCredentials, dialOpts []grpc.DialOption) *Forwarder {
	return &Forwarder{
		cache: NewConnectionCache(ka, creds, dialOpts),
	}
}

// Forward proxies a gRPC stream between client and backend server.
//
// It creates two goroutines for bidirectional forwarding:
//   - c2s (client to backend): serverStream -> clientStream
//   - s2c (backend to client): clientStream -> serverStream
//
// Parameters:
//   - ctx: request context
//   - fullMethodName: gRPC method name (e.g., "/service.Service/Method")
//   - serverStream: incoming stream from client
//   - backend: backend server address (e.g., "localhost:50051")
//   - additionalMD: extra metadata to send to backend (can be set by middlewares)
//   - firstFrame: first message frame already read from client (can be nil)
//
// Returns an error if proxying fails, or nil on successful completion.
func (f *Forwarder) Forward(
	ctx context.Context,
	fullMethodName string,
	serverStream grpc.ServerStream,
	backend string,
	additionalMD metadata.MD,
	firstFrame *Frame,
) error {
	// Get or create connection to backend server
	conn, err := f.cache.Get(backend)
	if err != nil {
		return status.Errorf(codes.Unavailable, "failed to connect to backend %s: %v", backend, err)
	}

	// Create cancellable context for backend stream.
	// This allows us to cancel the backend stream when client disconnects.
	clientCtx, clientCancel := context.WithCancel(ctx)
	defer clientCancel()

	// Prepare metadata for backend request.
	// Merge incoming metadata with additional metadata from middlewares.
	if len(additionalMD) > 0 {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			clientCtx = metadata.NewOutgoingContext(clientCtx, metadata.Join(md, additionalMD))
		} else {
			clientCtx = metadata.NewOutgoingContext(clientCtx, additionalMD)
		}
	} else if md, ok := metadata.FromIncomingContext(ctx); ok {
		clientCtx = metadata.NewOutgoingContext(clientCtx, md)
	}

	// Create bidirectional stream to backend server
	clientStream, err := grpc.NewClientStream(clientCtx, clientStreamDesc, conn, fullMethodName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create client stream: %v", err)
	}

	// If we already read the first frame (for middleware inspection),
	// send it to backend before starting bidirectional forwarding.
	if firstFrame != nil {
		if err := clientStream.SendMsg(firstFrame); err != nil {
			return status.Errorf(codes.Internal, "failed to send first frame: %v", err)
		}
	}

	// Start two goroutines for bidirectional forwarding.
	// Each returns when its direction completes or errors.
	s2cErrChan := f.forwardBackendToClient(clientStream, serverStream)
	c2sErrChan := f.forwardClientToBackend(serverStream, clientStream)

	// CRITICAL: We must wait for BOTH goroutines to complete.
	// The loop runs exactly twice - once for each direction.
	//
	// We don't know which direction will finish first:
	//   1. Unary: client sends, then backend responds -> c2s finishes first
	//   2. Server streaming: client sends once, backend streams -> c2s finishes first
	//   3. Client streaming: client streams, backend responds once -> s2c finishes first
	//   4. Bidirectional: both stream concurrently -> either can finish first
	//
	// When one direction finishes with io.EOF (normal completion), the other
	// direction may still be pumping data, so we must continue the loop.
	//
	// CloseSend() signals to backend that we won't send more data.
	// We call it when either direction completes (EOF), ensuring it's called exactly once.
	var closedSend bool
	for i := 0; i < 2; i++ {
		select {
		case s2cErr := <-s2cErrChan:
			if s2cErr == io.EOF {
				// Backend finished sending responses.
				// Close our send side to backend (if not already closed).
				// Even if c2s is still forwarding, backend completing usually means
				// it won't accept more data, so we signal completion.
				if !closedSend {
					clientStream.CloseSend()
					closedSend = true
				}
			} else {
				// Error from backend stream. gRPC returns a status.Error here,
				// carrying the backend's real code/message/details. Forward it
				// verbatim plus the backend's trailers so the client sees the
				// exact failure the backend produced.
				clientCancel()
				serverStream.SetTrailer(clientStream.Trailer())
				return s2cErr
			}
		case c2sErr := <-c2sErrChan:
			if c2sErr != io.EOF {
				// Error reading from client (disconnect, cancellation). Cancel
				// the backend stream and return the client's error as-is so
				// its status code (typically Canceled) is preserved.
				clientCancel()
				return c2sErr
			}
			// Client finished sending all requests (io.EOF).
			// Close send side to backend to signal end of request stream.
			if !closedSend {
				clientStream.CloseSend()
				closedSend = true
			}
		}
	}
	// Both directions finished successfully (both returned io.EOF).
	// Copy trailers from backend to client before returning.
	serverStream.SetTrailer(clientStream.Trailer())
	return nil
}

// forwardBackendToClient forwards messages from backend to client.
// Runs in a separate goroutine and returns a channel that receives the first error or io.EOF.
//
// IMPORTANT: This uses a loop counter (for i := 0; ; i++) to detect the first message.
// This is NOT a performance issue - it's required by gRPC protocol:
//   - Headers can only be read AFTER receiving the first message from backend
//   - Headers must be sent to client BEFORE forwarding the first message
//   - This is the only way to properly proxy gRPC headers
//
// The channel is buffered (size 1) to prevent goroutine leak if the caller stops reading.
func (f *Forwarder) forwardBackendToClient(src grpc.ClientStream, dst grpc.ServerStream) chan error {
	ret := make(chan error, 1)
	go func() {
		frame := &Frame{}
		for i := 0; ; i++ {
			if err := src.RecvMsg(frame); err != nil {
				ret <- err
				break
			}
			if i == 0 {
				md, err := src.Header()
				if err != nil {
					ret <- err
					break
				}
				if err := dst.SendHeader(md); err != nil {
					ret <- err
					break
				}
			}
			if err := dst.SendMsg(frame); err != nil {
				ret <- err
				break
			}
		}
	}()
	return ret
}

// forwardClientToBackend forwards messages from client to backend.
// Runs in a separate goroutine and returns a channel that receives the first error or io.EOF.
//
// This direction is simpler than forwardBackendToClient because:
//   - We don't need to forward headers (already handled in Forward method via metadata context)
//   - We just pump messages from client to backend until one side closes
//
// The channel is buffered (size 1) to prevent goroutine leak if the caller stops reading.
func (f *Forwarder) forwardClientToBackend(src grpc.ServerStream, dst grpc.ClientStream) chan error {
	ret := make(chan error, 1)
	go func() {
		frame := &Frame{}
		for {
			if err := src.RecvMsg(frame); err != nil {
				ret <- err
				break
			}
			if err := dst.SendMsg(frame); err != nil {
				ret <- err
				break
			}
		}
	}()
	return ret
}

// Close releases all cached backend connections.
// Should be called when shutting down the proxy server.
func (f *Forwarder) Close() {
	f.cache.Close()
}
