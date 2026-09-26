// Package proxy implements transparent gRPC message forwarding.
// It forwards raw gRPC frames without deserializing protobuf messages.
package proxy

import (
	"context"
	"errors"
	"io"
	"strings"

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
//   - md: metadata to send to the backend, normally the client's incoming
//     metadata as edited by middleware (middleware.RequestInfo.Metadata);
//     nil sends none. Forward leaves the map itself untouched.
//   - firstFrame: first message frame already read from client (can be nil).
//     It stays owned by the caller: Forward only sends it, which takes gRPC's
//     own reference on its buffers, so the caller is still responsible for
//     Free-ing it once Forward returns.
//
// Returns an error if proxying fails, or nil on successful completion.
func (f *Forwarder) Forward(
	ctx context.Context,
	fullMethodName string,
	serverStream grpc.ServerStream,
	backend string,
	md metadata.MD,
	firstFrame *Frame,
) error {
	// Get or create connection to backend server. acquire pins it against
	// idle eviction until release runs, so a long-lived proxied stream is
	// never cancelled by the cache's idle sweeper.
	conn, release, err := f.cache.acquire(backend)
	if err != nil {
		return status.Errorf(codes.Unavailable, "failed to connect to backend %s: %v", backend, err)
	}
	defer release()

	// Create cancellable context for backend stream.
	// This allows us to cancel the backend stream when client disconnects.
	clientCtx, clientCancel := context.WithCancel(ctx)
	defer clientCancel()

	// The backend leg mirrors the client's content-type. By default grpc-go
	// derives the subtype from ProxyCodec.Name(), which is empty so that the
	// default is the bare application/grpc a stock client sends. A client
	// that did send a subtype (application/grpc+json, say) gets it forwarded,
	// so the backend decodes the payload as it would have directly.
	var callOpts []grpc.CallOption
	if len(md) > 0 {
		if ct := md.Get("content-type"); len(ct) > 0 {
			if subtype := contentSubtype(ct[0]); subtype != "" {
				callOpts = append(callOpts, grpc.CallContentSubtype(subtype))
			}
		}
		// The proxy decompresses backend responses itself, so the backend
		// must only see the proxy's own grpc-accept-encoding, which the
		// transport adds; the client's list could name a compressor the proxy
		// has not registered. Copy first: the map belongs to the caller.
		out := md.Copy()
		out.Delete("grpc-accept-encoding")
		clientCtx = metadata.NewOutgoingContext(clientCtx, out)
	}

	// Create bidirectional stream to backend server. A status error here is
	// already the right answer for the client: Unavailable when the backend
	// cannot be reached, DeadlineExceeded/Canceled when the client's own
	// context ended while connecting.
	clientStream, err := grpc.NewClientStream(clientCtx, clientStreamDesc, conn, fullMethodName, callOpts...)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Errorf(codes.Internal, "failed to create client stream: %v", err)
	}

	// If we already read the first frame (for middleware inspection),
	// send it to backend before starting bidirectional forwarding.
	if firstFrame != nil {
		if err := sendFirstFrame(clientStream, firstFrame); err != nil {
			return err
		}
	}

	// Start two goroutines for bidirectional forwarding. Each reports exactly
	// once on its channel.
	s2cCh := f.forwardBackendToClient(clientStream, serverStream)
	c2sErrChan := f.forwardClientToBackend(serverStream, clientStream)

	// The RPC is over when the backend says so: io.EOF from the backend
	// direction is the backend's OK status, anything else is its failure.
	// Neither waits for the client direction. Waiting would hold the status
	// back until the client half-closed - a client that only sends after
	// reading would never get it - and that goroutine may still be mid-SendMsg
	// on the backend stream, while grpc-go forbids CloseSend concurrently with
	// SendMsg. The backend stream is therefore only half-closed once the
	// client direction has reported io.EOF, i.e. its goroutine has exited.
	//
	// Cases by RPC shape:
	//   1. Unary / server streaming: the client half-closes, then the backend
	//      completes.
	//   2. Client streaming: the client half-closes, the backend answers.
	//   3. Bidirectional: either side may finish first, including a backend
	//      that completes while the client is still sending.
	for {
		select {
		case res := <-s2cCh:
			if !res.backendDone {
				// The write to the client failed (it went away or stopped
				// reading) while the backend stream is still live. Its
				// trailers are not readable yet - grpc-go only guarantees
				// Trailer once RecvMsg has failed - and the client would not
				// see them anyway.
				clientCancel()
				return res.err
			}
			// Copy the backend's trailers to the client; on failure they carry
			// the details of the status the client is about to see.
			serverStream.SetTrailer(clientStream.Trailer())
			if errors.Is(res.err, io.EOF) {
				// Backend completed the RPC. forwardClientToBackend may still
				// be running; see the error branch below for why returning
				// with it in flight is safe.
				return nil
			}
			// gRPC returns a status.Error here, carrying the backend's real
			// code/message/details. Forward it verbatim so the client sees
			// the exact failure the backend produced.
			clientCancel()
			// Deliberately NOT waiting for forwardClientToBackend here:
			// it is parked in serverStream.RecvMsg, which only unblocks on
			// client activity or on gRPC cancelling the stream context -
			// and gRPC does that when this handler returns. Waiting would
			// deadlock. Returning is safe because that goroutine only
			// READS serverStream, and once the stream is done its cleanup
			// (WriteStatus) is a no-op.
			return res.err
		case c2sErr := <-c2sErrChan:
			if !errors.Is(c2sErr, io.EOF) {
				// Error reading from client (disconnect, cancellation). Cancel
				// the backend stream and return the client's error as-is so
				// its status code (typically Canceled) is preserved.
				clientCancel()
				// forwardBackendToClient WRITES to serverStream, which is only
				// valid while this handler runs, so drain it before returning -
				// but ONLY once the server stream's context is already done.
				// A pending serverStream.SendMsg parks on HTTP/2 write flow
				// control, and that wait is released by the client reading or
				// by the stream context being cancelled - which gRPC only does
				// after this handler returns. Waiting unconditionally is a
				// circular wait: a client that stops reading would pin the
				// handler, its stream and the backend stream forever.
				// A live context here means the failure was on the backend leg
				// (e.g. a message over MaxSendMsgSize) while the client is
				// still connected. We then return with that goroutine possibly
				// still in flight; the surviving window is one SendMsg, which
				// gRPC rejects with an error once the stream is done rather
				// than corrupting it.
				if serverStream.Context().Err() != nil {
					<-s2cCh
				}
				return c2sErr
			}
			// Client finished sending all requests. Its goroutine has exited,
			// so half-closing the backend stream cannot race with a SendMsg.
			// grpc-go's CloseSend never returns an error.
			clientStream.CloseSend()
		}
	}
}

// sendFirstFrame writes the frame the handler already read for middleware.
//
// io.EOF is not an error here: it means the backend has already ended the
// stream, e.g. rejected the RPC at header time without reading the body, and
// its status is only available from RecvMsg. The caller then starts the pumps
// and forwardBackendToClient delivers that status, as happens for every later
// frame. A status error already carries the real code (e.g. ResourceExhausted
// when the frame is over MaxSendMsgSize) and is passed through so the first
// message fails with the same code forwardClientToBackend gives later ones.
func sendFirstFrame(stream grpc.ClientStream, frame *Frame) error {
	err := stream.SendMsg(frame)
	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Errorf(codes.Internal, "failed to send first frame: %v", err)
}

// backendReport is what forwardBackendToClient sends when it stops.
type backendReport struct {
	err error
	// backendDone is true when err came from reading the backend stream: the
	// backend has finished (err is io.EOF or its status) and its trailers may
	// be read. It is false when a write to the client failed while the backend
	// stream was still live.
	backendDone bool
}

// forwardBackendToClient forwards messages from backend to client.
// Runs in a separate goroutine and returns a channel that receives one
// backendReport when the direction stops.
//
// Response headers are forwarded as soon as the backend sends them: Header
// blocks until they arrive or the stream ends, so a backend that sends
// headers and then streams slowly does not leave the client's Header() call
// waiting for the first message, and headers a backend sent before failing
// are not lost. A trailers-only response yields no headers and stays
// trailers-only for the client.
//
// The channel is buffered (size 1) to prevent goroutine leak if the caller stops reading.
func (f *Forwarder) forwardBackendToClient(src grpc.ClientStream, dst grpc.ServerStream) chan backendReport {
	ret := make(chan backendReport, 1)
	go func() {
		// grpc-go's Header never reports an error: a stream that ended
		// without headers (trailers-only) yields nil metadata and its status
		// surfaces from RecvMsg below, which is also where any other failure
		// is picked up.
		if md, err := src.Header(); err == nil && md != nil {
			if err := dst.SendHeader(md); err != nil {
				ret <- backendReport{err: err}
				return
			}
		}
		// One Frame is reused for the whole stream. It is touched by this
		// goroutine only, and each message's buffers are released before the
		// next RecvMsg refills it, so reuse costs nothing in buffer lifetime
		// and saves an allocation per message.
		frame := &Frame{}
		// Releases the message still held when the loop exits on an error
		// path that runs after a successful RecvMsg (a RecvMsg that reports
		// an error having already unmarshalled).
		defer frame.Free()
		for {
			if err := src.RecvMsg(frame); err != nil {
				ret <- backendReport{err: err, backendDone: true}
				return
			}
			// SendMsg has taken its own reference on the buffers by the time
			// it returns - the transport Refs them before queueing the write -
			// so dropping ours here is safe even though the write itself is
			// asynchronous. On failure nothing was queued and this is the last
			// reference, which returns the buffers to the pool immediately.
			// The next RecvMsg would release them too, but only whenever the
			// next message shows up; on an idle stream that can be minutes.
			sendErr := dst.SendMsg(frame)
			frame.Free()
			if sendErr != nil {
				ret <- backendReport{err: sendErr}
				return
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
		// See forwardBackendToClient for why one Frame is reused and why
		// freeing right after SendMsg is safe. The deferred Free also covers
		// the case where this goroutine outlives Forward: the "return s2cErr"
		// path deliberately does not drain it, so it may still be parked in
		// RecvMsg. That wait ends when gRPC cancels the stream context after
		// the handler returns, and this Free then hands back any buffers the
		// last RecvMsg had already unmarshalled.
		frame := &Frame{}
		defer frame.Free()
		for {
			if err := src.RecvMsg(frame); err != nil {
				ret <- err
				break
			}
			sendErr := dst.SendMsg(frame)
			frame.Free()
			if sendErr != nil {
				ret <- sendErr
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

// contentSubtype returns the subtype of a gRPC content-type such as
// "application/grpc+proto" ("proto"), or "" for a bare "application/grpc" or
// anything else.
func contentSubtype(contentType string) string {
	rest, ok := strings.CutPrefix(contentType, "application/grpc")
	if !ok || len(rest) < 2 || rest[0] != '+' {
		return ""
	}
	return strings.ToLower(rest[1:])
}
