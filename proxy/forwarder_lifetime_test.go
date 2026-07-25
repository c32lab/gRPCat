package proxy

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// lifetimeServerStream is a grpc.ServerStream stand-in that records when the
// forwarding goroutines touch it. grpc-go only guarantees a ServerStream is
// valid for the duration of the handler, so these tests assert that no write
// to it is in flight when Forward returns and that none starts afterwards.
//
// It also mimics the two things a real ServerStream does that matter here:
// RecvMsg blocks until there is client activity or the stream context is
// cancelled, and the stream context is only cancelled once the handler
// returns.
type lifetimeServerStream struct {
	ctx context.Context

	// recvGate, if non-nil, is waited on before RecvMsg returns recvErr.
	// A nil gate means RecvMsg only unblocks via ctx (a connected client
	// that sends nothing further).
	recvGate <-chan struct{}
	// recvDelay is slept after recvGate fires, before recvErr is returned.
	recvDelay time.Duration
	recvErr   error
	// recvFrame, if non-nil, is delivered by the first RecvMsg that gets past
	// the gate instead of recvErr (a client that is still sending).
	recvFrame []byte
	// sendParksUntilCtxDone models HTTP/2 write flow control against a client
	// that has stopped reading: grpc@v1.80.0 http2_server.go:610 initialises
	// the stream's write quota with s.ctxDone, and flowcontrol.go writeQuota.get
	// then selects only on a quota replenish (the client reading) or on that
	// context. grpc-go cancels the stream context in finishStream/closeStream,
	// i.e. only once the handler has returned.
	sendParksUntilCtxDone bool
	// sendDelay keeps SendMsg running long enough that a Forward returning
	// without waiting is caught with a write still in flight.
	sendDelay time.Duration

	// firstWrite is closed when any write (SendHeader/SendMsg) starts.
	firstWrite chan struct{}
	// firstSendMsg is closed when the first SendMsg starts.
	firstSendMsg chan struct{}
	// recvExited is closed when RecvMsg returns.
	recvExited chan struct{}

	mu             sync.Mutex
	firstWriteDone bool
	firstSendDone  bool
	recvExitedDone bool
	returned       bool
	writesInFlight int
	writesAtReturn int
	lateWrites     []string
}

func newLifetimeServerStream(ctx context.Context) *lifetimeServerStream {
	return &lifetimeServerStream{
		ctx:          ctx,
		firstWrite:   make(chan struct{}),
		firstSendMsg: make(chan struct{}),
		recvExited:   make(chan struct{}),
	}
}

func (s *lifetimeServerStream) beginWrite(op string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.firstWriteDone {
		s.firstWriteDone = true
		close(s.firstWrite)
	}
	if op == "SendMsg" && !s.firstSendDone {
		s.firstSendDone = true
		close(s.firstSendMsg)
	}
	if s.returned {
		s.lateWrites = append(s.lateWrites, op)
	}
	s.writesInFlight++
}

func (s *lifetimeServerStream) endWrite() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writesInFlight--
}

// markReturned must be called the moment Forward returns. It snapshots how
// many writes were still executing at that instant, and arms late-write
// recording.
func (s *lifetimeServerStream) markReturned() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.returned = true
	s.writesAtReturn = s.writesInFlight
}

func (s *lifetimeServerStream) report() (int, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writesAtReturn, append([]string(nil), s.lateWrites...)
}

func (s *lifetimeServerStream) SetHeader(metadata.MD) error { return nil }

func (s *lifetimeServerStream) SendHeader(metadata.MD) error {
	s.beginWrite("SendHeader")
	defer s.endWrite()
	return nil
}

func (s *lifetimeServerStream) SetTrailer(metadata.MD) {}

func (s *lifetimeServerStream) Context() context.Context { return s.ctx }

func (s *lifetimeServerStream) SendMsg(any) error {
	s.beginWrite("SendMsg")
	defer s.endWrite()
	if s.sendParksUntilCtxDone {
		<-s.ctx.Done()
	}
	if s.sendDelay > 0 {
		time.Sleep(s.sendDelay)
	}
	if s.sendParksUntilCtxDone {
		return s.ctx.Err()
	}
	return nil
}

func (s *lifetimeServerStream) RecvMsg(m any) error {
	select {
	case <-s.recvGate:
	case <-s.ctx.Done():
		s.markRecvExited()
		// grpc-go's transport turns a dead stream context into a status
		// error (internal/transport.ContextErr), not the bare context error.
		return status.FromContextError(s.ctx.Err()).Err()
	}
	if s.recvDelay > 0 {
		time.Sleep(s.recvDelay)
	}
	if s.recvFrame != nil {
		// Mimic ProxyCodec.Unmarshal: release whatever the frame held, then
		// hand it the new payload.
		f := m.(*Frame)
		f.Free()
		*f = *frameFromBytes(s.recvFrame)
		s.recvFrame = nil
		return nil
	}
	s.markRecvExited()
	return s.recvErr
}

func (s *lifetimeServerStream) markRecvExited() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.recvExitedDone {
		s.recvExitedDone = true
		close(s.recvExited)
	}
}

// TestForward_ClientErrorWaitsForBackendToClientGoroutine pins that when the
// client-to-backend direction fails after the client has gone away, Forward
// waits for the backend-to-client goroutine to finish before returning. That
// goroutine WRITES to the ServerStream, which is only valid while the handler
// is running.
func TestForward_ClientErrorWaitsForBackendToClientGoroutine(t *testing.T) {
	backendAddr := startPushStreamBackend(t, 5)

	f := NewForwarder(nil, nil, nil)
	defer f.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ss := newLifetimeServerStream(ctx)
	// The client has stopped reading, so the backend-to-client goroutine parks
	// in SendMsg on write flow control; recvGate is nil, so the client-to-
	// backend goroutine is parked in RecvMsg. Cancelling the stream context
	// (the client going away) releases both, exactly as grpc-go does.
	ss.sendParksUntilCtxDone = true
	ss.sendDelay = 300 * time.Millisecond

	errCh := make(chan error, 1)
	go func() {
		err := f.Forward(ctx, "/test.Echo/ServerStream", ss, backendAddr, nil,
			frameFromBytes(buildGRPCMessage([]byte("go"))))
		ss.markReturned()
		errCh <- err
	}()

	// Let the backend-to-client goroutine get parked writing to the client
	// first, then have the client disappear.
	<-ss.firstSendMsg
	cancel()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(20 * time.Second):
		t.Fatal("Forward did not return")
	}

	if status.Code(err) != codes.Canceled {
		t.Fatalf("expected the client's Canceled error back, got %v", err)
	}

	inFlight, _ := ss.report()
	if inFlight != 0 {
		t.Errorf("Forward returned with %d write(s) still executing on the ServerStream; "+
			"the backend-to-client goroutine must be drained before the handler returns", inFlight)
	}

	// Nothing may start writing after the handler returned either. The
	// goroutine has already exited when the fix is in place, so this window
	// only needs to outlast one more SendMsg.
	time.Sleep(3 * ss.sendDelay)
	if _, late := ss.report(); len(late) != 0 {
		t.Errorf("ServerStream written to after Forward returned: %v", late)
	}
}

// TestForward_ClientErrorDoesNotWaitWhileClientIsConnected is the liveness
// counterpart to the test above: the drain must NOT happen while the server
// stream's context is still live.
//
// Here the client-to-backend direction fails on the BACKEND leg
// (ResourceExhausted, the documented Config.MaxSendMsgSize path) while the
// client is still connected but no longer reading, so the backend-to-client
// goroutine is parked in serverStream.SendMsg on HTTP/2 write flow control.
// Cancelling the backend stream does not release that wait; only the client
// reading or the stream context does, and grpc-go cancels the stream context
// only after this handler returns. Waiting there is a circular wait that pins
// the handler, the server stream and the backend stream for as long as the
// client holds the connection open.
func TestForward_ClientErrorDoesNotWaitWhileClientIsConnected(t *testing.T) {
	backendAddr := startPushStreamBackend(t, 5)

	// Cap the backend leg so the proxy's send to the backend is what fails.
	f := NewForwarder(nil, nil, []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(64)),
	})
	defer f.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ss := newLifetimeServerStream(ctx)
	ss.sendParksUntilCtxDone = true
	// The client's next message arrives once the backend-to-client goroutine
	// is parked writing, and is over the backend's send limit.
	ss.recvGate = ss.firstSendMsg
	ss.recvFrame = buildGRPCMessage(make([]byte, 4096))

	errCh := make(chan error, 1)
	go func() {
		errCh <- f.Forward(ctx, "/test.Echo/ServerStream", ss, backendAddr, nil,
			frameFromBytes(buildGRPCMessage([]byte("go"))))
	}()

	select {
	case err := <-errCh:
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("expected the backend leg's ResourceExhausted error back, got %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Forward deadlocked: it must not wait for a backend-to-client write that " +
			"only the server stream's context can release, because grpc-go cancels that " +
			"context only after this handler returns")
	}
}

// TestForward_BackendErrorReturnsWhileClientIsSilent pins the deliberate
// asymmetry: the backend-error path must NOT wait for the client-to-backend
// goroutine. That goroutine is parked in serverStream.RecvMsg, which unblocks
// only on client activity or on grpc-go cancelling the stream context — and
// grpc-go does that when the handler returns (closeStream -> s.cancel()).
// Waiting here would deadlock; a regression shows up as this test timing out.
func TestForward_BackendErrorReturnsWhileClientIsSilent(t *testing.T) {
	backendAddr := startImmediateErrorStreamBackend(t, codes.Unavailable, "backend down")

	f := NewForwarder(nil, nil, nil)
	defer f.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// recvGate is nil: the client is connected but sends nothing more, so
	// RecvMsg can only be released by the context.
	ss := newLifetimeServerStream(ctx)

	errCh := make(chan error, 1)
	go func() {
		errCh <- f.Forward(ctx, "/test.Echo/ServerStream", ss, backendAddr, nil, nil)
	}()

	select {
	case err := <-errCh:
		ss.markReturned()
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("expected the backend's Unavailable error back, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Forward blocked on the backend-error path: it must not wait for the " +
			"client-to-backend goroutine, which cannot finish until the handler returns")
	}

	// grpc-go cancels the stream context once the handler returns; do the same
	// and confirm the surviving goroutine unwinds promptly.
	cancel()
	select {
	case <-ss.recvExited:
	case <-time.After(10 * time.Second):
		t.Fatal("client-to-backend goroutine did not unwind after the stream context was cancelled")
	}
}

// TestForward_ClientErrorAfterBackendEOFDoesNotWait covers the case where the
// backend direction has already been received (loop iteration 1) before the
// client direction fails. Its goroutine is long gone and its channel is
// drained, so waiting for it again would block forever.
func TestForward_ClientErrorAfterBackendEOFDoesNotWait(t *testing.T) {
	// This backend sends a header and returns OK without any message, so the
	// backend direction completes with io.EOF first.
	backendAddr := startHeaderOnlyStreamBackend(t)

	f := NewForwarder(nil, nil, nil)
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ss := newLifetimeServerStream(ctx)
	ss.recvErr = status.Error(codes.Canceled, "client went away")
	// Release the client error only after the backend direction's header
	// write, i.e. after it has finished; the delay gives Forward ample room to
	// consume the backend result before the client error is delivered.
	ss.recvGate = ss.firstWrite
	ss.recvDelay = 500 * time.Millisecond

	errCh := make(chan error, 1)
	go func() {
		errCh <- f.Forward(ctx, "/test.Echo/ServerStream", ss, backendAddr, nil,
			frameFromBytes(buildGRPCMessage([]byte("go"))))
	}()

	select {
	case err := <-errCh:
		ss.markReturned()
		if status.Code(err) != codes.Canceled {
			t.Fatalf("expected the client's Canceled error back, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Forward blocked waiting for a direction that had already completed")
	}
}

// TestForwarder_ClientDisconnectMidStream is the end-to-end counterpart: a
// real client cancels while the backend is still streaming. The proxy must
// unwind cleanly (no hang, no race) and keep serving.
func TestForwarder_ClientDisconnectMidStream(t *testing.T) {
	backendAddr := startInfiniteStreamBackend(t)
	proxyAddr := startProxyServer(t, backendAddr)

	conn, err := grpc.NewClient(proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	streamCtx, streamCancel := context.WithCancel(context.Background())
	stream, err := conn.NewStream(streamCtx, clientStreamDesc, "/test.Echo/ServerStream")
	if err != nil {
		streamCancel()
		t.Fatalf("new stream: %v", err)
	}
	if err := stream.SendMsg(frameFromBytes(buildGRPCMessage([]byte("go")))); err != nil {
		streamCancel()
		t.Fatalf("send: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := stream.RecvMsg(&Frame{}); err != nil {
			streamCancel()
			t.Fatalf("recv %d: %v", i, err)
		}
	}
	// Disconnect mid-stream while the backend keeps streaming.
	streamCancel()

	// The proxy must still serve afterwards.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream2, err := conn.NewStream(ctx, clientStreamDesc, "/test.Echo/ServerStream")
	if err != nil {
		t.Fatalf("new stream after disconnect: %v", err)
	}
	if err := stream2.SendMsg(frameFromBytes(buildGRPCMessage([]byte("go")))); err != nil {
		t.Fatalf("send after disconnect: %v", err)
	}
	if err := stream2.RecvMsg(&Frame{}); err != nil {
		t.Fatalf("recv after disconnect: %v", err)
	}
}

// TestForwarder_BackendErrorWhileClientSends is the end-to-end counterpart for
// the other direction: the backend fails mid-stream while the client is still
// sending. The client must see the backend's status and the proxy must keep
// serving.
func TestForwarder_BackendErrorWhileClientSends(t *testing.T) {
	backendAddr := startFailAfterOneBidiBackend(t, codes.ResourceExhausted, "backend gave up")
	proxyAddr := startProxyServer(t, backendAddr)

	conn, err := grpc.NewClient(proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	stream, err := conn.NewStream(ctx,
		&grpc.StreamDesc{ClientStreams: true, ServerStreams: true},
		"/test.Echo/Bidi",
	)
	if err != nil {
		t.Fatalf("new stream: %v", err)
	}

	// Keep sending until the stream dies.
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		for i := 0; i < 1000; i++ {
			if err := stream.SendMsg(frameFromBytes(buildGRPCMessage([]byte("x")))); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	var recvErr error
	for {
		if err := stream.RecvMsg(&Frame{}); err != nil {
			recvErr = err
			break
		}
	}
	<-sendDone

	if status.Code(recvErr) != codes.ResourceExhausted {
		t.Fatalf("expected the backend's ResourceExhausted status, got %v", recvErr)
	}

	// The proxy must still serve afterwards.
	stream2, err := conn.NewStream(ctx,
		&grpc.StreamDesc{ClientStreams: true, ServerStreams: true},
		"/test.Echo/Bidi",
	)
	if err != nil {
		t.Fatalf("new stream after backend error: %v", err)
	}
	if err := stream2.SendMsg(frameFromBytes(buildGRPCMessage([]byte("x")))); err != nil {
		t.Fatalf("send after backend error: %v", err)
	}
	if err := stream2.RecvMsg(&Frame{}); err != nil {
		t.Fatalf("recv after backend error: %v", err)
	}
}

// startImmediateErrorStreamBackend starts a backend whose stream handler fails
// straight away without reading anything from the client.
//
// Backends in this file all register ClientStreams: true, because grpc-go only
// delivers request messages to a handler before half-close for
// client-streaming methods, and these tests deliberately never half-close.
func startImmediateErrorStreamBackend(t *testing.T, code codes.Code, msg string) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.ForceServerCodecV2(&ProxyCodec{}))
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "ServerStream",
				ClientStreams: true,
				ServerStreams: true,
				Handler: func(_ any, _ grpc.ServerStream) error {
					return status.Error(code, msg)
				},
			},
		},
	}, nil)
	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })
	return lis.Addr().String()
}

// startPushStreamBackend starts a backend that reads one request message and
// then pushes msgCount responses.
func startPushStreamBackend(t *testing.T, msgCount int) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.ForceServerCodecV2(&ProxyCodec{}))
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "ServerStream",
				ClientStreams: true,
				ServerStreams: true,
				Handler: func(_ any, stream grpc.ServerStream) error {
					if err := stream.RecvMsg(&Frame{}); err != nil {
						return err
					}
					for i := 0; i < msgCount; i++ {
						if err := stream.SendMsg(frameFromBytes(buildGRPCMessage([]byte("tick")))); err != nil {
							return err
						}
					}
					return nil
				},
			},
		},
	}, nil)
	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })
	return lis.Addr().String()
}

// startHeaderOnlyStreamBackend starts a backend that reads one request message,
// sends response headers and completes without any response message.
func startHeaderOnlyStreamBackend(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.ForceServerCodecV2(&ProxyCodec{}))
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "ServerStream",
				ClientStreams: true,
				ServerStreams: true,
				Handler: func(_ any, stream grpc.ServerStream) error {
					if err := stream.RecvMsg(&Frame{}); err != nil {
						return err
					}
					return stream.SendHeader(metadata.Pairs("x-header-only", "true"))
				},
			},
		},
	}, nil)
	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })
	return lis.Addr().String()
}

// startInfiniteStreamBackend starts a backend that streams responses until the
// stream breaks.
func startInfiniteStreamBackend(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.ForceServerCodecV2(&ProxyCodec{}))
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "ServerStream",
				ClientStreams: true,
				ServerStreams: true,
				Handler: func(_ any, stream grpc.ServerStream) error {
					if err := stream.RecvMsg(&Frame{}); err != nil {
						return err
					}
					for {
						if err := stream.SendMsg(frameFromBytes(buildGRPCMessage([]byte("tick")))); err != nil {
							return err
						}
						select {
						case <-stream.Context().Done():
							return stream.Context().Err()
						case <-time.After(5 * time.Millisecond):
						}
					}
				},
			},
		},
	}, nil)
	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })
	return lis.Addr().String()
}

// startFailAfterOneBidiBackend starts a bidi backend that echoes one message
// and then fails, while the client is still sending.
func startFailAfterOneBidiBackend(t *testing.T, code codes.Code, msg string) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.ForceServerCodecV2(&ProxyCodec{}))
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "Bidi",
				ClientStreams: true,
				ServerStreams: true,
				Handler: func(_ any, stream grpc.ServerStream) error {
					in := &Frame{}
					if err := stream.RecvMsg(in); err != nil {
						if err == io.EOF {
							return nil
						}
						return err
					}
					if err := stream.SendMsg(frameFromBytes(buildGRPCMessage(extractPayload(in.Data())))); err != nil {
						return err
					}
					return status.Error(code, msg)
				},
			},
		},
	}, nil)
	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })
	return lis.Addr().String()
}
