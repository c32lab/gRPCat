package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// idleTestTimeout is the Config.BackendIdleTimeout used by these tests. It is
// short so a sweep happens quickly; every assertion polls rather than assuming
// a sweep landed within a fixed window.
const idleTestTimeout = 100 * time.Millisecond

// TestConnectionCache_SweepsConnectionIdlePastTTL verifies that a connection
// that has served an RPC and then gone quiet past the idle timeout is closed
// and dropped from the cache.
func TestConnectionCache_SweepsConnectionIdlePastTTL(t *testing.T) {
	backend := startIdleEchoBackend(t)
	cache := newIdleTestCache(t, idleTestTimeout)

	conn, err := cache.Get(backend)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	finishEchoStream(t, openEchoStream(t, ctx, conn), "warm")

	// The RPC left the connection connected: eviction now depends on gRPC's
	// idle machinery moving it back to IDLE, not on a never-dialed connection
	// sitting in IDLE from birth.
	if got := conn.GetState(); got != connectivity.Ready {
		t.Fatalf("state after RPC: got %v, want %v", got, connectivity.Ready)
	}

	if !waitUntil(10*time.Second, func() bool { return !cacheHas(cache, backend) }) {
		t.Fatal("connection idle past the TTL was not evicted from the cache")
	}
	if got := conn.GetState(); got != connectivity.Shutdown {
		t.Errorf("evicted connection state: got %v, want %v (Close was not called)", got, connectivity.Shutdown)
	}
}

// TestConnectionCache_KeepsConnectionWithInFlightStream verifies the whole
// point of the eviction guard: ClientConn.Close cancels in-flight RPCs, so a
// connection carrying a live stream must survive the sweep even though nothing
// has called Get on it for well over the idle timeout.
//
// A second, unused backend acts as a canary: once it has been evicted, the
// sweeper has demonstrably run.
func TestConnectionCache_KeepsConnectionWithInFlightStream(t *testing.T) {
	backend := startIdleEchoBackend(t)
	canary := startIdleEchoBackend(t)
	cache := newIdleTestCache(t, idleTestTimeout)

	conn, err := cache.Get(backend)
	if err != nil {
		t.Fatalf("Get(backend): %v", err)
	}
	canaryConn, err := cache.Get(canary)
	if err != nil {
		t.Fatalf("Get(canary): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Canary: one complete RPC, then nothing.
	finishEchoStream(t, openEchoStream(t, ctx, canaryConn), "canary")

	// Subject: a stream left open and quiet for the rest of the test.
	stream := openEchoStream(t, ctx, conn)
	if err := echoRoundTrip(stream, "before-sweep"); err != nil {
		t.Fatalf("echo before sweep: %v", err)
	}

	if !waitUntil(10*time.Second, func() bool { return !cacheHas(cache, canary) }) {
		t.Fatal("sweeper never evicted the idle canary connection; test proves nothing")
	}

	if !cacheHas(cache, backend) {
		t.Fatal("connection with an in-flight stream was evicted from the cache")
	}
	if got := conn.GetState(); got == connectivity.Shutdown {
		t.Fatal("connection with an in-flight stream was closed")
	}
	if err := echoRoundTrip(stream, "after-sweep"); err != nil {
		t.Fatalf("in-flight stream broke across the sweep: %v", err)
	}
}

// TestConnectionCache_ReDialsAfterSweep verifies that an address whose
// connection was swept is usable again: Get returns a fresh connection, not
// the closed one.
func TestConnectionCache_ReDialsAfterSweep(t *testing.T) {
	backend := startIdleEchoBackend(t)
	cache := newIdleTestCache(t, idleTestTimeout)

	first, err := cache.Get(backend)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	finishEchoStream(t, openEchoStream(t, ctx, first), "first")

	if !waitUntil(10*time.Second, func() bool { return !cacheHas(cache, backend) }) {
		t.Fatal("connection idle past the TTL was not evicted from the cache")
	}

	second, err := cache.Get(backend)
	if err != nil {
		t.Fatalf("Get after sweep: %v", err)
	}
	if second == first {
		t.Fatal("Get returned the swept (closed) connection")
	}
	finishEchoStream(t, openEchoStream(t, ctx, second), "second")
}

// TestConnectionCache_ZeroIdleTimeoutNeverSweeps verifies that the zero value
// keeps the cache's original behavior: no sweeper, nothing ever evicted.
func TestConnectionCache_ZeroIdleTimeoutNeverSweeps(t *testing.T) {
	backend := startIdleEchoBackend(t)

	cache := NewConnectionCache(nil, nil, nil)
	cache.startSweeper()
	t.Cleanup(cache.Close)

	conn, err := cache.Get(backend)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	finishEchoStream(t, openEchoStream(t, ctx, conn), "warm")

	// Long enough that a sweeper running at idleTestTimeout would have fired
	// many times.
	time.Sleep(10 * idleTestTimeout)

	if !cacheHas(cache, backend) {
		t.Fatal("connection evicted although the idle timeout is unset")
	}
	if got := conn.GetState(); got == connectivity.Shutdown {
		t.Fatal("connection closed although the idle timeout is unset")
	}
}

// TestServer_StopStopsIdleSweeper verifies the sweeper goroutine's lifecycle:
// NewServer starts it when BackendIdleTimeout is set, and Stop ends it.
func TestServer_StopStopsIdleSweeper(t *testing.T) {
	// Let goroutines from earlier tests wind down before taking a baseline.
	time.Sleep(200 * time.Millisecond)
	before := runtime.NumGoroutine()

	srv, err := NewServer(&Config{
		DefaultBackend:     "127.0.0.1:1",
		BackendIdleTimeout: idleTestTimeout,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	if !waitUntil(5*time.Second, func() bool { return runtime.NumGoroutine() > before }) {
		t.Fatalf("NewServer did not start a sweeper goroutine (count still %d)", before)
	}

	srv.Stop()

	if !waitUntil(10*time.Second, func() bool { return runtime.NumGoroutine() <= before }) {
		t.Fatalf("goroutine leak after Stop: %d before, %d after", before, runtime.NumGoroutine())
	}
}

// TestConnectionCache_GetRefreshesIdleDeadline verifies that Get marks a
// connection as used. A connection that is handed out repeatedly but has not
// carried an RPC yet sits in connectivity.Idle from birth, so only the
// last-use timestamp keeps the sweeper from closing it out from under the
// caller that just took it.
func TestConnectionCache_GetRefreshesIdleDeadline(t *testing.T) {
	backend := startIdleEchoBackend(t)
	cache := newIdleTestCache(t, idleTestTimeout)

	first, err := cache.Get(backend)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := first.GetState(); got != connectivity.Idle {
		t.Fatalf("state of a freshly dialed connection: got %v, want %v", got, connectivity.Idle)
	}

	// Keep taking the connection for several idle timeouts. It must stay the
	// same connection throughout.
	for deadline := time.Now().Add(5 * idleTestTimeout); time.Now().Before(deadline); {
		again, err := cache.Get(backend)
		if err != nil {
			t.Fatalf("Get while refreshing: %v", err)
		}
		if again != first {
			t.Fatal("connection was swept while Get kept being called on it")
		}
		time.Sleep(idleTestTimeout / 5)
	}

	// Stop calling Get: now it must be swept, which also proves the sweeper
	// was running for the whole loop above.
	if !waitUntil(10*time.Second, func() bool { return !cacheHas(cache, backend) }) {
		t.Fatal("connection was not evicted once Get stopped being called")
	}
}

// newIdleTestCache builds a cache with idle eviction enabled, the way
// NewServer does it.
func newIdleTestCache(t *testing.T, ttl time.Duration) *ConnectionCache {
	t.Helper()
	cache := NewConnectionCache(nil, nil, nil)
	cache.idleTimeout = ttl
	cache.startSweeper()
	t.Cleanup(cache.Close)
	return cache
}

// cacheHas reports whether the cache still holds an entry for backend.
func cacheHas(c *ConnectionCache, backend string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.conns[backend]
	return ok
}

// waitUntil polls cond until it is true or the timeout expires.
func waitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// openEchoStream starts a bidi stream to the echo backend on conn.
func openEchoStream(t *testing.T, ctx context.Context, conn *grpc.ClientConn) grpc.ClientStream {
	t.Helper()
	stream, err := grpc.NewClientStream(ctx, clientStreamDesc, conn, "/test.IdleEcho/Bidi")
	if err != nil {
		t.Fatalf("NewClientStream: %v", err)
	}
	return stream
}

// echoRoundTrip sends one payload and checks that it comes back unchanged.
func echoRoundTrip(stream grpc.ClientStream, payload string) error {
	if err := stream.SendMsg(&Frame{data: []byte(payload)}); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	out := &Frame{}
	if err := stream.RecvMsg(out); err != nil {
		return fmt.Errorf("recv: %w", err)
	}
	if string(out.data) != payload {
		return fmt.Errorf("echo: got %q, want %q", out.data, payload)
	}
	return nil
}

// finishEchoStream runs one round trip and then closes the stream, so gRPC's
// active-RPC count for the connection drops back to zero.
func finishEchoStream(t *testing.T, stream grpc.ClientStream, payload string) {
	t.Helper()
	if err := echoRoundTrip(stream, payload); err != nil {
		t.Fatalf("echo round trip: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if err := stream.RecvMsg(&Frame{}); !errors.Is(err, io.EOF) {
		t.Fatalf("stream end: got %v, want io.EOF", err)
	}
}

// startIdleEchoBackend starts a ProxyCodec bidi backend that echoes every
// frame it receives until the client closes its send side. opts are extra
// server options (keepalive parameters, etc.).
func startIdleEchoBackend(t *testing.T, opts ...grpc.ServerOption) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(append([]grpc.ServerOption{grpc.ForceServerCodec(&ProxyCodec{})}, opts...)...)
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.IdleEcho",
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "Bidi",
				ClientStreams: true,
				ServerStreams: true,
				Handler: func(_ any, stream grpc.ServerStream) error {
					for {
						in := &Frame{}
						if err := stream.RecvMsg(in); err != nil {
							if errors.Is(err, io.EOF) {
								return nil
							}
							return err
						}
						echo := make([]byte, len(in.data))
						copy(echo, in.data)
						if err := stream.SendMsg(&Frame{data: echo}); err != nil {
							return err
						}
					}
				},
			},
		},
	}, nil)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// goAwayTestAge is the backend MaxConnectionAge used by the GOAWAY tests. It
// is longer than idleTestTimeout so the stream is already running - and the
// sweeper already ticking - when the GOAWAY lands.
const goAwayTestAge = 300 * time.Millisecond

// goAwayServerOption makes a backend drop its transport with a GOAWAY shortly
// after a client connects, the way keepalive.ServerParameters.MaxConnectionAge
// or a GracefulStop during a rolling deploy does in production. The long grace
// keeps the already-open streams draining rather than killing them.
func goAwayServerOption() grpc.ServerOption {
	return grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionAge:      goAwayTestAge,
		MaxConnectionAgeGrace: time.Hour,
	})
}

// TestConnectionCache_KeepsAcquiredConnectionAfterBackendGoAway pins the case
// that connectivity.Idle alone gets wrong.
//
// When a backend sends GOAWAY, grpc-go deliberately keeps the streams below
// its LastStreamID running on the draining transport, but addrConn's onClose
// still publishes connectivity.Idle ("Always go idle and wait for the LB
// policy to initiate a new connection attempt") and pickfirst forwards that to
// the ClientConn. So a connection can report Idle while a stream is happily
// round-tripping on it, and a sweeper that treats Idle as "no in-flight
// streams" closes the connection and kills the stream. Only the acquire /
// release count on the entry can tell the two apart.
func TestConnectionCache_KeepsAcquiredConnectionAfterBackendGoAway(t *testing.T) {
	backend := startIdleEchoBackend(t, goAwayServerOption())
	canary := startIdleEchoBackend(t)
	cache := newIdleTestCache(t, idleTestTimeout)

	conn, release, err := cache.acquire(backend)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream := openEchoStream(t, ctx, conn)
	if err := echoRoundTrip(stream, "before-goaway"); err != nil {
		t.Fatalf("echo before GOAWAY: %v", err)
	}

	// Wait for the backend's GOAWAY to move the connection to Idle while the
	// stream is still live. Without this the test would prove nothing.
	if !waitUntil(30*time.Second, func() bool { return conn.GetState() == connectivity.Idle }) {
		t.Fatalf("backend GOAWAY never moved the connection to Idle (state %v)", conn.GetState())
	}

	// Only now start the canary, so that its eviction proves a sweep ran
	// while the subject was sitting in Idle with a live stream.
	canaryConn, err := cache.Get(canary)
	if err != nil {
		t.Fatalf("Get(canary): %v", err)
	}
	finishEchoStream(t, openEchoStream(t, ctx, canaryConn), "canary")
	if !waitUntil(30*time.Second, func() bool { return !cacheHas(cache, canary) }) {
		t.Fatal("sweeper never evicted the idle canary connection; test proves nothing")
	}

	if !cacheHas(cache, backend) {
		t.Fatal("connection with an in-flight stream was evicted after the backend sent GOAWAY")
	}
	if got := conn.GetState(); got == connectivity.Shutdown {
		t.Fatal("connection with an in-flight stream was closed after the backend sent GOAWAY")
	}
	if err := echoRoundTrip(stream, "after-sweep"); err != nil {
		t.Fatalf("in-flight stream broken by the idle sweep after the backend sent GOAWAY: %v", err)
	}
}

// TestServer_ProxiedStreamSurvivesIdleSweepAfterBackendGoAway is the
// end-to-end counterpart: it goes through NewServer and Forward, so it also
// pins that the forwarding path acquires the connection (which pins it against
// eviction) rather than merely Getting it.
func TestServer_ProxiedStreamSurvivesIdleSweepAfterBackendGoAway(t *testing.T) {
	backend := startIdleEchoBackend(t, goAwayServerOption())

	srv, err := NewServer(&Config{
		DefaultBackend:     backend,
		BackendIdleTimeout: idleTestTimeout,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.grpcServer.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := conn.NewStream(ctx,
		&grpc.StreamDesc{ClientStreams: true, ServerStreams: true},
		"/test.IdleEcho/Bidi",
	)
	if err != nil {
		t.Fatalf("new stream: %v", err)
	}
	if err := proxiedEchoRoundTrip(stream, "before-goaway"); err != nil {
		t.Fatalf("echo before GOAWAY: %v", err)
	}

	// Hold the stream open and quiet across the backend's GOAWAY and several
	// sweeper ticks - the long-lived-stream case BackendIdleTimeout promises
	// not to break.
	time.Sleep(goAwayTestAge + 5*idleTestTimeout)

	if err := proxiedEchoRoundTrip(stream, "after-sweep"); err != nil {
		t.Fatalf("long-lived proxied stream broken by the backend idle sweep: %v", err)
	}
}

// proxiedEchoRoundTrip is echoRoundTrip for a stream that runs through the
// proxy, where payloads travel as complete gRPC message frames.
func proxiedEchoRoundTrip(stream grpc.ClientStream, payload string) error {
	if err := stream.SendMsg(&Frame{data: buildGRPCMessage([]byte(payload))}); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	out := &Frame{}
	if err := stream.RecvMsg(out); err != nil {
		return fmt.Errorf("recv: %w", err)
	}
	if got := string(extractPayload(out.data)); got != payload {
		return fmt.Errorf("echo: got %q, want %q", got, payload)
	}
	return nil
}

// TestConnectionCache_ZeroIdleTimeoutNeverSweepsFreshConn is the counterpart
// of TestConnectionCache_ZeroIdleTimeoutNeverSweeps that actually pins
// startSweeper's zero-timeout guard. The warmed case above leaves the
// connection in connectivity.Ready, where sweepIdle's state check spares it
// whether or not a sweeper is running; a connection that has never carried an
// RPC is connectivity.Idle from birth, and with idleTimeout zero the
// "unused for at least idleTimeout" test is trivially true, so a sweeper that
// ran at all would evict it on its first tick.
func TestConnectionCache_ZeroIdleTimeoutNeverSweepsFreshConn(t *testing.T) {
	backend := startIdleEchoBackend(t)

	cache := NewConnectionCache(nil, nil, nil)
	cache.startSweeper()
	t.Cleanup(cache.Close)

	conn, err := cache.Get(backend)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := conn.GetState(); got != connectivity.Idle {
		t.Fatalf("state of a freshly dialed connection: got %v, want %v", got, connectivity.Idle)
	}

	// Long enough that a sweeper running at idleTestTimeout would have fired
	// many times.
	time.Sleep(10 * idleTestTimeout)

	if !cacheHas(cache, backend) {
		t.Fatal("fresh connection evicted although the idle timeout is unset")
	}
	if got := conn.GetState(); got == connectivity.Shutdown {
		t.Fatal("fresh connection closed although the idle timeout is unset")
	}
}

// TestConnectionCache_FreshlyDialedConnSurvivesItsFirstTTL pins the last-use
// stamp on the connection-creating path of Get. Without it the entry's
// timestamp is the zero value, i.e. the Unix epoch, so it reads as idle
// forever - and a freshly dialed connection is connectivity.Idle from birth,
// so both sweep conditions hold on the very first tick after creation. In
// Forward that is the window between taking the connection and starting the
// stream on it, where a real request would fail with "client connection is
// closing".
func TestConnectionCache_FreshlyDialedConnSurvivesItsFirstTTL(t *testing.T) {
	backend := startIdleEchoBackend(t)
	cache := newIdleTestCache(t, idleTestTimeout)

	// Offset from the sweeper's tick phase so the first tick after Get lands
	// well inside the idle timeout that Get just restarted.
	time.Sleep(idleTestTimeout * 6 / 10)

	// Taken before Get so the measured age can only over-estimate how long
	// the connection had been cached when it was evicted.
	before := time.Now()
	conn, err := cache.Get(backend)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := conn.GetState(); got != connectivity.Idle {
		t.Fatalf("state of a freshly dialed connection: got %v, want %v", got, connectivity.Idle)
	}

	// The connection is never used again, so eviction is expected - just not
	// before it has been cached for a full idle timeout.
	if !waitUntil(30*time.Second, func() bool { return !cacheHas(cache, backend) }) {
		t.Fatal("connection was never evicted; the sweeper is not running")
	}
	if age := time.Since(before); age < idleTestTimeout {
		t.Fatalf("connection evicted %v after the Get that created it, inside its %v idle timeout", age, idleTestTimeout)
	}
}
