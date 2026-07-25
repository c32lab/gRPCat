// Package proxy implements a transparent gRPC proxy with plugin support
package proxy

import (
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// ConnectionCache maintains a pool of reusable gRPC connections to backend servers.
// It's safe for concurrent use by multiple goroutines.
type ConnectionCache struct {
	mu             sync.RWMutex
	conns          map[string]*cachedConn
	keepalive      *keepalive.ClientParameters
	transportCreds credentials.TransportCredentials
	dialOpts       []grpc.DialOption
	// maxRecvMsgSize / maxSendMsgSize bound backend call message sizes.
	// Zero leaves gRPC's own defaults in place; NewServer sets them from
	// Config.MaxRecvMsgSize / Config.MaxSendMsgSize.
	maxRecvMsgSize int
	maxSendMsgSize int
	// idleTimeout evicts connections that have gone unused for this long.
	// Zero disables eviction, keeping the cache grow-only; NewServer sets it
	// from Config.BackendIdleTimeout and then calls startSweeper.
	idleTimeout time.Duration
	stopSweeper chan struct{}
	stopOnce    sync.Once
}

// cachedConn pairs a pooled connection with the last time it was handed out.
type cachedConn struct {
	conn *grpc.ClientConn
	// lastUsed is a Unix nanosecond timestamp written on every Get. It is
	// atomic so Get's read-locked fast path can refresh it.
	lastUsed atomic.Int64
	// inFlight counts the streams the proxy currently has open on conn:
	// acquire increments it, the returned release decrements it. sweepIdle
	// never evicts an entry while it is non-zero, because ClientConn.Close
	// cancels in-flight RPCs.
	inFlight atomic.Int64
}

// NewConnectionCache creates an empty connection cache.
// If ka is nil, no client keepalive parameters are applied (gRPC defaults).
// If creds is nil, insecure credentials are used. dialOpts are appended
// after credentials and keepalive (interceptors, stats handlers, etc.).
func NewConnectionCache(ka *keepalive.ClientParameters, creds credentials.TransportCredentials, dialOpts []grpc.DialOption) *ConnectionCache {
	return &ConnectionCache{
		conns:          make(map[string]*cachedConn),
		keepalive:      ka,
		transportCreds: creds,
		dialOpts:       dialOpts,
	}
}

// Get retrieves or creates a connection to the backend server.
// Uses double-check locking for thread-safe lazy initialization.
//
// Get marks the connection as used, which defers idle eviction by one idle
// timeout. It does not pin the connection for the lifetime of a stream: a
// caller that starts a long-lived stream on the returned connection and then
// stops calling Get can see it evicted - and the stream cancelled - once the
// idle timeout elapses. The proxy's own forwarding path uses acquire instead,
// which pins the connection until the stream ends.
func (c *ConnectionCache) Get(backend string) (*grpc.ClientConn, error) {
	entry, err := c.getEntry(backend)
	if err != nil {
		return nil, err
	}
	return entry.conn, nil
}

// acquire returns a connection to backend plus a release function the caller
// must invoke exactly once when it is done with it. sweepIdle does not evict
// the entry in between, so a stream started on the returned connection cannot
// be cancelled by idle eviction however long it runs.
func (c *ConnectionCache) acquire(backend string) (*grpc.ClientConn, func(), error) {
	entry, err := c.getEntry(backend)
	if err != nil {
		return nil, nil, err
	}
	// getEntry already stamped lastUsed under c.mu, so a sweep landing before
	// this increment still sees a connection used less than idleTimeout ago.
	entry.inFlight.Add(1)
	return entry.conn, func() {
		// Stamp before decrementing: sweepIdle reads inFlight before lastUsed,
		// so a sweep that observes zero here also observes this timestamp and
		// gives the connection a full idle timeout before evicting it.
		entry.lastUsed.Store(time.Now().UnixNano())
		entry.inFlight.Add(-1)
	}, nil
}

// getEntry retrieves or creates the cache entry for backend, marking it used.
func (c *ConnectionCache) getEntry(backend string) (*cachedConn, error) {
	// Fast path: check if connection exists (read lock)
	c.mu.RLock()
	entry, exists := c.conns[backend]
	if exists {
		entry.lastUsed.Store(time.Now().UnixNano())
	}
	c.mu.RUnlock()

	if exists {
		return entry, nil
	}

	// Slow path: create new connection (write lock)
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check: another goroutine might have created it while we waited for the lock
	if entry, exists := c.conns[backend]; exists {
		entry.lastUsed.Store(time.Now().UnixNano())
		return entry, nil
	}

	callOpts := []grpc.CallOption{grpc.ForceCodecV2(&ProxyCodec{})}
	if c.maxRecvMsgSize > 0 {
		callOpts = append(callOpts, grpc.MaxCallRecvMsgSize(c.maxRecvMsgSize))
	}
	if c.maxSendMsgSize > 0 {
		callOpts = append(callOpts, grpc.MaxCallSendMsgSize(c.maxSendMsgSize))
	}

	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(callOpts...),
	}
	if c.transportCreds != nil {
		opts = append(opts, grpc.WithTransportCredentials(c.transportCreds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if c.keepalive != nil {
		opts = append(opts, grpc.WithKeepaliveParams(*c.keepalive))
	}
	if c.idleTimeout > 0 {
		// Make gRPC drop the transport after the same idle period and report
		// connectivity.Idle, which sweepIdle requires before evicting.
		opts = append(opts, grpc.WithIdleTimeout(c.idleTimeout))
	}
	opts = append(opts, c.dialOpts...)

	conn, err := grpc.NewClient(backend, opts...)
	if err != nil {
		return nil, err
	}

	created := &cachedConn{conn: conn}
	created.lastUsed.Store(time.Now().UnixNano())
	c.conns[backend] = created
	return created, nil
}

// startSweeper launches the goroutine that evicts idle connections. It is a
// no-op when idleTimeout is zero, which leaves the cache grow-only. NewServer
// calls it after setting idleTimeout; the goroutine exits on Close.
func (c *ConnectionCache) startSweeper() {
	if c.idleTimeout <= 0 {
		return
	}

	c.stopSweeper = make(chan struct{})
	go func() {
		ticker := time.NewTicker(c.idleTimeout)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.sweepIdle(time.Now())
			case <-c.stopSweeper:
				return
			}
		}
	}()
}

// sweepIdle closes and drops every connection that has no stream the proxy is
// forwarding on, that has not been handed out for idleTimeout, and that gRPC
// reports as idle.
//
// The inFlight check is what makes eviction safe: ClientConn.Close cancels
// in-flight RPCs, and "nobody called Get recently" does not mean "no streams
// are running" - a single Get can start a stream that lives for hours.
//
// connectivity.Idle alone is NOT a sufficient in-flight signal, despite gRPC's
// own idleness manager refusing to enter idle mode while activeCallsCount > 0.
// When a backend sends GOAWAY (keepalive.ServerParameters.MaxConnectionAge, or
// a GracefulStop during a rolling deploy) grpc-go deliberately keeps streams
// below the GOAWAY's LastStreamID running on the draining transport, yet
// addrConn's onClose publishes connectivity.Idle anyway ("Always go idle and
// wait for the LB policy to initiate a new connection attempt", clientconn.go)
// and pickfirst forwards that to the ClientConn. The state check is kept only
// as a cheap extra filter that also covers streams started by external callers
// of the exported Get, which the proxy cannot count.
func (c *ConnectionCache) sweepIdle(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for backend, entry := range c.conns {
		// Read inFlight before lastUsed: release stamps lastUsed and only
		// then decrements, so seeing zero here guarantees a fresh timestamp.
		if entry.inFlight.Load() > 0 {
			continue
		}
		if now.Sub(time.Unix(0, entry.lastUsed.Load())) < c.idleTimeout {
			continue
		}
		if entry.conn.GetState() != connectivity.Idle {
			continue
		}
		entry.conn.Close()
		delete(c.conns, backend)
	}
}

// Close closes all cached connections and clears the cache.
// Should be called when shutting down the proxy server.
func (c *ConnectionCache) Close() {
	c.stopOnce.Do(func() {
		if c.stopSweeper != nil {
			close(c.stopSweeper)
		}
	})

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, entry := range c.conns {
		entry.conn.Close()
	}
	c.conns = make(map[string]*cachedConn)
}

// Remove closes and removes a specific backend connection from the cache.
// Useful when a backend server is no longer available.
func (c *ConnectionCache) Remove(backend string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, exists := c.conns[backend]; exists {
		entry.conn.Close()
		delete(c.conns, backend)
	}
}
