// Package proxy implements a transparent gRPC proxy with plugin support
package proxy

import (
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// ConnectionCache maintains a pool of reusable gRPC connections to backend servers.
// It's safe for concurrent use by multiple goroutines.
type ConnectionCache struct {
	mu             sync.RWMutex
	conns          map[string]*grpc.ClientConn
	keepalive      *keepalive.ClientParameters
	transportCreds credentials.TransportCredentials
	dialOpts       []grpc.DialOption
	// maxRecvMsgSize / maxSendMsgSize bound backend call message sizes.
	// Zero leaves gRPC's own defaults in place; NewServer sets them from
	// Config.MaxRecvMsgSize / Config.MaxSendMsgSize.
	maxRecvMsgSize int
	maxSendMsgSize int
}

// NewConnectionCache creates an empty connection cache.
// If ka is nil, no client keepalive parameters are applied (gRPC defaults).
// If creds is nil, insecure credentials are used. dialOpts are appended
// after credentials and keepalive (interceptors, stats handlers, etc.).
func NewConnectionCache(ka *keepalive.ClientParameters, creds credentials.TransportCredentials, dialOpts []grpc.DialOption) *ConnectionCache {
	return &ConnectionCache{
		conns:          make(map[string]*grpc.ClientConn),
		keepalive:      ka,
		transportCreds: creds,
		dialOpts:       dialOpts,
	}
}

// Get retrieves or creates a connection to the backend server.
// Uses double-check locking for thread-safe lazy initialization.
func (c *ConnectionCache) Get(backend string) (*grpc.ClientConn, error) {
	// Fast path: check if connection exists (read lock)
	c.mu.RLock()
	conn, exists := c.conns[backend]
	c.mu.RUnlock()

	if exists {
		return conn, nil
	}

	// Slow path: create new connection (write lock)
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check: another goroutine might have created it while we waited for the lock
	if conn, exists := c.conns[backend]; exists {
		return conn, nil
	}

	callOpts := []grpc.CallOption{grpc.ForceCodec(&ProxyCodec{})}
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
	opts = append(opts, c.dialOpts...)

	conn, err := grpc.NewClient(backend, opts...)
	if err != nil {
		return nil, err
	}

	c.conns[backend] = conn
	return conn, nil
}

// Close closes all cached connections and clears the cache.
// Should be called when shutting down the proxy server.
func (c *ConnectionCache) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, conn := range c.conns {
		conn.Close()
	}
	c.conns = make(map[string]*grpc.ClientConn)
}

// Remove closes and removes a specific backend connection from the cache.
// Useful when a backend server is no longer available.
func (c *ConnectionCache) Remove(backend string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, exists := c.conns[backend]; exists {
		conn.Close()
		delete(c.conns, backend)
	}
}
