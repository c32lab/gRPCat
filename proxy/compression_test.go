package proxy

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/encoding/gzip" // registers "gzip" for this test binary
	"google.golang.org/grpc/stats"
)

// payloadRecorder captures message-level stats so a test can tell whether a
// payload travelled compressed on a given leg: CompressedLength equals Length
// when the message was not compressed.
type payloadRecorder struct {
	mu  sync.Mutex
	in  []*stats.InPayload
	out []*stats.OutPayload
}

func (r *payloadRecorder) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (r *payloadRecorder) HandleRPC(_ context.Context, s stats.RPCStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch v := s.(type) {
	case *stats.InPayload:
		r.in = append(r.in, v)
	case *stats.OutPayload:
		r.out = append(r.out, v)
	}
}

func (r *payloadRecorder) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (r *payloadRecorder) HandleConn(context.Context, stats.ConnStats) {}

// firstIn waits for the first received payload to be recorded.
func (r *payloadRecorder) firstIn(t *testing.T) *stats.InPayload {
	t.Helper()
	return waitFor(t, func() *stats.InPayload {
		r.mu.Lock()
		defer r.mu.Unlock()
		if len(r.in) == 0 {
			return nil
		}
		return r.in[0]
	}, "in payload")
}

// firstOut waits for the first sent payload to be recorded.
func (r *payloadRecorder) firstOut(t *testing.T) *stats.OutPayload {
	t.Helper()
	return waitFor(t, func() *stats.OutPayload {
		r.mu.Lock()
		defer r.mu.Unlock()
		if len(r.out) == 0 {
			return nil
		}
		return r.out[0]
	}, "out payload")
}

func waitFor[T any](t *testing.T, get func() *T, what string) *T {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if v := get(); v != nil {
			return v
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
	return nil
}

// startEchoBackendWithStats starts an echo backend reporting payload stats to
// rec. If sendCompressor is non-empty the handler asks gRPC to compress its
// response with that encoding.
func startEchoBackendWithStats(t *testing.T, rec *payloadRecorder, sendCompressor string) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer(grpc.ForceServerCodec(&ProxyCodec{}), grpc.StatsHandler(rec))
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Echo",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					frame := &Frame{}
					if err := dec(frame); err != nil {
						return nil, err
					}
					if sendCompressor != "" {
						if err := grpc.SetSendCompressor(ctx, sendCompressor); err != nil {
							return nil, err
						}
					}
					return frame, nil
				},
			},
		},
		Streams: []grpc.StreamDesc{},
	}, nil)

	go server.Serve(listener)
	t.Cleanup(func() { server.Stop() })

	return listener.Addr().String()
}

// echoThroughProxy makes one unary call through the proxy with a highly
// compressible payload and returns the client-side payload stats.
func echoThroughProxy(t *testing.T, proxyAddr string, useCompressor string) *payloadRecorder {
	t.Helper()

	rec := &payloadRecorder{}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(&ProxyCodec{})),
		grpc.WithStatsHandler(rec),
	}
	if useCompressor != "" {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor(useCompressor)))
	}

	conn, err := grpc.NewClient(proxyAddr, opts...)
	if err != nil {
		t.Fatalf("failed to dial proxy: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := []byte(strings.Repeat("a", 2000))
	reqFrame := &Frame{data: buildGRPCMessage(body)}
	respFrame := &Frame{}
	if err := conn.Invoke(ctx, "/test.Echo/Echo", reqFrame, respFrame); err != nil {
		t.Fatalf("invoke failed: %v", err)
	}
	if got := extractPayload(respFrame.data); string(got) != string(body) {
		t.Fatalf("response payload mismatch: got %d bytes, want %d", len(got), len(body))
	}
	return rec
}

// TestCompression_TerminatesAtProxy pins the documented behavior: gRPC
// decompresses a compressed request at the proxy, so the frame forwarded to the
// backend is NOT compressed. The proxy's server side then compresses the
// response back to the client, mirroring the client's request encoding.
func TestCompression_TerminatesAtProxy(t *testing.T) {
	backendRec := &payloadRecorder{}
	backendAddr := startEchoBackendWithStats(t, backendRec, "")
	proxyAddr := startProxyServer(t, backendAddr)

	clientRec := echoThroughProxy(t, proxyAddr, "gzip")

	// Client -> proxy leg: compressed, as requested.
	if sent := clientRec.firstOut(t); sent.CompressedLength >= sent.Length {
		t.Errorf("client request should be compressed: length=%d compressedLength=%d",
			sent.Length, sent.CompressedLength)
	}

	// Proxy -> backend leg: compression terminated at the proxy.
	got := backendRec.firstIn(t)
	if got.CompressedLength != got.Length {
		t.Errorf("backend should receive an uncompressed frame: length=%d compressedLength=%d",
			got.Length, got.CompressedLength)
	}

	// Proxy -> client leg: re-compressed by gRPC, mirroring the request encoding.
	if recv := clientRec.firstIn(t); recv.CompressedLength >= recv.Length {
		t.Errorf("client response should be compressed: length=%d compressedLength=%d",
			recv.Length, recv.CompressedLength)
	}
}

// TestCompression_BackendResponseEncodingNotPropagated pins the other half:
// when the backend picks a response encoding itself, the proxy decompresses it
// and the client - which did not request compression - receives it uncompressed.
func TestCompression_BackendResponseEncodingNotPropagated(t *testing.T) {
	backendRec := &payloadRecorder{}
	backendAddr := startEchoBackendWithStats(t, backendRec, "gzip")
	proxyAddr := startProxyServer(t, backendAddr)

	clientRec := echoThroughProxy(t, proxyAddr, "")

	// Backend -> proxy leg: compressed by the backend.
	sent := backendRec.firstOut(t)
	if sent.CompressedLength >= sent.Length {
		t.Fatalf("backend response should be compressed: length=%d compressedLength=%d",
			sent.Length, sent.CompressedLength)
	}

	// Proxy -> client leg: the backend's encoding does not carry through.
	recv := clientRec.firstIn(t)
	if recv.CompressedLength != recv.Length {
		t.Errorf("client should receive an uncompressed response: length=%d compressedLength=%d",
			recv.Length, recv.CompressedLength)
	}
}
