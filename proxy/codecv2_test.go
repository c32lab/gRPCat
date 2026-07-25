package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/experimental"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/stats"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Tests for the encoding.CodecV2 implementation: reference counting in the
// codec itself, the multi-buffer receive path, and end-to-end payload
// integrity under concurrency. See proxy/codec.go for the ownership rules
// these pin down.

// countingPool is a mem.BufferPool that records how many buffers were handed
// out and how many came back, which is how these tests observe a reference
// count reaching zero. With poison set it also scribbles over every buffer it
// takes back, so a slice that aliases a released buffer is caught instead of
// merely happening to still read correctly.
type countingPool struct {
	mu     sync.Mutex
	got    int
	put    int
	poison bool
}

func (p *countingPool) Get(length int) *[]byte {
	p.mu.Lock()
	p.got++
	p.mu.Unlock()
	b := make([]byte, length)
	return &b
}

func (p *countingPool) Put(b *[]byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.put++
	if p.poison {
		for i := range *b {
			(*b)[i] = 0xAA
		}
	}
}

func (p *countingPool) puts() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.put
}

// outstanding is the number of buffers handed out and not yet returned.
func (p *countingPool) outstanding() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.got - p.put
}

// pooledSlice builds a reference-counted, pool-backed BufferSlice holding
// data. The buffer must be over mem's pooling threshold, otherwise mem
// silently returns a non-counted SliceBuffer and the test would prove nothing.
func pooledSlice(t *testing.T, pool mem.BufferPool, data []byte) mem.BufferSlice {
	t.Helper()
	if mem.IsBelowBufferPoolingThreshold(len(data)) {
		t.Fatalf("test buffer of %d bytes is below the pooling threshold; refcounts would not be exercised", len(data))
	}
	buf := pool.Get(len(data))
	copy(*buf, data)
	return mem.BufferSlice{mem.NewBuffer(buf, pool)}
}

// TestProxyCodec_RefcountHandoff walks the exact sequence gRPC performs around
// a forwarded message and asserts the buffer survives every step and is
// released exactly once, at the point the Frame is freed.
//
// grpc-go's recv() frees the received slice as soon as Unmarshal returns
// (rpc_util.go) and SendMsg frees whatever Marshal returned once the write is
// queued (stream.go), so the codec must hold its own reference at both ends.
func TestProxyCodec_RefcountHandoff(t *testing.T) {
	pool := &countingPool{}
	payload := bulkPayload(7, 4096)

	codec := &ProxyCodec{}
	frame := &Frame{}

	// --- receive side: what recv() does ---
	received := pooledSlice(t, pool, payload)
	if err := codec.Unmarshal(received, frame); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	received.Free() // grpc's "defer data.Free()"

	if got := pool.puts(); got != 0 {
		t.Fatalf("buffer went back to the pool while the Frame still holds it (puts=%d)", got)
	}
	if !bytes.Equal(frame.Data(), payload) {
		t.Fatal("frame payload corrupted after grpc released its reference")
	}

	// --- send side: what SendMsg does ---
	marshalled, err := codec.Marshal(frame)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !bytes.Equal(marshalled.Materialize(), payload) {
		t.Fatal("marshalled payload does not match the frame")
	}

	// --- the owner is done with it, while the write is still in flight ---
	//
	// This is the assertion that pins the zero-copy handoff rather than only
	// the content: Marshal must return an extra reference to the *same*
	// buffers. If it returned a private copy instead, the Frame would hold
	// the only reference and freeing it here would return the buffer to the
	// pool - content comparisons would still pass, but the whole point of
	// CodecV2 (no copy on send) would be gone.
	frame.Free()
	if got := pool.puts(); got != 0 {
		t.Fatalf("Marshal did not hand off a reference to the frame's own buffers: "+
			"freeing the Frame returned the buffer to the pool while the send was still in flight (puts=%d)", got)
	}
	if !bytes.Equal(marshalled.Materialize(), payload) {
		t.Fatal("in-flight payload corrupted after the Frame was freed")
	}

	marshalled.Free() // grpc's "defer data.Free()" in SendMsg
	if got := pool.puts(); got != 1 {
		t.Fatalf("once both references are dropped the buffer should be back in the pool exactly once, got %d", got)
	}

	// Free is idempotent: a double free would return the buffer twice and
	// hand the same memory to two future messages.
	frame.Free()
	if got := pool.puts(); got != 1 {
		t.Fatalf("Frame.Free is not idempotent: pool puts %d", got)
	}
}

// TestProxyCodec_UnmarshalReleasesPreviousMessage pins the invariant that lets
// the forwarder reuse a single Frame for a whole stream: refilling a Frame
// releases the message it held, so reuse cannot leak pooled buffers.
func TestProxyCodec_UnmarshalReleasesPreviousMessage(t *testing.T) {
	pool := &countingPool{}
	codec := &ProxyCodec{}
	frame := &Frame{}

	first := pooledSlice(t, pool, bulkPayload(1, 2048))
	if err := codec.Unmarshal(first, frame); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	first.Free()

	second := pooledSlice(t, pool, bulkPayload(2, 2048))
	if err := codec.Unmarshal(second, frame); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	second.Free()

	if got := pool.puts(); got != 1 {
		t.Fatalf("refilling a Frame should release the previous message exactly once, pool puts %d", got)
	}
	if !bytes.Equal(frame.Data(), bulkPayload(2, 2048)) {
		t.Fatal("frame holds the wrong message after being refilled")
	}

	frame.Free()
	if got := pool.puts(); got != 2 {
		t.Fatalf("pool puts after final Free: want 2 got %d", got)
	}
}

// TestProxyCodec_UnmarshalWrongTypeKeepsFrame checks the error path does not
// steal a reference it cannot release.
func TestProxyCodec_UnmarshalWrongTypeKeepsFrame(t *testing.T) {
	pool := &countingPool{}
	codec := &ProxyCodec{}

	slice := pooledSlice(t, pool, bulkPayload(3, 2048))
	if err := codec.Unmarshal(slice, &wrapperspb.BytesValue{}); err == nil {
		t.Fatal("Unmarshal into a non-Frame should fail")
	}
	slice.Free()

	if got := pool.puts(); got != 1 {
		t.Fatalf("a failed Unmarshal must not retain the buffer, pool puts %d (want 1)", got)
	}
}

// TestProxyCodec_DataOutlivesSingleBufferFree pins the promise Frame.Data
// makes to middleware, and that RequestInfo.FirstPayload passes on: the bytes
// it returns are a private copy that stays valid after the frame's pooled
// buffers have gone back to gRPC.
//
// The single-buffer case is the one that tempts an implementation to alias
// ReadOnlyData instead of copying, and it is also the common one: any message
// between mem's 1KB pooling threshold and the 16KB HTTP/2 frame size arrives
// as exactly one pooled buffer. TestProxyCodec_MultiBufferReceive covers the
// >=2-buffer case, where a copy is unavoidable anyway.
func TestProxyCodec_DataOutlivesSingleBufferFree(t *testing.T) {
	pool := &countingPool{poison: true}
	payload := bulkPayload(13, 8192)

	frame := &Frame{}
	slice := pooledSlice(t, pool, payload)
	if err := (&ProxyCodec{}).Unmarshal(slice, frame); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	slice.Free() // grpc's "defer data.Free()"

	got := frame.Data()
	frame.Free() // the buffer goes back to the pool, which overwrites it

	if !bytes.Equal(got, payload) {
		t.Fatalf("Frame.Data aliased the frame's pooled buffer instead of copying it: "+
			"its bytes changed when the buffer was released (first difference at byte %d)",
			firstDiff(got, payload))
	}
}

// TestProxyCodec_MultiBufferReceive verifies the interesting receive path is
// actually reachable: a message well over gRPC's 16KB HTTP/2 frame size
// arrives as a BufferSlice made of several pooled buffers, and the codec keeps
// all of them.
//
// Without this the whole suite could pass while only ever seeing single-buffer
// messages, which are the easy case.
func TestProxyCodec_MultiBufferReceive(t *testing.T) {
	backendAddr := startBulkEchoBackend(t)

	// Talk to the backend directly with ProxyCodec, exactly as the proxy's
	// backend leg does.
	conn, err := grpc.NewClient(backendAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("dial backend: %v", err)
	}
	defer conn.Close()

	payload := bulkPayload(11, 256*1024)
	body, err := proto.Marshal(wrapperspb.Bytes(payload))
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp := &Frame{}
	defer resp.Free()
	if err := conn.Invoke(ctx, "/test.BulkEcho/Unary", frameFromBytes(body), resp); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	if n := len(resp.data); n < 2 {
		t.Errorf("a %d-byte message arrived in %d buffer(s); the multi-buffer path is no longer covered", len(body), n)
	}

	out := &wrapperspb.BytesValue{}
	if err := resp.ParseAs(out); err != nil {
		t.Fatalf("ParseAs: %v", err)
	}
	if !bytes.Equal(out.GetValue(), payload) {
		t.Errorf("multi-buffer payload mangled: %d bytes back, want %d", len(out.GetValue()), len(payload))
	}
	// Data must reassemble the same bytes and survive the frame being freed.
	got := resp.Data()
	resp.Free()
	if !bytes.Equal(got, body) {
		t.Error("Frame.Data did not reassemble the buffers, or did not outlive Free")
	}
}

// straddleSizes brackets the two boundaries that change how gRPC lays a
// message out in memory: mem's 1KB pooling threshold (below it buffers are
// plain slices with no refcount) and the 16KB HTTP/2 frame size (above it a
// message spans several buffers).
var straddleSizes = []int{0, 1, 1023, 1024, 1025, 16383, 16384, 16385, 32767, 32768, 32769, 65536, 131072}

// TestProxy_StreamingModes_PayloadIntegrity exercises all four streaming modes
// through the proxy with a stock protobuf client and a stock protobuf backend,
// at sizes that straddle both buffer boundaries, asserting every payload comes
// back byte for byte.
func TestProxy_StreamingModes_PayloadIntegrity(t *testing.T) {
	conn := startBulkProxy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	t.Run("unary", func(t *testing.T) {
		for _, size := range straddleSizes {
			want := bulkPayload(uint64(size), size)
			resp := &wrapperspb.BytesValue{}
			if err := conn.Invoke(ctx, "/test.BulkEcho/Unary", wrapperspb.Bytes(want), resp); err != nil {
				t.Fatalf("size %d: invoke: %v", size, err)
			}
			if !bytes.Equal(resp.GetValue(), want) {
				t.Fatalf("size %d: payload mismatch", size)
			}
		}
	})

	t.Run("server-stream", func(t *testing.T) {
		for _, size := range straddleSizes {
			want := bulkPayload(uint64(size)+1, size)
			stream, err := grpc.NewClientStream(ctx, bidiStreamDesc, conn, "/test.BulkEcho/ServerStream")
			if err != nil {
				t.Fatalf("size %d: open: %v", size, err)
			}
			if err := stream.SendMsg(wrapperspb.Bytes(want)); err != nil {
				t.Fatalf("size %d: send: %v", size, err)
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatalf("size %d: close send: %v", size, err)
			}
			for i := 0; i < serverStreamEchoCount; i++ {
				resp := &wrapperspb.BytesValue{}
				if err := stream.RecvMsg(resp); err != nil {
					t.Fatalf("size %d: recv %d: %v", size, i, err)
				}
				if !bytes.Equal(resp.GetValue(), want) {
					t.Fatalf("size %d: response %d payload mismatch", size, i)
				}
			}
			if err := stream.RecvMsg(&wrapperspb.BytesValue{}); !errorIsEOF(err) {
				t.Fatalf("size %d: want io.EOF after the last response, got %v", size, err)
			}
		}
	})

	t.Run("client-stream", func(t *testing.T) {
		stream, err := grpc.NewClientStream(ctx, bidiStreamDesc, conn, "/test.BulkEcho/ClientStream")
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		h := sha256.New()
		for _, size := range straddleSizes {
			p := bulkPayload(uint64(size)+2, size)
			h.Write(p)
			if err := stream.SendMsg(wrapperspb.Bytes(p)); err != nil {
				t.Fatalf("size %d: send: %v", size, err)
			}
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("close send: %v", err)
		}
		resp := &wrapperspb.BytesValue{}
		if err := stream.RecvMsg(resp); err != nil {
			t.Fatalf("recv digest: %v", err)
		}
		if !bytes.Equal(resp.GetValue(), h.Sum(nil)) {
			t.Fatal("backend digest differs: the client->backend direction did not deliver the exact bytes")
		}
	})

	t.Run("bidi-stream", func(t *testing.T) {
		stream, err := grpc.NewClientStream(ctx, bidiStreamDesc, conn, "/test.BulkEcho/BidiStream")
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		for _, size := range straddleSizes {
			want := bulkPayload(uint64(size)+3, size)
			if err := stream.SendMsg(wrapperspb.Bytes(want)); err != nil {
				t.Fatalf("size %d: send: %v", size, err)
			}
			resp := &wrapperspb.BytesValue{}
			if err := stream.RecvMsg(resp); err != nil {
				t.Fatalf("size %d: recv: %v", size, err)
			}
			if !bytes.Equal(resp.GetValue(), want) {
				t.Fatalf("size %d: payload mismatch", size)
			}
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("close send: %v", err)
		}
		if err := stream.RecvMsg(&wrapperspb.BytesValue{}); !errorIsEOF(err) {
			t.Fatalf("want io.EOF after CloseSend, got %v", err)
		}
	})
}

// TestProxy_ConcurrentSoak_PayloadIntegrity runs many concurrent streams with
// mixed message sizes and asserts every response matches its own request byte
// for byte.
//
// This is the test that catches a missing Ref: a buffer released while still
// in flight gets handed to another stream, which then reads - or writes - a
// payload belonging to somebody else. -race does not see that, because it is
// not a data race; only comparing content does.
func TestProxy_ConcurrentSoak_PayloadIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test skipped in -short mode")
	}

	const (
		streams          = 24
		messagesPerStrea = 16
	)
	// Mixed sizes, all of them large enough that a mix-up cannot be masked by
	// two payloads happening to be the same length.
	sizes := []int{512, 1024, 4096, 16 * 1024, 17 * 1024, 33 * 1024, 64 * 1024}

	conn := startBulkProxy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, streams)
	for s := 0; s < streams; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			stream, err := grpc.NewClientStream(ctx, bidiStreamDesc, conn, "/test.BulkEcho/BidiStream")
			if err != nil {
				errs <- fmt.Errorf("stream %d: open: %w", s, err)
				return
			}
			for m := 0; m < messagesPerStrea; m++ {
				size := sizes[(s+m)%len(sizes)]
				// Unique per (stream, message): another request's bytes can
				// never compare equal.
				want := bulkPayload(uint64(s)*1000+uint64(m)+1, size)
				if err := stream.SendMsg(wrapperspb.Bytes(want)); err != nil {
					errs <- fmt.Errorf("stream %d msg %d: send: %w", s, m, err)
					return
				}
				resp := &wrapperspb.BytesValue{}
				if err := stream.RecvMsg(resp); err != nil {
					errs <- fmt.Errorf("stream %d msg %d: recv: %w", s, m, err)
					return
				}
				got := resp.GetValue()
				if len(got) != len(want) {
					errs <- fmt.Errorf("stream %d msg %d: length %d, want %d", s, m, len(got), len(want))
					return
				}
				if !bytes.Equal(got, want) {
					errs <- fmt.Errorf("stream %d msg %d: payload corrupted at byte %d",
						s, m, firstDiff(got, want))
					return
				}
			}
			if err := stream.CloseSend(); err != nil {
				errs <- fmt.Errorf("stream %d: close send: %w", s, err)
				return
			}
			if err := stream.RecvMsg(&wrapperspb.BytesValue{}); !errorIsEOF(err) {
				errs <- fmt.Errorf("stream %d: want io.EOF, got %v", s, err)
			}
		}(s)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

func errorIsEOF(err error) bool {
	return errors.Is(err, io.EOF)
}

func firstDiff(a, b []byte) int {
	for i := range a {
		if i >= len(b) || a[i] != b[i] {
			return i
		}
	}
	return len(a)
}

// startPooledProxy starts a proxy whose both legs allocate their transport
// buffers from pool, and returns a stock protobuf client connected to it.
// Every buffer the proxy retains or releases is therefore counted.
func startPooledProxy(t *testing.T, pool *countingPool) *grpc.ClientConn {
	t.Helper()

	srv, err := NewServer(&Config{
		DefaultBackend:     startBulkEchoBackend(t),
		ServerOptions:      []grpc.ServerOption{experimental.BufferPool(pool)},
		BackendDialOptions: []grpc.DialOption{experimental.WithBufferPool(pool)},
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
	)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// TestProxy_ForwardingDoesNotLeakPooledBuffers checks the other half of the
// refcount contract: every buffer the proxy retains is eventually released.
// A missing Free is invisible to a correctness test - the data is right, the
// buffers simply never go back to gRPC's pool - so it is measured directly.
//
// Both proxy legs are given a counting pool, and the number of buffers still
// outstanding is compared after two identical batches of traffic. A per-message
// leak makes the second batch's figure grow by roughly one buffer per HTTP/2
// frame forwarded; a constant offset (buffers grpc holds for the connection
// itself, or handed out below mem's pooling threshold and therefore never
// returned) cancels out.
//
// All the traffic runs on ONE stream, so this measures per-message ownership
// only; TestProxy_UnaryRPCsDoNotLeakPooledBuffers covers per-stream ownership.
func TestProxy_ForwardingDoesNotLeakPooledBuffers(t *testing.T) {
	const (
		batch       = 40
		payloadSize = 64 * 1024
	)

	pool := &countingPool{}
	conn := startPooledProxy(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stream, err := grpc.NewClientStream(ctx, bidiStreamDesc, conn, "/test.BulkEcho/BidiStream")
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	runBatch := func() {
		for i := 0; i < batch; i++ {
			if err := stream.SendMsg(wrapperspb.Bytes(bulkPayload(uint64(i)+1, payloadSize))); err != nil {
				t.Fatalf("send: %v", err)
			}
			if err := stream.RecvMsg(&wrapperspb.BytesValue{}); err != nil {
				t.Fatalf("recv: %v", err)
			}
		}
	}

	runBatch()
	// The last response's buffers are released by the forwarder just after
	// RecvMsg returns to the client, so let both legs settle.
	time.Sleep(200 * time.Millisecond)
	afterFirst := pool.outstanding()

	runBatch()
	time.Sleep(200 * time.Millisecond)
	afterSecond := pool.outstanding()

	// Each 64KB message spans several pooled buffers per direction, so a
	// per-message leak would put the growth in the hundreds.
	if growth := afterSecond - afterFirst; growth > 8 {
		t.Errorf("outstanding pooled buffers grew by %d over an identical batch of %d messages "+
			"(%d -> %d): the forwarding path is not releasing what it retains",
			growth, batch, afterFirst, afterSecond)
	}
}

// TestProxy_UnaryRPCsDoNotLeakPooledBuffers measures per-STREAM ownership,
// which the per-message test above structurally cannot see.
//
// TransparentHandler reads the first message of every stream itself and owns
// that reference for the whole RPC (proxy/server.go); the forwarder never
// receives it. A missing Free there leaks once per stream, so a test that
// sends all its traffic down one long-lived stream contains exactly one first
// frame and its leak cannot grow between batches. Each unary RPC here is a
// whole stream - and is nothing but a first frame - so every iteration is one
// complete ownership cycle.
func TestProxy_UnaryRPCsDoNotLeakPooledBuffers(t *testing.T) {
	const (
		batch       = 40
		payloadSize = 64 * 1024
	)

	pool := &countingPool{}
	conn := startPooledProxy(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	runBatch := func() {
		for i := 0; i < batch; i++ {
			req := wrapperspb.Bytes(bulkPayload(uint64(i)+1, payloadSize))
			if err := conn.Invoke(ctx, "/test.BulkEcho/Unary", req, &wrapperspb.BytesValue{}); err != nil {
				t.Fatalf("invoke: %v", err)
			}
		}
	}

	runBatch()
	time.Sleep(200 * time.Millisecond)
	afterFirst := pool.outstanding()

	runBatch()
	time.Sleep(200 * time.Millisecond)
	afterSecond := pool.outstanding()

	// A leaked first frame costs several pooled buffers per RPC, so a
	// per-stream leak puts the growth in the hundreds.
	if growth := afterSecond - afterFirst; growth > 8 {
		t.Errorf("outstanding pooled buffers grew by %d over an identical batch of %d unary RPCs "+
			"(%d -> %d): a per-stream reference is not being released",
			growth, batch, afterFirst, afterSecond)
	}
}

// statsFrameHandler mimics a stats.Handler that both reads the payload
// synchronously (correct) and retains the *Frame to read later (incorrect).
type statsFrameHandler struct {
	mu        sync.Mutex
	inCall    [][]byte
	retained  []*Frame
	sawFrames int
}

func (h *statsFrameHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}
func (h *statsFrameHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (h *statsFrameHandler) HandleConn(context.Context, stats.ConnStats) {}

func (h *statsFrameHandler) HandleRPC(_ context.Context, s stats.RPCStats) {
	in, ok := s.(*stats.InPayload)
	if !ok {
		return
	}
	f, ok := in.Payload.(*Frame)
	if !ok {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sawFrames++
	h.inCall = append(h.inCall, f.Data())
	h.retained = append(h.retained, f)
}

// TestStatsHandler_FramePayloadLifetime pins the contract documented on
// Config.ServerOptions and Frame.Data: a stats handler reading the payload
// synchronously inside the callback sees the real bytes, while one that
// retains the *Frame and reads it after the RPC sees nil rather than another
// request's recycled bytes.
func TestStatsHandler_FramePayloadLifetime(t *testing.T) {
	h := &statsFrameHandler{}
	backendAddr := startProtoEchoBackend(t)

	srv, err := NewServer(&Config{
		DefaultBackend: backendAddr,
		ServerOptions:  []grpc.ServerOption{grpc.StatsHandler(h)},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	cc := dialProtoProxy(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const want = "stats-payload"
	out := &wrapperspb.StringValue{}
	if err := cc.Invoke(ctx, "/test.ProtoEcho/Echo", &wrapperspb.StringValue{Value: want}, out); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	// Churn the buffer pool so a use-after-free would surface as someone
	// else's bytes rather than staying quietly intact.
	for i := 0; i < 20; i++ {
		if err := cc.Invoke(ctx, "/test.ProtoEcho/Echo",
			&wrapperspb.StringValue{Value: "churn"}, &wrapperspb.StringValue{}); err != nil {
			t.Fatalf("churn invoke %d: %v", i, err)
		}
	}

	wantWire, err := proto.Marshal(&wrapperspb.StringValue{Value: want})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.sawFrames == 0 {
		t.Fatal("stats handler received no *Frame payloads; the contract under test never ran")
	}
	if !bytes.Equal(h.inCall[0], wantWire) {
		t.Errorf("synchronous read inside the callback must see the real payload:\n want %x\n  got %x",
			wantWire, h.inCall[0])
	}
	if got := h.retained[0].Data(); got != nil {
		t.Errorf("deferred read of a retained *Frame must be nil, not stale or recycled bytes; got %x", got)
	}
}
