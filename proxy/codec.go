// Package proxy implements a transparent gRPC proxy with plugin support
package proxy

import (
	"fmt"

	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
)

// Frame represents a raw gRPC message frame containing unparsed bytes.
// It's used as the message type for the ProxyCodec to enable transparent forwarding.
//
// The payload is held as a mem.BufferSlice: reference-counted, possibly
// pooled buffers owned by gRPC, which is what lets the proxy forward a
// message without copying it. That makes ownership explicit:
//
//   - A Frame that RecvMsg has filled owns one reference to the received
//     buffers. The owner MUST call Free (or Reset) when done with it,
//     otherwise the buffers never return to gRPC's pool.
//   - SendMsg takes its own reference, so a Frame may be freed as soon as
//     SendMsg returns even though the write itself completes later.
//   - Free is idempotent and safe on a zero Frame, and a freed Frame can be
//     refilled by another RecvMsg.
//
// A Frame is not safe for concurrent use.
type Frame struct {
	data mem.BufferSlice
}

// frameFromBytes wraps b in a Frame without copying it. mem.SliceBuffer is
// not reference counted and never returns to a pool, so the resulting Frame
// needs no Free and b stays valid for as long as its own owner keeps it.
func frameFromBytes(b []byte) *Frame {
	return &Frame{data: mem.BufferSlice{mem.SliceBuffer(b)}}
}

// Data returns the frame payload as a contiguous byte slice.
//
// The payload may span several pooled buffers that Free returns to gRPC, so
// the bytes cannot be aliased out: Data allocates and copies. The returned
// slice is independent of the frame and stays valid after Free, which is what
// makes it safe to hand to middleware. It is nil for an empty frame.
//
// Data on an already-freed Frame returns nil rather than stale bytes. This
// matters for stats handlers: gRPC passes the *Frame itself as
// stats.InPayload.Payload / stats.OutPayload.Payload, and the proxy frees it
// once the message has been handed on. Handlers are invoked synchronously, so
// calling Data inside the callback is correct; retaining the *Frame and
// reading it after the RPC yields nil, and reading it from another goroutine
// races with Free.
func (f *Frame) Data() []byte {
	return f.data.Materialize()
}

// ParseAs parses the frame data into a specific protobuf message type.
// This is useful when middlewares need to inspect the actual message content.
// The frame holds the bare protobuf payload: gRPC strips the 5-byte message
// header before the codec runs and re-adds it on send.
func (f *Frame) ParseAs(msg proto.Message) error {
	if err := proto.Unmarshal(f.data.Materialize(), msg); err != nil {
		return fmt.Errorf("failed to unmarshal protobuf: %w", err)
	}

	return nil
}

// Free releases the frame's reference to its buffers and clears it.
// It is idempotent, and safe to call while a SendMsg of this frame is still
// in flight because gRPC holds its own reference for the duration of the
// write.
func (f *Frame) Free() {
	f.data.Free()
	f.data = nil
}

// Reset clears the frame data. It releases the frame's buffers, like Free.
func (f *Frame) Reset() {
	f.Free()
}

// String returns a human-readable representation of the frame.
func (f *Frame) String() string {
	return fmt.Sprintf("Frame{%d bytes}", f.data.Len())
}

// ProtoMessage is a marker method to satisfy the proto.Message interface.
func (f *Frame) ProtoMessage() {}

// ProxyCodec is a custom codec for transparent gRPC message proxying.
//
// Unlike standard gRPC codecs (like protobuf), ProxyCodec:
//   - Does NOT deserialize messages
//   - Does NOT serialize messages
//   - Simply wraps/unwraps raw buffers in Frame objects
//
// This enables transparent forwarding: the buffers received from the client
// are handed to the backend as-is, with no protobuf parsing and no copy. It
// implements encoding.CodecV2 rather than the legacy encoding.Codec precisely
// so that gRPC does not materialize every message into a fresh []byte on the
// way in (see codecV0Bridge in grpc's codec.go).
type ProxyCodec struct{}

// Marshal hands the frame's buffers to gRPC.
// Called by gRPC when sending messages.
//
// gRPC frees the returned slice once the message has been queued for writing,
// so Marshal returns an extra reference and leaves the Frame's own reference
// intact. The two are independent: the caller may Free the frame right after
// SendMsg returns, and the transport keeps the buffers alive until the write
// actually drains.
func (c *ProxyCodec) Marshal(v any) (mem.BufferSlice, error) {
	frame, ok := v.(*Frame)
	if !ok {
		return nil, fmt.Errorf("gRPCat codec: expected *Frame, got %T", v)
	}
	frame.data.Ref()
	return frame.data, nil
}

// Unmarshal retains the received buffers in a Frame.
// Called by gRPC when receiving messages.
//
// IMPORTANT: no copy is made. gRPC frees data as soon as Unmarshal returns
// (rpc_util.go recv), so the Frame takes its own reference and thereby owns
// the payload: whoever called RecvMsg must Free the frame when done with it.
//
// Buffers the Frame already held are released first, so one Frame can be
// reused for every message on a stream.
func (c *ProxyCodec) Unmarshal(data mem.BufferSlice, v any) error {
	frame, ok := v.(*Frame)
	if !ok {
		return fmt.Errorf("gRPCat codec: expected *Frame, got %T", v)
	}
	frame.Free()
	data.Ref()
	frame.data = data
	return nil
}

// Name returns the codec name.
func (c *ProxyCodec) Name() string {
	return "gRPCat"
}

// String returns the codec description.
func (c *ProxyCodec) String() string {
	return "gRPCat-codec"
}
