// Package proxy implements a transparent gRPC proxy with plugin support
package proxy

import (
	"fmt"

	"github.com/c32lab/gRPCat/parser"
	"google.golang.org/protobuf/proto"
)

// Frame represents a raw gRPC message frame containing unparsed bytes.
// It's used as the message type for the ProxyCodec to enable transparent forwarding.
type Frame struct {
	data []byte
}

// Data returns the raw frame bytes.
func (f *Frame) Data() []byte {
	return f.data
}

// ParseAs parses the frame data into a specific protobuf message type.
// This is useful when middlewares need to inspect the actual message content.
func (f *Frame) ParseAs(msg proto.Message) error {
	grpcMsg, err := parser.ParseGRPCMessage(f.data)
	if err != nil {
		return fmt.Errorf("failed to parse gRPC message: %w", err)
	}

	if err := proto.Unmarshal(grpcMsg.Payload, msg); err != nil {
		return fmt.Errorf("failed to unmarshal protobuf: %w", err)
	}

	return nil
}

// Reset clears the frame data.
func (f *Frame) Reset() {
	f.data = nil
}

// String returns a human-readable representation of the frame.
func (f *Frame) String() string {
	return fmt.Sprintf("Frame{%d bytes}", len(f.data))
}

// ProtoMessage is a marker method to satisfy the proto.Message interface.
func (f *Frame) ProtoMessage() {}

// ProxyCodec is a custom codec for transparent gRPC message proxying.
//
// Unlike standard gRPC codecs (like protobuf), ProxyCodec:
//   - Does NOT deserialize messages
//   - Does NOT serialize messages
//   - Simply wraps/unwraps raw bytes in Frame objects
//
// This enables transparent forwarding: bytes received from client are directly
// forwarded to backend without any protobuf parsing overhead.
type ProxyCodec struct{}

// Marshal extracts raw bytes from a Frame.
// Called by gRPC when sending messages.
func (c *ProxyCodec) Marshal(v any) ([]byte, error) {
	frame, ok := v.(*Frame)
	if !ok {
		return nil, fmt.Errorf("gRPCat codec: expected *Frame, got %T", v)
	}
	return frame.data, nil
}

// Unmarshal wraps raw bytes into a Frame.
// Called by gRPC when receiving messages.
//
// IMPORTANT: This directly references the input data slice.
// The data slice is owned by gRPC and will be reused after RecvMsg returns,
// so callers must NOT retain references to frame.data beyond RecvMsg.
func (c *ProxyCodec) Unmarshal(data []byte, v any) error {
	frame, ok := v.(*Frame)
	if !ok {
		return fmt.Errorf("gRPCat codec: expected *Frame, got %T", v)
	}
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
