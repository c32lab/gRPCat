// Package parser provides gRPC packet parsing functionality
package parser

import (
	"encoding/binary"
	"fmt"
	"io"
)

// GRPCMessage represents a parsed gRPC message
type GRPCMessage struct {
	Compressed bool   // Whether the message is compressed
	Length     uint32 // Length of the message payload
	Payload    []byte // The actual message data
}

// ParseGRPCMessage parses a gRPC message from a byte stream
// gRPC message format:
// | compressed flag (1 byte) | message length (4 bytes, big-endian) | message data |
func ParseGRPCMessage(data []byte) (*GRPCMessage, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("data too short: need at least 5 bytes, got %d", len(data))
	}

	msg := &GRPCMessage{}

	// Parse compressed flag (1 byte)
	msg.Compressed = data[0] == 1

	// Parse message length (4 bytes, big-endian)
	msg.Length = binary.BigEndian.Uint32(data[1:5])

	// Validate data length
	if len(data) < int(5+msg.Length) {
		return nil, fmt.Errorf("incomplete message: expected %d bytes, got %d", 5+msg.Length, len(data))
	}

	// Extract payload
	msg.Payload = data[5 : 5+msg.Length]

	return msg, nil
}

// MaxMessageSize is the maximum allowed gRPC message payload size (4 MB),
// matching the default grpc-go limit.
const MaxMessageSize = 4 << 20

// ParseGRPCMessageFromReader reads and parses a gRPC message from an io.Reader
func ParseGRPCMessageFromReader(reader io.Reader) (*GRPCMessage, error) {
	// Read header (5 bytes)
	header := make([]byte, 5)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, fmt.Errorf("failed to read message header: %w", err)
	}

	msg := &GRPCMessage{}
	msg.Compressed = header[0] == 1
	msg.Length = binary.BigEndian.Uint32(header[1:5])

	if msg.Length > MaxMessageSize {
		return nil, fmt.Errorf("message too large: %d bytes (max %d)", msg.Length, MaxMessageSize)
	}

	// Read payload
	msg.Payload = make([]byte, msg.Length)
	if _, err := io.ReadFull(reader, msg.Payload); err != nil {
		return nil, fmt.Errorf("failed to read message payload: %w", err)
	}

	return msg, nil
}

// EncodeGRPCMessage encodes a gRPC message into bytes
func EncodeGRPCMessage(msg *GRPCMessage) []byte {
	buf := make([]byte, 5+len(msg.Payload))

	// Set compressed flag
	if msg.Compressed {
		buf[0] = 1
	} else {
		buf[0] = 0
	}

	// Set message length
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(msg.Payload)))

	// Copy payload
	copy(buf[5:], msg.Payload)

	return buf
}
