package parser

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestParseGRPCMessageFromReader_TooLarge(t *testing.T) {
	var header [5]byte
	header[0] = 0
	binary.BigEndian.PutUint32(header[1:5], MaxMessageSize+1)

	_, err := ParseGRPCMessageFromReader(bytes.NewReader(header[:]))
	if err == nil {
		t.Fatal("expected error for oversized message")
	}
}

func TestParseGRPCMessageFromReader_AtLimit(t *testing.T) {
	payload := make([]byte, 64)
	var header [5]byte
	header[0] = 0
	binary.BigEndian.PutUint32(header[1:5], uint32(len(payload)))

	buf := append(header[:], payload...)
	msg, err := ParseGRPCMessageFromReader(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if int(msg.Length) != len(payload) {
		t.Errorf("length: want %d, got %d", len(payload), msg.Length)
	}
}
