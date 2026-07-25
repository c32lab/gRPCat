package parser

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestParseGRPCMessage(t *testing.T) {
	frame := func(compressed bool, payload []byte) []byte {
		b := make([]byte, 5+len(payload))
		if compressed {
			b[0] = 1
		}
		binary.BigEndian.PutUint32(b[1:5], uint32(len(payload)))
		copy(b[5:], payload)
		return b
	}

	tests := []struct {
		name           string
		data           []byte
		wantErr        bool
		wantCompressed bool
		wantPayload    []byte
	}{
		{name: "simple", data: frame(false, []byte("hello")), wantPayload: []byte("hello")},
		{name: "compressed flag", data: frame(true, []byte("gz")), wantCompressed: true, wantPayload: []byte("gz")},
		{name: "empty payload", data: frame(false, nil), wantPayload: []byte{}},
		{name: "nil", data: nil, wantErr: true},
		{name: "header only, 4 bytes", data: []byte{0, 0, 0, 0}, wantErr: true},
		{name: "exactly 5 bytes, zero length", data: []byte{0, 0, 0, 0, 0}, wantPayload: []byte{}},
		{name: "length exceeds data", data: []byte{0, 0, 0, 0, 9, 'a', 'b'}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, err := ParseGRPCMessage(tt.data)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("want error, got msg %+v", msg)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if msg.Compressed != tt.wantCompressed {
				t.Errorf("Compressed: want %v, got %v", tt.wantCompressed, msg.Compressed)
			}
			if int(msg.Length) != len(tt.wantPayload) {
				t.Errorf("Length: want %d, got %d", len(tt.wantPayload), msg.Length)
			}
			if !bytes.Equal(msg.Payload, tt.wantPayload) {
				t.Errorf("Payload: want %q, got %q", tt.wantPayload, msg.Payload)
			}
		})
	}
}

// TestParseGRPCMessage_TrailingBytesIgnored pins that a buffer holding more
// than one framed message yields only the first: the length prefix bounds the
// payload rather than the buffer doing so.
func TestParseGRPCMessage_TrailingBytesIgnored(t *testing.T) {
	buf := []byte{0, 0, 0, 0, 2, 'h', 'i', 0, 0, 0, 0, 3, 'b', 'y', 'e'}

	msg, err := ParseGRPCMessage(buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := string(msg.Payload); got != "hi" {
		t.Errorf("want first message %q, got %q", "hi", got)
	}
}

func TestEncodeGRPCMessage(t *testing.T) {
	tests := []struct {
		name       string
		msg        *GRPCMessage
		wantHeader []byte
	}{
		{"simple", &GRPCMessage{Payload: []byte("hello")}, []byte{0, 0, 0, 0, 5}},
		{"compressed", &GRPCMessage{Compressed: true, Payload: []byte("gz")}, []byte{1, 0, 0, 0, 2}},
		{"empty", &GRPCMessage{}, []byte{0, 0, 0, 0, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EncodeGRPCMessage(tt.msg)
			if !bytes.Equal(got[:5], tt.wantHeader) {
				t.Errorf("header: want %v, got %v", tt.wantHeader, got[:5])
			}
			if !bytes.Equal(got[5:], tt.msg.Payload) {
				t.Errorf("payload: want %q, got %q", tt.msg.Payload, got[5:])
			}
		})
	}
}

// TestEncodeParseRoundTrip pins that the two halves agree: whatever Encode
// produces, Parse must read back unchanged. Length is derived from the payload
// rather than from GRPCMessage.Length, so a stale Length field is ignored.
func TestEncodeParseRoundTrip(t *testing.T) {
	for _, payload := range [][]byte{
		nil,
		[]byte(""),
		[]byte("x"),
		[]byte("a longer payload with spaces and \x00 nul bytes"),
		bytes.Repeat([]byte("z"), 1024),
	} {
		for _, compressed := range []bool{false, true} {
			in := &GRPCMessage{Compressed: compressed, Length: 9999, Payload: payload}

			out, err := ParseGRPCMessage(EncodeGRPCMessage(in))
			if err != nil {
				t.Fatalf("round trip of %d bytes (compressed=%v): %v", len(payload), compressed, err)
			}
			if out.Compressed != compressed {
				t.Errorf("Compressed: want %v, got %v", compressed, out.Compressed)
			}
			if int(out.Length) != len(payload) {
				t.Errorf("Length: want %d (from payload, not the stale field), got %d", len(payload), out.Length)
			}
			if !bytes.Equal(out.Payload, payload) && !(len(out.Payload) == 0 && len(payload) == 0) {
				t.Errorf("Payload: want %q, got %q", payload, out.Payload)
			}
		}
	}
}

func TestFormatGRPCPath(t *testing.T) {
	tests := []struct {
		service, method, want string
	}{
		{"helloworld.Greeter", "SayHello", "/helloworld.Greeter/SayHello"},
		{"a", "b", "/a/b"},
		{"", "", "//"},
	}

	for _, tt := range tests {
		if got := FormatGRPCPath(tt.service, tt.method); got != tt.want {
			t.Errorf("FormatGRPCPath(%q, %q): want %q, got %q", tt.service, tt.method, tt.want, got)
		}
	}
}

// TestFormatParsePathRoundTrip pins that FormatGRPCPath produces paths
// ParseGRPCPath accepts, which is the only reason both exist.
func TestFormatParsePathRoundTrip(t *testing.T) {
	const wantService, wantMethod = "pkg.Service", "Method"

	service, method, err := ParseGRPCPath(FormatGRPCPath(wantService, wantMethod))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if service != wantService || method != wantMethod {
		t.Errorf("want %q/%q, got %q/%q", wantService, wantMethod, service, method)
	}
}
