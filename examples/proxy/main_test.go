package main

import (
	"testing"

	"google.golang.org/grpc/encoding"
)

// TestGzipCompressorRegistered pins the gzip registration this example relies
// on: gRPC decompresses requests at the proxy, so a proxy process without the
// compressor rejects gzip requests with
// `Unimplemented: grpc: Decompressor is not installed for grpc-encoding "gzip"`.
// The rejection itself can only be reproduced in a separate process (encoding
// registration is global and this test binary links the example's import), so
// the registration is what is asserted here.
func TestGzipCompressorRegistered(t *testing.T) {
	if encoding.GetCompressor("gzip") == nil {
		t.Fatal("gzip compressor not registered: the proxy would reject gzip-compressed requests with Unimplemented")
	}
}

func TestParseRoute(t *testing.T) {
	tests := []struct {
		name        string
		route       string
		wantService string
		wantBackend string
		wantErr     bool
	}{
		{"simple", "user.Service=localhost:50052", "user.Service", "localhost:50052", false},
		{"equals in backend", "svc=host:1234?x=y", "svc", "host:1234?x=y", false},
		{"surrounding spaces", " svc = host:1234 ", "svc", "host:1234", false},
		{"no separator", "svc", "", "", true},
		{"empty", "", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, backend, err := parseRoute(tt.route)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error: want %v, got %v", tt.wantErr, err)
			}
			if service != tt.wantService {
				t.Errorf("service: want %q, got %q", tt.wantService, service)
			}
			if backend != tt.wantBackend {
				t.Errorf("backend: want %q, got %q", tt.wantBackend, backend)
			}
		})
	}
}
