package parser

import "testing"

func TestParseGRPCPath(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		wantService string
		wantMethod  string
		wantErr     bool
	}{
		{"leading slash", "/helloworld.Greeter/SayHello", "helloworld.Greeter", "SayHello", false},
		{"no leading slash", "a/b", "a", "b", false},
		{"extra segment", "/a/b/c", "", "", true},
		{"missing method", "/a", "", "", true},
		{"empty path", "", "", "", true},
		{"only separators", "//", "", "", true},
		{"empty method", "/a/", "", "", true},
		{"empty service", "//b", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, method, err := ParseGRPCPath(tt.path)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error: want %v, got %v", tt.wantErr, err)
			}
			if service != tt.wantService {
				t.Errorf("service: want %q, got %q", tt.wantService, service)
			}
			if method != tt.wantMethod {
				t.Errorf("method: want %q, got %q", tt.wantMethod, method)
			}
		})
	}
}

// TestParseGRPCPathAllocs pins the allocation-free split. ParseGRPCPath runs
// once per RPC (proxy/server.go, TransparentHandler), so a slice-returning
// split would allocate on every request.
func TestParseGRPCPathAllocs(t *testing.T) {
	allocs := testing.AllocsPerRun(1000, func() {
		_, _, _ = ParseGRPCPath("/helloworld.Greeter/SayHello")
	})
	if allocs != 0 {
		t.Errorf("allocs per call: want 0, got %v", allocs)
	}
}
