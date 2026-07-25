package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

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

// writeCertPair generates a self-signed cert/key pair on disk and returns their
// paths, so the TLS flags can be exercised without checked-in fixtures.
func writeCertPair(t *testing.T) (certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}

// setFlags points the package-level flag variables at the given values for the
// duration of one test, restoring them afterwards.
func setFlags(t *testing.T, apply func()) {
	t.Helper()
	saved := struct {
		backend, tlsCert, tlsKey, backendCA string
		backendTLS                          bool
		maxRecv, maxSend                    int
		idle                                time.Duration
	}{*backend, *tlsCert, *tlsKey, *backendCA, *backendTLS, *maxRecvSize, *maxSendSize, *backendIdleTimeout}

	t.Cleanup(func() {
		*backend, *tlsCert, *tlsKey, *backendCA = saved.backend, saved.tlsCert, saved.tlsKey, saved.backendCA
		*backendTLS = saved.backendTLS
		*maxRecvSize, *maxSendSize = saved.maxRecv, saved.maxSend
		*backendIdleTimeout = saved.idle
	})

	*backend, *tlsCert, *tlsKey, *backendCA = "localhost:50051", "", "", ""
	*backendTLS = false
	*maxRecvSize, *maxSendSize = 0, 0
	*backendIdleTimeout = 0
	apply()
}

// TestBuildProxyConfig_SizesAndTimeout pins that the size and timeout flags
// reach proxy.Config. Without this, the flags could be silently dropped.
func TestBuildProxyConfig_SizesAndTimeout(t *testing.T) {
	setFlags(t, func() {
		*maxRecvSize = 16 << 20
		*maxSendSize = 8 << 20
		*backendIdleTimeout = 90 * time.Second
	})

	cfg, err := buildProxyConfig()
	if err != nil {
		t.Fatalf("buildProxyConfig: %v", err)
	}
	if cfg.DefaultBackend != "localhost:50051" {
		t.Errorf("DefaultBackend: got %q", cfg.DefaultBackend)
	}
	if cfg.MaxRecvMsgSize != 16<<20 {
		t.Errorf("MaxRecvMsgSize: want %d, got %d", 16<<20, cfg.MaxRecvMsgSize)
	}
	if cfg.MaxSendMsgSize != 8<<20 {
		t.Errorf("MaxSendMsgSize: want %d, got %d", 8<<20, cfg.MaxSendMsgSize)
	}
	if cfg.BackendIdleTimeout != 90*time.Second {
		t.Errorf("BackendIdleTimeout: want 90s, got %v", cfg.BackendIdleTimeout)
	}
}

// TestBuildProxyConfig_Defaults pins that an otherwise-bare invocation leaves
// every optional field zero — i.e. the flags do not silently impose policy.
func TestBuildProxyConfig_Defaults(t *testing.T) {
	setFlags(t, func() {})

	cfg, err := buildProxyConfig()
	if err != nil {
		t.Fatalf("buildProxyConfig: %v", err)
	}
	if cfg.ServerOptions != nil {
		t.Errorf("ServerOptions: want nil without -tls-cert, got %d", len(cfg.ServerOptions))
	}
	if cfg.BackendTransportCreds != nil {
		t.Error("BackendTransportCreds: want nil without -backend-tls")
	}
	if cfg.MaxRecvMsgSize != 0 || cfg.MaxSendMsgSize != 0 || cfg.BackendIdleTimeout != 0 {
		t.Errorf("expected zero values, got recv=%d send=%d idle=%v",
			cfg.MaxRecvMsgSize, cfg.MaxSendMsgSize, cfg.BackendIdleTimeout)
	}
}

// TestBuildProxyConfig_ServingTLS pins that -tls-cert/-tls-key produce a
// ServerOption, and that giving only one of the pair is rejected rather than
// silently serving plaintext.
func TestBuildProxyConfig_ServingTLS(t *testing.T) {
	certPath, keyPath := writeCertPair(t)

	setFlags(t, func() { *tlsCert, *tlsKey = certPath, keyPath })
	cfg, err := buildProxyConfig()
	if err != nil {
		t.Fatalf("buildProxyConfig: %v", err)
	}
	if len(cfg.ServerOptions) != 1 {
		t.Fatalf("ServerOptions: want 1 credential option, got %d", len(cfg.ServerOptions))
	}

	for _, tc := range []struct{ name, cert, key string }{
		{"cert without key", certPath, ""},
		{"key without cert", "", keyPath},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setFlags(t, func() { *tlsCert, *tlsKey = tc.cert, tc.key })
			if _, err := buildProxyConfig(); err == nil {
				t.Error("want error when only one of -tls-cert/-tls-key is set, got nil")
			}
		})
	}

	t.Run("unreadable cert", func(t *testing.T) {
		setFlags(t, func() { *tlsCert, *tlsKey = filepath.Join(t.TempDir(), "nope.pem"), keyPath })
		if _, err := buildProxyConfig(); err == nil {
			t.Error("want error for a missing certificate file, got nil")
		}
	})
}

// TestBuildProxyConfig_BackendTLS pins backend credentials, including that
// -backend-ca alone is an error: treating it as plaintext would silently
// downgrade a connection the operator asked to secure.
func TestBuildProxyConfig_BackendTLS(t *testing.T) {
	certPath, _ := writeCertPair(t)

	t.Run("system roots", func(t *testing.T) {
		setFlags(t, func() { *backendTLS = true })
		cfg, err := buildProxyConfig()
		if err != nil {
			t.Fatalf("buildProxyConfig: %v", err)
		}
		if cfg.BackendTransportCreds == nil {
			t.Error("BackendTransportCreds: want credentials with -backend-tls, got nil")
		}
	})

	t.Run("explicit CA", func(t *testing.T) {
		setFlags(t, func() { *backendTLS, *backendCA = true, certPath })
		cfg, err := buildProxyConfig()
		if err != nil {
			t.Fatalf("buildProxyConfig: %v", err)
		}
		if cfg.BackendTransportCreds == nil {
			t.Error("BackendTransportCreds: want credentials with -backend-ca, got nil")
		}
	})

	t.Run("CA without backend-tls is rejected", func(t *testing.T) {
		setFlags(t, func() { *backendCA = certPath })
		if _, err := buildProxyConfig(); err == nil {
			t.Error("want error for -backend-ca without -backend-tls, got nil")
		}
	})

	t.Run("unreadable CA", func(t *testing.T) {
		setFlags(t, func() { *backendTLS, *backendCA = true, filepath.Join(t.TempDir(), "nope.pem") })
		if _, err := buildProxyConfig(); err == nil {
			t.Error("want error for a missing CA file, got nil")
		}
	})
}
