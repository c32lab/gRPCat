package proxy

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// TestServer_ServerOptionsTLS terminates TLS at the proxy: the client speaks
// TLS to the proxy, the proxy speaks plaintext to the backend. *grpc.Server
// options are not introspectable, so this asserts Config.ServerOptions reached
// grpc.NewServer through observable behavior — a plaintext client cannot use
// this proxy, a TLS client round-trips.
func TestServer_ServerOptionsTLS(t *testing.T) {
	backendAddr := startEchoBackend(t)
	serverCert, certPool := selfSignedCert(t)

	srv, err := NewServer(&Config{
		DefaultBackend: backendAddr,
		ServerOptions: []grpc.ServerOption{
			grpc.Creds(credentials.NewTLS(&tls.Config{
				Certificates: []tls.Certificate{serverCert},
			})),
		},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.grpcServer.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: certPool})),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	respFrame := &Frame{}
	if err := conn.Invoke(ctx, "/test.Echo/Echo",
		&Frame{data: buildGRPCMessage([]byte("over-tls"))},
		respFrame,
	); err != nil {
		t.Fatalf("invoke over TLS: %v", err)
	}
	if got := string(extractPayload(respFrame.data)); got != "over-tls" {
		t.Errorf("expected 'over-tls', got %q", got)
	}

	// A plaintext client must not be able to talk to the same listener.
	plain, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("dial plaintext: %v", err)
	}
	defer plain.Close()

	plainCtx, plainCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer plainCancel()
	if err := plain.Invoke(plainCtx, "/test.Echo/Echo",
		&Frame{data: buildGRPCMessage([]byte("plaintext"))},
		&Frame{},
	); err == nil {
		t.Error("expected plaintext call to a TLS listener to fail")
	}
}

// TestServer_ServerOptionsWinOnConflict pins the append ORDER, not just the
// append: Config.ServerOptions are applied after the proxy's own options, so a
// user-supplied grpc.MaxRecvMsgSize overrides the one derived from
// Config.MaxRecvMsgSize. gRPC applies server options sequentially to a single
// struct and both are last-write-wins setters, so appending the other way
// round silently ignores the user's limit — the 4096-byte request below would
// then fail with ResourceExhausted. The backend answers with 8 bytes, keeping
// the proxy's backend-side receive limit (also 1024) out of the picture.
func TestServer_ServerOptionsWinOnConflict(t *testing.T) {
	srv, err := NewServer(&Config{
		DefaultBackend: startFixedSizeBackend(t, 8),
		MaxRecvMsgSize: 1024,
		ServerOptions:  []grpc.ServerOption{grpc.MaxRecvMsgSize(1 << 20)},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.grpcServer.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	respFrame := &Frame{}
	if err := conn.Invoke(ctx, "/test.Echo/Echo",
		&Frame{data: make([]byte, 4096)},
		respFrame,
	); err != nil {
		t.Fatalf("invoke with a 4096-byte request: %v", err)
	}
	if len(respFrame.data) != 8 {
		t.Errorf("response size: want 8 got %d", len(respFrame.data))
	}
}

// TestServer_ServerOptionsEmpty verifies that a nil or empty ServerOptions
// leaves the plaintext behavior of the proxy unchanged.
func TestServer_ServerOptionsEmpty(t *testing.T) {
	backendAddr := startEchoBackend(t)

	for _, tt := range []struct {
		name string
		opts []grpc.ServerOption
	}{
		{name: "nil", opts: nil},
		{name: "empty", opts: []grpc.ServerOption{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			srv, err := NewServer(&Config{
				DefaultBackend: backendAddr,
				ServerOptions:  tt.opts,
			})
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}

			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			go srv.grpcServer.Serve(lis)
			defer srv.Stop()

			conn, err := grpc.NewClient(lis.Addr().String(),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultCallOptions(grpc.ForceCodec(&ProxyCodec{})),
			)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			respFrame := &Frame{}
			if err := conn.Invoke(ctx, "/test.Echo/Echo",
				&Frame{data: buildGRPCMessage([]byte("plain"))},
				respFrame,
			); err != nil {
				t.Fatalf("invoke: %v", err)
			}
			if got := string(extractPayload(respFrame.data)); got != "plain" {
				t.Errorf("expected 'plain', got %q", got)
			}
		})
	}
}

// selfSignedCert mints an in-memory self-signed certificate for 127.0.0.1 and
// returns it together with a pool trusting it. No fixture files on disk.
func selfSignedCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "gRPCat-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, pool
}
