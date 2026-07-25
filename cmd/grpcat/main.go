package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/c32lab/gRPCat/cmd/grpcat/middlewares"
	"github.com/c32lab/gRPCat/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	// gRPC decompresses requests at the proxy, so this process must have the
	// client's compressor registered or compressed requests are rejected with
	// Unimplemented. Register gzip, the common case; add other encodings the
	// same way.
	_ "google.golang.org/grpc/encoding/gzip"
)

const banner = `
        ____  ____   ____      _
   __ _|  _ \|  _ \ / ___|__ _| |_
  / _` + "`" + ` | |_) | |_) | |   / _` + "`" + ` | __|
 | (_| |  _ <|  __/| |__| (_| | |_
  \__, |_| \_\_|    \____\__,_|\__|
  |___/

      /)/)
     ( ･ω･)   Customizable gRPC Proxy
     ( づ♡    Fast & Transparent

`

type routeFlags []string

func (r *routeFlags) String() string {
	return ""
}

func (r *routeFlags) Set(value string) error {
	*r = append(*r, value)
	return nil
}

var (
	listen  = flag.String("listen", ":8080", "Address to listen on (e.g., :8080 or 0.0.0.0:8080)")
	backend = flag.String("backend", "", "Backend gRPC server address (e.g., localhost:50051)")
	routes  routeFlags
	verbose = flag.Bool("v", false, "Enable verbose logging")
	version = flag.Bool("version", false, "Show version information")

	tlsCert = flag.String("tls-cert", "", "PEM certificate for serving TLS (requires -tls-key)")
	tlsKey  = flag.String("tls-key", "", "PEM private key for serving TLS (requires -tls-cert)")

	backendTLS = flag.Bool("backend-tls", false, "Dial backends over TLS")
	backendCA  = flag.String("backend-ca", "", "PEM CA bundle for verifying backends (implies -backend-tls; system roots if unset)")

	maxRecvSize = flag.Int("max-recv-size", 0, "Max received message size in bytes (0 = unlimited)")
	maxSendSize = flag.Int("max-send-size", 0, "Max sent message size in bytes (0 = unlimited)")

	backendIdleTimeout = flag.Duration("backend-idle-timeout", 0, "Evict pooled backend connections idle this long (0 = never)")
)

func init() {
	flag.Var(&routes, "route", "Route service to backend (format: service=backend, can specify multiple times)")
}

const versionInfo = "gRPCat v0.1.0"

// parseRoute splits a -route value into service and backend. It cuts on the
// first '=' only, so backends containing '=' are preserved.
func parseRoute(r string) (service, backend string, err error) {
	service, backend, ok := strings.Cut(r, "=")
	if !ok {
		return "", "", fmt.Errorf("invalid route format: %s (expected: service=backend)", r)
	}
	return strings.TrimSpace(service), strings.TrimSpace(backend), nil
}

// buildProxyConfig assembles a proxy.Config from the parsed flags, loading any
// certificates the TLS flags refer to.
func buildProxyConfig() (*proxy.Config, error) {
	cfg := &proxy.Config{
		DefaultBackend:     *backend,
		MaxRecvMsgSize:     *maxRecvSize,
		MaxSendMsgSize:     *maxSendSize,
		BackendIdleTimeout: *backendIdleTimeout,
	}

	if (*tlsCert == "") != (*tlsKey == "") {
		return nil, fmt.Errorf("-tls-cert and -tls-key must be given together")
	}
	if *tlsCert != "" {
		creds, err := credentials.NewServerTLSFromFile(*tlsCert, *tlsKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load serving certificate: %w", err)
		}
		cfg.ServerOptions = []grpc.ServerOption{grpc.Creds(creds)}
	}

	// -backend-ca on its own is a typo, not a request for plaintext: fail
	// rather than silently ignoring the CA and dialing insecurely.
	if *backendCA != "" && !*backendTLS {
		return nil, fmt.Errorf("-backend-ca requires -backend-tls")
	}
	if *backendTLS {
		if *backendCA != "" {
			creds, err := credentials.NewClientTLSFromFile(*backendCA, "")
			if err != nil {
				return nil, fmt.Errorf("failed to load backend CA: %w", err)
			}
			cfg.BackendTransportCreds = creds
		} else {
			cfg.BackendTransportCreds = credentials.NewTLS(&tls.Config{})
		}
	}

	return cfg, nil
}

func main() {
	flag.Parse()

	if *version {
		fmt.Println(versionInfo)
		os.Exit(0)
	}

	fmt.Print(banner)

	if *backend == "" {
		log.Fatal("Error: -backend flag is required\n\nUsage: grpcat -backend <address> [-listen <address>] [-route service=backend] [-v]\n\nExample:\n  grpcat -backend localhost:50051 -listen :8080 -v\n  grpcat -backend localhost:50051 -route \"user.Service=localhost:50052\" -route \"order.Service=localhost:50053\" -v")
	}

	config, err := buildProxyConfig()
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	server, err := proxy.NewServer(config)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	if *verbose {
		server.Use(middlewares.NewLoggingMiddleware())
	}

	if len(routes) > 0 {
		router := middlewares.NewRouteMiddleware()
		for _, r := range routes {
			service, backend, err := parseRoute(r)
			if err != nil {
				log.Fatal(err)
			}
			router.AddRoute(service, backend)
			log.Printf("Route registered: %s -> %s", service, backend)
		}
		server.Use(router)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\n\nShutting down gracefully...")
		cancel()
	}()

	log.Printf("Starting gRPCat proxy")
	log.Printf("Listening on: %s", *listen)
	log.Printf("Backend: %s", *backend)
	// Whether TLS actually engaged is worth stating plainly — silently serving
	// plaintext because a flag was mistyped is the failure worth catching here.
	if *tlsCert != "" {
		log.Printf("Serving TLS: enabled")
	}
	if *backendTLS {
		log.Printf("Backend TLS: enabled")
	}
	if *verbose {
		log.Printf("Verbose logging: enabled")
	}
	fmt.Println()

	if err := server.Start(ctx, *listen); err != nil {
		log.Fatalf("Server error: %v", err)
	}

	log.Println("Proxy stopped")
}
