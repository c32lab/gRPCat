package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/c32lab/gRPCat/cmd/grpcat/middlewares"
	"github.com/c32lab/gRPCat/proxy"
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
)

func init() {
	flag.Var(&routes, "route", "Route service to backend (format: service=backend, can specify multiple times)")
}

const versionInfo = "gRPCat v0.1.0"

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

	config := &proxy.Config{
		DefaultBackend: *backend,
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
			parts := strings.Split(r, "=")
			if len(parts) != 2 {
				log.Fatalf("Invalid route format: %s (expected: service=backend)", r)
			}
			service := strings.TrimSpace(parts[0])
			backend := strings.TrimSpace(parts[1])
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
	if *verbose {
		log.Printf("Verbose logging: enabled")
	}
	fmt.Println()

	if err := server.Start(ctx, *listen); err != nil {
		log.Fatalf("Server error: %v", err)
	}

	log.Println("Proxy stopped")
}
