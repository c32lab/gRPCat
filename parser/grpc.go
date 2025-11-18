// Package parser provides gRPC protocol parsing utilities
package parser

import (
	"fmt"
	"strings"
)

// GRPCRequest represents a parsed gRPC request
type GRPCRequest struct {
	Service string // gRPC service name (e.g., "helloworld.Greeter")
	Method  string // gRPC method name (e.g., "SayHello")
	Message *GRPCMessage
}

// ParseGRPCPath extracts service and method from a gRPC path
// Path format: /{service}/{method}
// Example: /helloworld.Greeter/SayHello
func ParseGRPCPath(path string) (service, method string, err error) {
	path = strings.TrimPrefix(path, "/")

	parts := strings.Split(path, "/")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid gRPC path format: %s", path)
	}

	return parts[0], parts[1], nil
}

// FormatGRPCPath formats service and method into a gRPC path
func FormatGRPCPath(service, method string) string {
	return fmt.Sprintf("/%s/%s", service, method)
}
