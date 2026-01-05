package proxy

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// echoServer implements a simple echo service for testing
type echoServer struct {
	grpc.ServerStream
}

func TestForwarder_UnaryRPC(t *testing.T) {
	// Start backend server
	backendAddr := startEchoBackend(t)

	// Start proxy server
	proxyAddr := startProxyServer(t, backendAddr)

	// Create client connection to proxy
	conn, err := grpc.Dial(proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("failed to dial proxy: %v", err)
	}
	defer conn.Close()

	// Make unary call through proxy
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send request
	reqFrame := &Frame{data: buildGRPCMessage([]byte("hello"))}
	respFrame := &Frame{}

	err = conn.Invoke(ctx, "/test.Echo/Echo", reqFrame, respFrame)
	if err != nil {
		t.Fatalf("invoke failed: %v", err)
	}

	// Verify response
	payload := extractPayload(respFrame.data)
	if string(payload) != "hello" {
		t.Errorf("expected 'hello', got '%s'", string(payload))
	}
}

func TestForwarder_ServerStreaming(t *testing.T) {
	// Start backend that sends 3 messages
	backendAddr := startStreamingBackend(t, 3)

	// Start proxy server
	proxyAddr := startProxyServer(t, backendAddr)

	// Create client connection to proxy
	conn, err := grpc.Dial(proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("failed to dial proxy: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create stream
	streamDesc := &grpc.StreamDesc{ServerStreams: true}
	stream, err := conn.NewStream(ctx, streamDesc, "/test.Echo/ServerStream")
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Send request
	reqFrame := &Frame{data: buildGRPCMessage([]byte("stream"))}
	if err := stream.SendMsg(reqFrame); err != nil {
		t.Fatalf("failed to send: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("failed to close send: %v", err)
	}

	// Receive responses
	count := 0
	for {
		respFrame := &Frame{}
		if err := stream.RecvMsg(respFrame); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("recv failed: %v", err)
		}
		count++
	}

	if count != 3 {
		t.Errorf("expected 3 messages, got %d", count)
	}
}

func TestForwarder_MetadataForwarding(t *testing.T) {
	// Channel to capture received metadata
	receivedMD := make(chan metadata.MD, 1)

	// Start backend that captures metadata
	backendAddr := startMetadataCapturingBackend(t, receivedMD)

	// Start proxy server
	proxyAddr := startProxyServer(t, backendAddr)

	// Create client connection to proxy
	conn, err := grpc.Dial(proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(&ProxyCodec{})),
	)
	if err != nil {
		t.Fatalf("failed to dial proxy: %v", err)
	}
	defer conn.Close()

	// Make call with custom metadata
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	md := metadata.Pairs("x-custom-header", "test-value")
	ctx = metadata.NewOutgoingContext(ctx, md)

	reqFrame := &Frame{data: buildGRPCMessage([]byte("meta"))}
	respFrame := &Frame{}

	err = conn.Invoke(ctx, "/test.Echo/Echo", reqFrame, respFrame)
	if err != nil {
		t.Fatalf("invoke failed: %v", err)
	}

	// Verify metadata was forwarded
	select {
	case gotMD := <-receivedMD:
		if vals := gotMD.Get("x-custom-header"); len(vals) == 0 || vals[0] != "test-value" {
			t.Errorf("expected x-custom-header=test-value, got %v", vals)
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for metadata")
	}
}

// Helper functions

func startEchoBackend(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer(grpc.ForceServerCodec(&ProxyCodec{}))
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Echo",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					frame := &Frame{}
					if err := dec(frame); err != nil {
						return nil, err
					}
					// Echo back the same data
					return frame, nil
				},
			},
		},
		Streams: []grpc.StreamDesc{},
	}, nil)

	go server.Serve(listener)
	t.Cleanup(func() { server.Stop() })

	return listener.Addr().String()
}

func startStreamingBackend(t *testing.T, msgCount int) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer(grpc.ForceServerCodec(&ProxyCodec{}))
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Methods:     []grpc.MethodDesc{},
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "ServerStream",
				ServerStreams: true,
				Handler: func(srv any, stream grpc.ServerStream) error {
					// Receive request
					frame := &Frame{}
					if err := stream.RecvMsg(frame); err != nil {
						return err
					}

					// Send multiple responses
					for i := 0; i < msgCount; i++ {
						resp := &Frame{data: buildGRPCMessage([]byte("response"))}
						if err := stream.SendMsg(resp); err != nil {
							return err
						}
					}
					return nil
				},
			},
		},
	}, nil)

	go server.Serve(listener)
	t.Cleanup(func() { server.Stop() })

	return listener.Addr().String()
}

func startMetadataCapturingBackend(t *testing.T, mdChan chan<- metadata.MD) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer(grpc.ForceServerCodec(&ProxyCodec{}))
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Echo",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Echo",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					// Capture incoming metadata
					if md, ok := metadata.FromIncomingContext(ctx); ok {
						mdChan <- md
					}

					frame := &Frame{}
					if err := dec(frame); err != nil {
						return nil, err
					}
					return frame, nil
				},
			},
		},
		Streams: []grpc.StreamDesc{},
	}, nil)

	go server.Serve(listener)
	t.Cleanup(func() { server.Stop() })

	return listener.Addr().String()
}

func startProxyServer(t *testing.T, backend string) string {
	t.Helper()

	config := &Config{DefaultBackend: backend}
	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("failed to create proxy server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	go server.grpcServer.Serve(listener)
	t.Cleanup(func() { server.Stop() })

	return listener.Addr().String()
}

// buildGRPCMessage creates a gRPC message frame from payload
// Format: [compressed(1)] [length(4)] [payload]
func buildGRPCMessage(payload []byte) []byte {
	msg := make([]byte, 5+len(payload))
	msg[0] = 0 // not compressed
	msg[1] = byte(len(payload) >> 24)
	msg[2] = byte(len(payload) >> 16)
	msg[3] = byte(len(payload) >> 8)
	msg[4] = byte(len(payload))
	copy(msg[5:], payload)
	return msg
}

// extractPayload extracts payload from gRPC message frame
func extractPayload(data []byte) []byte {
	if len(data) < 5 {
		return nil
	}
	return data[5:]
}
