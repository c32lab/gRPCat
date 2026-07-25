package proxy

import (
	"context"
	"crypto/sha256"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// This file is deliberately free of any reference to Frame / ProxyCodec: it
// drives the proxy with a stock protobuf client and a stock protobuf backend
// only, so it compiles unchanged against both the legacy-Codec and the
// CodecV2 implementation and the two can be benchmarked head to head.

// serverStreamEchoCount is how many copies of the request the
// /test.BulkEcho/ServerStream method sends back.
const serverStreamEchoCount = 5

// bulkPayload builds a size-byte payload whose every byte depends on seed, so
// that a buffer swapped in from another in-flight message is detected by a
// content comparison rather than only by a length check.
func bulkPayload(seed uint64, size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(seed*31 + uint64(i)*7 + uint64(i>>8))
	}
	return b
}

// startBulkEchoBackend starts a backend that uses gRPC's default protobuf
// codec and exposes all four streaming modes over wrapperspb.BytesValue:
//
//	Unary        - echoes the request payload
//	ServerStream - echoes the request payload serverStreamEchoCount times
//	ClientStream - returns the SHA-256 of every payload it received, in order
//	BidiStream   - echoes each payload as it arrives
func startBulkEchoBackend(t testing.TB) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.BulkEcho",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Unary",
				Handler: func(_ any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
					in := &wrapperspb.BytesValue{}
					if err := dec(in); err != nil {
						return nil, err
					}
					return wrapperspb.Bytes(in.GetValue()), nil
				},
			},
		},
		Streams: []grpc.StreamDesc{
			{
				StreamName:    "ServerStream",
				ServerStreams: true,
				Handler: func(_ any, stream grpc.ServerStream) error {
					in := &wrapperspb.BytesValue{}
					if err := stream.RecvMsg(in); err != nil {
						return err
					}
					for i := 0; i < serverStreamEchoCount; i++ {
						if err := stream.SendMsg(wrapperspb.Bytes(in.GetValue())); err != nil {
							return err
						}
					}
					return nil
				},
			},
			{
				StreamName:    "ClientStream",
				ClientStreams: true,
				Handler: func(_ any, stream grpc.ServerStream) error {
					h := sha256.New()
					for {
						in := &wrapperspb.BytesValue{}
						if err := stream.RecvMsg(in); err != nil {
							if err == io.EOF {
								break
							}
							return err
						}
						h.Write(in.GetValue())
					}
					return stream.SendMsg(wrapperspb.Bytes(h.Sum(nil)))
				},
			},
			{
				StreamName:    "BidiStream",
				ServerStreams: true,
				ClientStreams: true,
				Handler: func(_ any, stream grpc.ServerStream) error {
					for {
						in := &wrapperspb.BytesValue{}
						if err := stream.RecvMsg(in); err != nil {
							if err == io.EOF {
								return nil
							}
							return err
						}
						if err := stream.SendMsg(wrapperspb.Bytes(in.GetValue())); err != nil {
							return err
						}
					}
				},
			},
		},
	}, nil)

	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

// startBulkProxy starts a proxy in front of a bulk echo backend and returns a
// stock protobuf client connected to it.
func startBulkProxy(t testing.TB) *grpc.ClientConn {
	t.Helper()

	srv, err := NewServer(&Config{DefaultBackend: startBulkEchoBackend(t)})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.grpcServer.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

var bidiStreamDesc = &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}

// benchmarkProxyRoundTrip measures one ping-pong of a size-byte payload
// through the proxy on a long-lived bidirectional stream, i.e. two forwarded
// messages per iteration.
func benchmarkProxyRoundTrip(b *testing.B, size int) {
	conn := startBulkProxy(b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := grpc.NewClientStream(ctx, bidiStreamDesc, conn, "/test.BulkEcho/BidiStream")
	if err != nil {
		b.Fatalf("open stream: %v", err)
	}

	req := wrapperspb.Bytes(bulkPayload(1, size))
	resp := &wrapperspb.BytesValue{}

	b.SetBytes(int64(size) * 2)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := stream.SendMsg(req); err != nil {
			b.Fatalf("send: %v", err)
		}
		if err := stream.RecvMsg(resp); err != nil {
			b.Fatalf("recv: %v", err)
		}
	}
	b.StopTimer()

	if len(resp.GetValue()) != size {
		b.Fatalf("response size: want %d got %d", size, len(resp.GetValue()))
	}
}

func BenchmarkProxyRoundTrip1KB(b *testing.B)   { benchmarkProxyRoundTrip(b, 1024) }
func BenchmarkProxyRoundTrip64KB(b *testing.B)  { benchmarkProxyRoundTrip(b, 64*1024) }
func BenchmarkProxyRoundTrip256KB(b *testing.B) { benchmarkProxyRoundTrip(b, 256*1024) }
