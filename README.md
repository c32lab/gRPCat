# gRPCat

```
        ____  ____   ____      _
   __ _|  _ \|  _ \ / ___|__ _| |_
  / _` | |_) | |_) | |   / _` | __|
 | (_| |  _ <|  __/| |__| (_| | |_
  \__, |_| \_\_|    \____\__,_|\__|
  |___/

      /)/)
     ( ･ω･)   Customizable gRPC Proxy
     ( づ♡    Fast & Transparent
```

A lightweight, high-performance gRPC proxy with gin-style middleware support.

## Features

- **No protobuf parsing** - Forwards raw gRPC frames; the first frame is buffered for middleware inspection, the rest pass straight through. Compressed messages are the exception: they are decompressed at the proxy (see [Compression](#compression)).
- **Middleware chain** - Logging, routing, rate limiting, etc.
- **All streaming modes** - Unary, server, client, bidirectional.
- **Service agnostic** - No `.proto` files required.

## Installation

As a library:

```bash
go get github.com/c32lab/gRPCat
```

As a CLI:

```bash
go install github.com/c32lab/gRPCat/cmd/grpcat@latest
```

## Quick Start

### CLI Tool

```bash
# Start proxy
grpcat -backend localhost:50051 -listen :8080 -v

# With routing
grpcat -backend localhost:50051 \
  -route "user.Service=localhost:50052" \
  -route "order.Service=localhost:50053" \
  -v

# Terminate TLS at the proxy, dial the backend over TLS, cap messages at 16MB
grpcat -backend backend.internal:50051 \
  -tls-cert server.pem -tls-key server-key.pem \
  -backend-tls -backend-ca ca.pem \
  -max-recv-size 16777216 \
  -backend-idle-timeout 5m
```

CLI flags:

| Flag                     | Description                                                      |
|--------------------------|------------------------------------------------------------------|
| `-backend`               | Default backend gRPC address (e.g. `localhost:50051`)            |
| `-listen`                | Listen address (default `:8080`)                                 |
| `-route`                 | Per-service route `service=backend`, repeatable                  |
| `-v`                     | Verbose logging                                                  |
| `-version`               | Print version and exit                                           |
| `-tls-cert` / `-tls-key` | PEM cert and key for serving TLS; must be given together         |
| `-backend-tls`           | Dial backends over TLS                                           |
| `-backend-ca`            | PEM CA bundle for verifying backends (implies TLS; system roots if unset) |
| `-max-recv-size`         | Max received message size in bytes (0 = unlimited)               |
| `-max-send-size`         | Max sent message size in bytes (0 = unlimited)                   |
| `-backend-idle-timeout`  | Evict pooled backend connections idle this long (0 = never)      |

The CLI covers the options that reduce to flags. `KeepaliveParams`,
`BackendDialOptions` and `Hooks` are library-only — use gRPCat as a package for
those.

### Library Usage

```go
package main

import (
    "context"
    "log"

    "github.com/c32lab/gRPCat/proxy"
)

func main() {
    server, err := proxy.NewServer(&proxy.Config{
        DefaultBackend: "localhost:50051",
    })
    if err != nil {
        log.Fatal(err)
    }

    server.Use(&LoggingMiddleware{})

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Start blocks until the listener stops; cancel ctx or call
    // server.Stop() from elsewhere to shut down gracefully.
    if err := server.Start(ctx, ":8080"); err != nil {
        log.Fatal(err)
    }
}
```

To attach extra services (health, reflection, your own gRPC handlers) on the
same listener, use `server.GetGRPCServer()` and register before `Start`.

### Config Options

```go
import (
    "github.com/c32lab/gRPCat/middleware"
    "github.com/c32lab/gRPCat/proxy"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/credentials"
    "google.golang.org/grpc/keepalive"
    "google.golang.org/grpc/status"
)

cfg := &proxy.Config{
    DefaultBackend:        "localhost:50051",
    KeepaliveParams:       &keepalive.ClientParameters{Time: 5 * time.Minute},
    BackendTransportCreds: credentials.NewTLS(tlsConfig), // nil = insecure
    BackendDialOptions:    []grpc.DialOption{grpc.WithStatsHandler(h)},
    ServerOptions:         []grpc.ServerOption{grpc.Creds(credentials.NewTLS(serverTLSConfig))},
    MaxRecvMsgSize:        16 << 20, // 0 = no limit
    MaxSendMsgSize:        16 << 20, // 0 = no limit
    Hooks: &proxy.Hooks{
        OnFirstFrameError: func(req *middleware.RequestInfo, err error) error {
            return status.Errorf(codes.InvalidArgument, "bad frame: %v", err)
        },
    },
}
```

- `KeepaliveParams` - Client keepalive for backend connections (nil = gRPC defaults).
- `BackendTransportCreds` - Transport credentials for backends (nil = insecure).
- `BackendDialOptions` - Additional dial options (stream interceptors, stats handlers, etc.). Unary interceptors have no effect because all RPCs are proxied as streams.
- `ServerOptions` - Additional server options for the listening side (TLS credentials to terminate TLS at the proxy, keepalive enforcement, max concurrent streams, stats handlers, etc.). Appended after the proxy's own options, so they win on conflict. Unary interceptors have no effect because all RPCs are handled as streams.
- `MaxRecvMsgSize` / `MaxSendMsgSize` - Message size limits in bytes, applied to both legs: the client-facing server side and the backend connections. **Zero means no limit** — this replaces grpc-go's 4 MB receive default, so an unset config forwards messages of any size. Set them explicitly on a public-facing deployment: an unbounded proxy buffers whatever a peer sends, which is a DoS surface.
  - A size option supplied through `ServerOptions` (listening side) or `BackendDialOptions` (backend side) is applied after these and therefore overrides them on that leg.
- `Hooks.OnFirstFrameError` - Called when the first client frame can't be read from the stream (transport error, cancellation, or an oversized message). Return non-nil to abort with that error; return nil to abort with the underlying read error. Either way gRPC has already sent the read status to the client, so the returned error only sets the server-side result. It is not called for well-formed requests — gRPC strips the message header before the codec runs, so there is no framing left for the proxy to get wrong.

## Writing Middleware

```go
type LoggingMiddleware struct{}

func (m *LoggingMiddleware) Handle(ctx *middleware.Context) {
    log.Printf("[%s] %s.%s", time.Now().Format(time.RFC3339),
        ctx.Request.Service, ctx.Request.Method)
    ctx.Next()
}
```

Inspecting the first request payload without a `.proto` file:

```go
func (m *AuthMiddleware) Handle(ctx *middleware.Context) {
    // RequestInfo.FirstPayload is the unframed protobuf body of the first
    // client message — a private copy taken off gRPC's transport buffers,
    // so it is safe to hold and read for as long as you like. Writing to it
    // does not change the request the backend receives; the proxy forwards
    // the original buffers. Use proto.Unmarshal into your own message type
    // if you have one, or treat it as opaque bytes for routing/auth.
    if !looksAuthorized(ctx.Request.FirstPayload) {
        ctx.AbortWithError(codes.PermissionDenied, "not allowed")
        return
    }
    ctx.Next()
}
```

### Middleware capabilities

**Control flow**
- `ctx.Next()` - Continue to next middleware. Gin-style: the chain runs to completion regardless of whether you call it; calling it explicitly is for the pre/post pattern (do work, `Next()`, do work).
- `ctx.AbortWithError(code, msg)` - Stop and return a gRPC error to the client.
- `ctx.SendResponse(data)` - Stop and return raw protobuf bytes to the client.
- `ctx.Abort()` - Stop the chain. Use only after setting a response yourself (via `SendResponse` / `AbortWithError`); a bare `Abort()` causes the client to receive `codes.Internal "middleware aborted without response"`.

**Routing & metadata**
- `ctx.SetBackend(addr)` - Override `Config.DefaultBackend` for this request.
- `ctx.AddMetadata(key, value)` - Add metadata to the backend request.

**State**
- `ctx.Set(key, value)` / `ctx.Get(key)` - Share data between middlewares (`any` value, thread-safe).
- `ctx.Request` - `Service`, `Method`, `Metadata`, `FirstPayload` (see streaming caveat below).

### Streaming caveat

`RequestInfo.FirstPayload` only holds the **first** client message. For
client-streaming and bidirectional RPCs, subsequent messages are forwarded
without passing through middleware. Don't rely on middleware for per-message
inspection of streaming RPCs — use a backend-side interceptor for that.

**See `cmd/grpcat/middlewares/` for complete examples.**

## How It Works

```
Client → gRPCat → Middleware Chain → Backend(s)
           │         ↓
           │      • Logging
           │      • Routing
           │      • Rate Limit
           │      • Custom Logic
           ↓
      Forward raw gRPC frames
      (no protobuf parsing)
```

Because gRPCat uses a custom codec that hands gRPC raw bytes instead of
parsed protobuf messages, it doesn't need `.proto` descriptors and avoids
per-message marshalling overhead. Middlewares get a parsed view of the
first frame for routing/auth decisions; everything else is byte-for-byte
forwarding — except for compressed messages, see below.

### Compression

**Compression is terminated at the proxy, not passed through.** gRPC
decompresses an incoming message before the codec sees it, so what gRPCat
forwards is the *uncompressed* frame. Concretely:

- **The proxy process must have the client's compressor registered**, otherwise
  compressed requests are rejected with
  `Unimplemented: grpc: Decompressor is not installed for grpc-encoding "gzip"`.
  Register it by importing the encoding package for its side effect, e.g.
  `import _ "google.golang.org/grpc/encoding/gzip"` (as `cmd/grpcat` does).
- **The proxy → backend leg is always uncompressed**, whatever the client sent.
  Bandwidth between proxy and backend is higher than the client asked for.
- **Responses to the client are compressed with the encoding the *client*
  requested**, because gRPC mirrors the request's encoding on the response. The
  encoding the *backend* chose is not propagated: a backend response compressed
  via `grpc.SetSendCompressor` is decompressed at the proxy and reaches a client
  that didn't request compression uncompressed.

End-to-end passthrough of compressed frames is not supported.

## Use Cases

- API Gateway for microservices
- gRPC load balancing
- Request logging & monitoring
- Service routing & traffic control

## License

MIT
