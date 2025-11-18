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

- **Zero-copy forwarding** - Proxies gRPC without deserializing protobuf
- **Middleware chain** - Logging, routing, rate limiting, etc.
- **All streaming modes** - Unary, server, client, bidirectional
- **Service agnostic** - No .proto files required

## Installation

```bash
go get github.com/c32lab/gRPCat
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
```

### Library Usage

```go
package main

import (
    "context"
    "github.com/c32lab/gRPCat/proxy"
    "github.com/c32lab/gRPCat/middleware"
)

func main() {
    server, _ := proxy.NewServer(&proxy.Config{
        DefaultBackend: "localhost:50051",
    })

    // Add middleware
    server.Use(&LoggingMiddleware{})

    server.Start(context.Background(), ":8080")
}
```

## Writing Middleware

```go
type LoggingMiddleware struct{}

func (m *LoggingMiddleware) Handle(ctx *middleware.Context) {
    log.Printf("[%s] %s/%s",
        ctx.Request.Service,
        ctx.Request.Method)

    ctx.Next() // Continue to next middleware
}
```

### Middleware Capabilities

- `ctx.Next()` - Continue to next middleware
- `ctx.Abort()` - Stop execution
- `ctx.SetBackend(addr)` - Route to specific backend
- `ctx.Set/Get(key, value)` - Share data between middlewares
- `ctx.Request` - Access service, method, metadata, payload

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

## Use Cases

- API Gateway for microservices
- gRPC load balancing
- Request logging & monitoring
- Service routing & traffic control

## License

MIT
