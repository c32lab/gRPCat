package middlewares

import (
	"log"
	"time"

	"github.com/c32lab/gRPCat/middleware"
)

type LoggingMiddleware struct{}

func NewLoggingMiddleware() *LoggingMiddleware {
	return &LoggingMiddleware{}
}

func (m *LoggingMiddleware) Handle(ctx *middleware.Context) {
	start := time.Now()

	log.Printf("[Request] %s/%s", ctx.Request.Service, ctx.Request.Method)

	ctx.Next()

	duration := time.Since(start)
	log.Printf("[Response] %s/%s - Duration: %v",
		ctx.Request.Service, ctx.Request.Method, duration)
}
