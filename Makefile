.PHONY: build clean test help

# Build the grpcat binary
build:
	@echo "Building grpcat..."
	@go build -o grpcat ./cmd/grpcat
	@echo "Build complete: ./grpcat"

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -f grpcat
	@echo "Clean complete"

# Run tests
test:
	@echo "Running tests..."
	@go test -v ./...

# Show help
help:
	@echo "gRPCat - Zero-copy gRPC Proxy"
	@echo ""
	@echo "Usage:"
	@echo "  make build   - Build grpcat binary"
	@echo "  make clean   - Remove build artifacts"
	@echo "  make test    - Run tests"
	@echo "  make help    - Show this help message"
