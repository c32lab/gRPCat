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

# Run tests. Mirrors CI, which runs with -race.
test:
	@echo "Running tests..."
	@go test -race ./...

# Show help
help:
	@echo "gRPCat - Customizable gRPC Proxy"
	@echo ""
	@echo "Usage:"
	@echo "  make build   - Build grpcat binary"
	@echo "  make clean   - Remove build artifacts"
	@echo "  make test    - Run tests with -race"
	@echo "  make help    - Show this help message"
