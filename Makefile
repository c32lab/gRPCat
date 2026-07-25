.PHONY: example test help

# Run the example proxy (see examples/proxy)
example:
	@go run ./examples/proxy $(ARGS)

# Run tests. Mirrors CI, which runs with -race.
test:
	@echo "Running tests..."
	@go test -race ./...

# Show help
help:
	@echo "gRPCat - Customizable gRPC Proxy"
	@echo ""
	@echo "Usage:"
	@echo "  make example ARGS='-backend localhost:50051 -v'  - Run the example proxy"
	@echo "  make test                                        - Run tests with -race"
	@echo "  make help                                        - Show this help message"
