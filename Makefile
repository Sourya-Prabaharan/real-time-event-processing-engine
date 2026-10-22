.PHONY: build run-infra stop-infra produce work api test fmt

# Build all binaries
build:
	go build -o bin/producer ./cmd/producer
	go build -o bin/worker ./cmd/worker
	go build -o bin/api ./cmd/api

# Start Kafka, PostgreSQL, and Redis
run-infra:
	docker compose up -d
	@echo "Waiting for Kafka to be ready..."
	@sleep 5

# Stop infrastructure
stop-infra:
	docker compose down

# Run the event producer (default: 100 events/sec)
produce: build
	./bin/producer -rate $(or $(RATE),100)

# Run a single worker
work: build
	./bin/worker

# Run the API server
api: build
	./bin/api

# Run all tests
test:
	go test ./... -v -count=1

# Format code
fmt:
	go fmt ./...
	go vet ./...

# Clean build artifacts
clean:
	rm -rf bin/
