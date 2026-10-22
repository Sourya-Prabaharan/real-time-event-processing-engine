.PHONY: build run-infra stop-infra produce work api test bench benchmark fmt clean

# Build all binaries
build:
	go build -o bin/producer ./cmd/producer
	go build -o bin/worker ./cmd/worker
	go build -o bin/api ./cmd/api
	go build -o bin/benchmark ./cmd/benchmark

# Start Kafka, PostgreSQL, Redis, Prometheus, and Grafana
run-infra:
	docker compose up -d
	@echo "Waiting for Kafka and dependencies to be healthy..."
	@sleep 6

# Stop infrastructure
stop-infra:
	docker compose down

# Run the event producer (e.g. make produce RATE=1000)
produce: build
	./bin/producer -rate $(or $(RATE),100)

# Run workers with configurable concurrency (e.g. make work WORKERS=8)
work: build
	./bin/worker -workers $(or $(WORKERS),4)

# Run the REST API and metrics server
api: build
	./bin/api

# Run all unit, integration, and reliability tests
test:
	go test ./... -v -count=1

# Run Go standard benchmarks with allocations
bench:
	go test ./... -bench=. -benchmem -run=^$$

# Run multi-tier load-testing benchmark engine (1k, 5k, 10k events/sec)
benchmark: build
	./bin/benchmark -all-tiers

# Format and vet code
fmt:
	go fmt ./...
	go vet ./...

# Clean build artifacts
clean:
	rm -rf bin/
