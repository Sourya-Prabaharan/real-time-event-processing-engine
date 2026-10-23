.PHONY: build run-infra stop-infra produce work api test test-race bench benchmark fmt clean docker-build

# Build all binaries locally
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

# Run tests with race detector enabled
test-race:
	go test -race ./... -v -count=1

# Run Go standard benchmarks with allocations
bench:
	go test ./... -bench=. -benchmem -run=^$$

# Run multi-tier in-memory pipeline benchmark (1k, 5k, 10k events/sec)
benchmark: build
	./bin/benchmark -mode=in-memory -all-tiers

# Run end-to-end distributed benchmark (Kafka -> Worker -> PostgreSQL)
benchmark-e2e: build
	./bin/benchmark -mode=e2e -rate=$(or $(RATE),1000) -duration=5s

# Build multi-stage Docker image
docker-build:
	docker build -t streamflow:latest .

# Format and vet code
fmt:
	go fmt ./...
	go vet ./...

# Clean build artifacts
clean:
	rm -rf bin/
