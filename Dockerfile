# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Install build tools
RUN apk add --no-cache git gcc musl-dev

# Cache dependency layer
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and migrations
COPY . .

# Compile optimized static binaries
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/producer ./cmd/producer
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/worker ./cmd/worker
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/benchmark ./cmd/benchmark

# Final minimal runtime image
FROM alpine:3.19 AS runner

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy binaries and migration SQL files
COPY --from=builder /bin/producer /app/producer
COPY --from=builder /bin/worker /app/worker
COPY --from=builder /bin/api /app/api
COPY --from=builder /bin/benchmark /app/benchmark
COPY --from=builder /app/migrations /app/migrations

EXPOSE 8080

ENTRYPOINT ["/app/api"]
