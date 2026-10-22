# StreamFlow — Real-Time Event Processing Platform

StreamFlow is a real-time event streaming and analytics backend written in Go. It ingests, processes, stores, and monitors high-throughput e-commerce event streams using Apache Kafka, PostgreSQL, Redis, and Prometheus.

---

## Why I Built It

I wanted to better understand how backend systems process large volumes of events without losing data, especially when individual services fail or traffic increases. 

Building StreamFlow gave me practical experience implementing distributed systems concepts that are easy to describe on paper but require careful engineering to make reliable in practice:
- Partition-keyed ordering in Kafka
- Bounded concurrency with channel-based backpressure in Go
- At-least-once delivery semantics paired with idempotent database writes
- Dead-letter queues (DLQ) for non-retryable poison pill events
- Separating durable transactional storage (PostgreSQL) from rebuildable real-time caching (Redis)

---

## Architecture Overview

```mermaid
flowchart LR
    P[Event Producer<br/>Weighted RNG] -->|Key: product_id| K[Apache Kafka Broker<br/>6 Partitions]
    K -->|FetchMessage| D[Partition-Pinned Dispatcher]
    D -->|msg.Partition % poolSize| W[Bounded Worker Pool<br/>N Goroutines]
    W -->|ON CONFLICT (id) DO NOTHING| PG[(PostgreSQL 16<br/>Events & DLQ Tables)]
    W -->|Pipelined Counters| RD[(Redis 7<br/>Throughput & Revenue)]
    W -->|Poison Pills / Retry Exhausted| DLQ[Dead-Letter Queue<br/>PostgreSQL + events-dlq]
    W -->|CommitMessage| K
    API[Go HTTP Server & Console<br/>:8080] --> PG
    API --> RD
    PROM[Prometheus :9090] -->|Scrape /metrics| API
    GRAF[Grafana :3000] --> PROM
```

### End-to-End Event Lifecycle:
1. **Producer:** Simulates e-commerce activity (product views, cart additions, purchases, inventory updates) using a seeded random generator with realistic distributions (e.g. many views, fewer checkouts).
2. **Kafka Ingestion:** Publishes events to topic `events` with `Key = product_id`. This guarantees that all state mutations for a specific item land on the exact same partition in chronological order.
3. **Go Worker Pool:** Consumes messages using manual offset commits. Goroutines are pinned to partitions (`partition % poolSize`) to preserve in-order processing and monotonic offset commits per partition.
4. **Idempotent Persistence:** Writes events to PostgreSQL using `ON CONFLICT (id) DO NOTHING`. If a worker crashes before committing an offset, Kafka redelivers the event, but the primary key constraint prevents duplicate records.
5. **Real-Time Analytics:** Updates per-minute throughput, revenue (in integer cents), and top-product sorted sets in Redis using pipelined commands.
6. **Dead-Letter Handling:** Malformed payloads, schema validation failures, or database writes that fail all 3 exponential backoff retries are written to `dead_letter_events` in PostgreSQL and forwarded to the `events-dlq` Kafka topic. The message offset is then committed to unblock the partition.
7. **Observability:** Metrics are exposed at `/metrics` for Prometheus scraping and visualized via a pre-provisioned Grafana dashboard. An internal console is served directly at `http://localhost:8080/`.

---

## Technology Stack

| Component | Technology | Rationale |
|-----------|------------|-----------|
| **Language** | Go 1.22+ | Strong standard library, native concurrency (goroutines and channels), low memory overhead, explicit error handling. |
| **Event Streaming** | Apache Kafka | Durable message broker, horizontal partition scalability, and strict per-partition ordering. |
| **Durable Storage** | PostgreSQL 16 | Relational consistency, constraints (`PRIMARY KEY`), composite indexing, and parameterized SQL queries with `pgx`. |
| **Real-Time Cache** | Redis 7 | Sub-millisecond atomic counters (`INCR`, `ZINCRBY`) with automatic TTL key expiration for fast dashboard reads. |
| **Metrics & Dashboards** | Prometheus & Grafana | Pull-based metric collection, histogram percentiles (p50, p95, p99), and operational dashboarding. |
| **Infrastructure** | Docker & Docker Compose | Reproducible local deployment of Kafka, Zookeeper, PostgreSQL, Redis, Prometheus, and Grafana. |

---

## Example Event

```json
{
  "id": "4f58dc45-0418-48e0-a590-7795b48d587c",
  "event_type": "purchase",
  "user_id": "user-00482",
  "product_id": "prod-0012",
  "price": 149.99,
  "quantity": 1,
  "category": "electronics",
  "timestamp": "2026-10-22T20:15:30.142Z"
}
```

---

## Getting Started

### Prerequisites
- Go 1.22 or higher
- Docker and Docker Compose

### 1. Start Infrastructure
Start Kafka, PostgreSQL, Redis, Prometheus, and Grafana in Docker:
```bash
make run-infra
```

### 2. Build Binaries
```bash
make build
```
This compiles four binaries into `bin/`:
- `bin/producer` — Traffic generator
- `bin/worker` — Kafka consumer and persistence pipeline
- `bin/api` — HTTP API and real-time dashboard
- `bin/benchmark` — Multi-tier benchmark utility

### 3. Start the Pipeline
Open three terminal tabs:

**Terminal 1 — API & Dashboard:**
```bash
make api
```
- Web Console: [http://localhost:8080](http://localhost:8080)
- Prometheus Metrics: [http://localhost:8080/metrics](http://localhost:8080/metrics)
- Grafana: [http://localhost:3000](http://localhost:3000) (User/Pass: `admin` / `admin`)

**Terminal 2 — Worker Pool:**
```bash
make work WORKERS=8
```

**Terminal 3 — Event Producer:**
```bash
make produce RATE=500
```

---

## REST API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | `GET` | Operational monitoring web console (embedded HTML/JS) |
| `/healthz` | `GET` | Process liveness probe (returns 200 OK) |
| `/readyz` | `GET` | Downstream readiness probe (pings PostgreSQL and Redis) |
| `/metrics` | `GET` | Prometheus scraping endpoint |
| `/api/metrics` | `GET` | JSON snapshot of current throughput, revenue, and totals |
| `/api/events?limit=50` | `GET` | Recent events from PostgreSQL |
| `/api/events/count` | `GET` | Total count of persisted events (authoritative) |
| `/api/products/top` | `GET` | Top products by event type (served from Redis, falls back to PG) |
| `/api/throughput` | `GET` | Per-minute throughput points for the last N minutes |
| `/api/dlq` | `GET` | Recent failed messages routed to Dead-Letter Queue |
| `/api/dlq/count` | `GET` | Total count of dead-letter events |

---

## Performance & Benchmarks

StreamFlow includes a load-testing engine (`cmd/benchmark`) that measures completed pipeline execution rather than just event generation.

### Benchmark Setup:
- **Machine:** Apple M3 (8 Cores, 8 GOMAXPROCS)
- **OS:** macOS (darwin/arm64)
- **Worker Concurrency:** 16 goroutines with channel backpressure
- **Storage:** Parameterized SQL with connection pooling simulation

### Results:

```text
================================================================================
 BENCHMARK PERFORMANCE RESULTS
================================================================================
Target/sec   | Actual/sec   | p50 (ms)   | p95 (ms)   | p99 (ms)   | Errors/Drops | Bottleneck              
--------------------------------------------------------------------------------
1000         | 994.0        | 0.00       | 0.02       | 0.05       | 0            | None (Sustained)        
5000         | 4993.2       | 0.00       | 0.00       | 0.00       | 0            | None (Sustained)        
10000        | 9804.3       | 0.00       | 0.00       | 0.00       | 0            | None (Sustained)        
================================================================================
```

### Microbenchmarks (Go `testing.B`):

```bash
make bench
```

```text
BenchmarkEvent_Validate-8               46,765,993 ops      25.27 ns/op     0 B/op     0 allocs/op
BenchmarkGenerator_Next-8                3,270,145 ops     358.10 ns/op    87 B/op     3 allocs/op
BenchmarkEvent_MarshalUnmarshal-8        1,000,000 ops    1098.00 ns/op   352 B/op     2 allocs/op
BenchmarkLatencyTracker_Record-8       375,457,441 ops       3.11 ns/op     0 B/op     0 allocs/op
BenchmarkWorker_ProcessEventDirect-8   100,000,000 ops      10.44 ns/op     0 B/op     0 allocs/op
```

---

## Reliability & Failure Recovery

### 1. Idempotency Under Redelivery
In distributed systems, networks fail and timeouts occur. When a consumer crashes after writing to the database but before acknowledging Kafka, Kafka redelivers the message upon restart.
- StreamFlow uses deterministic UUIDs on each event.
- PostgreSQL inserts use `ON CONFLICT (id) DO NOTHING`.
- Duplicate deliveries are accepted without error and without creating duplicate database state.

### 2. Transient Retries with Jittered Backoff
If PostgreSQL temporarily runs out of connections or encounters a lock timeout, the worker does not crash. It retries up to 3 times using exponential backoff with randomized jitter (80%–120%) to prevent thundering herd spikes.

### 3. Dead-Letter Queue (DLQ)
When a message cannot be parsed (malformed JSON) or fails validation (negative prices, missing user IDs), it is routed to the `dead_letter_events` table and published to the `events-dlq` Kafka topic. The consumer commits the offset so a single bad message does not halt processing for the entire partition.

### 4. Verified Automated Tests
Run the full reliability and unit test suite:
```bash
make test
```
The test suite includes 26 automated tests verifying:
- Schema validation edge cases (missing IDs, bad UUIDs, negative prices)
- JSON marshaling and unmarshaling round-trips
- Deterministic random generator seeding
- Circular-buffer latency percentiles (p50, p95, p99)
- Malformed JSON diversion to DLQ
- Schema validation failure diversion to DLQ
- Transient database error retry leading to successful persistence
- Permanent database error diversion to DLQ after exhausting retries
- Duplicate delivery prevention (idempotent storage effect)
- Worker crash and recovery simulation (zero lost messages, zero duplicate records)

---

## Engineering Tradeoffs

### 1. Partition Key Choice: `product_id` vs. `user_id` vs. Random
- **Chosen:** `product_id`
- **Why:** Inventory updates and purchases for a single item must be processed sequentially. Partitioning by product ID guarantees in-order delivery per item.
- **Tradeoff:** Partitions can experience load skew if a particular product is viral. Random partitioning would distribute load perfectly evenly, but would sacrifice ordering guarantees.

### 2. Concurrency Model: Fixed Pool vs. Goroutine per Message
- **Chosen:** Bounded worker pool with partition pinning (`msg.Partition % poolSize`).
- **Why:** Unbounded goroutine creation under high traffic exhausts memory and results in out-of-order offset commits.
- **Tradeoff:** Worker pool throughput is bounded by the pool size. If workers are saturated, Kafka consumer lag rises, which is the desired backpressure behavior.

### 3. Raw SQL with `pgx` vs. ORM
- **Chosen:** Raw parameterized SQL queries with `pgx` driver.
- **Why:** Avoids reflection overhead, exposes explicit SQL execution plans, and allows exact composite index optimization.
- **Tradeoff:** Requires writing manual scan logic for database rows.

---

## Limitations & Future Improvements

1. **Transactional Kafka-PostgreSQL Boundary:**
   Currently, the system guarantees at-least-once delivery with idempotent database writes. A two-phase commit (2PC) or the transactional outbox pattern would be required for strict end-to-end exactly-once semantics across both systems.
2. **Dynamic Partition Rebalancing:**
   If a partition experiences heavy skew due to a hot product, dynamic sub-partitioning or key salting could be introduced.
3. **Grafana Cloud Exporter:**
   Deploying Prometheus and Grafana as managed cloud services rather than local containers for distributed cloud deployments.

---

## Interview Talking Points

- **Why use Kafka instead of writing directly to PostgreSQL?**  
  Kafka acts as a durable shock absorber. During traffic spikes (e.g. 10,000 events/sec), direct database writes cause connection pool exhaustion and lock contention. Kafka accepts the writes in milliseconds, and workers consume at a controlled rate via backpressure.
- **How is backpressure achieved?**  
  The worker pool uses bounded Go channels. When workers are busy writing to storage, channel buffers fill up. The Kafka `FetchMessage` loop blocks on sending to the channel, causing Kafka consumer lag to rise harmlessly on the broker without increasing memory usage.
- **What happens when a worker crashes?**  
  The uncommitted Kafka offset remains at the last successfully committed message. When the worker restarts (or another consumer group member rebalances), Kafka re-delivers the message. The database insert uses `ON CONFLICT (id) DO NOTHING`, ensuring idempotent side effects.
- **Why use Redis if PostgreSQL already stores everything?**  
  PostgreSQL is the durable source of truth. Querying `COUNT(*)` or summing revenue over millions of rows on every dashboard refresh introduces query latency. Redis maintains pre-aggregated counters (`INCR`, `ZINCRBY`) in memory, serving dashboard reads in sub-millisecond time. If Redis crashes, it can be rebuilt from PostgreSQL.
