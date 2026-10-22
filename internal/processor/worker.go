package processor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sourya/streamflow/internal/events"
	streamkafka "github.com/sourya/streamflow/internal/kafka"
	"github.com/sourya/streamflow/internal/metrics"
	"github.com/sourya/streamflow/internal/retry"
)

// MessageConsumer defines the reader interface required by the worker.
// Idiomatic Go: consumer accepted as interface to enable unit testing and crash simulations.
type MessageConsumer interface {
	FetchMessage(ctx context.Context) (streamkafka.Message, error)
	CommitMessage(ctx context.Context, msg streamkafka.Message) error
	Close() error
}

// EventPersister defines the database persistence contract.
type EventPersister interface {
	InsertEvent(ctx context.Context, e *events.Event) error
}

// DeadLetterRecorder persists unprocessable messages for auditing and re-drive.
type DeadLetterRecorder interface {
	RecordDeadLetter(ctx context.Context, topic string, partition int, offset int64, payload []byte, reason string) error
}

// DeadLetterPublisher publishes failed messages to a Kafka DLQ topic.
type DeadLetterPublisher interface {
	PublishDLQ(ctx context.Context, key string, value []byte, reason string) error
}

// EventCache updates real-time analytics counters.
type EventCache interface {
	RecordEvent(ctx context.Context, e *events.Event) error
}

// Worker coordinates Kafka consumption, idempotent database persistence,
// exponential backoff retries, dead-letter routing, and offset commits.
type Worker struct {
	consumer     MessageConsumer
	store        EventPersister
	dlqStore     DeadLetterRecorder
	dlqPublisher DeadLetterPublisher
	cache        EventCache
	poolSize     int
	retryCfg     retry.Config

	processed atomic.Int64
	errors    atomic.Int64
	dlqCount  atomic.Int64
	latencies *LatencyTracker
}

func NewWorker(
	consumer MessageConsumer,
	store EventPersister,
	dlqStore DeadLetterRecorder,
	dlqPublisher DeadLetterPublisher,
	cache EventCache,
	poolSize int,
) *Worker {
	if poolSize < 1 {
		poolSize = 1
	}
	return &Worker{
		consumer:     consumer,
		store:        store,
		dlqStore:     dlqStore,
		dlqPublisher: dlqPublisher,
		cache:        cache,
		poolSize:     poolSize,
		retryCfg:     retry.DefaultConfig(),
		latencies:    NewLatencyTracker(10000),
	}
}

func (w *Worker) SetRetryConfig(cfg retry.Config) {
	w.retryCfg = cfg
}

func (w *Worker) Processed() int64 { return w.processed.Load() }
func (w *Worker) Errors() int64    { return w.errors.Load() }
func (w *Worker) DLQCount() int64  { return w.dlqCount.Load() }

// Run starts partition-pinned worker goroutines and the main fetch loop.
//
// Concurrency & Ordering Guarantee:
// Instead of random worker assignment, messages are dispatched by partition ID
// (partition % poolSize). This guarantees that:
//  1. All events from a given Kafka partition are processed sequentially.
//  2. Offsets for each partition are committed monotonically, preventing
//     the classic race condition where offset N+1 commits before offset N finishes.
//  3. Goroutine count is strictly bounded by poolSize.
func (w *Worker) Run(ctx context.Context) error {
	queues := make([]chan streamkafka.Message, w.poolSize)
	for i := range queues {
		queues[i] = make(chan streamkafka.Message, 100)
	}

	var wg sync.WaitGroup
	for i := 0; i < w.poolSize; i++ {
		wg.Add(1)
		go func(workerID int, ch <-chan streamkafka.Message) {
			defer wg.Done()
			for msg := range ch {
				w.handleMessage(ctx, msg)
			}
		}(i, queues[i])
	}

	slog.Info("worker pool started", "pool_size", w.poolSize)

	for {
		msg, err := w.consumer.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			slog.Error("fetch message", "error", err)
			w.errors.Add(1)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		// Partition pinning: route by partition to guarantee per-partition sequence
		targetWorker := int(msg.Partition) % w.poolSize
		if targetWorker < 0 {
			targetWorker = -targetWorker
		}

		select {
		case queues[targetWorker] <- msg:
		case <-ctx.Done():
			break
		}
	}

	// Graceful shutdown: close input queues and wait for in-flight messages to drain
	for _, ch := range queues {
		close(ch)
	}
	wg.Wait()

	slog.Info("worker pool drained and stopped",
		"total_processed", w.processed.Load(),
		"total_errors", w.errors.Load(),
		"total_dlq", w.dlqCount.Load(),
	)
	return nil
}

func (w *Worker) handleMessage(ctx context.Context, msg streamkafka.Message) {
	start := time.Now()

	// 1. Deserialization
	event, err := events.Unmarshal(msg.Value)
	if err != nil {
		w.routeToDLQ(ctx, msg, fmt.Sprintf("unmarshal error: %v", err))
		return
	}

	// 2. Schema Validation
	if err := event.Validate(); err != nil {
		w.routeToDLQ(ctx, msg, fmt.Sprintf("validation error: %v", err))
		return
	}

	// 3. Idempotent Database Insert with Exponential Backoff Retry
	insertOp := func(opCtx context.Context) error {
		return w.store.InsertEvent(opCtx, event)
	}

	if err := retry.Do(ctx, w.retryCfg, insertOp); err != nil {
		slog.Error("database insert exhausted retries", "error", err, "event_id", event.ID)
		w.routeToDLQ(ctx, msg, fmt.Sprintf("database write exhausted retries: %v", err))
		return
	}

	// 4. Redis Cache Update (Non-blocking / Best-effort)
	// The database is the authoritative source of truth. Redis failure does not
	// block message commit or pipeline forward progress.
	if w.cache != nil {
		if err := w.cache.RecordEvent(ctx, event); err != nil {
			slog.Warn("redis update failed (non-fatal)", "error", err, "event_id", event.ID)
		}
	}

	// 5. Commit Offset (At-least-once guarantee)
	if err := w.consumer.CommitMessage(ctx, msg); err != nil {
		slog.Error("commit offset failed", "error", err, "event_id", event.ID)
		w.errors.Add(1)
		return
	}

	elapsed := time.Since(start)
	w.latencies.Record(elapsed)
	w.processed.Add(1)

	// Prometheus metrics
	metrics.EventsProcessedTotal.WithLabelValues(string(event.Type), "success").Inc()
	metrics.ProcessingDurationSeconds.WithLabelValues(string(event.Type)).Observe(elapsed.Seconds())

	if w.processed.Load()%5000 == 0 {
		p50, p95, p99 := w.latencies.Percentiles()
		slog.Info("processing milestone",
			"processed", w.processed.Load(),
			"errors", w.errors.Load(),
			"dlq", w.dlqCount.Load(),
			"p50_ms", p50.Milliseconds(),
			"p95_ms", p95.Milliseconds(),
			"p99_ms", p99.Milliseconds(),
		)
	}
}

// routeToDLQ handles permanently failed messages by logging to DLQ storage/Kafka
// and committing the message offset so the partition is not blocked indefinitely.
func (w *Worker) routeToDLQ(ctx context.Context, msg streamkafka.Message, reason string) {
	w.errors.Add(1)
	w.dlqCount.Add(1)

	metrics.EventsProcessedTotal.WithLabelValues("unknown", "failure").Inc()
	metrics.DeadLetterEventsTotal.WithLabelValues(reason).Inc()

	slog.Warn("routing message to dead-letter queue",
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
		"reason", reason,
	)

	// Persist to DLQ table in PostgreSQL
	if w.dlqStore != nil {
		if err := w.dlqStore.RecordDeadLetter(ctx, msg.Topic, int(msg.Partition), msg.Offset, msg.Value, reason); err != nil {
			slog.Error("failed to record dead letter in database", "error", err)
		}
	}

	// Publish to Kafka events-dlq topic if publisher available
	if w.dlqPublisher != nil {
		if err := w.dlqPublisher.PublishDLQ(ctx, string(msg.Key), msg.Value, reason); err != nil {
			slog.Error("failed to publish to kafka dlq topic", "error", err)
		}
	}

	// Commit offset to avoid poison-pill retry loop
	if err := w.consumer.CommitMessage(ctx, msg); err != nil {
		slog.Error("failed to commit offset after dlq routing", "error", err)
	}
}

func (w *Worker) GetLatencies() (p50, p95, p99 time.Duration) {
	return w.latencies.Percentiles()
}
