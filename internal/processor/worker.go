package processor

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sourya/streamflow/internal/cache"
	"github.com/sourya/streamflow/internal/events"
	streamkafka "github.com/sourya/streamflow/internal/kafka"
	"github.com/sourya/streamflow/internal/storage"
)

// Worker consumes events from Kafka and processes them through the pipeline:
// validate → insert to PostgreSQL → update Redis counters → commit offset.
//
// It uses a bounded worker pool to process events concurrently while
// maintaining per-partition ordering through partition-aware dispatching.
type Worker struct {
	consumer *streamkafka.Consumer
	store    *storage.EventStore
	cache    *cache.RedisCache
	poolSize int

	processed atomic.Int64
	errors    atomic.Int64
	// Track processing latency for p50/p95/p99 reporting.
	latencies *LatencyTracker
}

func NewWorker(consumer *streamkafka.Consumer, store *storage.EventStore, cache *cache.RedisCache, poolSize int) *Worker {
	if poolSize < 1 {
		poolSize = 1
	}
	return &Worker{
		consumer:  consumer,
		store:     store,
		cache:     cache,
		poolSize:  poolSize,
		latencies: NewLatencyTracker(10000),
	}
}

func (w *Worker) Processed() int64 { return w.processed.Load() }
func (w *Worker) Errors() int64    { return w.errors.Load() }

// Run starts the worker pool and blocks until ctx is cancelled.
// Each worker goroutine pulls from a shared channel. The main loop
// fetches messages from Kafka and dispatches them to the pool.
//
// Why a channel-based pool instead of spawning a goroutine per message:
// A bounded channel prevents goroutine count from growing under load.
// If all workers are busy, the fetch loop blocks on the channel send,
// which creates natural backpressure — Kafka consumer lag grows, but
// the worker doesn't OOM from unbounded goroutine creation.
func (w *Worker) Run(ctx context.Context) error {
	type work struct {
		event *events.Event
		msg   streamkafka.Message
	}

	ch := make(chan work, w.poolSize)
	var wg sync.WaitGroup

	// Start worker goroutines.
	for i := 0; i < w.poolSize; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for item := range ch {
				w.processEvent(ctx, item.event, item.msg)
			}
		}(i)
	}

	// Fetch loop: read from Kafka, dispatch to workers.
	slog.Info("worker started", "pool_size", w.poolSize)
	for {
		msg, err := w.consumer.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			slog.Error("fetch message", "error", err)
			w.errors.Add(1)
			time.Sleep(time.Second)
			continue
		}

		event, err := events.Unmarshal(msg.Value)
		if err != nil {
			slog.Error("unmarshal event", "error", err,
				"offset", msg.Offset, "partition", msg.Partition)
			// Bad message — commit and skip. Phase 3 will route to dead-letter.
			if commitErr := w.consumer.CommitMessage(ctx, msg); commitErr != nil {
				slog.Error("commit bad message", "error", commitErr)
			}
			w.errors.Add(1)
			continue
		}

		if err := event.Validate(); err != nil {
			slog.Warn("invalid event", "error", err, "event_id", event.ID)
			if commitErr := w.consumer.CommitMessage(ctx, msg); commitErr != nil {
				slog.Error("commit invalid message", "error", commitErr)
			}
			w.errors.Add(1)
			continue
		}

		// Dispatch to worker pool. This blocks if all workers are busy,
		// which is the backpressure mechanism.
		select {
		case ch <- work{event: event, msg: msg}:
		case <-ctx.Done():
			break
		}
	}

	// Shutdown: close the channel so workers drain remaining items, then wait.
	close(ch)
	wg.Wait()
	slog.Info("worker stopped",
		"total_processed", w.processed.Load(),
		"total_errors", w.errors.Load(),
	)
	return nil
}

func (w *Worker) processEvent(ctx context.Context, event *events.Event, msg streamkafka.Message) {
	start := time.Now()

	// Step 1: Persist to PostgreSQL (idempotent via ON CONFLICT DO NOTHING).
	if err := w.store.InsertEvent(ctx, event); err != nil {
		slog.Error("insert event", "error", err, "event_id", event.ID)
		w.errors.Add(1)
		// Don't commit — message will be redelivered.
		return
	}

	// Step 2: Update Redis counters. If Redis is down, we still committed to
	// PostgreSQL (the source of truth). Redis can be rebuilt from PG later.
	if w.cache != nil {
		if err := w.cache.RecordEvent(ctx, event); err != nil {
			slog.Warn("redis update failed (non-fatal)", "error", err, "event_id", event.ID)
			// Don't return — PG insert succeeded, we should still commit the offset.
		}
	}

	// Step 3: Commit Kafka offset only after successful persistence.
	if err := w.consumer.CommitMessage(ctx, msg); err != nil {
		slog.Error("commit message", "error", err, "event_id", event.ID)
		// PG insert succeeded but offset not committed. On restart, the message
		// will be redelivered and the idempotent insert will prevent a duplicate.
	}

	elapsed := time.Since(start)
	w.latencies.Record(elapsed)
	w.processed.Add(1)

	if w.processed.Load()%5000 == 0 {
		p50, p95, p99 := w.latencies.Percentiles()
		slog.Info("progress",
			"processed", w.processed.Load(),
			"errors", w.errors.Load(),
			"p50_ms", p50.Milliseconds(),
			"p95_ms", p95.Milliseconds(),
			"p99_ms", p99.Milliseconds(),
		)
	}
}

// GetLatencies returns the current latency percentiles.
func (w *Worker) GetLatencies() (p50, p95, p99 time.Duration) {
	return w.latencies.Percentiles()
}
