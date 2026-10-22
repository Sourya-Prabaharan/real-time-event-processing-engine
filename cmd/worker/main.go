package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sourya/streamflow/internal/events"
	streamkafka "github.com/sourya/streamflow/internal/kafka"
	"github.com/sourya/streamflow/internal/storage"
)

func main() {
	var (
		broker     = flag.String("broker", "localhost:9092", "Kafka broker address")
		dbDSN      = flag.String("db", "postgres://streamflow:streamflow@localhost:5432/streamflow?sslmode=disable", "PostgreSQL DSN")
		migrations = flag.String("migrations", "migrations", "Path to migration files")
		group      = flag.String("group", "streamflow-workers", "Kafka consumer group ID")
	)
	flag.Parse()

	slog.Info("starting worker",
		"broker", *broker,
		"group", *group,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Connect to PostgreSQL and run migrations.
	db, err := storage.Connect(ctx, *dbDSN)
	if err != nil {
		slog.Error("connect to database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := storage.RunMigrations(ctx, db, *migrations); err != nil {
		slog.Error("run migrations", "error", err)
		os.Exit(1)
	}
	slog.Info("database ready")

	store := storage.NewEventStore(db)
	consumer := streamkafka.NewConsumer([]string{*broker}, *group)
	defer consumer.Close()

	var processed, errors int
	for {
		msg, err := consumer.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break // shutdown signal
			}
			slog.Error("fetch message", "error", err)
			time.Sleep(time.Second) // brief backoff before retry
			continue
		}

		event, err := events.Unmarshal(msg.Value)
		if err != nil {
			slog.Error("unmarshal event", "error", err, "offset", msg.Offset, "partition", msg.Partition)
			// Commit the offset anyway — a malformed message won't become valid
			// on retry. In Phase 3 we'll route these to a dead-letter topic.
			if err := consumer.CommitMessage(ctx, msg); err != nil {
				slog.Error("commit bad message", "error", err)
			}
			errors++
			continue
		}

		if err := event.Validate(); err != nil {
			slog.Warn("invalid event", "error", err, "event_id", event.ID)
			if err := consumer.CommitMessage(ctx, msg); err != nil {
				slog.Error("commit invalid message", "error", err)
			}
			errors++
			continue
		}

		// Insert with idempotency — ON CONFLICT DO NOTHING handles redeliveries.
		if err := store.InsertEvent(ctx, event); err != nil {
			slog.Error("insert event", "error", err, "event_id", event.ID)
			// Don't commit — the message will be redelivered on next fetch.
			// This is the at-least-once guarantee in action.
			errors++
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// Only commit after successful database insert.
		if err := consumer.CommitMessage(ctx, msg); err != nil {
			slog.Error("commit message", "error", err, "event_id", event.ID)
			// The event was inserted but the offset wasn't committed.
			// On restart this message will be redelivered, but the idempotent
			// insert will prevent a duplicate row.
		}

		processed++
		if processed%1000 == 0 {
			slog.Info("progress", "processed", processed, "errors", errors)
		}
	}

	slog.Info("worker stopped", "total_processed", processed, "total_errors", errors)
}

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
	fmt.Fprintln(os.Stderr, "StreamFlow Worker")
}
