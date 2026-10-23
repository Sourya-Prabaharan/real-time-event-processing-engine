package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/sourya/streamflow/internal/cache"
	streamkafka "github.com/sourya/streamflow/internal/kafka"
	"github.com/sourya/streamflow/internal/processor"
	"github.com/sourya/streamflow/internal/storage"
)

func envOr(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if val := os.Getenv(key); val != "" {
		var n int
		if _, err := fmt.Sscanf(val, "%d", &n); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func main() {
	var (
		broker     = flag.String("broker", envOr("KAFKA_BROKER", "localhost:9092"), "Kafka broker address")
		dbDSN      = flag.String("db", envOr("POSTGRES_DSN", "postgres://streamflow:streamflow@localhost:5432/streamflow?sslmode=disable"), "PostgreSQL DSN")
		redisAddr  = flag.String("redis", envOr("REDIS_ADDR", "localhost:6379"), "Redis address")
		migrations = flag.String("migrations", envOr("MIGRATIONS_PATH", "migrations"), "Path to migration files")
		group      = flag.String("group", envOr("CONSUMER_GROUP", "streamflow-workers"), "Kafka consumer group ID")
		workers    = flag.Int("workers", envInt("WORKERS_COUNT", 4), "Number of concurrent processing goroutines")
	)
	flag.Parse()

	slog.Info("starting worker",
		"broker", *broker,
		"group", *group,
		"workers", *workers,
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
	slog.Info("database and migrations ready")

	// Connect to Redis (optional - graceful degradation).
	redisCache, err := cache.NewRedisCache(*redisAddr)
	if err != nil {
		slog.Warn("redis unavailable, running without cache", "error", err)
		redisCache = nil
	} else {
		defer redisCache.Close()
		slog.Info("redis connected")
	}

	store := storage.NewEventStore(db)
	dlqStore := storage.NewDLQStore(db)

	// Kafka Producer for DLQ forwarding
	dlqProducer := streamkafka.NewProducer([]string{*broker})
	defer dlqProducer.Close()

	consumer := streamkafka.NewConsumer([]string{*broker}, *group)
	defer consumer.Close()

	w := processor.NewWorker(consumer, store, dlqStore, dlqProducer, redisCache, *workers)
	if err := w.Run(ctx); err != nil {
		slog.Error("worker error", "error", err)
		os.Exit(1)
	}
}

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
	fmt.Fprintln(os.Stderr, "StreamFlow Worker")
}
