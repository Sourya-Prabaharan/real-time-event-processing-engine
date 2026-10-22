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
)

func main() {
	var (
		broker = flag.String("broker", "localhost:9092", "Kafka broker address")
		rate   = flag.Int("rate", 100, "Events per second")
		seed   = flag.Int64("seed", 0, "Random seed (0 = use current time)")
		count  = flag.Int("count", 0, "Total events to produce (0 = unlimited)")
	)
	flag.Parse()

	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}

	slog.Info("starting producer",
		"broker", *broker,
		"rate", *rate,
		"seed", *seed,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Ensure the topic exists before producing.
	if err := streamkafka.EnsureTopic(ctx, []string{*broker}, 6); err != nil {
		slog.Error("ensure topic", "error", err)
		os.Exit(1)
	}

	producer := streamkafka.NewProducer([]string{*broker})
	defer producer.Close()

	gen := events.NewGenerator(*seed)
	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	defer ticker.Stop()

	var produced int
	for {
		select {
		case <-ctx.Done():
			slog.Info("shutting down producer", "total_produced", produced)
			return
		case <-ticker.C:
			event := gen.Next()
			data, err := event.Marshal()
			if err != nil {
				slog.Error("marshal event", "error", err)
				continue
			}
			if err := producer.Publish(ctx, event.ProductID, data); err != nil {
				slog.Error("publish event", "error", err, "event_id", event.ID)
				continue
			}
			produced++
			if produced%1000 == 0 {
				slog.Info("progress", "produced", produced)
			}
			if *count > 0 && produced >= *count {
				slog.Info("reached target count", "total_produced", produced)
				return
			}
		}
	}
}

func init() {
	// Use text handler for readable local development logs.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
	fmt.Fprintln(os.Stderr, "StreamFlow Event Producer")
}
