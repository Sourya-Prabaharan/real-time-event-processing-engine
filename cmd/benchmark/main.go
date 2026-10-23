package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sourya/streamflow/internal/events"
	streamkafka "github.com/sourya/streamflow/internal/kafka"
	"github.com/sourya/streamflow/internal/processor"
	"github.com/sourya/streamflow/internal/storage"
)

type BenchmarkResult struct {
	Mode             string
	TargetRate       int
	ActualRate       float64
	TotalEvents      int64
	CompletedEvents  int64
	Duration         time.Duration
	P50Latency       time.Duration
	P95Latency       time.Duration
	P99Latency       time.Duration
	ErrorCount       int64
	BottleneckReason string
}

func main() {
	var (
		mode       = flag.String("mode", "auto", "Benchmark mode: 'auto' (detects infra), 'e2e' (live Kafka + PostgreSQL), or 'in-memory' (baseline)")
		targetRate = flag.Int("rate", 1000, "Target events per second (e.g. 500, 1000, 2500)")
		duration   = flag.Duration("duration", 5*time.Second, "Benchmark duration per tier")
		runAll     = flag.Bool("all-tiers", false, "Run across multiple throughput tiers")
		poolSize   = flag.Int("workers", runtime.NumCPU()*2, "Worker pool concurrency")
		broker     = flag.String("broker", "localhost:9092", "Kafka broker address (for e2e mode)")
		dbDSN      = flag.String("db", "postgres://streamflow:streamflow@localhost:5432/streamflow?sslmode=disable", "PostgreSQL DSN (for e2e mode)")
	)
	flag.Parse()

	selectedMode := *mode
	if selectedMode == "auto" {
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		db, dbErr := storage.Connect(probeCtx, *dbDSN)
		var kErr error
		if dbErr == nil {
			db.Close()
			kErr = streamkafka.EnsureTopic(probeCtx, []string{*broker}, 6)
		}
		probeCancel()

		if dbErr == nil && kErr == nil {
			selectedMode = "e2e"
			fmt.Printf("[AUTO-DETECT] Infrastructure detected (PostgreSQL + Kafka). Running genuine End-to-End Distributed Pipeline Benchmark.\n")
		} else {
			selectedMode = "in-memory"
			fmt.Printf("[AUTO-DETECT] Infrastructure not running (run 'make run-infra' to enable). Falling back to In-Memory Concurrency Baseline.\n")
		}
	}

	fmt.Printf("================================================================================\n")
	fmt.Printf(" StreamFlow Ingestion & Processing Benchmark Engine\n")
	fmt.Printf(" Mode: %s | System: %s/%s | CPUs: %d | GOMAXPROCS: %d\n", selectedMode, runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.GOMAXPROCS(0))
	fmt.Printf("================================================================================\n\n")

	if selectedMode == "e2e" {
		rates := []int{*targetRate}
		if *runAll {
			rates = []int{500, 1000, 2000}
		}

		var results []BenchmarkResult
		for _, rate := range rates {
			fmt.Printf("--> Running E2E Tier: Target %d events/sec for %v (Workers: %d)...\n", rate, *duration, *poolSize)
			res := runE2ETier(*broker, *dbDSN, rate, *duration, *poolSize)
			results = append(results, res)
			time.Sleep(500 * time.Millisecond)
		}

		printSummary(results, "END-TO-END DISTRIBUTED PIPELINE RESULTS (Kafka -> Worker Pool -> PostgreSQL)")
		return
	}

	// In-Memory Baseline
	fmt.Printf("NOTE: Running In-Memory Baseline Benchmark.\n")
	fmt.Printf("This measures Go runtime channel dispatch, serialization, and validation without external I/O.\n\n")

	rates := []int{*targetRate}
	if *runAll {
		rates = []int{1000, 5000, 10000}
	}

	var results []BenchmarkResult
	for _, rate := range rates {
		fmt.Printf("--> Running In-Memory Tier: Target %d events/sec for %v (Workers: %d)...\n", rate, *duration, *poolSize)
		res := runInMemoryTier(rate, *duration, *poolSize)
		results = append(results, res)
		time.Sleep(300 * time.Millisecond)
	}

	printSummary(results, "IN-MEMORY BASELINE RESULTS (Go Concurrency & Channel Backpressure)")
}

func runE2ETier(broker, dbDSN string, targetRate int, duration time.Duration, poolSize int) BenchmarkResult {
	ctx, cancel := context.WithTimeout(context.Background(), duration+25*time.Second)
	defer cancel()

	db, err := storage.Connect(ctx, dbDSN)
	if err != nil {
		fmt.Printf("FAIL: Cannot connect to PostgreSQL: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	benchTopic := fmt.Sprintf("bench-events-%d", time.Now().UnixNano()%1000000)
	if err := streamkafka.EnsureCustomTopic(ctx, []string{broker}, benchTopic, 6); err != nil {
		fmt.Printf("FAIL: Cannot connect to Kafka: %v\n", err)
		os.Exit(1)
	}

	store := storage.NewEventStore(db)
	dlqStore := storage.NewDLQStore(db)

	startCount, _ := store.CountEvents(ctx)
	fmt.Printf("--> PostgreSQL initial row count: %d (Topic: %s)\n", startCount, benchTopic)

	group := fmt.Sprintf("bench-group-%d", time.Now().UnixNano())
	consumer := streamkafka.NewConsumerForTopic([]string{broker}, group, benchTopic)
	defer consumer.Close()

	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	worker := processor.NewWorker(consumer, store, dlqStore, nil, nil, poolSize)
	go func() {
		_ = worker.Run(workerCtx)
	}()

	// Wait for consumer group partition assignment
	time.Sleep(1000 * time.Millisecond)

	producer := streamkafka.NewProducerForTopic([]string{broker}, benchTopic)
	defer producer.Close()

	gen := events.NewGenerator(time.Now().UnixNano())
	batchSize := 50
	if targetRate >= 1000 {
		batchSize = 100
	} else if targetRate < 50 {
		batchSize = targetRate
	}
	batchesPerSec := float64(targetRate) / float64(batchSize)
	interval := time.Duration(float64(time.Second) / batchesPerSec)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	fmt.Printf("--> Producing events at target %d events/sec for %v (batch size %d)...\n", targetRate, duration, batchSize)

	var produced atomic.Int64
	testEnd := time.Now().Add(duration)
	produceStart := time.Now()

	for time.Now().Before(testEnd) {
		<-ticker.C
		batch := make([]kafka.Message, batchSize)
		for i := 0; i < batchSize; i++ {
			evt := gen.Next()
			data, _ := evt.Marshal()
			batch[i] = kafka.Message{
				Topic: benchTopic,
				Key:   []byte(evt.ProductID),
				Value: data,
			}
		}
		if err := producer.PublishBatch(ctx, batch); err == nil {
			produced.Add(int64(batchSize))
		}
	}

	produceDuration := time.Since(produceStart)
	totalProduced := produced.Load()
	fmt.Printf("--> Published %d events to Kafka in %v (%.1f events/sec ingress)\n",
		totalProduced, produceDuration.Round(time.Millisecond), float64(totalProduced)/produceDuration.Seconds())

	// Wait for worker pool to drain and verify persistence in PostgreSQL
	fmt.Printf("--> Awaiting worker pool drain & PostgreSQL persistence verification...\n")
	drainDeadline := time.Now().Add(20 * time.Second)
	var finalCount int64
	for time.Now().Before(drainDeadline) {
		finalCount, _ = store.CountEvents(ctx)
		if (finalCount - startCount) >= totalProduced {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	workerCancel()
	totalElapsed := time.Since(produceStart)
	persisted := finalCount - startCount
	p50, p95, p99 := worker.GetLatencies()

	bottleneck := "None (Sustained)"
	if float64(persisted)/totalElapsed.Seconds() < float64(targetRate)*0.85 {
		bottleneck = "PostgreSQL fsync / Disk I/O"
	}

	return BenchmarkResult{
		Mode:             "e2e",
		TargetRate:       targetRate,
		ActualRate:       float64(persisted) / totalElapsed.Seconds(),
		TotalEvents:      totalProduced,
		CompletedEvents:  persisted,
		Duration:         totalElapsed,
		P50Latency:       p50,
		P95Latency:       p95,
		P99Latency:       p99,
		ErrorCount:       worker.Errors(),
		BottleneckReason: bottleneck,
	}
}

func runInMemoryTier(targetRate int, duration time.Duration, poolSize int) BenchmarkResult {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	gen := events.NewGenerator(time.Now().UnixNano())
	var produced atomic.Int64
	var completed atomic.Int64
	var errors atomic.Int64

	var latenciesMu sync.Mutex
	var latencies []time.Duration

	store := &benchStore{records: make(map[string]bool)}
	workCh := make(chan benchMessage, poolSize*100)
	var wg sync.WaitGroup

	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range workCh {
				if err := item.event.Validate(); err != nil {
					errors.Add(1)
					continue
				}
				if err := store.Insert(item.event.ID); err != nil {
					errors.Add(1)
					continue
				}
				lat := time.Since(item.producedAt)
				latenciesMu.Lock()
				if len(latencies) < 500000 {
					latencies = append(latencies, lat)
				}
				latenciesMu.Unlock()
				completed.Add(1)
			}
		}()
	}

	interval := time.Duration(float64(time.Second) / float64(targetRate))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			goto DRAIN
		case <-ticker.C:
			evt := gen.Next()
			produced.Add(1)
			select {
			case workCh <- benchMessage{event: evt, producedAt: time.Now()}:
			default:
				errors.Add(1)
			}
		}
	}

DRAIN:
	close(workCh)
	wg.Wait()
	actualDuration := time.Since(start)

	latenciesMu.Lock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var p50, p95, p99 time.Duration
	if n := len(latencies); n > 0 {
		p50 = latencies[n*50/100]
		p95 = latencies[n*95/100]
		p99 = latencies[n*99/100]
	}
	latenciesMu.Unlock()

	actualRate := float64(completed.Load()) / actualDuration.Seconds()
	bottleneck := "None (Sustained)"
	if actualRate < float64(targetRate)*0.9 {
		bottleneck = "Channel Backpressure / Go CPU"
	}

	return BenchmarkResult{
		Mode:             "in-memory",
		TargetRate:       targetRate,
		ActualRate:       actualRate,
		TotalEvents:      produced.Load(),
		CompletedEvents:  completed.Load(),
		Duration:         actualDuration,
		P50Latency:       p50,
		P95Latency:       p95,
		P99Latency:       p99,
		ErrorCount:       errors.Load(),
		BottleneckReason: bottleneck,
	}
}

type benchMessage struct {
	event      events.Event
	producedAt time.Time
}

type benchStore struct {
	mu      sync.Mutex
	records map[string]bool
}

func (s *benchStore) Insert(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[id] = true
	return nil
}

func printSummary(results []BenchmarkResult, title string) {
	fmt.Printf("\n================================================================================\n")
	fmt.Printf(" %s\n", title)
	fmt.Printf("================================================================================\n")
	fmt.Printf("%-12s | %-12s | %-10s | %-10s | %-10s | %-12s | %-24s\n",
		"Target/sec", "Actual/sec", "p50 (ms)", "p95 (ms)", "p99 (ms)", "Errors/Drops", "Bottleneck")
	fmt.Printf("--------------------------------------------------------------------------------\n")
	for _, r := range results {
		fmt.Printf("%-12d | %-12.1f | %-10.2f | %-10.2f | %-10.2f | %-12d | %-24s\n",
			r.TargetRate,
			r.ActualRate,
			float64(r.P50Latency.Microseconds())/1000.0,
			float64(r.P95Latency.Microseconds())/1000.0,
			float64(r.P99Latency.Microseconds())/1000.0,
			r.ErrorCount,
			r.BottleneckReason,
		)
	}
	fmt.Printf("================================================================================\n\n")
}
