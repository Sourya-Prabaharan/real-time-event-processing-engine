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

	"github.com/google/uuid"
	"github.com/sourya/streamflow/internal/cache"
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
		mode       = flag.String("mode", "in-memory", "Benchmark mode: 'in-memory' (baseline) or 'e2e' (live Kafka + PostgreSQL)")
		targetRate = flag.Int("rate", 1000, "Target events per second (e.g. 1000, 5000, 10000)")
		duration   = flag.Duration("duration", 4*time.Second, "Benchmark duration per tier")
		runAll     = flag.Bool("all-tiers", false, "Run across 1,000, 5,000, and 10,000 events/sec tiers")
		poolSize   = flag.Int("workers", runtime.NumCPU()*2, "Worker pool concurrency")
		broker     = flag.String("broker", "localhost:9092", "Kafka broker address (for e2e mode)")
		dbDSN      = flag.String("db", "postgres://streamflow:streamflow@localhost:5432/streamflow?sslmode=disable", "PostgreSQL DSN (for e2e mode)")
	)
	flag.Parse()

	fmt.Printf("================================================================================\n")
	fmt.Printf(" StreamFlow Ingestion & Processing Benchmark Engine\n")
	fmt.Printf(" Mode: %s | System: %s/%s | CPUs: %d | GOMAXPROCS: %d\n", *mode, runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.GOMAXPROCS(0))
	fmt.Printf("================================================================================\n\n")

	if *mode == "e2e" {
		runE2EBenchmark(*broker, *dbDSN, *targetRate, *duration, *poolSize)
		return
	}

	// Default: In-Memory Baseline
	fmt.Printf("NOTE: Running In-Memory Baseline Benchmark.\n")
	fmt.Printf("This measures Go runtime concurrency, channel dispatch, serialization, and validation\n")
	fmt.Printf("overhead without database network I/O or disk fsync bottlenecks.\n\n")

	rates := []int{*targetRate}
	if *runAll {
		rates = []int{1000, 5000, 10000}
	}

	var results []BenchmarkResult
	for _, rate := range rates {
		fmt.Printf("--> Running tier: Target %d events/sec for %v (Workers: %d)...\n", rate, *duration, *poolSize)
		res := runInMemoryTier(rate, *duration, *poolSize)
		results = append(results, res)
		time.Sleep(300 * time.Millisecond)
	}

	printSummary(results, "IN-MEMORY BASELINE RESULTS (Go Concurrency & Channel Backpressure)")
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
				start := time.Now()
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
				_ = start
			}
		}()
	}

	startTime := time.Now()
	interval := time.Second / time.Duration(targetRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

produceLoop:
	for {
		select {
		case <-ctx.Done():
			break produceLoop
		case <-ticker.C:
			evt := gen.Next()
			evt.ID = uuid.New().String()
			produced.Add(1)

			select {
			case workCh <- benchMessage{event: evt, producedAt: time.Now()}:
			default:
				errors.Add(1)
			}
		}
	}

	close(workCh)
	wg.Wait()
	actualDuration := time.Since(startTime)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var p50, p95, p99 time.Duration
	n := len(latencies)
	if n > 0 {
		p50 = latencies[int(float64(n-1)*0.50)]
		p95 = latencies[int(float64(n-1)*0.95)]
		p99 = latencies[int(float64(n-1)*0.99)]
	}

	actualRate := float64(completed.Load()) / actualDuration.Seconds()
	bottleneck := "None (Sustained)"
	if actualRate < float64(targetRate)*0.92 {
		bottleneck = "Worker Pool Saturated / Channel Backpressure"
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

func runE2EBenchmark(broker, dbDSN string, targetRate int, duration time.Duration, poolSize int) {
	ctx, cancel := context.WithTimeout(context.Background(), duration+10*time.Second)
	defer cancel()

	fmt.Printf("--> Verifying PostgreSQL and Kafka connectivity...\n")
	db, err := storage.Connect(ctx, dbDSN)
	if err != nil {
		fmt.Printf("FAIL: Cannot connect to PostgreSQL at %s: %v\n", dbDSN, err)
		fmt.Printf("Please run 'make run-infra' to start PostgreSQL and Kafka before running e2e benchmarks.\n")
		os.Exit(1)
	}
	defer db.Close()

	if err := streamkafka.EnsureTopic(ctx, []string{broker}, 6); err != nil {
		fmt.Printf("FAIL: Cannot connect to Kafka at %s: %v\n", broker, err)
		os.Exit(1)
	}

	store := storage.NewEventStore(db)
	dlqStore := storage.NewDLQStore(db)

	startCount, _ := store.CountEvents(ctx)
	fmt.Printf("--> Initial authoritative event count in PostgreSQL: %d\n", startCount)

	// Start worker pool consuming from Kafka
	group := fmt.Sprintf("bench-group-%d", time.Now().UnixNano())
	consumer := streamkafka.NewConsumer([]string{broker}, group)
	defer consumer.Close()

	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	worker := processor.NewWorker(consumer, store, dlqStore, nil, nil, poolSize)
	go func() {
		_ = worker.Run(workerCtx)
	}()

	// Produce events
	producer := streamkafka.NewProducer([]string{broker})
	defer producer.Close()

	gen := events.NewGenerator(42)
	fmt.Printf("--> Producing events at target %d events/sec for %v...\n", targetRate, duration)

	produceStart := time.Now()
	ticker := time.NewTicker(time.Second / time.Duration(targetRate))
	defer ticker.Stop()

	var produced int64
	testEnd := time.Now().Add(duration)

	for time.Now().Before(testEnd) {
		<-ticker.C
		evt := gen.Next()
		data, _ := evt.Marshal()
		if err := producer.Publish(ctx, evt.ProductID, data); err == nil {
			produced++
		}
	}

	produceDuration := time.Since(produceStart)
	fmt.Printf("--> Produced %d events in %v (%.1f events/sec published to Kafka)\n",
		produced, produceDuration.Round(time.Millisecond), float64(produced)/produceDuration.Seconds())

	// Wait for consumer to process events and persist to DB
	fmt.Printf("--> Waiting for Kafka consumer pool to persist all events to PostgreSQL...\n")
	drainTimeout := time.Now().Add(10 * time.Second)
	var finalCount int64
	for time.Now().Before(drainTimeout) {
		finalCount, _ = store.CountEvents(ctx)
		if (finalCount - startCount) >= produced {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	workerCancel()
	persisted := finalCount - startCount
	p50, p95, p99 := worker.GetLatencies()

	res := BenchmarkResult{
		Mode:             "e2e",
		TargetRate:       targetRate,
		ActualRate:       float64(persisted) / produceDuration.Seconds(),
		TotalEvents:      produced,
		CompletedEvents:  persisted,
		Duration:         produceDuration,
		P50Latency:       p50,
		P95Latency:       p95,
		P99Latency:       p99,
		ErrorCount:       worker.Errors(),
		BottleneckReason: "PostgreSQL Disk fsync / Connection Pool Limits",
	}

	printSummary([]BenchmarkResult{res}, "END-TO-END DISTRIBUTED PIPELINE RESULTS (Kafka -> Worker -> PostgreSQL)")
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

var _ = cache.NewRedisCache
