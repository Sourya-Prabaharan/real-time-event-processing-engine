package processor

import (
	"sort"
	"sync"
	"time"
)

// LatencyTracker records processing durations and computes percentiles.
// It uses a circular buffer to keep memory bounded — only the most recent
// N measurements are retained.
//
// This is a simple approach. For production systems you'd use a histogram
// (e.g., HDR histogram) or stream to Prometheus. This is good enough for
// local benchmarking and avoids adding a dependency.
type LatencyTracker struct {
	mu      sync.Mutex
	samples []time.Duration
	pos     int
	full    bool
}

func NewLatencyTracker(capacity int) *LatencyTracker {
	return &LatencyTracker{
		samples: make([]time.Duration, capacity),
	}
}

func (lt *LatencyTracker) Record(d time.Duration) {
	lt.mu.Lock()
	lt.samples[lt.pos] = d
	lt.pos++
	if lt.pos >= len(lt.samples) {
		lt.pos = 0
		lt.full = true
	}
	lt.mu.Unlock()
}

// Percentiles returns p50, p95, p99 from the current sample buffer.
func (lt *LatencyTracker) Percentiles() (p50, p95, p99 time.Duration) {
	lt.mu.Lock()
	n := lt.pos
	if lt.full {
		n = len(lt.samples)
	}
	if n == 0 {
		lt.mu.Unlock()
		return 0, 0, 0
	}

	// Copy to avoid holding the lock during sort.
	sorted := make([]time.Duration, n)
	if lt.full {
		copy(sorted, lt.samples)
	} else {
		copy(sorted, lt.samples[:n])
	}
	lt.mu.Unlock()

	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	percentile := func(p float64) time.Duration {
		idx := int(float64(n-1) * p)
		return sorted[idx]
	}

	return percentile(0.50), percentile(0.95), percentile(0.99)
}
