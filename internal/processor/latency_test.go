package processor

import (
	"testing"
	"time"
)

func TestLatencyTracker_Empty(t *testing.T) {
	lt := NewLatencyTracker(100)
	p50, p95, p99 := lt.Percentiles()
	if p50 != 0 || p95 != 0 || p99 != 0 {
		t.Errorf("expected all zeros for empty tracker, got %v %v %v", p50, p95, p99)
	}
}

func TestLatencyTracker_SingleSample(t *testing.T) {
	lt := NewLatencyTracker(100)
	lt.Record(5 * time.Millisecond)

	p50, p95, p99 := lt.Percentiles()
	if p50 != 5*time.Millisecond {
		t.Errorf("p50 = %v, want 5ms", p50)
	}
	if p95 != 5*time.Millisecond {
		t.Errorf("p95 = %v, want 5ms", p95)
	}
	if p99 != 5*time.Millisecond {
		t.Errorf("p99 = %v, want 5ms", p99)
	}
}

func TestLatencyTracker_Distribution(t *testing.T) {
	lt := NewLatencyTracker(10000)

	// Record 100 samples: 1ms, 2ms, ..., 100ms
	for i := 1; i <= 100; i++ {
		lt.Record(time.Duration(i) * time.Millisecond)
	}

	p50, p95, p99 := lt.Percentiles()

	// p50 should be around 50ms
	if p50 < 45*time.Millisecond || p50 > 55*time.Millisecond {
		t.Errorf("p50 = %v, expected ~50ms", p50)
	}
	// p95 should be around 95ms
	if p95 < 90*time.Millisecond || p95 > 100*time.Millisecond {
		t.Errorf("p95 = %v, expected ~95ms", p95)
	}
	// p99 should be around 99ms
	if p99 < 95*time.Millisecond || p99 > 100*time.Millisecond {
		t.Errorf("p99 = %v, expected ~99ms", p99)
	}
}

func TestLatencyTracker_CircularBuffer(t *testing.T) {
	lt := NewLatencyTracker(10)

	// Fill beyond capacity — old values should be overwritten.
	for i := 0; i < 20; i++ {
		lt.Record(time.Duration(i+1) * time.Millisecond)
	}

	// Only the last 10 samples (11ms-20ms) should remain.
	p50, _, _ := lt.Percentiles()
	if p50 < 14*time.Millisecond || p50 > 17*time.Millisecond {
		t.Errorf("p50 = %v, expected ~15ms (last 10 of 11-20ms)", p50)
	}
}
