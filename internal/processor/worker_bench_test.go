package processor

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sourya/streamflow/internal/events"
)

func BenchmarkLatencyTracker_Record(b *testing.B) {
	lt := NewLatencyTracker(10000)
	d := 5 * time.Millisecond

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		lt.Record(d)
	}
}

func BenchmarkLatencyTracker_Percentiles(b *testing.B) {
	lt := NewLatencyTracker(10000)
	for i := 0; i < 10000; i++ {
		lt.Record(time.Duration(i%50) * time.Millisecond)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, _ = lt.Percentiles()
	}
}

func BenchmarkWorker_ProcessEventDirect(b *testing.B) {
	store := NewMockStore()
	dlq := &MockDLQ{}
	w := NewWorker(nil, store, dlq, nil, nil, 1)

	evt := &events.Event{
		ID:        uuid.New().String(),
		Type:      events.Purchase,
		UserID:    "user-benchmark",
		ProductID: "prod-benchmark",
		Price:     99.99,
		Quantity:  1,
		Timestamp: time.Now(),
	}

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = store.InsertEvent(ctx, evt)
		w.latencies.Record(100 * time.Microsecond)
	}
}
