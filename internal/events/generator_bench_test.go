package events

import (
	"testing"
	"time"
)

func BenchmarkGenerator_Next(b *testing.B) {
	gen := NewGenerator(42)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = gen.Next()
	}
}

func BenchmarkEvent_MarshalUnmarshal(b *testing.B) {
	e := Event{
		ID:        "4f58dc45-0418-48e0-a590-7795b48d587c",
		Type:      Purchase,
		UserID:    "user-00123",
		ProductID: "prod-0042",
		Price:     149.99,
		Quantity:  2,
		Category:  "electronics",
		Timestamp: time.Now(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data, err := e.Marshal()
		if err != nil {
			b.Fatal(err)
		}
		_, err = Unmarshal(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEvent_Validate(b *testing.B) {
	e := Event{
		ID:        "4f58dc45-0418-48e0-a590-7795b48d587c",
		Type:      Purchase,
		UserID:    "user-00123",
		ProductID: "prod-0042",
		Price:     149.99,
		Quantity:  2,
		Category:  "electronics",
		Timestamp: time.Now(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := e.Validate(); err != nil {
			b.Fatal(err)
		}
	}
}
