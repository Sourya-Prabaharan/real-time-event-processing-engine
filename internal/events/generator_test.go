package events

import (
	"testing"
)

func TestGenerator_Deterministic(t *testing.T) {
	g1 := NewGenerator(42)
	g2 := NewGenerator(42)

	for i := 0; i < 100; i++ {
		e1 := g1.Next()
		e2 := g2.Next()

		// Same seed should produce same user, product, type, price, quantity.
		// IDs differ because they use uuid.New() (not seeded).
		if e1.UserID != e2.UserID {
			t.Errorf("event %d: user mismatch: %s vs %s", i, e1.UserID, e2.UserID)
		}
		if e1.ProductID != e2.ProductID {
			t.Errorf("event %d: product mismatch: %s vs %s", i, e1.ProductID, e2.ProductID)
		}
		if e1.Type != e2.Type {
			t.Errorf("event %d: type mismatch: %s vs %s", i, e1.Type, e2.Type)
		}
		if e1.Price != e2.Price {
			t.Errorf("event %d: price mismatch: %f vs %f", i, e1.Price, e2.Price)
		}
	}
}

func TestGenerator_TypeDistribution(t *testing.T) {
	g := NewGenerator(1)
	counts := make(map[EventType]int)

	n := 10000
	for i := 0; i < n; i++ {
		e := g.Next()
		counts[e.Type]++
	}

	// Product views should be most common (3/7 ≈ 43%),
	// add-to-cart next (2/7 ≈ 29%), then purchase and inventory (1/7 ≈ 14% each).
	viewPct := float64(counts[ProductView]) / float64(n)
	if viewPct < 0.35 || viewPct > 0.50 {
		t.Errorf("product_view percentage out of range: %.2f", viewPct)
	}

	// Purchases should be less common than views.
	if counts[Purchase] >= counts[ProductView] {
		t.Error("expected fewer purchases than product views")
	}
}

func TestGenerator_FieldsPopulated(t *testing.T) {
	g := NewGenerator(99)
	for i := 0; i < 50; i++ {
		e := g.Next()
		if e.UserID == "" {
			t.Error("empty user_id")
		}
		if e.ProductID == "" {
			t.Error("empty product_id")
		}
		if e.Category == "" {
			t.Error("empty category")
		}
		if e.Timestamp.IsZero() {
			t.Error("zero timestamp")
		}
		if err := e.Validate(); err != nil {
			// Generator should always produce valid events (except ID is uuid.New, always valid).
			// But we set the ID, so validate should pass.
			t.Errorf("generated event failed validation: %v", err)
		}
	}
}
