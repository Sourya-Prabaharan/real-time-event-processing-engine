package events

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

// Generator produces randomized but realistic e-commerce events.
// It uses a seeded random source so benchmark runs can be reproduced
// with the same seed.
type Generator struct {
	rng *rand.Rand
}

func NewGenerator(seed int64) *Generator {
	return &Generator{rng: rand.New(rand.NewSource(seed))}
}

var (
	eventTypes = []EventType{ProductView, ProductView, ProductView, AddToCart, AddToCart, Purchase, InventoryUpdate}
	categories = []string{"electronics", "clothing", "books", "home", "sports", "food"}
	// 50 simulated products to create realistic hot-product skew.
	productIDs = func() []string {
		ids := make([]string, 50)
		for i := range ids {
			ids[i] = fmt.Sprintf("prod-%04d", i+1)
		}
		return ids
	}()
)

// Next returns a new random event. Product views are weighted more heavily
// than purchases, which matches real e-commerce traffic patterns
// (many views, fewer carts, even fewer purchases).
func (g *Generator) Next() Event {
	eventType := eventTypes[g.rng.Intn(len(eventTypes))]
	productID := productIDs[g.rng.Intn(len(productIDs))]

	e := Event{
		ID:        uuid.New().String(),
		Type:      eventType,
		UserID:    fmt.Sprintf("user-%05d", g.rng.Intn(10000)),
		ProductID: productID,
		Category:  categories[g.rng.Intn(len(categories))],
		Timestamp: time.Now(),
	}

	switch eventType {
	case Purchase, AddToCart:
		e.Price = float64(g.rng.Intn(20000)+100) / 100 // $1.00 - $200.99
		e.Quantity = g.rng.Intn(5) + 1
	case InventoryUpdate:
		e.Quantity = g.rng.Intn(500) + 1
	}

	return e
}
