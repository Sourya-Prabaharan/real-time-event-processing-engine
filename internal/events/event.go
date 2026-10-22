package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// EventType represents the kind of e-commerce activity.
type EventType string

const (
	ProductView     EventType = "product_view"
	AddToCart       EventType = "add_to_cart"
	Purchase        EventType = "purchase"
	InventoryUpdate EventType = "inventory_update"
)

var validTypes = map[EventType]bool{
	ProductView:     true,
	AddToCart:       true,
	Purchase:        true,
	InventoryUpdate: true,
}

// Event is the core data structure that flows through the system.
// The ID field is a UUID generated at creation time and used as the
// idempotency key in PostgreSQL (the primary key on the events table).
type Event struct {
	ID        string    `json:"id"`
	Type      EventType `json:"event_type"`
	UserID    string    `json:"user_id"`
	ProductID string    `json:"product_id"`
	Price     float64   `json:"price,omitempty"`
	Quantity  int       `json:"quantity,omitempty"`
	Category  string    `json:"category,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// Validate checks that the event has all required fields and a known type.
func (e *Event) Validate() error {
	if e.ID == "" {
		return fmt.Errorf("event ID is required")
	}
	if _, err := uuid.Parse(e.ID); err != nil {
		return fmt.Errorf("event ID must be a valid UUID: %w", err)
	}
	if !validTypes[e.Type] {
		return fmt.Errorf("unknown event type: %q", e.Type)
	}
	if e.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	if e.ProductID == "" {
		return fmt.Errorf("product_id is required")
	}
	if e.Timestamp.IsZero() {
		return fmt.Errorf("timestamp is required")
	}
	if e.Price < 0 {
		return fmt.Errorf("price cannot be negative")
	}
	if e.Quantity < 0 {
		return fmt.Errorf("quantity cannot be negative")
	}
	return nil
}

func (e *Event) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func Unmarshal(data []byte) (*Event, error) {
	var e Event
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, fmt.Errorf("unmarshal event: %w", err)
	}
	return &e, nil
}
