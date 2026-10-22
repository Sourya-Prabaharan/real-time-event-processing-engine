package events

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func validEvent() Event {
	return Event{
		ID:        uuid.New().String(),
		Type:      Purchase,
		UserID:    "user-1",
		ProductID: "prod-1",
		Price:     29.99,
		Quantity:  1,
		Category:  "electronics",
		Timestamp: time.Now(),
	}
}

func TestValidate_Valid(t *testing.T) {
	e := validEvent()
	if err := e.Validate(); err != nil {
		t.Fatalf("expected valid event, got error: %v", err)
	}
}

func TestValidate_MissingID(t *testing.T) {
	e := validEvent()
	e.ID = ""
	if err := e.Validate(); err == nil {
		t.Fatal("expected error for missing ID")
	}
}

func TestValidate_BadUUID(t *testing.T) {
	e := validEvent()
	e.ID = "not-a-uuid"
	if err := e.Validate(); err == nil {
		t.Fatal("expected error for invalid UUID")
	}
}

func TestValidate_UnknownType(t *testing.T) {
	e := validEvent()
	e.Type = "refund"
	if err := e.Validate(); err == nil {
		t.Fatal("expected error for unknown event type")
	}
}

func TestValidate_MissingUserID(t *testing.T) {
	e := validEvent()
	e.UserID = ""
	if err := e.Validate(); err == nil {
		t.Fatal("expected error for missing user_id")
	}
}

func TestValidate_NegativePrice(t *testing.T) {
	e := validEvent()
	e.Price = -10
	if err := e.Validate(); err == nil {
		t.Fatal("expected error for negative price")
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	e := validEvent()
	data, err := e.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ID != e.ID {
		t.Errorf("ID mismatch: got %s, want %s", got.ID, e.ID)
	}
	if got.Type != e.Type {
		t.Errorf("Type mismatch: got %s, want %s", got.Type, e.Type)
	}
	if got.Price != e.Price {
		t.Errorf("Price mismatch: got %f, want %f", got.Price, e.Price)
	}
}

func TestUnmarshal_InvalidJSON(t *testing.T) {
	_, err := Unmarshal([]byte(`{invalid`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}
