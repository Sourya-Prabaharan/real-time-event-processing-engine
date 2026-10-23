package storage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/sourya/streamflow/internal/events"
)

// EventStore handles persistence of events to PostgreSQL.
type EventStore struct {
	db *sql.DB
}

func NewEventStore(db *sql.DB) *EventStore {
	return &EventStore{db: db}
}

// InsertEvent writes an event to the database using ON CONFLICT DO NOTHING.
// This makes the insert idempotent: if a worker crashes after inserting but
// before committing the Kafka offset, the redelivered message will be safely
// ignored instead of creating a duplicate row.
//
// Returns (inserted = true, nil) if a new event row was inserted.
// Returns (inserted = false, nil) if the event already existed (conflict ignored).
func (s *EventStore) InsertEvent(ctx context.Context, e *events.Event) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO events (id, event_type, user_id, product_id, price, quantity, category, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO NOTHING
	`, e.ID, e.Type, e.UserID, e.ProductID, e.Price, e.Quantity, e.Category, e.Timestamp)
	if err != nil {
		return false, fmt.Errorf("insert event %s: %w", e.ID, err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		// If driver doesn't support RowsAffected, assume inserted to be safe
		return true, nil
	}
	return rows > 0, nil
}

// GetRecentEvents returns the most recent events, ordered by creation time.
func (s *EventStore) GetRecentEvents(ctx context.Context, limit int) ([]events.Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, event_type, user_id, product_id, price, quantity, category, created_at
		FROM events
		ORDER BY created_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query recent events: %w", err)
	}
	defer rows.Close()

	var result []events.Event
	for rows.Next() {
		var e events.Event
		if err := rows.Scan(&e.ID, &e.Type, &e.UserID, &e.ProductID, &e.Price, &e.Quantity, &e.Category, &e.Timestamp); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

// CountEvents returns the total number of events in the database.
func (s *EventStore) CountEvents(ctx context.Context) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&count)
	return count, err
}

// TopProducts returns the products with the most events of the given type.
func (s *EventStore) TopProducts(ctx context.Context, eventType events.EventType, limit int) ([]ProductCount, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT product_id, COUNT(*) as cnt
		FROM events
		WHERE event_type = $1
		GROUP BY product_id
		ORDER BY cnt DESC
		LIMIT $2
	`, eventType, limit)
	if err != nil {
		return nil, fmt.Errorf("query top products: %w", err)
	}
	defer rows.Close()

	var result []ProductCount
	for rows.Next() {
		var pc ProductCount
		if err := rows.Scan(&pc.ProductID, &pc.Count); err != nil {
			return nil, fmt.Errorf("scan product count: %w", err)
		}
		result = append(result, pc)
	}
	return result, rows.Err()
}

type ProductCount struct {
	ProductID string `json:"product_id"`
	Count     int64  `json:"count"`
}
