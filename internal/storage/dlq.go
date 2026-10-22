package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// DeadLetterEntry represents a permanently unprocessable message.
type DeadLetterEntry struct {
	ID        int64     `json:"id"`
	Topic     string    `json:"kafka_topic"`
	Partition int       `json:"kafka_partition"`
	Offset    int64     `json:"kafka_offset"`
	Payload   string    `json:"payload"`
	Reason    string    `json:"reason"`
	FailedAt  time.Time `json:"failed_at"`
}

// DLQStore persists and retrieves dead-lettered events.
type DLQStore struct {
	db *sql.DB
}

func NewDLQStore(db *sql.DB) *DLQStore {
	return &DLQStore{db: db}
}

// RecordDeadLetter inserts a failed event into the dead_letter_events table.
func (s *DLQStore) RecordDeadLetter(ctx context.Context, topic string, partition int, offset int64, payload []byte, reason string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO dead_letter_events (kafka_topic, kafka_partition, kafka_offset, payload, reason)
		VALUES ($1, $2, $3, $4, $5)
	`, topic, partition, offset, payload, reason)
	if err != nil {
		return fmt.Errorf("record dead letter: %w", err)
	}
	return nil
}

// GetRecentDeadLetters fetches the most recent dead-letter events for operator inspection.
func (s *DLQStore) GetRecentDeadLetters(ctx context.Context, limit int) ([]DeadLetterEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, kafka_topic, kafka_partition, kafka_offset, encode(payload, 'escape'), reason, failed_at
		FROM dead_letter_events
		ORDER BY failed_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query dead letters: %w", err)
	}
	defer rows.Close()

	var entries []DeadLetterEntry
	for rows.Next() {
		var e DeadLetterEntry
		if err := rows.Scan(&e.ID, &e.Topic, &e.Partition, &e.Offset, &e.Payload, &e.Reason, &e.FailedAt); err != nil {
			return nil, fmt.Errorf("scan dead letter: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// CountDeadLetters returns the total count of dead letters recorded.
func (s *DLQStore) CountDeadLetters(ctx context.Context) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letter_events`).Scan(&count)
	return count, err
}
