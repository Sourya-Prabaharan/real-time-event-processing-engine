package processor

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sourya/streamflow/internal/events"
	streamkafka "github.com/sourya/streamflow/internal/kafka"
	"github.com/sourya/streamflow/internal/retry"
)

// MockConsumer implements MessageConsumer for controlled test simulations.
type MockConsumer struct {
	mu        sync.Mutex
	messages  []streamkafka.Message
	committed map[int64]bool
	pos       int
}

func NewMockConsumer(msgs []streamkafka.Message) *MockConsumer {
	return &MockConsumer{
		messages:  msgs,
		committed: make(map[int64]bool),
	}
}

func (m *MockConsumer) FetchMessage(ctx context.Context) (streamkafka.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.pos >= len(m.messages) {
		// Wait for ctx done to simulate EOF / blocking poll
		<-ctx.Done()
		return streamkafka.Message{}, ctx.Err()
	}

	msg := m.messages[m.pos]
	m.pos++
	return msg, nil
}

func (m *MockConsumer) CommitMessage(ctx context.Context, msg streamkafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.committed[msg.Offset] = true
	return nil
}

func (m *MockConsumer) Close() error { return nil }

func (m *MockConsumer) CommittedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.committed)
}

// MockStore implements EventPersister and counts duplicate/total inserts.
type MockStore struct {
	mu         sync.Mutex
	records    map[string]*events.Event
	failUntil  int
	callCount  int
	failAlways bool
}

func NewMockStore() *MockStore {
	return &MockStore{records: make(map[string]*events.Event)}
}

func (s *MockStore) InsertEvent(ctx context.Context, e *events.Event) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.callCount++
	if s.failAlways {
		return false, errors.New("database connection refused")
	}
	if s.failUntil > 0 && s.callCount <= s.failUntil {
		return false, errors.New("transient lock error")
	}

	// Idempotent behavior: ON CONFLICT DO NOTHING
	if _, exists := s.records[e.ID]; !exists {
		s.records[e.ID] = e
		return true, nil
	}
	return false, nil
}

func (s *MockStore) RecordCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}

// MockCache tracks cache record invocations to verify double-count prevention
type MockCache struct {
	mu    sync.Mutex
	calls int
}

func (c *MockCache) RecordEvent(ctx context.Context, e *events.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return nil
}

func (c *MockCache) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// MockDLQ implements DeadLetterRecorder and DeadLetterPublisher
type MockDLQ struct {
	mu         sync.Mutex
	entries    []string
	failAlways bool
}

func (d *MockDLQ) RecordDeadLetter(ctx context.Context, topic string, partition int, offset int64, payload []byte, reason string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failAlways {
		return errors.New("dlq database write failed")
	}
	d.entries = append(d.entries, reason)
	return nil
}

func (d *MockDLQ) PublishDLQ(ctx context.Context, key string, value []byte, reason string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failAlways {
		return errors.New("dlq kafka publish failed")
	}
	return nil
}

func (d *MockDLQ) Count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.entries)
}

func makeEventMessage(e events.Event, partition int, offset int64) streamkafka.Message {
	data, _ := json.Marshal(e)
	return streamkafka.Message{
		Topic:     "events",
		Partition: partition,
		Offset:    offset,
		Key:       []byte(e.ProductID),
		Value:     data,
	}
}

// Test 1: Normal processing and offset commit
func TestWorker_NormalProcessing(t *testing.T) {
	evt := events.Event{
		ID:        uuid.New().String(),
		Type:      events.Purchase,
		UserID:    "user-1",
		ProductID: "prod-1",
		Price:     49.99,
		Quantity:  1,
		Timestamp: time.Now(),
	}

	mockConsumer := NewMockConsumer([]streamkafka.Message{makeEventMessage(evt, 0, 100)})
	mockStore := NewMockStore()
	mockDLQ := &MockDLQ{}

	w := NewWorker(mockConsumer, mockStore, mockDLQ, nil, nil, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = w.Run(ctx)

	if w.Processed() != 1 {
		t.Fatalf("expected 1 processed event, got %d", w.Processed())
	}
	if mockStore.RecordCount() != 1 {
		t.Fatalf("expected 1 stored record, got %d", mockStore.RecordCount())
	}
	if mockConsumer.CommittedCount() != 1 {
		t.Fatalf("expected 1 committed message, got %d", mockConsumer.CommittedCount())
	}
	if mockDLQ.Count() != 0 {
		t.Fatalf("expected 0 DLQ entries, got %d", mockDLQ.Count())
	}
}

// Test 2: Malformed JSON routes to Dead-Letter Queue
func TestWorker_MalformedJSONRoutesToDLQ(t *testing.T) {
	badMsg := streamkafka.Message{
		Topic:     "events",
		Partition: 0,
		Offset:    101,
		Key:       []byte("bad-key"),
		Value:     []byte(`{not valid json`),
	}

	mockConsumer := NewMockConsumer([]streamkafka.Message{badMsg})
	mockStore := NewMockStore()
	mockDLQ := &MockDLQ{}

	w := NewWorker(mockConsumer, mockStore, mockDLQ, nil, nil, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = w.Run(ctx)

	if mockDLQ.Count() != 1 {
		t.Fatalf("expected 1 dead letter, got %d", mockDLQ.Count())
	}
	if mockConsumer.CommittedCount() != 1 {
		t.Fatalf("expected bad message offset to be committed to prevent partition stall, got %d", mockConsumer.CommittedCount())
	}
	if mockStore.RecordCount() != 0 {
		t.Fatalf("expected 0 stored records, got %d", mockStore.RecordCount())
	}
}

// Test 3: Schema validation failure routes to Dead-Letter Queue
func TestWorker_InvalidSchemaRoutesToDLQ(t *testing.T) {
	invalidEvt := events.Event{
		ID:        "invalid-non-uuid",
		Type:      events.ProductView,
		UserID:    "", // Missing required UserID
		ProductID: "prod-2",
		Timestamp: time.Now(),
	}

	mockConsumer := NewMockConsumer([]streamkafka.Message{makeEventMessage(invalidEvt, 0, 102)})
	mockStore := NewMockStore()
	mockDLQ := &MockDLQ{}

	w := NewWorker(mockConsumer, mockStore, mockDLQ, nil, nil, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = w.Run(ctx)

	if mockDLQ.Count() != 1 {
		t.Fatalf("expected 1 dead letter, got %d", mockDLQ.Count())
	}
	if mockConsumer.CommittedCount() != 1 {
		t.Fatalf("expected offset commit for invalid schema, got %d", mockConsumer.CommittedCount())
	}
}

// Test 4: Transient DB failure is retried and succeeds
func TestWorker_TransientDBFailureRetriesAndSucceeds(t *testing.T) {
	evt := events.Event{
		ID:        uuid.New().String(),
		Type:      events.AddToCart,
		UserID:    "user-99",
		ProductID: "prod-99",
		Price:     19.99,
		Quantity:  1,
		Timestamp: time.Now(),
	}

	mockConsumer := NewMockConsumer([]streamkafka.Message{makeEventMessage(evt, 0, 103)})
	mockStore := NewMockStore()
	mockStore.failUntil = 2 // Fail first 2 attempts, succeed on 3rd
	mockDLQ := &MockDLQ{}

	w := NewWorker(mockConsumer, mockStore, mockDLQ, nil, nil, 1)
	w.SetRetryConfig(retry.Config{
		MaxAttempts:     3,
		InitialInterval: 5 * time.Millisecond,
		MaxInterval:     20 * time.Millisecond,
		Multiplier:      2.0,
		Jitter:          false,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_ = w.Run(ctx)

	if w.Processed() != 1 {
		t.Fatalf("expected 1 processed event after retry, got %d", w.Processed())
	}
	if mockStore.RecordCount() != 1 {
		t.Fatalf("expected 1 stored record, got %d", mockStore.RecordCount())
	}
	if mockDLQ.Count() != 0 {
		t.Fatalf("expected 0 DLQ entries, got %d", mockDLQ.Count())
	}
}

// Test 5: Permanent DB failure routes to DLQ after exhausting retries
func TestWorker_PermanentDBFailureRoutesToDLQ(t *testing.T) {
	evt := events.Event{
		ID:        uuid.New().String(),
		Type:      events.ProductView,
		UserID:    "user-100",
		ProductID: "prod-100",
		Timestamp: time.Now(),
	}

	mockConsumer := NewMockConsumer([]streamkafka.Message{makeEventMessage(evt, 0, 104)})
	mockStore := NewMockStore()
	mockStore.failAlways = true
	mockDLQ := &MockDLQ{}

	w := NewWorker(mockConsumer, mockStore, mockDLQ, nil, nil, 1)
	w.SetRetryConfig(retry.Config{
		MaxAttempts:     2,
		InitialInterval: 5 * time.Millisecond,
		MaxInterval:     10 * time.Millisecond,
		Multiplier:      2.0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_ = w.Run(ctx)

	if mockDLQ.Count() != 1 {
		t.Fatalf("expected 1 dead letter after exhausted retries, got %d", mockDLQ.Count())
	}
	if mockConsumer.CommittedCount() != 1 {
		t.Fatalf("expected offset committed after DLQ routing, got %d", mockConsumer.CommittedCount())
	}
}

// Test 6: Idempotent processing prevents duplicate database entries
func TestWorker_IdempotencyDuplicateEvents(t *testing.T) {
	sharedID := uuid.New().String()
	evt := events.Event{
		ID:        sharedID,
		Type:      events.Purchase,
		UserID:    "user-200",
		ProductID: "prod-200",
		Price:     99.99,
		Quantity:  1,
		Timestamp: time.Now(),
	}

	// Redelivery of the exact same event twice (offset 200 and offset 201)
	msgs := []streamkafka.Message{
		makeEventMessage(evt, 0, 200),
		makeEventMessage(evt, 0, 201),
	}

	mockConsumer := NewMockConsumer(msgs)
	mockStore := NewMockStore()
	mockDLQ := &MockDLQ{}

	w := NewWorker(mockConsumer, mockStore, mockDLQ, nil, nil, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_ = w.Run(ctx)

	if w.Processed() != 2 {
		t.Fatalf("expected 2 processed events, got %d", w.Processed())
	}
	// Authoritative record count in store must be exactly 1 due to idempotency!
	if mockStore.RecordCount() != 1 {
		t.Fatalf("idempotency failed: expected exactly 1 unique record, got %d", mockStore.RecordCount())
	}
	if mockConsumer.CommittedCount() != 2 {
		t.Fatalf("expected both offsets committed, got %d", mockConsumer.CommittedCount())
	}
}

// Test 7: Worker crash and recovery simulation
// Scenario: Worker 1 crashes midway through a batch of 5 messages.
// Worker 2 starts up, receives the uncommitted messages, and resumes.
// Asserts that no messages are lost and no duplicate entries are stored.
func TestWorker_CrashAndRecoverySimulation(t *testing.T) {
	sharedID1 := uuid.New().String()
	sharedID2 := uuid.New().String()
	sharedID3 := uuid.New().String()

	evts := []events.Event{
		{ID: sharedID1, Type: events.ProductView, UserID: "u1", ProductID: "p1", Timestamp: time.Now()},
		{ID: sharedID2, Type: events.AddToCart, UserID: "u2", ProductID: "p2", Price: 10, Quantity: 1, Timestamp: time.Now()},
		{ID: sharedID3, Type: events.Purchase, UserID: "u3", ProductID: "p3", Price: 25, Quantity: 1, Timestamp: time.Now()},
	}

	mockStore := NewMockStore()
	mockDLQ := &MockDLQ{}

	// Worker 1 runs on messages 0 and 1, then crashes (simulated by cancelling context early)
	msgsWorker1 := []streamkafka.Message{
		makeEventMessage(evts[0], 0, 301),
		makeEventMessage(evts[1], 0, 302),
	}
	consumer1 := NewMockConsumer(msgsWorker1)
	w1 := NewWorker(consumer1, mockStore, mockDLQ, nil, nil, 1)

	ctx1, cancel1 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_ = w1.Run(ctx1)
	cancel1()

	if mockStore.RecordCount() != 2 {
		t.Fatalf("worker 1 expected 2 stored records, got %d", mockStore.RecordCount())
	}

	// Worker 2 resumes! Because msg 302 crashed before commit was acknowledged (simulated by re-delivering 302 and 303)
	msgsWorker2 := []streamkafka.Message{
		makeEventMessage(evts[1], 0, 302), // Re-delivered!
		makeEventMessage(evts[2], 0, 303),
	}
	consumer2 := NewMockConsumer(msgsWorker2)
	w2 := NewWorker(consumer2, mockStore, mockDLQ, nil, nil, 1)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	_ = w2.Run(ctx2)
	cancel2()

	// Verify all 3 events are present, no duplicate rows created, and zero data loss!
	if mockStore.RecordCount() != 3 {
		t.Fatalf("crash recovery failed: expected exactly 3 unique stored records, got %d", mockStore.RecordCount())
	}
	if mockDLQ.Count() != 0 {
		t.Fatalf("expected 0 DLQ entries during recovery, got %d", mockDLQ.Count())
	}
}

// Test 8: Verifies that duplicate/redelivered events do NOT double-count in Redis
func TestWorker_IdempotencyPreventsRedisDoubleCount(t *testing.T) {
	sharedID := uuid.New().String()
	evt := events.Event{
		ID:        sharedID,
		Type:      events.Purchase,
		UserID:    "user-redis-test",
		ProductID: "prod-redis-test",
		Price:     150.00,
		Quantity:  1,
		Timestamp: time.Now(),
	}

	// Two messages with the exact same event ID (Kafka at-least-once redelivery)
	msgs := []streamkafka.Message{
		makeEventMessage(evt, 0, 501),
		makeEventMessage(evt, 0, 502),
	}

	mockConsumer := NewMockConsumer(msgs)
	mockStore := NewMockStore()
	mockDLQ := &MockDLQ{}
	mockCache := &MockCache{}

	w := NewWorker(mockConsumer, mockStore, mockDLQ, nil, mockCache, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	_ = w.Run(ctx)

	// Both messages were processed through the pipeline
	if w.Processed() != 2 {
		t.Fatalf("expected 2 processed events, got %d", w.Processed())
	}
	// PostgreSQL only stores 1 unique record
	if mockStore.RecordCount() != 1 {
		t.Fatalf("expected 1 record in store, got %d", mockStore.RecordCount())
	}
	// CRITICAL TEST: Redis RecordEvent was called EXACTLY ONCE, preventing double-counting!
	if mockCache.Calls() != 1 {
		t.Fatalf("Redis double-counted: expected RecordEvent called exactly 1 time, got %d", mockCache.Calls())
	}
	// Both offsets committed
	if mockConsumer.CommittedCount() != 2 {
		t.Fatalf("expected both offsets committed, got %d", mockConsumer.CommittedCount())
	}
}

// Test 9: Verifies that if DLQ storage fails on all targets, the offset is NOT committed,
// preventing unrecoverable message loss.
func TestWorker_DLQFailurePreventsOffsetCommit(t *testing.T) {
	badMsg := streamkafka.Message{
		Topic:     "events",
		Partition: 0,
		Offset:    999,
		Key:       []byte("poison-pill"),
		Value:     []byte(`{malformed json`),
	}

	mockConsumer := NewMockConsumer([]streamkafka.Message{badMsg})
	mockStore := NewMockStore()
	mockDLQ := &MockDLQ{failAlways: true} // Both DB and Kafka DLQ fail

	w := NewWorker(mockConsumer, mockStore, mockDLQ, mockDLQ, nil, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = w.Run(ctx)

	// CRITICAL TEST: Offset must NOT be committed if DLQ failed!
	if mockConsumer.CommittedCount() != 0 {
		t.Fatalf("Data loss bug: expected 0 committed offsets when DLQ fails, got %d", mockConsumer.CommittedCount())
	}
	// DLQ count should remain 0 because it failed to save
	if w.DLQCount() != 0 {
		t.Fatalf("expected 0 dlq count on failure, got %d", w.DLQCount())
	}
}
