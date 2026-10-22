package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDo_SuccessFirstAttempt(t *testing.T) {
	calls := 0
	err := Do(context.Background(), DefaultConfig(), func(ctx context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestDo_SucceedsAfterRetries(t *testing.T) {
	calls := 0
	cfg := Config{
		MaxAttempts:     3,
		InitialInterval: 5 * time.Millisecond,
		MaxInterval:     20 * time.Millisecond,
		Multiplier:      2.0,
		Jitter:          false,
	}

	err := Do(context.Background(), cfg, func(ctx context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("transient error")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected eventual success, got error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestDo_FailsExhaustedAttempts(t *testing.T) {
	calls := 0
	cfg := Config{
		MaxAttempts:     3,
		InitialInterval: 5 * time.Millisecond,
		MaxInterval:     20 * time.Millisecond,
		Multiplier:      2.0,
		Jitter:          false,
	}
	expectedErr := errors.New("permanent failure")

	err := Do(context.Background(), cfg, func(ctx context.Context) error {
		calls++
		return expectedErr
	})
	if err == nil {
		t.Fatal("expected error after exhausted retries, got nil")
	}
	if calls != 3 {
		t.Fatalf("expected exactly 3 attempts, got %d", calls)
	}
}

func TestDo_AbortsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := Config{
		MaxAttempts:     5,
		InitialInterval: 50 * time.Millisecond,
		MaxInterval:     100 * time.Millisecond,
		Multiplier:      2.0,
	}

	calls := 0
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	err := Do(ctx, cfg, func(ctx context.Context) error {
		calls++
		return errors.New("error")
	})
	if err == nil {
		t.Fatal("expected error on cancelled context")
	}
	if calls >= 5 {
		t.Fatalf("expected abort before 5 attempts, got %d", calls)
	}
}
