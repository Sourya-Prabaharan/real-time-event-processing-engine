package retry

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// Config defines exponential backoff retry parameters.
type Config struct {
	MaxAttempts     int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	Jitter          bool
}

// DefaultConfig provides sensible production defaults for transient downstream retries.
func DefaultConfig() Config {
	return Config{
		MaxAttempts:     3,
		InitialInterval: 50 * time.Millisecond,
		MaxInterval:     1 * time.Second,
		Multiplier:      2.0,
		Jitter:          true,
	}
}

// Do executes an operation with exponential backoff and jitter.
// If the context is cancelled during backoff or execution, Do aborts immediately.
func Do(ctx context.Context, cfg Config, op func(ctx context.Context) error) error {
	var err error
	interval := cfg.InitialInterval

	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if err = op(ctx); err == nil {
			return nil
		}

		if attempt == cfg.MaxAttempts {
			break
		}

		// Calculate backoff delay with optional random jitter to prevent thundering herd.
		delay := interval
		if cfg.Jitter {
			jitterFraction := 0.8 + rand.Float64()*0.4 // 80% to 120%
			delay = time.Duration(float64(interval) * jitterFraction)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("retry aborted by context after %d attempts: %w (last op err: %v)", attempt, ctx.Err(), err)
		case <-time.After(delay):
		}

		interval = time.Duration(float64(interval) * cfg.Multiplier)
		if interval > cfg.MaxInterval {
			interval = cfg.MaxInterval
		}
	}

	return fmt.Errorf("operation failed after %d attempts: %w", cfg.MaxAttempts, err)
}
