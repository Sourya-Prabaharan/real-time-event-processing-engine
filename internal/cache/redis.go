package cache

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sourya/streamflow/internal/events"
)

// RedisCache maintains real-time counters and aggregations in Redis.
// Redis serves as a fast read layer for the dashboard — it is NOT the
// source of truth. PostgreSQL holds authoritative state, and Redis
// can be rebuilt from PG data if it's ever lost or corrupted.
type RedisCache struct {
	client *redis.Client
}

func NewRedisCache(addr string) (*RedisCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		PoolSize:     20,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &RedisCache{client: client}, nil
}

// RecordEvent updates multiple Redis counters for a single event.
// Uses a pipeline to batch the Redis commands into one round trip.
func (c *RedisCache) RecordEvent(ctx context.Context, e *events.Event) error {
	pipe := c.client.Pipeline()
	now := time.Now()
	minuteKey := now.Format("2006-01-02T15:04")

	// Total events per minute, by type. Expires after 1 hour so old data
	// doesn't accumulate indefinitely.
	countKey := fmt.Sprintf("events:%s:%s", e.Type, minuteKey)
	pipe.Incr(ctx, countKey)
	pipe.Expire(ctx, countKey, time.Hour)

	// Global throughput counter per minute.
	throughputKey := fmt.Sprintf("throughput:%s", minuteKey)
	pipe.Incr(ctx, throughputKey)
	pipe.Expire(ctx, throughputKey, time.Hour)

	// Revenue tracking for purchases.
	if e.Type == events.Purchase && e.Price > 0 {
		revenueKey := fmt.Sprintf("revenue:%s", minuteKey)
		// Store revenue in cents to avoid float precision issues in Redis.
		cents := int64(e.Price * float64(e.Quantity) * 100)
		pipe.IncrBy(ctx, revenueKey, cents)
		pipe.Expire(ctx, revenueKey, time.Hour)
	}

	// Top products: sorted set scored by event count. The dashboard reads
	// the top N from this set. Uses a daily bucket so the "top" is relative
	// to today, not all time.
	dayKey := now.Format("2006-01-02")
	topKey := fmt.Sprintf("top:%s:%s", e.Type, dayKey)
	pipe.ZIncrBy(ctx, topKey, 1, e.ProductID)
	pipe.Expire(ctx, topKey, 25*time.Hour) // slightly over 24h to avoid timezone edge cases

	_, err := pipe.Exec(ctx)
	return err
}

// GetThroughput returns the event count for the current minute.
func (c *RedisCache) GetThroughput(ctx context.Context) (int64, error) {
	key := fmt.Sprintf("throughput:%s", time.Now().Format("2006-01-02T15:04"))
	val, err := c.client.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

// GetRecentThroughput returns throughput for the last N minutes.
func (c *RedisCache) GetRecentThroughput(ctx context.Context, minutes int) ([]ThroughputPoint, error) {
	now := time.Now()
	points := make([]ThroughputPoint, 0, minutes)
	for i := minutes - 1; i >= 0; i-- {
		t := now.Add(-time.Duration(i) * time.Minute)
		key := fmt.Sprintf("throughput:%s", t.Format("2006-01-02T15:04"))
		val, err := c.client.Get(ctx, key).Int64()
		if err == redis.Nil {
			val = 0
		} else if err != nil {
			return nil, err
		}
		points = append(points, ThroughputPoint{
			Minute: t.Format("15:04"),
			Count:  val,
		})
	}
	return points, nil
}

// GetEventCounts returns event counts per type for the current minute.
func (c *RedisCache) GetEventCounts(ctx context.Context) (map[string]int64, error) {
	minuteKey := time.Now().Format("2006-01-02T15:04")
	counts := make(map[string]int64)
	types := []events.EventType{events.ProductView, events.AddToCart, events.Purchase, events.InventoryUpdate}
	for _, t := range types {
		key := fmt.Sprintf("events:%s:%s", t, minuteKey)
		val, err := c.client.Get(ctx, key).Int64()
		if err == redis.Nil {
			val = 0
		} else if err != nil {
			return nil, err
		}
		counts[string(t)] = val
	}
	return counts, nil
}

// GetRevenue returns revenue in dollars for the current minute.
func (c *RedisCache) GetRevenue(ctx context.Context) (float64, error) {
	key := fmt.Sprintf("revenue:%s", time.Now().Format("2006-01-02T15:04"))
	val, err := c.client.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return float64(val) / 100, nil // cents → dollars
}

// GetTopProducts returns the top N products by event count for today.
func (c *RedisCache) GetTopProducts(ctx context.Context, eventType events.EventType, limit int) ([]ProductScore, error) {
	dayKey := time.Now().Format("2006-01-02")
	key := fmt.Sprintf("top:%s:%s", eventType, dayKey)
	results, err := c.client.ZRevRangeWithScores(ctx, key, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	products := make([]ProductScore, len(results))
	for i, z := range results {
		products[i] = ProductScore{
			ProductID: z.Member.(string),
			Score:     int64(z.Score),
		}
	}
	return products, nil
}

// GetTotalProcessed returns a rough total from Redis throughput keys.
// For an exact count, query PostgreSQL.
func (c *RedisCache) GetTotalProcessed(ctx context.Context) (int64, error) {
	// Scan for throughput keys and sum them.
	var total int64
	iter := c.client.Scan(ctx, 0, "throughput:*", 1000).Iterator()
	for iter.Next(ctx) {
		val, err := c.client.Get(ctx, iter.Val()).Result()
		if err != nil {
			continue
		}
		n, _ := strconv.ParseInt(val, 10, 64)
		total += n
	}
	return total, iter.Err()
}

func (c *RedisCache) Close() error {
	return c.client.Close()
}

// Ping checks Redis connectivity.
func (c *RedisCache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

type ThroughputPoint struct {
	Minute string `json:"minute"`
	Count  int64  `json:"count"`
}

type ProductScore struct {
	ProductID string `json:"product_id"`
	Score     int64  `json:"score"`
}
