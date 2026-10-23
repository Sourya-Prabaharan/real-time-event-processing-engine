package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/sourya/streamflow/internal/cache"
	"github.com/sourya/streamflow/internal/events"
	"github.com/sourya/streamflow/internal/metrics"
	"github.com/sourya/streamflow/internal/storage"
	"github.com/sourya/streamflow/internal/web"
)

func envOr(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func main() {
	var (
		addr       = flag.String("addr", envOr("HTTP_ADDR", ":8080"), "HTTP listen address")
		dbDSN      = flag.String("db", envOr("POSTGRES_DSN", "postgres://streamflow:streamflow@localhost:5432/streamflow?sslmode=disable"), "PostgreSQL DSN")
		redisAddr  = flag.String("redis", envOr("REDIS_ADDR", "localhost:6379"), "Redis address")
		migrations = flag.String("migrations", envOr("MIGRATIONS_PATH", "migrations"), "Path to migration files")
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	db, err := storage.Connect(ctx, *dbDSN)
	if err != nil {
		slog.Error("connect to database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := storage.RunMigrations(ctx, db, *migrations); err != nil {
		slog.Error("run migrations", "error", err)
		os.Exit(1)
	}

	var redisCache *cache.RedisCache
	redisCache, err = cache.NewRedisCache(*redisAddr)
	if err != nil {
		slog.Warn("redis unavailable", "error", err)
	} else {
		defer redisCache.Close()
	}

	store := storage.NewEventStore(db)
	dlqStore := storage.NewDLQStore(db)

	mux := http.NewServeMux()

	// Embedded Operational Dashboard
	mux.Handle("GET /{$}", web.Handler())
	mux.Handle("GET /dashboard", web.Handler())

	// Prometheus metrics scraping endpoint
	mux.Handle("GET /metrics", metrics.Handler())

	// Health check — always returns 200 if the process is running.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})

	// Readiness check — verifies downstream dependencies.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.PingContext(r.Context()); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		if redisCache != nil {
			if err := redisCache.Ping(r.Context()); err != nil {
				http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ready")
	})

	// Recent events from PostgreSQL.
	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
		limit := 50
		if l := r.URL.Query().Get("limit"); l != "" {
			if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 200 {
				limit = n
			}
		}
		evts, err := store.GetRecentEvents(r.Context(), limit)
		if err != nil {
			slog.Error("get recent events", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		writeJSON(w, evts)
	})

	// Total event count from PostgreSQL (authoritative).
	mux.HandleFunc("GET /api/events/count", func(w http.ResponseWriter, r *http.Request) {
		count, err := store.CountEvents(r.Context())
		if err != nil {
			slog.Error("count events", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]int64{"total_events": count})
	})

	// DLQ: Recent dead-letter events.
	mux.HandleFunc("GET /api/dlq", func(w http.ResponseWriter, r *http.Request) {
		limit := 20
		if l := r.URL.Query().Get("limit"); l != "" {
			if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 100 {
				limit = n
			}
		}
		entries, err := dlqStore.GetRecentDeadLetters(r.Context(), limit)
		if err != nil {
			slog.Error("get dead letters", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		writeJSON(w, entries)
	})

	// DLQ: Total dead-letter count.
	mux.HandleFunc("GET /api/dlq/count", func(w http.ResponseWriter, r *http.Request) {
		count, err := dlqStore.CountDeadLetters(r.Context())
		if err != nil {
			slog.Error("count dead letters", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]int64{"total_dead_letters": count})
	})

	// Top products — reads from Redis for speed, falls back to PostgreSQL.
	mux.HandleFunc("GET /api/products/top", func(w http.ResponseWriter, r *http.Request) {
		eventType := events.EventType(r.URL.Query().Get("type"))
		if eventType == "" {
			eventType = events.ProductView
		}
		limit := 10
		if l := r.URL.Query().Get("limit"); l != "" {
			if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 50 {
				limit = n
			}
		}

		// Try Redis first (faster, scoped to today).
		if redisCache != nil {
			products, err := redisCache.GetTopProducts(r.Context(), eventType, limit)
			if err == nil && len(products) > 0 {
				writeJSON(w, products)
				return
			}
		}

		// Fall back to PostgreSQL (slower, all-time).
		products, err := store.TopProducts(r.Context(), eventType, limit)
		if err != nil {
			slog.Error("top products", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		writeJSON(w, products)
	})

	// Real-time throughput from Redis.
	mux.HandleFunc("GET /api/throughput", func(w http.ResponseWriter, r *http.Request) {
		if redisCache == nil {
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}
		minutes := 10
		if m := r.URL.Query().Get("minutes"); m != "" {
			if n, err := strconv.Atoi(m); err == nil && n > 0 && n <= 60 {
				minutes = n
			}
		}
		points, err := redisCache.GetRecentThroughput(r.Context(), minutes)
		if err != nil {
			slog.Error("get throughput", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		writeJSON(w, points)
	})

	// Real-time metrics: event counts by type and revenue.
	mux.HandleFunc("GET /api/metrics", func(w http.ResponseWriter, r *http.Request) {
		result := make(map[string]any)

		// Event count from PG (authoritative total).
		total, err := store.CountEvents(r.Context())
		if err == nil {
			result["total_events"] = total
		}

		if redisCache != nil {
			counts, err := redisCache.GetEventCounts(r.Context())
			if err == nil {
				result["events_this_minute"] = counts
			}
			revenue, err := redisCache.GetRevenue(r.Context())
			if err == nil {
				result["revenue_this_minute"] = revenue
			}
			throughput, err := redisCache.GetThroughput(r.Context())
			if err == nil {
				result["throughput_this_minute"] = throughput
			}
		}

		writeJSON(w, result)
	})

	srv := &http.Server{
		Addr:         *addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		slog.Info("api server starting", "addr", *addr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down api server")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "error", err)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write json response", "error", err)
	}
}

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
	fmt.Fprintln(os.Stderr, "StreamFlow API Server")
}
