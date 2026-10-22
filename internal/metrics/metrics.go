package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// EventsProcessedTotal counts processed events categorized by type and terminal status.
	EventsProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "streamflow",
			Subsystem: "processor",
			Name:      "events_processed_total",
			Help:      "Total number of events processed by the worker pool.",
		},
		[]string{"event_type", "status"},
	)

	// ProcessingDurationSeconds observes the end-to-end processing latency of an event.
	ProcessingDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "streamflow",
			Subsystem: "processor",
			Name:      "processing_duration_seconds",
			Help:      "Duration in seconds to process an event from fetch to offset commit.",
			Buckets:   []float64{0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5},
		},
		[]string{"event_type"},
	)

	// DeadLetterEventsTotal tracks messages diverted to the Dead-Letter Queue.
	DeadLetterEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "streamflow",
			Subsystem: "processor",
			Name:      "dead_letter_events_total",
			Help:      "Total number of unprocessable events routed to the dead-letter queue.",
		},
		[]string{"reason_category"},
	)

	// ConsumerLag tracks the unconsumed offset lag per Kafka partition.
	ConsumerLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "streamflow",
			Subsystem: "consumer",
			Name:      "lag",
			Help:      "Current consumer lag per Kafka topic partition.",
		},
		[]string{"topic", "partition"},
	)

	// DatabaseLatencySeconds observes query execution duration against PostgreSQL.
	DatabaseLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "streamflow",
			Subsystem: "storage",
			Name:      "db_query_duration_seconds",
			Help:      "Duration in seconds for PostgreSQL queries.",
			Buckets:   []float64{0.0005, 0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.500},
		},
		[]string{"operation"},
	)

	// RedisLatencySeconds observes command execution duration against Redis.
	RedisLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "streamflow",
			Subsystem: "cache",
			Name:      "redis_cmd_duration_seconds",
			Help:      "Duration in seconds for Redis operations.",
			Buckets:   []float64{0.0002, 0.0005, 0.001, 0.002, 0.005, 0.010, 0.025},
		},
		[]string{"command"},
	)
)

// Handler returns the standard Prometheus HTTP metrics handler.
func Handler() http.Handler {
	return promhttp.Handler()
}
