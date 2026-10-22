package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	Topic    = "events"
	DLQTopic = "events-dlq"
)

// Message is a type alias for kafka.Message, allowing other packages
// in this project to reference Kafka messages without importing the
// kafka-go library directly. This keeps the kafka dependency contained.
type Message = kafka.Message

// EnsureTopic creates the Kafka topic if it doesn't exist. We disable
// auto-create on the broker and manage topics explicitly so the partition
// count is intentional, not accidental.
func EnsureTopic(ctx context.Context, brokers []string, partitions int) error {
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             Topic,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		},
		{
			Topic:             DLQTopic,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		},
	}
	err = conn.CreateTopics(topicConfigs...)
	if err != nil {
		// kafka-go returns an error even if the topic already exists;
		// there's no clean way to distinguish "already exists" from real errors
		// in all cases, so we log and continue.
		slog.Warn("create topics (may already exist)", "error", err)
	}
	return nil
}

// Producer wraps a kafka-go Writer and provides a simple interface
// for publishing serialized events.
type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        Topic,
		Balancer:     &kafka.Hash{}, // partition by key
		RequiredAcks: kafka.RequireOne,
		// BatchTimeout and BatchSize allow configurable batching
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    100,
	}
	return &Producer{writer: w}
}

// Publish sends a single event to Kafka. The key is the product ID,
// which ensures all events for the same product land on the same partition.
// This preserves per-product ordering — critical for inventory updates
// and purchase sequences.
func (p *Producer) Publish(ctx context.Context, key string, value []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: Topic,
		Key:   []byte(key),
		Value: value,
	})
}

// PublishDLQ forwards an unprocessable or poison-pill message to the dead-letter topic.
func (p *Producer) PublishDLQ(ctx context.Context, key string, value []byte, reason string) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: DLQTopic,
		Key:   []byte(key),
		Value: value,
		Headers: []kafka.Header{
			{Key: "dlq_reason", Value: []byte(reason)},
		},
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

// Consumer reads from a Kafka topic using a consumer group. Each consumer
// in the group is assigned a subset of partitions, so adding more consumers
// increases parallelism up to the partition count.
type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, groupID string) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          Topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6, // 10 MB
		CommitInterval: time.Second,
		// StartOffset only matters when the group has no committed offset.
		StartOffset: kafka.FirstOffset,
	})
	return &Consumer{reader: r}
}

// FetchMessage reads the next message without committing the offset.
// The caller must call CommitMessage after successful processing.
// This prevents data loss: if the worker crashes between fetch and commit,
// the message will be redelivered (at-least-once semantics).
func (c *Consumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	return c.reader.FetchMessage(ctx)
}

func (c *Consumer) CommitMessage(ctx context.Context, msg kafka.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
