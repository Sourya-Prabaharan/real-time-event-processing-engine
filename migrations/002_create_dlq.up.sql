-- Dead-letter events table: records events that failed unmarshaling, schema validation,
-- or exhausted all transient retries against downstream storage.
-- Serves as an audit trail and provides data for re-drive / replay operations.
CREATE TABLE IF NOT EXISTS dead_letter_events (
    id              BIGSERIAL PRIMARY KEY,
    kafka_topic     VARCHAR(64) NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset    BIGINT NOT NULL,
    payload         BYTEA,
    reason          TEXT NOT NULL,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index on failed_at: allows time-based querying and pruning of dead letters
CREATE INDEX IF NOT EXISTS idx_dlq_failed_at ON dead_letter_events(failed_at);

-- Index on kafka_topic + kafka_partition + kafka_offset: ensures fast lookups by Kafka coordinates
CREATE INDEX IF NOT EXISTS idx_dlq_kafka_coords ON dead_letter_events(kafka_topic, kafka_partition, kafka_offset);
