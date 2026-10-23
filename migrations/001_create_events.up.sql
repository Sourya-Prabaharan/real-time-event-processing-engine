-- Events table: one row per ingested event.
-- This is the authoritative record of every event the system processes.
CREATE TABLE IF NOT EXISTS events (
    id          UUID PRIMARY KEY,
    event_type  VARCHAR(32) NOT NULL,
    user_id     VARCHAR(64) NOT NULL,
    product_id  VARCHAR(64) NOT NULL,
    price       DECIMAL(10,2),
    quantity    INTEGER DEFAULT 1,
    category    VARCHAR(64),
    metadata    JSONB,
    created_at  TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index on event_type: used by analytics queries that filter/aggregate by type
-- (e.g., "count purchases in the last minute").
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);

-- Index on product_id: used by top-products queries and per-product lookups.
CREATE INDEX IF NOT EXISTS idx_events_product_id ON events(product_id);

-- Index on created_at: used by time-range queries for dashboards and throughput calculations.
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);

-- Composite index on (event_type, created_at): covers the most common analytics query pattern
-- "give me all purchases in the last N minutes" without needing to scan the full type index
-- and then filter by time.
CREATE INDEX IF NOT EXISTS idx_events_type_created ON events(event_type, created_at);
