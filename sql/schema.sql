-- Real-Time Customer Heart Beat Monitoring System
-- PostgreSQL schema

CREATE TABLE IF NOT EXISTS heartbeats (
    id          SERIAL PRIMARY KEY,
    customer_id VARCHAR(10)  NOT NULL,
    name        VARCHAR(100),
    heart_rate  INTEGER      NOT NULL,
    status      VARCHAR(10)  NOT NULL CHECK (status IN ('normal', 'anomaly')),
    timestamp   TIMESTAMPTZ  NOT NULL,
    created_at  TIMESTAMPTZ  DEFAULT NOW()
);

-- Fast lookups by time (used by dashboard ORDER BY timestamp DESC)
CREATE INDEX IF NOT EXISTS idx_heartbeats_timestamp
    ON heartbeats (timestamp DESC);

-- Fast lookups / aggregations per customer
CREATE INDEX IF NOT EXISTS idx_heartbeats_customer
    ON heartbeats (customer_id);
