CREATE DATABASE IF NOT EXISTS telemetry;

CREATE TABLE IF NOT EXISTS telemetry.inference_events_raw
(
    event_id UUID,
    event_time DateTime64(3, 'UTC'),
    tenant_id String,
    project_id String,
    request_id String,
    model LowCardinality(String),
    prompt_tokens UInt64,
    completion_tokens UInt64,
    latency_ms UInt64,
    status LowCardinality(String),
    payload_hash FixedString(64),
    ingest_time DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (tenant_id, project_id, event_time, request_id, event_id)
TTL event_time + INTERVAL 30 DAY DELETE;

-- This raw table may contain repeats after an uncertain INSERT response.
-- NEVER use unqualified SUM(tokens) here for billing or as a reconciliation oracle.
