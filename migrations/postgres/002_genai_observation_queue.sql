-- Non-authoritative GenAI observation delivery. Deliberately no FK to usage_ledger/event_outbox.
-- Receipts retain only a projection hash and identity for a bounded replay window.
CREATE TABLE IF NOT EXISTS genai_observation_receipts (
    tenant_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT now() + interval '24 hours',
    PRIMARY KEY (tenant_id, trace_id, span_id),
    CHECK (expires_at > accepted_at)
);
CREATE INDEX IF NOT EXISTS idx_genai_observation_receipts_expiry
    ON genai_observation_receipts (expires_at);

CREATE TABLE IF NOT EXISTS genai_observations (
    observation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    schema_version TEXT NOT NULL CHECK (schema_version = 'genai-semconv/v0.1.0'),
    tenant_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    request_id TEXT NOT NULL DEFAULT '',
    attempt_id TEXT NOT NULL DEFAULT '',
    provider TEXT NOT NULL,
    operation TEXT NOT NULL,
    request_model TEXT NOT NULL DEFAULT '',
    response_model TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN ('unset', 'ok', 'error')),
    input_tokens BIGINT CHECK (input_tokens >= 0),
    output_tokens BIGINT CHECK (output_tokens >= 0),
    payload_hash CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    terminal_at TIMESTAMPTZ,
    terminal_reason TEXT,
    last_error TEXT,
    UNIQUE (tenant_id, trace_id, span_id),
    CHECK ((lease_owner IS NULL) = (lease_until IS NULL)),
    CHECK ((terminal_at IS NULL) = (terminal_reason IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_genai_observations_pending
    ON genai_observations (available_at, observation_id)
    WHERE terminal_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_genai_observations_terminal
    ON genai_observations (terminal_at, observation_id)
    WHERE terminal_at IS NOT NULL;

-- O(1) capacity counters avoid a full table scan on every telemetry batch.
-- The singleton capacity is initialized by the first writer and must match on all replicas.
CREATE TABLE IF NOT EXISTS genai_observation_queue_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    queue_items BIGINT NOT NULL DEFAULT 0 CHECK (queue_items >= 0),
    receipt_items BIGINT NOT NULL DEFAULT 0 CHECK (receipt_items >= 0),
    capacity_limit BIGINT NOT NULL DEFAULT 0 CHECK (capacity_limit >= 0)
);
INSERT INTO genai_observation_queue_state(singleton)
VALUES(TRUE) ON CONFLICT(singleton) DO NOTHING;
