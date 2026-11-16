-- Local-development schema. Production requires migrations, access controls and backup policy.
CREATE TABLE IF NOT EXISTS usage_ledger (
    event_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type = 'usage.finalized'),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    model TEXT NOT NULL,
    prompt_tokens BIGINT NOT NULL CHECK (prompt_tokens >= 0),
    completion_tokens BIGINT NOT NULL CHECK (completion_tokens >= 0),
    payload_hash CHAR(64) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT one_finalized_usage_per_request UNIQUE (tenant_id, request_id, event_type)
);

CREATE TABLE IF NOT EXISTS event_outbox (
    outbox_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE REFERENCES usage_ledger(event_id),
    event_payload JSONB NOT NULL,
    topic TEXT NOT NULL DEFAULT 'usage.critical.v1',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    last_error TEXT
);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON event_outbox (next_attempt_at, outbox_id)
    WHERE published_at IS NULL;

-- INSERT the ledger and the outbox row in ONE transaction.
-- When a duplicate event_id or request_id is detected, read the existing ledger row
-- and verify payload_hash. A conflicting hash is a data-integrity error, not a no-op.
-- The relay must CLAIM using a short transaction/lease, send outside that transaction,
-- and record published_at only after Kafka delivery ACK. Crash may produce duplicates.
