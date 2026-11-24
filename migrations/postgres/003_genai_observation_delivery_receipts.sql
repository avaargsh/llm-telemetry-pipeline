-- Receiver-side durable delivery idempotency. Keys are scoped by the
-- authenticated tenant and retained for a bounded 30-day replay window.
ALTER TABLE genai_observation_queue_state
    ADD COLUMN IF NOT EXISTS delivery_receipt_items BIGINT NOT NULL DEFAULT 0
    CHECK (delivery_receipt_items >= 0);

CREATE TABLE IF NOT EXISTS genai_observation_delivery_receipts (
    tenant_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT now() + interval '30 days',
    PRIMARY KEY (tenant_id, idempotency_key),
    CHECK (length(idempotency_key) BETWEEN 1 AND 256 AND idempotency_key ~ '^[A-Za-z0-9._:/@-]+$'),
    CHECK (expires_at > accepted_at)
);
CREATE INDEX IF NOT EXISTS idx_genai_observation_delivery_receipts_expiry
    ON genai_observation_delivery_receipts (expires_at);
