-- Event-level analytical deduplication without relying on MergeTree merges.
-- The raw table already has payload_hash FixedString(64); older writer
-- omitted it and those unsealed rows are flagged in integrity_v1.
-- Do not run token-based billing from these views: PostgreSQL usage_ledger
-- remains authoritative.

CREATE VIEW IF NOT EXISTS telemetry.inference_events_dedup_v1 AS
SELECT
    event_id,
    any(event_time) AS event_time,
    any(tenant_id) AS tenant_id,
    any(project_id) AS project_id,
    any(request_id) AS request_id,
    any(model) AS model,
    any(prompt_tokens) AS prompt_tokens,
    any(completion_tokens) AS completion_tokens,
    any(latency_ms) AS latency_ms,
    any(status) AS status,
    min(payload_hash) AS canonical_hash,
    count() AS raw_copies
FROM telemetry.inference_events_raw
GROUP BY event_id
HAVING uniqExact(payload_hash) = 1
   AND countIf(payload_hash = toFixedString('', 64)) = 0;

-- An event ID with multiple hashes is not safe to aggregate; quarantine
-- it rather than silently selecting a row. Also show legacy missing hashes.
CREATE VIEW IF NOT EXISTS telemetry.inference_events_integrity_v1 AS
SELECT
    event_id,
    count() AS raw_copies,
    uniqExact(payload_hash) AS hash_versions,
    countIf(payload_hash = toFixedString('', 64)) AS missing_hash_rows
FROM telemetry.inference_events_raw
GROUP BY event_id
HAVING hash_versions > 1 OR missing_hash_rows > 0;
