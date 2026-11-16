# M2c · ClickHouse Replay-Safe Analytics (query-level, not billing)

## Why

The original ClickHouse raw table already contained `payload_hash FixedString(64)`, but the Go writer's INSERT list omitted the field. Historical rows written through that version carry ClickHouse's zero-padded default instead of the signed event hash. Merely doing `uniqExact(event_id)` or `sum(tokens)` over raw data cannot distinguish clean replays from payload conflicts.

## Implementation

- The Go HTTP writer now includes the validated 64-hex-character `payload_hash` in every synchronous `JSONEachRow` INSERT.
- `inference_events_dedup_v1` produces one logical record per `event_id` **only when** all copies have one nonempty payload hash. It exposes `raw_copies`.
- `inference_events_integrity_v1` lists any event ID with multiple distinct hashes or any copy missing a hash.
- Conflicting events are **excluded from the trusted projection** and must be investigated, not silently selected by `any()`.
- Existing zero-hash legacy rows remain excluded until they are re-ingested from the authoritative source. There is no attempt to invent an event hash from incomplete raw fields.
- This is an **on-read aggregation** over all matching raw rows and is not yet performance-benchmarked. Large-scale production query optimization (partition/time scoping, projections or materialized dedup) remains M4.

## Queries

```sql
-- Dashboard source: only hash-consistent unique usage events
SELECT tenant_id,
       count() AS requests,
       sum(prompt_tokens) AS prompt_tokens,
       sum(completion_tokens) AS completion_tokens,
       sum(raw_copies - 1) AS replayed_raw_rows
FROM telemetry.inference_events_dedup_v1
GROUP BY tenant_id ORDER BY tenant_id;

-- Alert condition: do not certify analytical counts while >0
SELECT count(), sum(raw_copies) FROM telemetry.inference_events_integrity_v1;

-- Explain physical duplication without changing logical counting
SELECT event_id, raw_copies, payload_hash
FROM telemetry.inference_events_dedup_v1
WHERE raw_copies > 1
LIMIT 50;
```

The `inference_events_dedup_v1` view is **not a financial ledger**. Ledger and Outbox remain in PostgreSQL. A matching hash makes duplicate events consistent; it is not an independent proof that the event originated from an authorized gateway.

## Verification

```bash
go test ./internal/clickhouse
# Real ClickHouse:
TEST_CLICKHOUSE_HTTP=http://localhost:18123 go test -count=1 -v ./internal/clickhouse -run 'TestAnalytics'
```

The real-container tests insert one sealed event twice and require identical logical totals; inject a conflicting hash and require it to appear in integrity diagnostics and disappear from trusted counts; insert a legacy hash-less raw event and require it to be excluded and reported.

`m1-smoke.sh` additionally checks that every ingested event is present in the trusted analytical view and there are no integrity exceptions. Single broker RF=1, absent fault injection and missing reconciler remain outside this slice.
