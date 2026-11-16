# Reliability Contract v0.1

## Scope

- The source of truth for finalized usage: PostgreSQL `usage_ledger`.
- The event transport: durable outbox → Kafka (at least once) → ClickHouse (at least once).
- ClickHouse raw facts are analytical materializations and can contain duplicates.
- Never claim absolute zero loss beyond configured durability, disk, backup, and retention boundaries.
- Never use direct `SUM(tokens)` over un-deduplicated ClickHouse raw events for billing.
- Prompt/response bodies are not included in the default event schema.

## Idempotency invariant

The logical billing identity is `(tenant_id, request_id, event_type)` for a `usage.finalized` event. It may be submitted more than once. Repetitions must keep the same `event_id` AND `payload_hash`; a mismatch must be rejected and alerted. Accepted events and outbox rows commit in one PostgreSQL transaction. Compensations/revisions require a separate future schema and explicit accounting logic.

## Offset invariant

- Consumer auto-commit disabled.
- Commit **next Kafka read offset** only after all *observed earlier records* in the partition have a durable outcome: ClickHouse success or an acknowledged durable DLQ record.
- Observed Kafka offsets may contain holes (compaction, transactions, etc.). Do NOT require integer adjacency.
- Unknown ClickHouse write result (ACK lost) is retried and may produce duplicate rows.
- On rebalance: stop old owner, handle or abandon in-flight results, fence late callbacks; the Kafka client must commit only with current group ownership. Local `offset.Tracker` generation checks are necessary but not sufficient for the broker-side protocol.
- Kafka commit failure must not advance local recorded progress. Restart must replay.

## DLQ invariant

Persist the failed original record, error classification, topic/partition/offset, and its stable `event_id`. Only after the DLQ producer ACK should the input offset become eligible to commit. Retain replay tooling and alerting. For critical ledger events, data-quality errors require investigation; a DLQ is not an authorization to permanently omit a billable record.

## Kafka and ClickHouse configuration

- Production target: Kafka replicas=3, min ISR=2, producer idempotence and acks=all (not implemented in local RF=1 compose).
- The P0 consumer uses client-side batch synchronous inserts.
- If later using async inserts, choose `async_insert=1,wait_for_async_insert=1` and explicitly test the confirmation semantics.
- Kafka retention policy is a bounded recovery window, not an unlimited archive.

## Reconciliation invariant

After bounded recovery, compare the set of `(event_id, payload_hash)` from ledger with the de-duplicated analytical set for the same interval. Missing events, extra events, or conflicting hashes cause failure. Compare totals grouped by tenant/project/model only *after* event-identity deduplication. Late events and the ClickHouse TTL must be explicitly accounted for. Reserve a durable archival path for historical replay beyond the raw analytical TTL.
