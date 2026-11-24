# M11.3: bounded in-memory observation exporter worker

Refs #56. This is a non-authoritative telemetry delivery worker and is separate from the PostgreSQL usage Ledger/Outbox relay.

The worker claims only the FIFO head and keeps it in the bounded queue until the exporter returns nil (the downstream ACK boundary). The queue capacity includes the leased head. Only one lease is active per queue; this first implementation intentionally serializes export to preserve FIFO order. Direct Pop cannot bypass an active lease. An exporter receives a deep copy of token pointers, so it cannot mutate the retained retry payload.

RunOnce performs at most one attempt; the caller schedules polling using RetryError.NextAttempt. RetryPolicy bounds attempts (1..20), initial/max delay (1ms..1h), and deterministic per-observation jitter (0..50%). The first attempt is immediate. Errors are transient unless explicitly wrapped with Permanent(err). Transient failures schedule capped exponential backoff. Permanent errors and exhausted retries retain and lease the head; no silent automatic drop occurs. An operator must call RetryFailed or DropFailed. DropFailed returns the normalized observation for an external audit/dead-letter action. Close releases the lease without dropping the item.

ACK contract: exporter nil means the receiver has acknowledged the complete observation. A timeout or connection loss can be ambiguous; retry may duplicate delivery. No exactly-once guarantee or idempotency key is introduced here. Exporter implementation and receiver deduplication remain future work.

Loss contract: both Queue contents and Worker retry/lease state are process-local volatile memory. Process crash/restart loses all queued and in-flight observations and resets backoff. There is no persistence, restart recovery, dead-letter store, or durable ACK. This slice does not write PostgreSQL Ledger/Outbox or billing authority. The existing cmd/outbox-relay remains a distinct authoritative Kafka relay with its own persisted lease/ACK protocol.

Hosted CI must validate Go race/vet/build and the existing real PostgreSQL ledger/outbox invariance suite. No production worker loop, external collector transport, or delivery SLO is claimed.

Hosted PostgreSQL regression: the real Ledger/Outbox snapshot test now drives the M11.3 Worker through a transient export failure, capped retry and downstream ACK, then a permanent failure with explicit DropFailed. Every transition compares complete authoritative Ledger and Outbox rows. This proves worker retry/drop has no billing authority; it does not prove persistence or restart recovery.
