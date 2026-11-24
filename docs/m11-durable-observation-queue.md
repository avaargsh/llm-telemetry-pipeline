# M11.4: durable non-authoritative GenAI observation queue

Refs #56. Migration 002 adds two tables that are deliberately independent of usage_ledger and event_outbox. No foreign key, trigger, or write path connects observations to billing authority.

## Admission and identity

- PostgreSQL is the durable source of truth; the in-memory M11.3 Queue remains available for volatile deployments.
- EnqueueBatch validates the bounded projection before a transaction, then takes a transaction-scoped advisory lock for a global capacity check. A batch is all-or-nothing.
- Natural identity is (tenant_id, trace_id, span_id). The payload hash is SHA-256 over the normalized projection. Same identity/same projection is idempotent; same identity/different projection returns ErrObservationConflict.
- A receipt retains only identity and hash for 24 hours, including after ACK or explicit drop. Expired receipts are pruned during admission; total retained receipts are capped at 2,000,000 and admission fails closed at that cap. This suppresses OTLP replay without retaining raw prompts, messages, tool arguments, or payloads. After the receipt expires, an old replay can be admitted again.
- The configured capacity must be identical across all writers sharing the database. Terminal rows count against capacity and require explicit operator action; this is fail-closed.

## Delivery and recovery

- Claim uses FOR UPDATE SKIP LOCKED and a persisted lease. Every claim increments attempts, which is also the fencing generation.
- ACK deletes only the current, unexpired lease generation. A stale worker cannot ACK after lease takeover.
- Retry persists available_at and releases the lease. Restart does not reset retry schedule or attempts.
- Permanent failures and exhausted attempts become terminal rows and are not automatically claimed. ListTerminal exposes failures scoped to one tenant; Tenant-scoped RetryFailed and DropFailed require both tenant ID and observation ID; mismatched tenants return the same not-found result. RetryFailed explicitly re-arms and resets attempts; DropFailed explicitly removes a terminal row. Receipts remain for their 24-hour replay window.
- A process crash leaves its lease until expiry; a replacement worker can then reclaim the same row. Configure lease TTL longer than the exporter's request timeout. Expired in-flight work may be exported twice; consumers must use Delivery.IdempotencyKey and persist it to deduplicate.
- M11.6 adds an HTTP DurableExporter and a receiver-persisted, tenant-scoped delivery-key receipt. See [M11.6 HTTP exporter and receiver idempotency](m11-durable-http-exporter.md). Delivery remains at-least-once, with duplicate suppression bounded by the 30-day key receipt window; this is not exactly-once.

## Bounds and loss semantics

- Queue capacity is bounded to 1..1,000,000 rows. Enqueue admission is serialized with a PostgreSQL advisory lock so concurrent writers cannot exceed the configured bound when they share the same capacity.
- NewDurableOTLPHTTPHandler connects the existing authenticated JSON OTLP decoder directly to this store. HTTP 200 is returned only after the PostgreSQL batch transaction commits. Backend outages return retryable 503; no hidden volatile-memory fallback occurs.
- The database itself is the durability boundary. PostgreSQL outage fails admission closed.
- Only normalized fields and token counts are stored. Error fields persist stable reason codes, never exporter error text.
- No production delivery SLO is claimed. Backups, retention, database role grants, and operational alerting must be configured by deployment owners.


## M11.8: atomic receipt expiry and capacity accounting

Migration 005 replaces the existing expiry function without changing its signature or returned deletion counts. The function first locks the singleton queue-state row, deletes expired natural and delivery receipts, and decrements both persisted counters in the same PostgreSQL transaction. Direct calls by the restricted Receiver role therefore preserve the invariant that counters equal retained rows.

Counter underflow is a hard error (23514). Because the deletes and counter update are in one statement transaction, an underflow aborts and rolls back both deletes. Migration 005 does not silently reconcile pre-existing drift; operators must inspect row counts and counters and perform any repair as a separately reviewed maintenance action.

Compatibility: the existing Go enqueue writer locks the same singleton row before calling the function, consumes the returned delete counts, and writes absolute post-expiry counter totals from its pre-expiry snapshot. That final absolute update converges to the same values after Migration 005; it does not subtract twice. A regression test covers this behavior along with direct restricted-role calls, rollback, repeated expiry, underflow, and concurrent expiry/admission.

Fresh PostgreSQL databases load migrations in filename order from migrations/postgres. Existing databases must explicitly apply 005_genai_observation_receipt_expiry_atomic.sql once through the deployment's approved migration process; mounting the directory into docker-entrypoint-initdb.d does not upgrade an already initialized volume. CI exercises both the normal Compose initialization path and the independent receiver database migration loop. This is migration validation, not a claim of production rollout.
