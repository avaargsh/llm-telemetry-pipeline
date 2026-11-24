# M11.6: durable HTTP exporter and receiver idempotency

Refs #65 / PR #66. This is a non-authoritative telemetry delivery path.

## Delivery contract

- The PostgreSQL queue remains the sender's durable source of truth. `DurableWorker` claims one row under a persisted lease and passes the stable `Delivery.IdempotencyKey()` to `HTTPDurableExporter`.
- The exporter sends one normalized OTLP/HTTP JSON span to `POST /v1/traces`, with an authenticated bearer credential and exactly one `Idempotency-Key: genaiobs/<observation_id>` header.
- The receiver derives tenant scope only from the bearer credential. It never trusts tenant identity from the OTLP payload.
- Receiver admission writes the tenant-scoped idempotency key, payload hash, natural trace/span receipt and observation queue row in one PostgreSQL transaction.
- Same tenant + key + same normalized payload is an acknowledged no-op, including after the original queue row has been ACKed. Reuse of the same key with a different payload returns HTTP 409. A different key for the same natural observation is also deduplicated by the existing natural identity receipt.
- Delivery-key receipts retain only the key, tenant, payload hash and timestamps for 30 days. They are bounded by a 2,000,000-row counter; admission fails closed at capacity. Natural OTLP identity receipts retain their existing 24-hour window. After the delivery-key receipt expires, a very late replay may be admitted again.
- The receiver must acknowledge only after the transaction commits. A timeout or lost ACK is ambiguous; the sender retries with the same key. A 2xx response is the export ACK boundary.

## HTTP classification and transport security

- 2xx: acknowledged.
- 408, 425, 429 and 5xx: transient; the durable worker schedules a retry.
- Other 4xx and redirects: permanent; the worker retains the row in terminal state for explicit operator action.
- Response bodies are discarded and never copied into errors. Authentication credentials and tenant IDs are not included in the payload.
- Remote receivers require HTTPS. Plain HTTP is accepted only for a literal loopback IP in tests. Redirects are not followed.
- Configure a finite HTTP client timeout shorter than the worker lease. The worker lease must exceed the exporter timeout plus the receiver's bounded admission time.

## Migration and verification

Apply `migrations/postgres/003_genai_observation_delivery_receipts.sql` before enabling keyed receiver admission on an existing database. Fresh disposable Compose databases apply it automatically from the migration mount.

Run the receiver key semantics and sender/receiver fault test with PostgreSQL:

```sh
TEST_POSTGRES_DSN='postgres://telemetry:localdev-only@localhost:15432/telemetry?sslmode=disable' \
  go test -race -count=1 -run '^TestDurableStore(IdempotencyKeyReceiptSemantics|RejectsInvalidIdempotencyKeys)$' ./internal/genaiobs

TEST_POSTGRES_DSN='postgres://telemetry:localdev-only@localhost:15432/telemetry?sslmode=disable' \
  go test -race -count=1 -run '^TestM11DurableHTTPExporterReceiverPersistsKeyAcrossAckLoss$' ./internal/ledger
```

The integration test commits the receiver receipt, deliberately loses the first HTTP ACK, replaces the worker, retries with the same key, and verifies a post-ACK duplicate. It snapshots full authoritative Ledger and Outbox rows before and after. It is a single-process PostgreSQL/loopback test, not a multi-database deployment benchmark.

## Explicit limits

This proves at-least-once sender delivery with receiver-side deduplication within the 30-day key-retention window. It is not end-to-end exactly-once, does not cover arbitrary downstream provider semantics, and establishes no production SLO/RPO/RTO. Prompt/completion content, tool arguments and credentials are not persisted or exported.
