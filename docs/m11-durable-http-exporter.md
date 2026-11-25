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

## Worker runtime

`cmd/genai-observation-worker` runs the PostgreSQL-backed `DurableWorker` continuously. It uses only the existing Go standard library and pgx dependency; no new metrics or HTTP client dependency is added.

Required environment:
- `GENAI_OBSERVATION_DATABASE_URL`: PostgreSQL DSN. All replicas must use the same queue capacity.
- `GENAI_OBSERVATION_RECEIVER_URL`: HTTPS receiver endpoint ending in `/v1/traces`.
- `GENAI_OBSERVATION_RECEIVER_TOKEN`: bearer token (at least 32 printable characters).

Optional bounded tuning:
- `GENAI_OBSERVATION_QUEUE_CAPACITY`: default 10000; must match all writers.
- `GENAI_OBSERVATION_WORKER_ID`: unique active lease owner; defaults to host + process ID.
- `GENAI_OBSERVATION_POLL_INTERVAL`: default 1s, allowed range 100ms..1m.
- `GENAI_OBSERVATION_LEASE`: default 30s; must exceed the HTTP timeout.
- `GENAI_OBSERVATION_HTTP_TIMEOUT`: default 10s.
- `GENAI_OBSERVATION_MAX_ATTEMPTS`, `GENAI_OBSERVATION_INITIAL_BACKOFF`, `GENAI_OBSERVATION_MAX_BACKOFF` and `GENAI_OBSERVATION_RETRY_JITTER`: bounded by `RetryPolicy` validation.
- `GENAI_OBSERVATION_METRICS_ADDR`: default `127.0.0.1:9109`.

The worker exposes `/healthz`, `/readyz` and `/metrics`. Metrics have fixed names and no tenant, model, trace, request or attempt labels: ACKs, retryable outcomes, terminal outcomes, operational errors, idle polls and queue length. The Compose overlay publishes metrics only on host loopback, runs as non-root, read-only, drops Linux capabilities and requires secrets through environment substitution. It is a conservative local deployment template, not a production HA/SLO claim.

Copy `deployments/genai-observation-worker.env.example`, replace the receiver URL/token and database DSN, then export the variables before starting the worker.

Start it with the base PostgreSQL Compose plus the worker overlay:

```sh
export GENAI_OBSERVATION_DATABASE_URL='postgres://telemetry:localdev-only@postgres:5432/telemetry?sslmode=disable'
export GENAI_OBSERVATION_RECEIVER_URL='https://receiver.example/v1/traces'
export GENAI_OBSERVATION_RECEIVER_TOKEN='replace-with-a-secret-of-at-least-32-characters'
docker compose -f deployments/docker-compose.yml -f deployments/docker-compose-genai-observation-worker.yml up --build -d genai-observation-worker
```

Do not commit `.env` files or receiver credentials. The local PostgreSQL password above is only the existing developer Compose default.

## Migration and verification

Apply `migrations/postgres/003_genai_observation_delivery_receipts.sql` before starting any M11.6 `DurableStore` writer, receiver or worker against an existing database. The M11.6 store reads the delivery-receipt counter for both keyed and non-keyed admission paths. For the current M11.8 runtime, apply all outstanding migrations in lexical order through `005_genai_observation_receipt_expiry_atomic.sql`, including migration 004 for capability roles. Migration 005 preserves the expiry function signature and atomically updates receipt counters; it does not repair pre-existing drift. Fresh disposable Compose databases apply migrations automatically from the migration mount; already initialized volumes require an explicit approved migration. See [M11.8 migration contract](m11-durable-observation-queue.md).

Run the receiver key semantics and sender/receiver fault test with PostgreSQL:

```sh
TEST_POSTGRES_DSN='postgres://telemetry:localdev-only@localhost:15432/telemetry?sslmode=disable' \
  go test -race -count=1 -run '^TestDurableStore(IdempotencyKeyReceiptSemantics|RejectsInvalidIdempotencyKeys)$' ./internal/genaiobs

TEST_POSTGRES_DSN='postgres://telemetry:localdev-only@localhost:15432/telemetry?sslmode=disable' \
  go test -race -count=1 -run '^TestM11DurableHTTPExporterReceiverPersistsKeyAcrossAckLoss$' ./internal/ledger
```

The integration test commits the receiver receipt, drops the first TCP response after commit, leaves the sender row under its original lease, expires that lease to simulate process death, replaces the worker, retries with the same key, and verifies a post-ACK duplicate. It snapshots full authoritative Ledger and Outbox rows before and after. It is a single-process PostgreSQL/loopback test, not a multi-database deployment benchmark.

## Explicit limits

This proves at-least-once sender delivery with receiver-side deduplication within the 30-day key-retention window. It is not end-to-end exactly-once, does not cover arbitrary downstream provider semantics, and establishes no production SLO/RPO/RTO. Prompt/completion content, tool arguments and credentials are not persisted or exported.
