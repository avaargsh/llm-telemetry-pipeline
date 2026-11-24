# M5a: authenticated usage ingestion (scoped HTTP boundary)

This is a deliberately small extension to the existing **PostgreSQL-authoritative** Ledger + transactional Outbox. It does **not** bill from Kafka or ClickHouse, calculate money, or claim exactly-once/zero-loss guarantees.

## Local quick start

Run `make infra-up` to start the existing local Postgres/Kafka/ClickHouse stack (the API only needs PostgreSQL). Use a **new** secret for every deployment:

```bash
export DATABASE_URL='postgres://telemetry:localdev-only@localhost:15432/telemetry?sslmode=disable'
export USAGE_INGEST_TOKEN="$(openssl rand -hex 32)"
export USAGE_INGEST_CREDENTIALS_JSON="$(printf '[{"token":"%s","tenant_id":"tenant_001","project_id":"project_001"}]' "$USAGE_INGEST_TOKEN")"
go run ./cmd/usage-ingest-api --listen 127.0.0.1:8085
```

In a second terminal, with `USAGE_INGEST_TOKEN` exported from the first terminal:

```bash
go run ./cmd/event-generator --count 1 --seed m5a-demo --start 2026-11-21T10:00:00Z | \
  curl --fail-with-body -sS -X POST 'http://127.0.0.1:8085/v1/usage-events' \
    -H "Authorization: Bearer $USAGE_INGEST_TOKEN" \
    -H 'Content-Type: application/json' --data-binary @-
```

The first request returns `201 {"event_id":"...","result":"created"}`. Resubmitting **exactly the same body** returns `200` with `result=duplicate`. The authenticated credential must match the payload's `tenant_id` **and** `project_id`. Multiple credential entries may be configured, one per tenant/project binding. Identity is not inferred from client-controlled headers.

## HTTP contract and failure semantics

| Status | Meaning | Client behavior |
| --- | --- | --- |
| `201` | New event committed to PostgreSQL Ledger and Outbox | Do not treat as Kafka/CH delivery ACK |
| `200` | Same immutable event already recorded | Successful idempotent retry |
| `400`/`413`/`415` | Invalid/mutated/oversized event or unsupported media type | Fix payload, do not auto-retry |
| `401`/`403` | Missing/invalid credential or tenant/project scope mismatch | Reject and investigate |
| `409` | Conflicting `event_id`/request identity or `payload_hash` | Never modify/retry as if duplicate |
| `503` | DB failure or uncertain commit/outcome | Retry exact same event identity and bytes with backoff |

`event_id` and `(tenant_id, request_id, event_type)` retain the existing ledger's unique constraints. The API accepts only a single, strictly decoded `usage.finalized` v1 sealed JSON event, max **64 KiB**, with no unknown fields, trailing JSON, or duplicate object members (including case-folded and escaped spellings of the same key). Every one of the 13 canonical v1 JSON member names must occur exactly once with a non-null value. Missing members and case-folded aliases are rejected; explicit numeric zero remains valid. Member order and whitespace do not affect the existing digest. Ambiguous or incomplete bodies are rejected with HTTP 400 before scope checking or any Ledger/Outbox write. The existing v1 digest and legitimate idempotent retry semantics are unchanged. The `payload_hash` is an integrity digest, **not** a signature, not a substitute for authentication. The API does not store prompt/response bodies.

`GET /healthz` is a **liveness** endpoint and does not assert PostgreSQL, Kafka or ClickHouse readiness.

## Security and operational limits

- Default bind is loopback. A non-loopback listener is **rejected** without `--tls-cert` and `--tls-key`; TLS private keys and bearer secrets must be provisioned securely, never committed or logged.
- Generate random 32-byte-or-stronger bearer secrets. Per-credential scope is fixed to one tenant/project. Token revocation is configuration reload/restart, **not** dynamic IAM.
- This milestone does not add distributed rate limits, token rotation, per-tenant quotas, production ingress/mesh policy, external IAM, a durable audit stream, cross-AZ PostgreSQL HA, or a production SLO. These are follow-up gates, not implicit capabilities.
- The API intentionally never directly publishes to Kafka. Kafka outages do not erase a successfully committed Ledger/Outbox row. Relay replay may create duplicate downstream events; ClickHouse is analysis only.

## Verification

- `go test -race ./internal/ingestapi` covers auth, scope, strict JSON, tampering, size, conflict, duplicate, method safety, and fail-closed unknown DB outcomes.
- `TEST_POSTGRES_DSN=... go test -count=1 -v ./internal/ingestapi` exercises a real PostgreSQL transaction and confirms exactly one Ledger row plus one Outbox row for create + duplicate + conflicting replay.
- GitHub Actions `m5a-authenticated-ingestion` starts PostgreSQL and runs the API integration test, then a **separate** disposable full-path fault scenario: HTTP → authoritative PostgreSQL Ledger/Outbox while Kafka is stopped → Kafka recovery → Relay → ClickHouse trusted projection → read-only PG↔CH reconciler.
- The full-path scenario submits 16 events across eight project-scoped credentials, checks one duplicate and two rejected requests, and verifies 16 Ledger + Outbox records during Kafka downtime and 16 reconciled events after recovery. It writes a commit/run-linked JSON artifact `m5a-http-kafka-recovery-evidence` containing no credentials.
- Run locally with `make m5a-http-recovery-smoke`. It uses a separately named, disposable Docker Compose project and destroys that project's volumes on exit. Requires Docker, Go, curl, openssl and Python 3.
- **Evidence boundary:** these results count only if the GitHub Actions job succeeds on the exact PR HEAD. This isolated local Docker test does not establish Kafka HA, PostgreSQL HA, live on-call delivery, sustained SLO, or exactly-once guarantees. Existing M1–M4 regression checks must also pass before merge.
