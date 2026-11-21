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

`event_id` and `(tenant_id, request_id, event_type)` retain the existing ledger's unique constraints. The API accepts only a single, strictly decoded `usage.finalized` v1 sealed JSON event, max **64 KiB**, with no unknown fields or trailing JSON. The `payload_hash` is an integrity digest, **not** a signature, not a substitute for authentication. The API does not store prompt/response bodies.

`GET /healthz` is a **liveness** endpoint and does not assert PostgreSQL, Kafka or ClickHouse readiness.

## Security and operational limits

- Default bind is loopback. A non-loopback listener is **rejected** without `--tls-cert` and `--tls-key`; TLS private keys and bearer secrets must be provisioned securely, never committed or logged.
- Generate random 32-byte-or-stronger bearer secrets. Per-credential scope is fixed to one tenant/project. Token revocation is configuration reload/restart, **not** dynamic IAM.
- This milestone does not add distributed rate limits, token rotation, per-tenant quotas, production ingress/mesh policy, external IAM, a durable audit stream, cross-AZ PostgreSQL HA, or a production SLO. These are follow-up gates, not implicit capabilities.
- The API intentionally never directly publishes to Kafka. Kafka outages do not erase a successfully committed Ledger/Outbox row. Relay replay may create duplicate downstream events; ClickHouse is analysis only.

## Verification

- `go test -race ./internal/ingestapi` covers auth, scope, strict JSON, tampering, size, conflict, duplicate, method safety, and fail-closed unknown DB outcomes.
- `TEST_POSTGRES_DSN=... go test -count=1 -v ./internal/ingestapi` exercises a real PostgreSQL transaction and confirms exactly one Ledger row plus one Outbox row for create + duplicate + conflicting replay.
- GitHub Actions `m5a-authenticated-ingestion` starts the repository's isolated PostgreSQL Compose service and runs this integration test.
- Existing M1–M4 reliability tests must remain green before merging; **no real Kafka/ClickHouse E2E evidence is claimed solely by this API change**.
