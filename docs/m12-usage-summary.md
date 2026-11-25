# M12.1: authoritative project usage summary

The independent `usage-query-api` is a read-only HTTP service. It reads
PostgreSQL `usage_ledger`, never Kafka, Outbox or ClickHouse. Replayed
analytics events cannot inflate these totals. This is usage accounting,
not pricing, invoicing, or a production performance claim.

## Contract

`GET /v1/usage-summary?from=2026-11-16T11:00:00Z&to=2026-11-16T12:00:00Z`

- Supply one Authorization: Bearer credential bound to exactly one
  tenant and project. Do not reuse an ingestion credential.
- Both timestamps are required RFC3339 instants; the event-time window
  is half-open [from,to), with positive duration up to 31 days.
  Percent-encode timezone offsets containing a plus sign.
- Unknown and repeated query parameters are rejected. The client
  cannot override tenant or project. Late-arriving finalized events
  can change a historical query.
- All finalized events in scope are counted, including error events
  carrying usage. Token totals are exact decimal strings to avoid
  JSON integer precision loss. A project with no matching records
  returns count 0 and token strings "0".
- 400 invalid window, 401 invalid credential, 405 unsupported method,
  503 database failure/timeout. Responses are not cacheable. SQL details
  are never returned. There is a five-second query deadline.

Example response:

```json
{"request_count":2,"prompt_tokens":"20","completion_tokens":"6","total_tokens":"26"}
```

## Local development

Start PostgreSQL and load the existing ledger schema. The existing
`make ledger-ingest` demo can populate deterministic finalized events
without waiting for Kafka delivery. Use a dedicated database login with
only CONNECT, schema USAGE and SELECT on public.usage_ledger; do not use
the writer or a superuser in deployment. Provision credentials through
your own secret management. Do not put credentials in URLs or Git.

Set these environment variables:

- USAGE_QUERY_DATABASE_URL: dedicated read-only PostgreSQL DSN
- USAGE_QUERY_TOKEN: separate high-entropy bearer token, at least 32 bytes
- USAGE_QUERY_TENANT_ID: exact tenant identifier
- USAGE_QUERY_PROJECT_ID: exact project identifier

Run:

```sh
go run ./cmd/usage-query-api
curl -fsS -H "Authorization: Bearer $USAGE_QUERY_TOKEN" \
  'http://127.0.0.1:8086/v1/usage-summary?from=2026-11-16T11:00:00Z&to=2026-11-16T12:00:00Z'
```

The runtime binds loopback by default and requires a certificate and key
for non-loopback listeners. The first runtime slice uses one tenant/project
credential per instance; the handler supports multiple credential
bindings. Shared deployment, model breakdown, time-series queries,
pagination, scoped database roles/RLS, query indexing and scale
validation are subsequent work. The application scope check is not a
database row-level security guarantee. Do not expose this prototype to
untrusted networks without deployment security and load validation.

## Verification

```sh
go test ./internal/ingestapi -run TestUsageQuery
go test ./internal/ledger -run TestUsageSummary
go build ./cmd/usage-query-api
```

The authoritative ledger test requires TEST_POSTGRES_DSN pointing to a
disposable PostgreSQL database with the schema applied. It checks two
tenants, two projects, inclusive lower/exclusive upper window bounds,
repeated ingestion and empty aggregates.
