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
bindings. Shared deployment,
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

## M12.2: per-model breakdown

`GET /v1/usage-summary/models?from=2026-11-16T11:00:00Z&to=2026-11-16T12:00:00Z`

Uses the same authenticated tenant/project, time bounds and exact decimal
token strings as the overall summary. Returns a JSON array ordered by model
name (PostgreSQL C collation); an empty window returns `[]`.

```json
[{"model":"Qwen3-32B","request_count":2,"prompt_tokens":"20","completion_tokens":"6","total_tokens":"26"}]
```

The response supports at most 100 distinct models per window. A larger
breakdown returns HTTP 422 with error `usage_model_limit_exceeded` and no
partial totals. Narrow the time window or use the overall summary; model
pagination is not implemented. The five-second query deadline and all
scope validation remain unchanged. This bounds returned rows, not database
scan work; the index and load validation are still outstanding.

## M12.3: UTC hourly and daily series

`GET /v1/usage-summary/series?from=2026-11-21T00:00:00Z&to=2026-11-21T02:00:00Z&granularity=hour`

The existing credential-derived tenant/project scope, five-second deadline,
exact decimal token strings and half-open window apply. Both endpoints of
the window must align to whole UTC hours (`hour`) or UTC midnight
(`day`). The maximum window is 31 days, yielding at most 744 hourly
or 31 daily buckets. Invalid granularity or unaligned bounds returns 400.

The response contains every bucket in ascending UTC order, even when
no finalized ledger events exist in that bucket. For example:

```json
[
  {"bucket_start":"2026-11-21T00:00:00Z","request_count":1,"prompt_tokens":"10","completion_tokens":"3","total_tokens":"13"},
  {"bucket_start":"2026-11-21T01:00:00Z","request_count":0,"prompt_tokens":"0","completion_tokens":"0","total_tokens":"0"}
]
```

Zero means no matching finalized ledger events in that read snapshot,
not proof that upstream telemetry is complete. A single SQL read
supplies the populated buckets; later arrivals can change a subsequent
query. UTC days are always 24 hours; local calendar days and daylight
saving boundaries are not supported. This is not a production SLO.
