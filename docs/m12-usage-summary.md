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
only CONNECT and membership in llm_telemetry_usage_query_reader, with a
matching administrator-managed scope binding. Apply migration 007 first.
Do not use the writer or a superuser. Provision credentials through
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
pagination, additional deployment controls and scale
validation are subsequent work. The query runtime uses a scoped database view, not writer-table RLS. Do not expose this prototype to
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
scan work; load validation remains outstanding.

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

## M12.4: scoped ledger query index

Migration `006_usage_ledger_scope_event_time_index.sql` creates a
non-unique B-tree on (tenant_id, project_id, event_time). It matches the
equality predicates and half-open time range shared by the summary, model
and series queries. It does not change usage identity, write transaction
semantics, authorization or query results.

Fresh disposable Compose databases apply the migration in lexical order.
Existing volumes require explicit migration through the approved rollout
process; initialization mounts do not upgrade existing databases.
The standard CREATE INDEX takes a lock that conflicts with writes.
For a large or continuously written database, plan a maintenance window
or a separately reviewed concurrent build procedure outside a transaction.
Do not blindly retry a failed concurrent build; inspect index validity
and readiness. Do not skip validation merely because an index name exists.

The PostgreSQL integration test checks the actual index catalog:
valid and ready, non-unique B-tree, full index, and exact ordered key
columns. This is schema evidence, not a latency benchmark. PostgreSQL
may legitimately choose a sequential scan for a small or non-selective
window. Measure EXPLAIN (ANALYZE, BUFFERS), index size, write throughput,
WAL volume and representative query latency before claiming a performance
improvement or setting a production SLO.

The disposable physical HA Compose profile also loads migration 006 after
its ledger and replication bootstrap. M7/M9 require the index to be ready
and valid on the primary, streaming standby and promoted standby. This
includes index maintenance in the measured write/failover path; older
measurements without the index are not directly comparable. The scripts
do not claim index-specific latency improvement.

## M12.5: database login scope and runtime integration

Migration `007_usage_query_database_scope.sql` introduces a non-login
reader group, an administrator-managed mapping from authenticated database
login to exactly one tenant/project, and two security-barrier views:

- `public.usage_query_scope`: the login's own binding.
- `public.usage_query_ledger`: only the columns and rows needed for
  scoped aggregation; an unbound login sees no rows.

The views use PostgreSQL session_user, not caller-supplied settings or a
SET ROLE target. The reader group has SELECT on these views and no grants
on the underlying ledger, outbox or binding table. The administrator must
provision a unique dedicated login per project, with no broader inherited
roles or direct grants, and maintain the mapping through a reviewed
process. Do not reuse login names without removing obsolete bindings.
Keep view/table ownership trusted and audit effective privileges, including
PUBLIC grants. The views are not row-level security on usage_ledger and
do not modify the existing writer's permissions.

Fresh Compose databases apply 007 in lexical order; existing databases
need an explicit migration. The physical HA write-only lab intentionally
does not provision reader accounts. The integration test uses disposable
transactional role and row fixtures, switches session authorization and
checks scope visibility and effective privileges. It rolls back the
fixtures. This is not a production deployment or penetration-test result.

The query API now requires the dedicated scoped login. Startup checks the
HTTP configuration against the database binding and rejects privileged,
unbound or mismatched sessions. Every request repeats the check in a
read-only repeatable-read transaction before querying the restricted view.
A revoked or changed binding fails closed on the next transaction; an
already-started transaction can finish against its earlier snapshot.
Migration 007 and administrator provisioning are required before starting
the updated service. Direct writer-store aggregation methods remain for
internal tests, but the HTTP runtime uses the scoped reader exclusively.
The readiness checks do not replace an audit of database-wide privileges.
