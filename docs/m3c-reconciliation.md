# M3c — PostgreSQL ↔ ClickHouse event-level reconciliation

## Contract

The authoritative `usage_ledger` and transactional `event_outbox` live in PostgreSQL. Kafka at-least-once and ClickHouse replayable raw rows are projections. This tool is **read only** and never repairs or bills.

Run against a bounded, nonempty event-time interval `[start,end)`, with at most 10,000 **distinct event IDs per source**. If a source reaches the limit, fail the run: an incomplete scan is not a PASS.

For each event, classify into:

| Status | Meaning | Final outcome |
|---|---|---|
| matched | Ledger and exactly one consistent raw event | PASS |
| replayed | Ledger and 2+ physically identical rows, matching Hash/metadata/tokens | PASS; duplicate count preserved |
| pending | Ledger exists, Outbox not yet confirmed published, analysis missing | INCOMPLETE |
| in_flight | Published recently, still in configured grace, analysis missing | INCOMPLETE |
| missing | Published and older than grace, but no matching ClickHouse event | FAIL |
| orphan | ClickHouse event exists, no authoritative ledger row in window | FAIL |
| conflict | Raw event ID has multiple payload hashes, missing Hash, or inconsistent raw copies; or differs from ledger Hash | FAIL |
| mismatch | Raw event metadata or Token values contradict PostgreSQL despite matching Hash | FAIL |
| outbox_missing | Ledger record missing its transactional Outbox record | FAIL |

The report also includes source counts and total extra physical duplicate copies. Findings are sorted by Event ID and include **no Prompt/Response content**.

CLI exit codes: `0=PASS`, `1=execution/incomplete scan error`, `2=FAIL with confirmed discrepancy`, `3=INCOMPLETE waiting on asynchronous delivery`. In particular, do not treat `INCOMPLETE` as success.

## Local command

```bash
export POSTGRES_DSN='postgres://telemetry:localdev-only@localhost:15432/telemetry?sslmode=disable'
export CLICKHOUSE_HTTP='http://localhost:18123'
export CLICKHOUSE_USER=telemetry
export CLICKHOUSE_PASSWORD=localdev-only
go run ./cmd/reconciler --start 2026-11-16T11:00:00Z --end 2026-11-16T11:01:00Z \
  --grace 3m --max-records 1000 > report.json
```

Use this single-node development password **only** in isolated local Docker. Production requires managed credentials, TLS, audit, and restricted database permissions.

## Evidence

- Unit tests: all eight categories, grace boundary, duplicate copies, missing Outbox and fail-closed bounded scans.
- Real Docker tests: 7 authoritative records, 5 distinct ClickHouse identities, including one orphan, one conflicting Hash, one token mismatch, one missing published event, one unconfirmed Outbox, one recent published event, one clean event and one replayed event. Their expected classifications are asserted.
- CI runs the CLI against the same isolated database fixture, checks its JSON `result=FAIL` and exit code `2`, and requires all eight classifications.
- Neither database is read in a distributed consistent snapshot. A concurrent insert, deletion or publication can change the comparison. Recheck on stable cutoffs before taking remediation actions.
- A row with the **same event_id but different event_time outside the selected window** may be reported as missing/orphan across runs; stable event-time selection and follow-up investigation are necessary. This is a diagnostics tool, not an absolute absence proof.
- The ClickHouse query groups raw events over a bounded time window. The result-count limit is **not a resource-cost guarantee**; production needs query budgets and scheduled small partitions. Do not use this first implementation to scan unbounded historical ClickHouse.
- Genuine source-authorization and full payload re-hashing are not proven by a copied Hash field; ledger authority remains PostgreSQL.
