# M9 — synchronous PostgreSQL failover during sustained usage ingestion

This is a **fault-injected CI lab** combining merged M7 manual synchronous
PostgreSQL failover and M8 paced HTTP→Ledger/Outbox→Kafka→ClickHouse tests.
No automatic routing, production HA, RPO/RTO or contractual SLO is claimed.

## Reproduce on a disposable Docker host

```bash
python3 -m unittest discover -s scripts -p 'test_m9_failover.py' -v
make m9-pg-ha-sustained-smoke
```

The default **25 offered RPS × 180 seconds = 4,500 unique finalized events**
profile uses 32 client workers. Inject an actual `SIGKILL` around 35 seconds
after the paced client starts, while a separate actual transaction holds both
Ledger and Outbox inserts without committing. A real PostgreSQL 17 physical
standby with `remote_apply` is confirmed synchronized **before workload**
and immediately before the fault.

After proving the original primary is stopped and its localhost listener no
longer accepts connections, the runner manually promotes the standby. It
explicitly restarts the existing API and Outbox Relay pointing at the new
PostgreSQL endpoint. There is no proxy, DCS, Patroni, automatic election or
automatic reconnect. The former primary is never restarted.

The open-loop client continues scheduling each event, even while the API
returns 503 or refuses connections; a 503/timeout is **UNKNOWN**, not a
confirmed loss or a successful billing write. Every retry carries the
**identical Event ID, payload, hash and project-scoped bearer identity**.
A new Event ID is never minted as a recovery shortcut. HTTP 201 with matching
`created` or HTTP 200 with matching `duplicate` is a confirmed response;
a conflict/other 4xx fails the experiment.

At the end, a private local CSV query against the promoted PostgreSQL writer
checks the complete authoritative Ledger/Outbox cardinality and the matching
payload hashes; every outbox row must be marked published by the real Relay.
The CLI independently observes every Event ID in the trusted ClickHouse
deduplicated projection. The deliberately held, uncommitted transaction must
remain absent. On failure the CI returns non-zero, rather than emitting a
passing experiment receipt.

## Artifact and measurement boundaries

The GitHub Actions artifact `m9-pg-ha-sustained-failover-evidence` contains:

- `fault-events.jsonl`: named milestones with one-host monotonic timestamps.
- `client.json`: initial 503/timeout counts, same-ID retry outcomes, offered
  request counts, P95/P99 HTTP ACK for **successes by phase**, P95/P99
  first-observed ClickHouse visibility, schedule lag, initial unknowns resolved,
  and intervals around SIGKILL/promotion/re-routing.
- `docker-stats.csv`: periodic PG/Kafka/CH CPU/memory snapshots.
- `summary.json`: redacted, exact-HEAD strict reconciliation receipt.

Private synthetic Event IDs and tokens are used during the test but are not
uploaded with the artifact. The observer uses 0.5-second ClickHouse polling.
A phase with no successful writes has `null` percentile, **never zero latency**.
The fault window consists of attempted requests, not successful-only sampling.
Write interruption is an observed successful-ACK gap, not a guaranteed
end-to-end maximum outage duration. All time deltas are from one CI host's
monotonic clock.

The artifact can say `PASS_EXPERIMENT_ONLY` only if all requested IDs obtain
a confirmed HTTP response, all are visible in trusted ClickHouse, all unique
Ledger/Outbox hashes and publish markers match, old primary isolation was
verified, and the deliberately uncommitted transaction did not survive.
`slo=NOT_SET` and `production_rpo_rto=NOT_ESTABLISHED` remain mandatory.

The baseline M8 25-RPS candidate regression SLO **must not** be applied
unchanged to a deliberate PostgreSQL writer outage. The pipeline does not
promise write availability during manual failover or failback; the newly
promoted writer has no synchronous replica. Kafka remains RF1 in this
combined experiment. Kafka KRaft 3-node ISR recovery Issue #28 is separate.

## Next production-oriented gaps

External leader election/strong fencing with a DCS, crash/network-partition
fault matrix, standby-failure tradeoffs, pool/endpoint switching, multi-AZ
storage, WAL retention and backup/restore, TLS/least-privilege secrets,
real on-call integration and multi-run tail-latency confidence remain future
work. Do not merge on the basis of an older SHA's successful artifacts.
