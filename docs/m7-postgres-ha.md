# M7-P0 — PostgreSQL physical standby failover lab

**Not a production HA deployment**: two PostgreSQL 17 containers, one primary and
one hot standby, Docker Compose, physical streaming replication, explicit
single synchronous standby and `synchronous_commit=remote_apply` on primary.
No Patroni, DCS leader election, automatic endpoint switch, quorum, fencing
service, failback, backup/restore or multi-AZ durability is implemented.

## Why this experiment matters

PostgreSQL's `usage_ledger` is the **single accounting authority**.
`event_outbox` must commit atomically with the ledger. Streaming a replica is
not enough: on primary failure, a new writer must avoid losing **acknowledged**
commits, must not expose half-transactions, and must reject stale Outbox leases.
All retries use the exact same Event ID/payload seal; Kafka publishing and
ClickHouse remain separate, at-least-once downstream concerns.

In this **controlled test** `remote_apply` waits for the synchronous hot
standby to apply the WAL record before the primary returns a successful
commit ACK. The probe asserts that the standby was streaming with
`sync_state=sync` and that primary `SHOW synchronous_commit` returned
`remote_apply` before sending 12 committed events. Do **not** generalize
this to all production PostgreSQL setups or all failure modes.

## Run in isolated Docker

```bash
make m7-postgres-ha-smoke
# Same experiment: bash scripts/m7-pgha-failover-smoke.sh
```

The script uses a unique Compose project with two named volumes. It never
connects to an external PostgreSQL server, never restarts a former primary
after promotion, and removes only its own disposable test volumes.

1. Initialize primary with the real ledger/Outbox schema, replication role and
   a **local-dev-only** replication password. Bootstrap a real physical standby
   using `pg_basebackup -R`, then confirm it is streaming and read-only.
2. Enable synchronous standby `FIRST 1 (standby1)` and
   `synchronous_commit=remote_apply`; verify `sync_state=sync`.
3. Call the actual Go `ledger.Store.Record` method for **12** sealed synthetic
   events. Confirm exactly 12 ledger rows + 12 Outbox rows, matching hashes;
   retry a confirmed commit and require `Duplicate`. Claim one Outbox
   lease before failover without falsely marking it Kafka published.
4. Verify all 12 records and hashes from the still-running, read-only standby.
   Open a *new real transaction* with event 100 inserted into both tables but
   not committed, then `SIGKILL` the PostgreSQL primary process.
5. Confirm the old primary is **not running or accepting connections**. Recheck
   that standby still exposes exactly 12 committed records, and manually
   `pg_ctl promote` the standby. **No old-primary restart or repoint occurs.**
6. On promoted primary require 12 idempotent duplicates; the interrupted
   uncommitted event must still be absent. Retrying event 100 with the **same
   stable Event ID** must create exactly one new ledger and Outbox record.
7. Expire a prior (physically replicated) Outbox lease in the disposable lab,
   reclaim under a new worker and prove old worker's
   `MarkPublished` fails with `ErrLeaseLost`. Schedule a retry for the
   current lease, leaving **zero** Outbox rows falsely marked published.
8. Archive a small JSON receipt: counts and boolean proofs only, not credentials,
   raw payloads, ledger event IDs or connection strings.

The CI artifact `m7-postgres-ha-failover-evidence` is required, and
`go test -count=1 ./cmd/pgha-probe` checks the fail-closed receipt contract.

## Explicit omissions and next acceptance gates

This is **manual** failover, with old primary remaining stopped. A successful
experiment does not prove automated primary fencing, protection against
split-brain, automatic service discovery/routing, bounded RTO, arbitrary
hardware loss, or full RPO=0 under every failure mode. With synchronous
`remote_apply`, writes can block when the only synchronous standby is lost:
availability trade-off and behavior under standby loss need a separate test.

A safe production HA phase requires:
- External leadership/quorum and fencing (e.g. Patroni + DCS), tested against
  network partitions, a stale primary and simultaneous node faults.
- Backup/restore, WAL retention, replication slots and capacity management.
- Real TLS, least-privilege connection credentials, secret rotation and
  monitored replication lag.
- Explicit client reconnect/routing and in-flight ACK uncertainty handling.
- Tested RPO/RTO bounds on defined fault envelopes; real on-call and rollback.

**No new billing, Kafka offset, or API gateway code is introduced.**
