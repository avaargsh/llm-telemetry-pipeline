# M3d: ClickHouse outage during Kafka batch + retention risk evidence

**Status:** implemented in PR; the final Docker CI run is the evidence gate.

## Contract

The test publishes 24 sealed usage events to a **single-partition Kafka topic** with a 10-minute configured retention period and **LogAppendTime** timestamps. A real generation-fenced GroupWorker processes eight records at a time.

1. First eight events are synchronously persisted to ClickHouse; Kafka confirms the consumer offset **8** (highest processed record offset 7).
2. During the second batch's `Insert` call, the test stops the real ClickHouse container. The insert returns failure and the worker must terminate **without committing** any of the second batch's offsets.
3. With ClickHouse still unavailable, inspect the actual broker log start/end offsets and fetch Kafka offset 8 to obtain its actual broker append timestamp.
4. Estimate the oldest unconsumed record's retention headroom (not an exact guaranteed deletion deadline). No offset may be inferred from request payload timestamps.
5. Restart ClickHouse, wait for a real SELECT query, then restart the worker with the **same Kafka group ID**. It must start at Event ID #8, commit offsets **16, 24**, and persist the remaining 16 events.
6. Final raw ClickHouse count and trusted logical count must both be **24** (no integrity conflicts), with a machine-readable JSON report.

The test uses Docker `stop` and `start` on the existing `clickhouse` service, preserving its named data volume. A real Kafka coordinator maintains the committed consumer offsets. No artificial commit stubs are involved in this integration path.

## Lag and retention policy

`internal/lagrisk` assesses the observed **offset distance**, oldest unconsumed Kafka record's **broker timestamp**, a finite configured retention period and a safety margin:

- `no_backlog`: committed offset equals log end
- `healthy`: estimated time-to-expiry greater than margin
- `warning`: equals margin
- `critical`: positive but less than margin
- `expiry_risk`: oldest age has reached/exceeded configured retention
- `data_gap`: committed offset is already below broker log start (potentially unrecoverable gap)

Invalid/unknown offsets, an absent timestamp while lag is present, invalid retention or a future timestamp **fail closed**, not `healthy`. Kafka offset distance is not a guaranteed record count (gaps and compaction exist). Broker retention removes segments rather than individual events, and other topic/cluster policies can alter actual expiry; this is a risk signal, **not** a guarantee.

## Evidence and limits

```bash
make m3d-outage-smoke
# or: bash scripts/m3d-ch-outage-smoke.sh
```

The CI artifact `m3d-clickhouse-recovery-report` includes ACK counts, committed prefixes, replay offsets, trusted ClickHouse counts, Kafka log start/end and measured timestamp-based retention headroom. This scope does not test abrupt disk loss, every possible partial insert failure, live multi-broker replication, Toxiproxy, or scheduled alert delivery.

For financial statements always use the authoritative PostgreSQL `usage_ledger`, not the ClickHouse raw table. This fault experiment uses direct Kafka publication, not the Ledger/Outbox path. No exactly-once or absolute zero-loss claim.
