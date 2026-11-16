# Implementation Plan and Evidence Gates

| Milestone | Deliverable | Evidence gate | Current status |
|---|---|---|---|
| Foundation | Event schema, payload hash, deterministic generator, offset tracker | Focused Go unit tests and race test | Implemented; tests validated locally |
| M1 | Kafka producer/consumer → ClickHouse end-to-end, 100k events | CLI query + 100k count + consumer health + integration test logs | NOT IMPLEMENTED |
| M2 | PostgreSQL ledger transaction + Outbox relay + DLQ | DB uniqueness test, relay restart, consumer replay test | SQL schema only |
| M3 | Toxiproxy, broker/consumer/CH faults, reconciliation | Machine-checkable fault assertions and evidence files | NOT IMPLEMENTED |
| M4 | Prometheus/Grafana + benchmarks and tuned batching | P95/P99, age-of-oldest, lag, rates, comparison reports | NOT IMPLEMENTED |

M1 acceptance counts **events actually written and queried**, not simply NDJSON generated.

M3 top three high-value cases:
1. ClickHouse committed INSERT but worker missed ACK: replay and detect duplicates.
2. Consumer crashed after insert but before Kafka commit: recover from last committed offset.
3. Poison record sent to DLQ: if DLQ write fails, original partition progress MUST NOT commit.

Numbers (throughput, success percentage, latency) are NOT measured until repeatable experiments exist.
