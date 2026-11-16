# Implementation Plan and Evidence Gates

| Stage | Delivered capabilities | Evidence status |
|---|---|---|
| Foundation | Immutable sealed usage event, deterministic generator, observed-offset tracking | Go unit + race tests passed |
| M1 | Kafka producer and explicit-commit consumer → ClickHouse | **PASS**: real Docker 1,000 / 100,000 events; signed payload hash now included |
| M2a | PostgreSQL atomic Ledger + Outbox and lease-fenced relay | **PASS**: concurrency/idempotency/rollback + Kafka-down recovery |
| M2b | Durable DLQ with source identity and ACK-before-offset-commit | **PASS**: unit + real Kafka DLQ poison event |
| M2c | Hash-consistent on-read dedup and integrity quarantine | **PASS**: real ClickHouse replay/conflict/legacy hash tests |
| M3a | Kafka replay after lost ClickHouse ACK / failed offset commit | **PASS/MERGED**: real Kafka + ClickHouse tests and full regression CI |
| M3b-P0 | Generation-fenced consumer, 2 workers and 2 partitions | **PASS/MERGED**: real revocation/replay and full 8-workflow CI matrix ([PR #11](https://github.com/avaargsh/llm-telemetry-pipeline/pull/11)) |
| M3b-P1 | 3-broker KRaft failure envelope (RF=3/minISR=2) | **NOT VERIFIED**, Issue #10 |
| M3c | Ledger/ClickHouse read-only reconciler, event-level discrepancy matrix and JSON evidence | **PASS/MERGED**: PostgreSQL+ClickHouse Docker matrix, CLI exit-code assertion and published CI Artifact |
| M2d | Authorized DLQ redrive, retention/ACLs, audit | **NOT IMPLEMENTED** |
| M4 | Prometheus/Grafana, P95/P99, lag age, recovery throughput/latency SLO | **NOT IMPLEMENTED** |

## Ground rules

- Financial totals always come from PostgreSQL `usage_ledger`, not raw ClickHouse sums.
- At-least-once plus event hash validation can establish repeatable analysis totals, **not** exactly-once delivery.
- Do not count synthetic NDJSON output as a successful end-to-end ingestion test.
- Kafka developer Compose has **one broker (RF=1)**. No replicated broker fault-tolerance claim.
- Future claims need CI job/commit references, scenarios, and measured outcomes.
