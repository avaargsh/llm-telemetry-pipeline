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
| M3b-P1 | Three combined KRaft brokers, RF3/minISR2, leader loss, ISR refusal and quorum loss | **PASS/MERGED**: real RF3/ISR/quorum fault matrix, 35/35 ACKed events verified; all 9 CI gates green ([PR #12](https://github.com/avaargsh/llm-telemetry-pipeline/pull/12)) |
| M3d | ClickHouse stopped with a batch in flight; safe offset resume, broker-timestamp retention risk | **PASS/MERGED**: real 24-event outage/replay CI and 10/10 regression suites ([PR #13](https://github.com/avaargsh/llm-telemetry-pipeline/pull/13)) |
| M3c | Ledger/ClickHouse read-only reconciler, event-level discrepancy matrix and JSON evidence | **PASS/MERGED**: PostgreSQL+ClickHouse Docker matrix, CLI exit-code assertion and published CI Artifact |
| M3e-P0 | Real Kafka lag sampler, effective retention, Prometheus alerts, slow CH gating and catch-up | **PASS/MERGED**: 11/11 CI incl real Kafka/ClickHouse slow-write recovery, promtool alert firing/recovery, 24-event measured catch-up ([PR #17](https://github.com/avaargsh/llm-telemetry-pipeline/pull/17)) |
| M2d | Authorized DLQ redrive, retention/ACLs, audit | **NOT IMPLEMENTED** |
| M4a | Actual Prometheus→Alertmanager webhook firing and resolved notifications | **PASS/MERGED**: 3 alerts × firing/resolved = 6 deliveries; real Prometheus/Alertmanager, 12/12 CI ([PR #22](https://github.com/avaargsh/llm-telemetry-pipeline/pull/22)) |
| M4a+ | Live Kafka group offsets and retention risk → Alertmanager webhook firing/resolution | **PR #21 IN REVIEW**: real Kafka 3-alert × firing/resolved CI PASS, full current HEAD pending |
| M4b | Further Grafana dashboards, end-to-end P95/P99, sustained saturation and production authenticated on-call/HA/SLO | **NOT IMPLEMENTED** |

## Ground rules

- Financial totals always come from PostgreSQL `usage_ledger`, not raw ClickHouse sums.
- At-least-once plus event hash validation can establish repeatable analysis totals, **not** exactly-once delivery.
- Do not count synthetic NDJSON output as a successful end-to-end ingestion test.
- Kafka developer Compose has **one broker (RF=1)**. No replicated broker fault-tolerance claim.
- Future claims need CI job/commit references, scenarios, and measured outcomes.
