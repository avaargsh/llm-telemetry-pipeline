# LLM Telemetry Pipeline

> **Status (2026-11-21):** M1–M6 correctness/recovery/monitoring baselines, M7 PostgreSQL synchronous standby manual-failover lab, M8 sustained P95/P99 scenario guardrails, and M9 controlled failover-under-load experiment are merged and CI verified. [M9 #36](https://github.com/avaargsh/llm-telemetry-pipeline/pull/36) processed 4,500 unique usage Event IDs during 25-RPS/180-second load with 23 uncertain ACKs recovered using the same IDs; [Kafka reconnect #37](https://github.com/avaargsh/llm-telemetry-pipeline/pull/37) passed 3 independent KRaft fault trials. **These are isolated reproducibility experiments, NOT production RPO/RTO, exactly-once Kafka, or a production SLO.** [KRaft quorum/ISR issue #28](https://github.com/avaargsh/llm-telemetry-pipeline/issues/28) is still open.

> **Distribution status:** This public repository does **not yet contain an explicit open-source `LICENSE`**. Do not assume unrestricted reuse, redistribution or community contribution rights. See [OSS readiness and provenance gates](docs/oss-readiness.md) and [Issue #26](https://github.com/avaargsh/llm-telemetry-pipeline/issues/26) for the required maintainer ownership review and license decision.

Go + Kafka (KRaft) + ClickHouse + PostgreSQL for reliable multi-tenant LLM inference usage telemetry.

## Architecture and authority

```text
                                 ┌─ usage_ledger (PostgreSQL; authoritative token usage)
LLM usage.finalized event ────┤
                                 └─ event_outbox (same PostgreSQL transaction)
                                      ↓ lease-fenced Outbox Relay
                                 Kafka usage.critical.v1
                                      ↓ Go Consumer (explicit offset commit)
                                 ClickHouse inference_events_raw (analysis only)
```

The ledger transaction is the **only accepted billing record**. Kafka events and ClickHouse rows may be duplicated by replay; never bill using an unqualified sum over the ClickHouse raw table.

## Quick start (local demonstration)

To generate ten deterministic, synthetic usage events without starting any services, use Go 1.27.4 or later and run:

```bash
make sample > sample.ndjson
```

The output is newline-delimited JSON, one event per line. This checks event generation only; nothing is written to the ledger or published to Kafka.

Requirements: Go 1.27.4 or later, Docker Engine with Compose. CI and Docker build stages validate the selected Go patch version; this is not a deployed production upgrade.

```bash
go mod download
go mod verify
make infra-up
make kafka-topics

# Terminal A:
make consume
# Terminal B:
make relay
# Terminal C:
make ledger-ingest
```

`make ledger-ingest` generates a reproducible 100-event usage stream and stores it atomically in PostgreSQL + Outbox. Relay handles subsequent Kafka publication; the consumer persists it to ClickHouse.

To run the existing direct-to-Kafka M1 smoke path, use `make publish` in terminal C instead.

## Current capabilities

- Stable Event IDs, v1 schema, SHA-256 payload sealing and deterministic synthetic generator.
- **M1:** Kafka Go Publisher/Consumer, manual offset commits after synchronous confirmed ClickHouse inserts, Docker CI at 1,000 and 100,000 events.
- **M2a merged:** PostgreSQL Ledger + Outbox atomic insert; unique event/request idempotency and conflict detection; lease-fenced Outbox claiming, retry with persistent backoff, and mark-published only after Kafka ACK.
- Real PostgreSQL integration tests and Docker smoke for persisted records across Kafka outage.
- **M2b merged:** malformed or invalidly sealed events are Kafka-ACKed to `events.dlq.v1` before source offsets advance; unknown DLQ ACK is fail-closed.
- **M2c merged:** signed event hash now persisted in ClickHouse; a query-level dedup view excludes unsealed/contradictory Event IDs and an integrity view reports them. This protects analysis counts, not billing.
- **M3a merged:** real Kafka group restart after confirmed CH INSERT/failed source offset commit and lost ClickHouse ACK, with identical event replay verified against raw and trusted views.
- **M3b-P0 merged:** the production ingest-worker now consumes per Kafka coordinator Generation; revoked assignments cannot commit offsets, and real 2-worker/2-partition replay was verified in Kafka/ClickHouse CI ([PR #11](https://github.com/avaargsh/llm-telemetry-pipeline/pull/11)).
- **M3b-P1 merged:** isolated 3-broker KRaft RF=3/minISR=2; real leader failover, ISR rejection, combined-controller quorum loss and recovery of 35 Kafka-ACKed sealed Event IDs (PR #12).
- **M3d merged:** real ClickHouse stop during a Kafka batch; 24 ACKed events, safe committed prefix 8, recovery replays the remaining 16; broker-derived retention headroom and risk classification ([PR #13](https://github.com/avaargsh/llm-telemetry-pipeline/pull/13), all 10 CI gates PASS).
- **M3e merged:** read-only Kafka lag/retention exporter with Prometheus/JSON endpoints, broker OffsetFetch + effective Topic DescribeConfigs, fail-closed stale metrics, 4 promtool-verified alerts and real ClickHouse throttling/catch-up CI ([PR #17](https://github.com/avaargsh/llm-telemetry-pipeline/pull/17); 11/11 CI PASS).
- **M4a merged:** real Prometheus→Alertmanager→webhook firing AND resolved delivery for retention critical, Kafka data gap and collector-unavailable alerts; isolated synthetic metrics, not an external on-call pager ([PR #22](https://github.com/avaargsh/llm-telemetry-pipeline/pull/22); 12/12 CI PASS).
- **M4a+ merged:** actual Kafka group lag/retention and broker unavailability → Dockerized Lag Exporter → Prometheus → Alertmanager → webhook. Warning/Critical/CollectorUnavailable firing+resolved delivered in real CI ([PR #21](https://github.com/avaargsh/llm-telemetry-pipeline/pull/21); 13/13 combined CI PASS); complementary to the synthetic metric delivery tests in PR #22.
- **M3c merged:** read-only bounded PostgreSQL↔ClickHouse reconciliation, report statuses for replay, pending, grace, missing, orphan, hash conflict, mismatched tokens, and orphaned Outbox; machine-readable JSON with nonzero failure/incomplete exit codes.
- Explicitly incomplete: authenticated external on-call destinations and production Alertmanager HA/SLO (CI proves delivery only to an isolated webhook), DLQ redrive/retention governance, resilience beyond tested rebalance and single-partition broker-failure envelopes, independent production Kafka controllers, scheduled reconciliation/alerting, P95/P99 performance SLO, production authentication/secret management.

## M7–M9 verification scope

- **M7 merged (#32):** real PostgreSQL 17 synchronous physical standby, old-primary SIGKILL, manual promotion, and Ledger/Outbox idempotency and lease-fencing proof. Not automatic HA or guaranteed RPO/RTO.
- **M8 merged (#33/#34):** three repeatable 25-RPS open-loop trials with scenario-only P95/P99 thresholds and strict evidence. No production SLA.
- **M9 merged (#36):** real open-loop 25 RPS × 180 seconds, PostgreSQL primary SIGKILL with a held uncommitted Ledger/Outbox transaction, manually promoted physical standby, explicit API/Relay reconnection, and all 4,500 unique Event IDs fully reconciled across authoritative PostgreSQL and trusted ClickHouse. The exact-HEAD raw CSV validator, fault-window latency report and final 20/20 regression matrix passed; see [M9 experiment](docs/m9-pg-ha-sustained.md) and [CI evidence](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/44868292150). **PASS_EXPERIMENT_ONLY**, never production RPO/RTO or a fault-mode SLO.
- **M3b reconnect hardening merged (#37):** failover clients rebootstrap using only verified live ISR nodes, retaining the exact event key and sealed payload. Three independent full KRaft fault trials passed (35/35 ACKed IDs in each); this does **not** resolve the intermittent voter-key/ISR root-cause investigation.
- [Kafka KRaft ISR recovery #28](https://github.com/avaargsh/llm-telemetry-pipeline/issues/28) and [OSS rights/license #26](https://github.com/avaargsh/llm-telemetry-pipeline/issues/26) remain open.

## Docs

- [M1 runbook](docs/m1-runbook.md)
- [M2 ledger + Outbox](docs/m2-ledger-outbox.md)
- [M2 durable DLQ](docs/m2-dlq.md)
- [M2c replay-safe analytics](docs/m2c-analytics.md)
- [M3a Kafka replay failure experiments](docs/m3a-replay.md)
- [M3b generation-fenced consumer and rebalance experiment](docs/m3b-rebalance.md)
- [M3b-P1 three-broker RF3/minISR2 fault matrix](docs/m3b-ha.md)
- [M3c Ledger/ClickHouse reconciliation](docs/m3c-reconciliation.md)
- [M6 scheduled reconciliation with redacted evidence and a Kubernetes CronJob template](docs/m6-scheduled-reconciliation.md)
- [M3d ClickHouse downtime and retention headroom](docs/m3d-outage.md)
- [M3e live Kafka Lag exporter, Prometheus alerts and slow ClickHouse recovery](docs/m3e-lag-monitor.md)
- [M4a synthetic metric delivery-layer tests](docs/m4a-alert-delivery.md)
- [M4a+ actual Kafka lag to Alertmanager delivery](docs/m4-live-alerts.md)
- [M5 performance baseline, reproducible SLO candidates, and profile comparator](docs/performance.md)
- [M8 sustained fixed-RPS P95/P99 trial reports and fail-closed SLO evaluator](docs/m8-sustained-slo.md)
- [M9 real PostgreSQL HA failover under sustained load](docs/m9-pg-ha-sustained.md)
- [M11.6 durable HTTP exporter, receiver idempotency and worker runtime](docs/m11-durable-http-exporter.md)
- [OSS readiness, rights provenance and release checklist](docs/oss-readiness.md)
- [Security reporting and limitations](SECURITY.md)
- [Contributor preparation and current license boundary](CONTRIBUTING.md)
- [Reliability contract](docs/reliability-contract.md)
- [Verification evidence](docs/verification-record.md)
- [Fault matrix](docs/fault-matrix.md)

**Warning:** The default Compose is single-node Kafka RF=1. The separate three-broker Compose is a disposable combined-mode CI fault lab, not a production deployment pattern. Do not expose local dev passwords or plaintext listeners.


## M11.7: Independent durable receiver runtime

The independent OTLP Receiver, PostgreSQL least-privilege roles, deployment template and verification contract are documented in [M11.7 Receiver Runtime](docs/m11-receiver-runtime.md). Apply migration 003 before M11.6/M11.7 durable writers and migration 004 for receiver/worker database capabilities.
