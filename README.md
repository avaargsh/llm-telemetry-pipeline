# LLM Telemetry Pipeline

> **Status:** M1 and M2a Ledger + Outbox merged and CI verified; M2b durable poison-message DLQ is in [PR #5](https://github.com/avaargsh/llm-telemetry-pipeline/pull/5). Not an exactly-once or zero-loss system.

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

Requirements: Go 1.23+, Docker Engine with Compose.

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
- **M2b in review:** malformed or invalidly sealed events are Kafka-ACKed to `events.dlq.v1` before source offsets advance; unknown DLQ ACK is fail-closed.
- Explicitly incomplete: DLQ redrive/retention policy, ACK-loss fault injection, recovery fencing under rebalances, 3-broker Kafka HA, reconciliation service, P95/P99 performance SLO, production authentication/secret management.

## Docs

- [M1 runbook](docs/m1-runbook.md)
- [M2 ledger + Outbox](docs/m2-ledger-outbox.md)
- [M2 durable DLQ](docs/m2-dlq.md)
- [Reliability contract](docs/reliability-contract.md)
- [Verification evidence](docs/verification-record.md)
- [Fault matrix](docs/fault-matrix.md)

**Warning:** Docker Compose contains local development passwords, single-node Kafka and RF=1. Never deploy it as a production HA configuration.
