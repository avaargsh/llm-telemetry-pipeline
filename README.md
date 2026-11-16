# llm-telemetry-pipeline — v0.1

> **Status: M1 implementation in review.** Unit tests cover batch insert and commit ordering; real Docker/Kafka/ClickHouse integration and fault-injection evidence are still pending.

A multi-tenant LLM inference telemetry system combining Kafka at-least-once delivery, ClickHouse analytical storage and (M2) PostgreSQL authoritative usage accounting.

## Quick start

Requires Go 1.23+, Docker Engine + Compose, and a network-enabled Go dependency download.

```bash
go mod tidy
go test ./...
make infra-up
make kafka-topics
# terminal A
make consume
# terminal B
make publish
```

See [M1 execution runbook](docs/m1-runbook.md) for exact query commands, expected evidence, and known limitations.

## Current implementation

- Stable event IDs, schema and payload hash validation, reproducible NDJSON generator.
- A Go Kafka publisher that requests `acks=all`, sends in bounded synchronous batches, and partitions by request identity.
- A Go consumer-group worker using `FetchMessage` and synchronous `CommitMessages`. It commits each partition's highest **confirmed** offset only after the full ClickHouse batch is acknowledged.
- A synchronous ClickHouse HTTP `JSONEachRow` writer with event validation, `async_insert=0`, and `wait_end_of_query=1`.
- Unit tests for no commit on write failure, bad event, unknown outcome, and per-partition commit selection.
- PostgreSQL ledger/Outbox and ClickHouse table schemas prepared but the ledger flow is **not yet connected**.

## Reliability boundaries

Kafka acks do not prove ClickHouse persistence. HTTP timeout means an *unknown* insertion result and can cause duplicate raw rows when replayed. Raw ClickHouse sums are not authoritative billing. The M1 broker has one node and **cannot support Kafka HA claims**. M2 adds transactional ledger/outbox, durable DLQ, and analysis-level deduplication; M3 adds fault injection and reconciliation.

Related docs: [reliability contract](docs/reliability-contract.md), [milestones](docs/milestones.md), [fault matrix](docs/fault-matrix.md).
