# llm-telemetry-pipeline — v0.1 starter

> **Status: foundation only**. This repository is NOT yet an end-to-end Kafka → ClickHouse pipeline and has NOT passed any Kafka, PostgreSQL or ClickHouse fault experiment.

Goal: a multi-tenant LLM inference event pipeline with reliable usage ledger, bounded at-least-once delivery, idempotent accounting, analytical replay and auditable failure recovery.

## Completed in this starter

- `internal/event`: v1 `usage.finalized` schema, SHA-256 event payload sealing and verification, deterministic synthetic events.
- `internal/offset`: per-partition tracker for **observed, acknowledged record prefixes**, including offset gaps and local reassignment fencing.
- `cmd/event-generator`: generates reproducible NDJSON datasets; no external Go dependencies.
- PostgreSQL ledger / transactional Outbox schema; ClickHouse raw MergeTree schema.
- Single-node developer Docker Compose configuration and Kafka topic bootstrap script (**not runtime tested here**).
- Reliability contract, milestones, failure matrix, and a Go CI workflow.

## Quick start (local Go; verified)

Requirements: Go 1.23+.

```bash
go test ./...
go test -race ./...
go run ./cmd/event-generator --count 3 --seed demo-1 --start 2026-11-16T11:00:00Z
# Generate 100k synthetic events for later ingestion experiments:
mkdir -p out
go run ./cmd/event-generator --count 100000 --seed load-001 --start 2026-11-16T11:00:00Z > out/events.ndjson
wc -l out/events.ndjson
```

Running the generator twice with the same `--seed` and `--start` produces identical records. This is useful for repeatable replay tests.

## Local infrastructure (NOT executed/tested in this environment)

Requires Docker Engine + Compose plugin. **Default passwords are for local loopback development only.**

```bash
make infra-up
make kafka-topics
# inspect Kafka, ClickHouse and PostgreSQL container logs
```

Kafka bootstrap address from host: `localhost:29092`; in Docker network: `kafka:19092`.
ClickHouse HTTP: `http://localhost:18123` (user `telemetry` / password `localdev-only`).
PostgreSQL: `localhost:15432`, DB/user `telemetry`, password `localdev-only`.

This environment runs **one Kafka node with replication factor 1**; do not use it as evidence of multi-broker fault tolerance. Three-broker experiments require a separate HA Compose profile in M3.

## Next implementation steps (not yet built)

1. M1: add `gateway-simulator` Kafka producer; implement `ingest-worker` with franz-go and ClickHouse Go Driver; disable auto-commit and commit only confirmed partition prefixes.
2. M1: integrate Kafka & ClickHouse containers and prove 100k records are written/queryable; report duplicate rate separately.
3. M2: implement transactional ledger insert + outbox insertion using PostgreSQL; relay with leases and Kafka ACK tracking; add durable DLQ.
4. M3: inject ACK-loss and crash windows and add automated reconciler with ledger-vs-dedup-CH assertions.
5. M4: metrics, alerting, retention-window risk calculation and benchmarks.

## Critical documentation

- [`docs/reliability-contract.md`](docs/reliability-contract.md)
- [`docs/milestones.md`](docs/milestones.md)
- [`docs/fault-matrix.md`](docs/fault-matrix.md)

## Upstream references

- Apache Kafka consumer offset semantics: https://kafka.apache.org/44/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
- ClickHouse async inserts: https://clickhouse.com/blog/asynchronous-data-inserts-in-clickhouse
- Official ClickHouse Kafka Sink design: https://github.com/ClickHouse/clickhouse-kafka-connect/blob/main/docs/DESIGN.md
