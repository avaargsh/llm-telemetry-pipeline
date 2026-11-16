# Verification record — 2026-11-16

## Foundation (authoring environment)

- Go tests and race tests for the deterministic event generator, event schema and offset tracker passed.
- NDJSON generator produced exactly 100,000 lines with deterministic replay under fixed seed/start.
- Docker was unavailable in the authoring container. The 100,000 lines here are **generated events**, not a Kafka ingestion benchmark.

## M1 — GitHub Actions verified

- Go unit + race tests, vet, and build: **PASS** on commit `781134f` ([unit CI](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43868109192)).
- Kafka/ClickHouse Docker-backed smoke: **PASS** on commit `d498e05` ([integration CI](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43868049497)).
- Test executed: bring up Kafka 4.1, PostgreSQL 17, ClickHouse 25.8 with Docker Compose; initialize topics; publish 1,000 sealed usage events; consume in bounded Go batches; query ClickHouse.
- Kafka producer logged: **1,000 acknowledged events**.
- ClickHouse query after ingestion: **1,000 raw rows** and **1,000 distinct event IDs**.
- An earlier Docker smoke run failed before ingestion because Apache Kafka ran as uid 1000 against a root-owned KRaft data volume. The Compose fix uses a one-shot root volume initializer, then runs Kafka as its regular non-root user. ([original failing CI](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43867275593)).

## Still unverified

- 100,000 events passing through Kafka and ClickHouse under measured throughput and latency.
- Crash after ClickHouse ACK/before Kafka commit, lost ClickHouse ACK, rebalance fencing, durable DLQ, and source Kafka broker faults.
- PostgreSQL transactional ledger + Outbox lifecycle and authoritative billing deduplication.
- Production Kafka 3-broker/replication-failure behavior; developer Compose has only one broker and RF=1.
- Full end-to-end at-least-once correctness under all documented failure injections.

**Interpretation:** M1 currently demonstrates a functional Docker-backed ingestion smoke test. It does not demonstrate end-to-end exactly-once delivery, zero loss, or financial accounting correctness.
