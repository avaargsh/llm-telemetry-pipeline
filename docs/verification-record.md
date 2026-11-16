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

## M1 100,000-event acceptance

- GitHub Actions matrix run: **PASS** for both **1,000** and **100,000** events ([integration run](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43868660650), branch commit `3f32148`).
- For 1,000: Kafka ACK **1,000**; ClickHouse raw rows **1,000**; `uniqExact(event_id)` **1,000**.
- For 100,000: Kafka ACK **100,000**; ClickHouse raw rows **100,000**; `uniqExact(event_id)` **100,000**.
- Test uses fresh Docker volumes in separate matrix jobs with fixed seeds and a single Kafka broker. It validates nominal ingestion counts, **not** latency SLOs, durability under crashes, or billing idempotency.

## Still unverified

- Measured and reproducible throughput, P95/P99 end-to-end query latency, and saturation limits; the 100,000 event count is verified but those performance measures are not.
- Crash after ClickHouse ACK/before Kafka commit, lost ClickHouse ACK, rebalance fencing, durable DLQ, and source Kafka broker faults.
- PostgreSQL transactional ledger + Outbox lifecycle and authoritative billing deduplication.
- Production Kafka 3-broker/replication-failure behavior; developer Compose has only one broker and RF=1.
- Full end-to-end at-least-once correctness under all documented failure injections.

**Interpretation:** M1 demonstrates 1k and 100k event Docker-backed ingestion tests. It does not demonstrate end-to-end exactly-once delivery, zero loss, or financial accounting correctness.
