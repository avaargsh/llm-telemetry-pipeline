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

## M2 transactional ledger and Outbox acceptance (2026-11-16)

- **Real PostgreSQL integration: PASS** ([M2 workflow](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43872591353)).
- Verified: duplicate event ID and identical hash produces one ledger/outbox entry; conflicting event hash or alternate ID for the same request is rejected; 12 simultaneous retries still produce one ledger and one outbox event.
- Verified: an injected Outbox INSERT failure causes the ledger INSERT to roll back — neither table contains the attempted event.
- Verified: `FOR UPDATE SKIP LOCKED` lease and attempt fencing rejects stale/incorrect worker acknowledgments, with durable retry scheduling.
- **Kafka-down end-to-end scenario: PASS** in a fresh Docker environment. With Kafka stopped, PostgreSQL stored **100 ledger events** and **100 pending Outbox events**. After Kafka restart, Relay published **100** and ClickHouse reported **100 distinct Event IDs**. Repeating the same immutable 100 inputs resulted in **0 new ledger entries / 100 duplicates**, with ledger and Outbox counts unchanged.
- Go vet, race tests, build and module checksum verification also run in CI. These tests establish a scoped transactional and replay contract, **not** absolute no-loss or exactly-once delivery.

## M2b poison-message DLQ integration (2026-11-16)

- [GitHub Actions Docker DLQ smoke](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43874203942): **PASS** for a valid usage record plus malformed JSON record.
- Normal usage event reached ClickHouse; malformed event was ACKed in `events.dlq.v1`.
- DLQ retained source topic, partition, offset, reason and original raw bytes (base64 encoded).
- Unit tests check DLQ ACK happens before source offset commit, and an ambiguous DLQ ACK prevents offset progress; invalid SHA-256 usage seal is also routed to DLQ.
- This is poison-message isolation, not exact-once DLQ delivery or complete redrive governance.

## M2c replay-safe analytics verification (2026-11-16)

- **RED confirmed** before implementation: the new ClickHouse tests could not find `inference_events_dedup_v1` ([test-only RED run](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43875805853)).
- **Real ClickHouse GREEN:** two sealed physical copies of the same Event ID remain **one** logical analytical record with the same token sum; injection of a second conflicting hash produces an integrity record and removes the event from trusted analytics; legacy hash-less rows are excluded and flagged ([M2c test run](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43876623583)).
- A ClickHouse 25.8 DDL failure caused by reusing `payload_hash` as an aggregate alias was found and fixed by renaming the view output to `canonical_hash`.
- The corrected Go writer preserves the SHA-256 event hash; M1 smoke now checks both the trusted logical event count and integrity exceptions.
- This verifies correctness for tested event-level replay and corruption scenarios; it does not make the analytical view a billing ledger or prove performance SLOs.

## M3a real Kafka/ClickHouse failure-window evidence (2026-11-16)

- [GitHub Actions real Docker integration](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43878130621): **PASS** for both crash-window scenarios.
- `offset_commit_failed`: first ClickHouse INSERT acknowledged, Kafka Committer deliberately returns failure. New reader in the **same consumer group** gets original offset `0`; raw ClickHouse rows `2`; trusted logical events `1`; deduped total `227` tokens.
- `clickhouse_ack_lost`: test HTTP proxy gets the actual ClickHouse success response, then closes client socket before forwarding ACK. New reader in the same group gets original offset `1`; raw rows `2`; logical events `1`; deduped total `227` tokens.
- Tightened follow-up test asserts that a missing ClickHouse ACK causes **zero attempts** to commit Kafka offsets, while failed-commit scenario must actually attempt a commit. Final CI status is tracked on PR #7.
- These are scoped single-broker tests; do not infer cross-broker durability, unattended recovery, full workflow exactly-once semantics, or business-ledger reconciliation.

## Still unverified

- Measured and reproducible throughput, P95/P99 end-to-end query latency, and saturation limits; the 100,000 event count is verified but those performance measures are not.
- Crash after ClickHouse ACK/before Kafka commit, lost ClickHouse ACK, rebalance fencing, durable DLQ replay/redrive and retention governance, and source Kafka broker faults.
- Production Kafka 3-broker/replication-failure behavior; developer Compose has only one broker and RF=1.
- Full end-to-end at-least-once correctness under all documented failure injections.

**Interpretation:** M1 demonstrates 1k and 100k event Docker-backed ingestion tests. It does not demonstrate end-to-end exactly-once delivery, zero loss, or financial accounting correctness.
