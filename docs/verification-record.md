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
- Tightened follow-up test asserts that a missing ClickHouse ACK causes **zero attempts** to commit Kafka offsets, while failed-commit scenario must actually attempt a commit. Strict commit-attempt assertions and all M1/M2 regressions passed on PR #7; merged into master.
- These are scoped single-broker tests; do not infer cross-broker durability, unattended recovery, full workflow exactly-once semantics, or business-ledger reconciliation.

## M3c PostgreSQL ↔ ClickHouse reconciliation evidence (2026-11-16)

- **RED:** test-only `reconcile_test.go` initially could not compile without the new bounded classification contracts; Go CI correctly failed before implementation ([RED job](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43882308777)).
- **GREEN:** real PostgreSQL + ClickHouse Docker CI ([M3c integration](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43883539767)) passed. The authoritative ledger had **7** events, ClickHouse had **5** unique Event IDs, and eight status categories each had **1** event: matched, replayed, pending, in_flight, missing, orphan, conflict, mismatch.
- In the same isolated data set the stand-alone CLI produced a machine-readable JSON report with `result=FAIL` and **exit code 2**. This is the expected negative test: detected discrepancies must never be reported as success.
- Sources enforce a maximum of 10,000 distinct Event IDs each, fail closed on truncated scans, and distinguish no-Outbox rows from merely unconfirmed Outbox publication. Grace periods are based on `published_at`, **not** on the inference event timestamp.
- The report is read-only and does not automatically repair or charge. Independent DB snapshots, different event-time partitions, historical scans and complete operational authorization are not yet covered. Kafka HA is not proven.

- Final PR #8 [CI evidence](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43884245869) passed and published the `m3c-reconciliation-report` downloadable JSON Artifact. The complete Go/M1/M2/M3a regression matrix passed on the same PR head; merged at commit `5bdf978`.

## M3b-P0 generation-fenced consumer/rebalance evidence (2026-11-16)

- [PR #11](https://github.com/avaargsh/llm-telemetry-pipeline/pull/11) makes the real production `ingest-worker` use `kafka.ConsumerGroup` generations and per-partition readers, rather than attaching an untrusted local epoch to the high-level Reader API.
- Unit tests cover next-offset semantics despite numeric Kafka offset gaps, rejection of unassigned partitions, a generation canceled after an INSERT but before broker commit, broker generation errors, and retryable coordinator startup errors vs. permanent authorization failures.
- Initial Go compile failure (unused imports) was corrected. A real Kafka DLQ regression then exposed `GroupCoordinatorNotAvailable` on startup; the new worker now backs off for transient group-coordinator/rebalance errors, and does not retry permanent authorization/configuration errors.
- [Real Docker 2-worker, 2-partition rebalance CI](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43887608098): **PASS**. Worker A first inserted both events in ClickHouse, was revoked while processing; Worker B acquired an uncommitted partition and committed at least one replayed event. **Each event had two raw ClickHouse copies, one trusted logical event, and no hash conflict**. Machine-readable `m3b-rebalance-evidence` artifact published.
- [1k/100k Kafka→ClickHouse new-worker regression](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43887607852): **PASS** on the same behavior implementation.
- **Final PR #11 HEAD `d75da33`: all eight GitHub Actions workflows PASS.** [Real M3b rebalance run](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43888316428) published the machine-readable `m3b-rebalance-evidence` Artifact; [M1 1k/100k regression](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43888316326) and all M2/M3a/M3c regression workflows also passed. PR merged as `1e2b4ad`.
- This is still **single-broker RF=1**, not a 3-broker HA proof. Replicated durability remains Issue #10.

## M3b-P1 three-broker KRaft fault matrix (2026-11-16)

- [PR #12](https://github.com/avaargsh/llm-telemetry-pipeline/pull/12) adds a separate three-node Kafka 4.1 combined-mode KRaft Compose profile (RF=3, permanent test-topic minISR=2, acks=all), leaving single-broker development Compose unchanged.
- [Initial 3-broker run](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43891556301): **PASS**. Leader 2→3 after stopping Broker 2, ISR 3→2→3, Kafka returned protocol code **19 NotEnoughReplicas** with test-topic minISR temporarily set to 3; all **35 broker-ACKed sealed events** were reread with identical bytes.
- [Extended quorum-loss run](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43892476284): **PASS**. Leader 3→1 under the single Broker failure. Later two combined Broker/Controller nodes were stopped; `acks=all` returned explicit NotEnoughReplicas, **not** a successful producer ACK. After restoring both nodes, all 35 previously ACKed event IDs passed the readback check again. Attached `m3b-kafka-ha-report` JSON Artifact.
- **Final PR #12 head `b6ed95d`: all nine CI workflows PASS**, including Go race/build, M1 1k/100k Kafka→ClickHouse, M2a/M2b/M2c, M3a/M3b-P0 and M3c regressions. [Final HA run](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43893131251) uploaded the `m3b-kafka-ha-report` JSON Artifact. PR merged as `8651745`.
- **Scope:** this proves RF3/minISR2 behavior under the stated single-partition, local-volume, combined-mode faults. It does not establish independent multi-AZ controllers, rack-awareness, durable storage surviving disk loss, exactly-once delivery or global zero loss.
- An operation that returned a timeout/non-ACK can still be present in Kafka after a failure; it must not be counted as broker-ACKed or silently assumed absent.

## M3d ClickHouse mid-batch outage, replay and retention headroom (2026-11-16)

- [PR #13](https://github.com/avaargsh/llm-telemetry-pipeline/pull/13) adds a real Kafka group and ClickHouse Docker fault experiment. 24 sealed broker-ACKed events were produced in 8-event batches.
- [First real M3d run](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43896683695) **PASS**: the first batch persisted and Kafka committed offset 8 (last event offset 7). The real ClickHouse container was then stopped during the second batch's `Insert`; the worker failed closed without advancing offsets. After restarting ClickHouse, the same group reprocessed offsets 8–23; final raw/trusted events **24/24**, integrity conflicts **0**.
- The test independently queries the Kafka broker log start/end and fetches the *first unconsumed record's Kafka LogAppendTime* for a scoped 10-minute retention setting. It evaluates offset distance and estimated expiry headroom, **not** event payload timestamps. The `m3d-clickhouse-recovery-report` JSON Actions Artifact preserves measured values.
- New `internal/lagrisk` distinguishes healthy/early warning/critical/estimated expiry and committed offsets that have fallen before the Kafka log start. Unknown offsets or a missing timestamp fail closed, never report healthy.
- TDD initial RED ([test-first source commit](https://github.com/avaargsh/llm-telemetry-pipeline/commit/ccddd8335178c6cb30da9b92d773eb0fc0d31e97)) and real E2E GREEN were recorded. A follow-up tightened the early-warning interval. **Final PR #13 head `44fdd02`: all 10 CI workflows PASS**, including [M3d outage/replay and retention-headroom](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43897760439) with JSON Artifact and [M1 1k/100k regression](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43897761211). Squash-merged as `0ef82c0`.
- **Boundaries:** one Kafka partition, RF=1 dev Compose, ClickHouse process stop/start without disk loss; the direct-to-Kafka experiment does not verify transactional Ledger/Outbox for these 24 records. Retention-expiry estimates are not guarantees and no scheduled alert integration exists.

## M4a Kafka lag monitor, throttled ClickHouse and alert transitions (2026-11-16)

- [PR #15](https://github.com/avaargsh/llm-telemetry-pipeline/pull/15) adds a read-only Kafka collector using `OffsetFetch`, actual partition log start/end, topic-effective `retention.ms` and `LogAppendTime`. It rejects unknown consumer offsets, disabled/unknown effective retention and non-broker timestamp policy instead of reporting zero lag.
- The independent `cmd/lag-monitor` serves Prometheus `/metrics`, structured `/status`, and `/healthz`; failed or stale Kafka polling emits unhealthy state even when a previous snapshot existed.
- [First real Kafka/ClickHouse throttle CI](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43901243415): **PASS**. A test-only HTTP proxy delayed the second ClickHouse batch for 1.2 seconds. The real Kafka group commit remained at offset 8, log end 24 and offset lag 16. After releasing the proxy, the group committed offset 24, lag fell to zero and ClickHouse retained **24 trusted Event IDs with zero hash conflicts**.
- That CI measured **162.31 events/sec** as the catch-up rate of its remaining 16-event micro workload; this is **not** a steady-state throughput benchmark or production SLO.
- Prometheus rules for monitor-unavailable, early-warning, critical retention and committed-before-log-start are syntax-checked. A separate `promtool test rules` scenario covers firing and clearing with multiple monitor instances; final CI status is tracked on PR #15.
- Dev topic bootstrap now sets `LogAppendTime` and finite 7-day `retention.ms` on newly created `usage.critical.v1` topics; pre-existing topics are **not silently modified**.
- **Boundaries:** one partition per exporter, local single broker/CH fault lab, non-distributed sample reads, no actual Alertmanager notification delivery, no multi-AZ durability or performance SLO claim.

## Still unverified

- Sustained benchmark throughput, P95/P99 query latency, saturation limits, full multi-partition lag monitoring, and an operational Prometheus+Alertmanager deployment/notification delivery test. The M4a micro workload catch-up measurement and 100,000 event count do not establish performance SLOs.
- Consumer rebalance fencing beyond tested 2-worker conditions; durable DLQ redrive and retention governance; and broker/partition/rack fault combinations beyond the tested one-partition RF3 HA lab.
- Production Kafka topology with dedicated controllers, cross-zone placement, authenticated listeners, real persistent storage and operational recovery. Developer Compose remains one Broker with RF=1.
- Full end-to-end at-least-once correctness under all documented failure injections.

**Interpretation:** M1 demonstrates 1k and 100k event Docker-backed ingestion tests. It does not demonstrate end-to-end exactly-once delivery, zero loss, or financial accounting correctness.
