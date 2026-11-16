# M3b-P0: Consumer-group generation fencing (merged)

## Why the original worker was insufficient

The `internal/offset` package already had local generation and acknowledged-prefix tests, but production `cmd/ingest-worker` used the high-level `kafka.Reader` with `CommitMessages`. The application did not directly observe the broker generation or partition revocations. A local incrementing epoch cannot replace Kafka's broker-side generation/member identity.

## Implementation

- Replace the production ingest-worker's implicit consumer group Reader with `kafka.ConsumerGroup.Next`, `Generation.Assignments`, and `Generation.Start`.
- Start one in-order, non-group Kafka Reader per **assigned** topic/partition at the exact offset returned by the current generation.
- Use the generation context for both downstream insertion and the source-offset commit decision. After revocation, never call the coordinator with a stale assignment.
- Commit via **that** `Generation.CommitOffsets` with offset **last observed + 1** only after ClickHouse or durable DLQ ACK. The Kafka Coordinator also validates the actual generation ID and member ID if a rebalance races the local check.
- On downstream uncertainty or process failure, leave the source offset uncommitted and replay. Raw ClickHouse rows may duplicate; the hash-consistent analytical view preserves one logical event.

## Initial CI evidence

[GitHub Actions run 43887608098](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43887608098) passed the real 2-worker test: Worker A had two ClickHouse-confirmed inserts when its old generation was revoked; Worker B committed at least one replayed partition; both Event IDs ended with two raw rows, one trusted event and unchanged Token totals. `m3b-rebalance-evidence` JSON was uploaded. Startup coordination errors were discovered through the M2b DLQ regression and handled with narrowly scoped retries.

## Evidence required for closure

Run `bash scripts/m3b-rebalance-smoke.sh`: start Kafka + ClickHouse, create a 2-partition topic, write one event to each partition. With Worker A initially owning both partitions, block two handlers **after ClickHouse acknowledges**. Start Worker B to cause a real Kafka rebalance. Worker A's old generation context must be canceled, old handlers cannot advance offsets, and B must re-fetch and commit at least one previously uncommitted event. Both events should end with two raw ClickHouse rows, one trusted analytical event, unchanged token usage, and no integrity conflict.

An isolated CI workflow publishes machine-readable `m3b-rebalance-evidence`. Final PR #11 commit `d75da33` passed all eight CI workflows; the review-ready PR was merged as `1e2b4ad`. [Final M3b run](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43888316428) contains the `m3b-rebalance-evidence` JSON Artifact.

## Limitations

- 2-partition broker profile is functional **RF=1**, not the three-broker RF=3/minISR=2 durability scenario tracked separately in Issue #10.
- A Kafka commit that legitimately completed *before* revocation is not incorrect, even if the client observes cancellation later; experiments focus on commits attempted **after** revocation.
- Generation fencing does not make ClickHouse inserts, DLQ writes or Kafka delivery exactly-once. Producer Outbox and PostgreSQL are separate authority layers.

## Final gate

- [x] Go vet, race and build passed
- [x] 1,000 and 100,000 event Kafka→ClickHouse regression passed
- [x] M2 Ledger/Outbox, Kafka DLQ, analysis integrity, M3a replay and M3c reconciliation passed
- [x] Real two-worker Kafka revocation after ClickHouse ACK with correct replay and group commit
- [x] Evidence artifact attached to the GitHub Actions run
- [ ] Three-broker RF=3/minISR=2 failover (separate Issue #10, not part of this milestone)
