# M3a — Real Kafka replay across ClickHouse ACK / Kafka commit fault windows

## Hypothesis

Kafka consumer groups implement **at-least-once** event delivery. If ClickHouse has persisted a record but the consumer has not successfully committed its next Kafka offset, restart should replay the same topic/partition/offset. The raw ClickHouse table may contain duplicates. A hash-consistent query-time analytical view must still count one logical event.

## Real integration tests

`TestKafkaCrashWindowsPreserveLogicalUsage` runs with an isolated Kafka 4.1 + ClickHouse 25.8 environment and fresh volumes:

1. `offset_commit_failed`: deliberately fail the Committer **after** a real, successful ClickHouse insert. Close the first reader, open another consumer in the same group and verify the original offset is re-fetched.
2. `clickhouse_ack_lost`: an HTTP proxy confirms a real ClickHouse insert but closes the socket before the caller sees the ACK. The first consumer must not commit. Reopen the group, replay and verify.

For **each** case, assert:

- First attempt: ClickHouse has 1 physical row but no committed Kafka offset.
- Second consumer: same partition and same offset are re-delivered.
- Recovery: ClickHouse has 2 physical rows, dedup view has 1 logical row and the original Token total, no false Hash conflict.
- No production worker intentionally crashes; failures are injected only in the test.

## Limitations

- This proves a finite, scoped crash-window replay, not generalized zero-loss guarantees.
- Testing one local broker with replication factor 1 does not prove Kafka replication failover.
- This does not cover rebalance while a batch is in flight, lease expiry, PostgreSQL Outbox ambiguous publish, DLQ redrive, or ClickHouse durability against host/volume loss.
- The analytical projection is not the PostgreSQL billing authority.

## Reproduction

```bash
TEST_KAFKA_BROKER=localhost:29092 TEST_CLICKHOUSE_HTTP=http://localhost:18123 \
  bash scripts/m3a-replay-smoke.sh
```
