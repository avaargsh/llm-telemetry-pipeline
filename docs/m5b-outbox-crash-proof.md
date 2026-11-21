# M5b-P0: Kafka ACK / Outbox MarkPublished crash-window proof

## Why this experiment exists

PostgreSQL `usage_ledger` and `event_outbox` are the billing-authoritative transaction. The Outbox Relay publishes to Kafka **at least once**, not exactly once. A Relay may receive Kafka's ACK and crash before saving `published_at`; another worker will later re-claim the expired lease and safely republish. Duplicate physical Kafka messages are expected.

This regression test exercises the failure boundary with **real PostgreSQL and Kafka**, not fake transport calls. It does not kill an actual process mid-instruction; it deliberately omits `MarkPublished` at the ACK/crash point and expires the test lease in PostgreSQL. That is a **deterministic crash-window simulation** rather than a full process-kill/PG-HA experiment.

## Reproduction

Requires Go 1.23+, Docker Engine with Compose. Run only in a disposable development environment, as the script destroys its own isolated Compose volumes:

```bash
bash scripts/m5b-relay-ack-crash-smoke.sh
```

The script starts the repository's real PostgreSQL/Kafka containers in a unique Compose project, creates an isolated one-partition topic, then runs:

```bash
TEST_POSTGRES_DSN=... TEST_KAFKA_BROKER=... TEST_M5B_KAFKA_TOPIC=... \
  go test -count=1 -timeout 90s -v ./internal/outbox \
  -run '^TestRelayKafkaACKCrashWindowRealBroker$'
```

The GitHub Actions workflow `m5b-outbox-ack-crash-window` archives a JSON receipt named `m5b-outbox-ack-crash-evidence`. The receipt is generated **only** after the strict test assertions pass; it contains no credentials.

## Assertions / evidence gate

1. One valid sealed `usage.finalized` event creates exactly one PostgreSQL Ledger and Outbox record atomically.
2. Worker A claims the Outbox row (attempt/fencing generation 1), publishes the payload to a real Kafka broker and receives an ACK. The test **does not** call `MarkPublished`.
3. The Outbox remains pending despite Kafka ACK. The test expires its lease and Worker B re-claims the same row (generation 2).
4. Worker A's stale `MarkPublished` is rejected by `ErrLeaseLost`. Worker B sends the same sealed payload, then marks its lease delivered. Double marking is rejected.
5. The real Kafka partition contains **two physical identical messages**; the PostgreSQL ledger has **one** row, the Outbox has **one** row, the attempts counter equals two, and `published_at` is set.
6. The JSON CI receipt must carry the exact `GITHUB_SHA`, expected counts and PASS verdict.

## Honest limits

- This verifies PostgreSQL lease fencing and two ACKed Kafka writes. It does not prove a real Linux process crash, PostgreSQL primary failover, multi-broker quorum durability, ClickHouse analysis reconstruction, or production exactly-once semantics.
- Kafka/ClickHouse physical replay may duplicate rows. **Never invoice from raw Kafka or ClickHouse SUM**; authoritative usage stays in PostgreSQL.
- This test complements M3a consumer replay and M5a authenticated HTTP/Kafka outage E2E; it does not duplicate their integration scope.
- Before merge: exact HEAD GitHub Actions PASS, JSON receipt present, `go test -race ./...`, `go vet ./...`, and existing required M1–M4 regression checks remain green.
