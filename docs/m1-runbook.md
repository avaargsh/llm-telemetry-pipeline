# M1 Kafka → ClickHouse smoke runbook

Status: **implementation proposed**; no Docker/Kafka/ClickHouse integration has been executed yet. This is not a fault-tolerance certificate.

## Start the local environment

Requires Go 1.23+, Docker Compose, and internet access to fetch Go dependencies.

```sh
go mod tidy
make infra-up
make kafka-topics
make consume
```

Keep the consumer running in its own terminal. Then publish events in another terminal:

```sh
make publish
```

The generator uses a fixed seed and a fixed start time, so replaying the command creates identical business `event_id` values.

## Query ClickHouse

```sh
curl -u telemetry:localdev-only \
  'http://localhost:18123/?database=telemetry' \
  --data-binary 'SELECT count() AS raw_rows, uniqExact(event_id) AS unique_events, sum(prompt_tokens + completion_tokens) AS raw_tokens FROM inference_events_raw FORMAT PrettyCompact'
```

The initial clean-run target is **100,000 distinct events**. Repeated ingestion can produce more raw rows; never interpret `raw_tokens` as a billing ledger. This project has not yet implemented a deduplicated analytical projection or PostgreSQL Outbox.

## Recovery experiments to execute next

1. Stop the worker after ClickHouse insertion but before Kafka commit; verify replay and compare raw count against `uniqExact(event_id)`.
2. Stop ClickHouse while the worker has a pending batch; verify the worker exits without confirming offsets.
3. Restart ClickHouse and the worker; verify it resumes from its committed group offset.
4. Run a second worker and investigate group rebalance and unknown commit outcomes.

## Known M1 limitations

- Failed Kafka writes may have reached the broker despite client errors; this producer is **not yet transactional or idempotent**.
- The Kafka broker is single-node with replication factor 1. No broker-failure guarantee is demonstrated.
- Invalid events fail closed and can block the consumer group until corrected; durable DLQ is M2.
- The worker stores one bounded batch and retries **on process restart**, rather than retrying in-place.
- Kafka group rebalances can cause replay. The ClickHouse raw table permits duplicates.
- Sigterm during a partial in-memory batch does not commit that batch; it will be replayed.
- PostgreSQL ledger/Outbox and billing consistency are **not implemented yet**.
- The CLI password flag is for isolated local development only; production credentials must use secret configuration.
