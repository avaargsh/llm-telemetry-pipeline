# M2 Ledger + Transactional Outbox (under development)

## Boundary
- `usage_ledger` in PostgreSQL is the only authoritative record of finalized usage.
- `event_outbox` is written **inside the same PostgreSQL transaction** as the new usage row.
- Both primary `event_id` and unique `(tenant_id, request_id, event_type)` identities must agree; a conflicting hash or event ID returns `ErrConflict`.
- The relay leases rows via `FOR UPDATE SKIP LOCKED`, increments `attempts` as a fencing generation, and ACKs Kafka before marking the row published.
- Kafka ACK loss or relay crash after ACK can produce a duplicate; raw ClickHouse rows may be duplicated and must not drive billing.
- A lease expiry lets another relay reclaim the event; the first relay must no longer mark/retry that attempt.

## Developer reproduction
```bash
go mod download
make infra-up
make kafka-topics
# Terminal 1 (start existing M1 consumer):
go run ./cmd/ingest-worker
# Terminal 2:
go run ./cmd/outbox-relay
# Terminal 3:
go run ./cmd/event-generator --count 100 --seed m2-demo --start 2026-11-16T11:00:00Z | go run ./cmd/usage-ledger-ingest
```

## CI evidence expected
- Run PostgreSQL integration tests with `TEST_POSTGRES_DSN` against the real schema.
- Docker smoke intentionally stops Kafka before storing 100 events, then starts Kafka and checks eventual ClickHouse visibility.
- Replay identical events: ledger and outbox row counts remain unchanged.
- Broker replica loss, uncertain ACK recovery, DLQ, consumer rebalance and verification of exactly-once analytical aggregation are **not yet proven**.
