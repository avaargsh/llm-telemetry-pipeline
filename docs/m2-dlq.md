# M2b — Durable Kafka DLQ (in review)

## Source offset commitment boundary

Each fetched Kafka record either:
- passes the usage schema + SHA-256 seal and is written to ClickHouse; **or**
- is rejected as malformed and synchronously ACKed to `events.dlq.v1` with original source topic, partition, offset, error class, and raw payload (base64 JSON).

A batch commits its source offsets only after all downstream writes succeed. An uncertain DLQ or ClickHouse result stops processing; the entire source batch can replay.

## Known limitations

- DLQ Kafka ACK loss may cause duplicate DLQ entries. `SourceTopic/SourcePartition/SourceOffset` is a stable logical key, not a physical exactly-once guarantee.
- Raw poison payloads can contain sensitive information. Restrict DLQ topic ACLs and retention in production. No raw payload is logged during normal handling.
- Oversized events can exceed Kafka's configured message size; without alternate durable storage they remain fail-closed.
- Backoff/incident escalation and redrive tooling are not yet implemented. Never mark poison messages as handled without a confirmed DLQ write.
- Multi-consumer rebalance and ClickHouse unknown ACK failures remain M3 verification tasks.
