# Fault Matrix (planned, not yet executed)

| ID | Fault | Non-negotiable assertion |
|---|---|---|
| F01 | ClickHouse stopped | No progress commit for unpersisted records |
| F02 | SIGKILL after successful ClickHouse insert, before offset commit | Replayed records become visible, ledger does not charge twice |
| F03 | Broker down in RF3 cluster | Quorum-aware availability, inspect ISR and recovery |
| F04 | CH ACK dropped after successful insert | Duplicates expected in raw table; de-duplicated reconciliation succeeds |
| F05 | ClickHouse write throughput throttled | Backpressure, lag and oldest-message age alarms |
| F06 | Schema invalid | Only commit after durable DLQ ACK; otherwise block/retry |
| F07 | Retention window closing | Recover-before-expiry alert, no absolute zero-loss claim |
| F08 | Rebalance while writes in flight | Old owner is fenced; committed progress remains safe |
| F09 | Outbox relay crashes after Kafka ACK but before DB update | Duplicate delivery but same event_id and payload_hash |
