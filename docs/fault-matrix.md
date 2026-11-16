# Fault Matrix — verified scopes and remaining gaps

The table distinguishes real Docker-backed fault experiments from simulations and unverified cases; none imply absolute zero-loss.

| ID | Fault | Required evidence | Current status |
|---|---|---|---|
| F01 | ClickHouse stopped during Kafka batch processing | No commit after failed CH write; source group resumes from first uncommitted offset | **PASS/MERGED / PR #13**: real stop on batch 2, committed offset 8, replayed offsets 8–23, trusted 24/24 |
| F02 | Crash between successful CH insert and offset commit | Replay accepted; no double count | **PARTIAL**: M3a injected a post-CH-ACK commit failure with real Kafka, but not a physical SIGKILL |
| F03 | Kafka Leader/ISR/broker fault | RF3/minISR2 failure envelope, ACKed event recovery | **PASS / PR #12**: real 3-broker experiment, 35/35 ACKed Event IDs |
| F04 | CH ACK dropped after successful insert | Duplicate raw rows, one logical record | **PASS / PR #7**: real CH plus test ACK-dropping proxy |
| F05 | CH write throughput throttled | Backpressure, lag/oldest age and catch-up throughput | **PR #17 IN REVIEW**: test-only second-batch CH HTTP gate, real group offset 8, lag 24, 32/32 trusted after release; single 24-record catch-up throughput measured |
| F06 | Invalid schema / poison event | Durable DLQ ACK before source commit | **PASS / PR #5** |
| F07 | Kafka retention expiration risk | Broker log start/end, unconsumed broker timestamp, bounded expiry estimate and actionable alert | **PR #17 IN REVIEW**: read-only group lag exporter + effective topic retention + Prometheus warning/critical/data-gap/collector-unavailable rules; Alertmanager delivery remains unbuilt |
| F08 | Group rebalance with in-flight CH write | Revoked owner cannot commit; new owner replays | **PASS / PR #11** |
| F09 | Outbox relay crash after Kafka ACK before DB mark | Same ID/hash, duplicate tolerated, no new ledger charge | **NOT VERIFIED as real fault experiment**; transactional/idempotent path is separately tested in PR #4 |

Billing always uses PostgreSQL `usage_ledger`. The ClickHouse raw table and Kafka delivery are at-least-once. Real tests use isolated development infrastructure, not production incident evidence.
