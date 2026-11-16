# M3e — Live Kafka Lag, effective Topic retention, and ClickHouse backpressure

**Status:** PASS/MERGED as `049a3d8` ([PR #17](https://github.com/avaargsh/llm-telemetry-pipeline/pull/17)); all 11 final CI workflows passed.

## Operator contract

The `cmd/lag-exporter` reads a **single Kafka Topic + Consumer Group** without becoming a group member or committing offsets:

1. Fetch metadata for the Topic and bound the total partitions (default 16, maximum 32).
2. Describe the effective Topic `retention.ms` and `message.timestamp.type` from Kafka, including inherited defaults where returned. Infinite or missing retention and `CreateTime` are **not** accepted for broker-append-time retention risk.
3. Discover the actual group coordinator and read group committed **next** offsets via `OffsetFetch` rather than inferring them from a Consumer Reader.
4. Fetch partition log-start/log-end and, where lag is positive, the first available unconsumed record's broker `LogAppendTime`.
5. Apply `internal/lagrisk` with explicit safety margin. Invalid committed positions, unknown timestamps, retention configuration or any incomplete partition sample make the **entire** collector sample fail.
6. Serve Prometheus `/metrics` and JSON `/status` (defaults to `127.0.0.1:9108`). An error immediately clears old per-partition metrics and sets `llm_telemetry_lag_scrape_success 0`; `/status` responds HTTP 503.

### Local run

Start the development Kafka stack and create the Topic/Consumer Group **with an actual successful group commit** first. Configure LogAppendTime and finite retention explicitly on a disposable Topic; changing this on production topics alters timestamp semantics and requires a migration plan.

```bash
make infra-up
make kafka-topics
# Only in a disposable test environment; changing topic configuration is an operator action.
docker compose -f deployments/docker-compose.yml exec -T kafka \
  /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:19092 \
  --alter --entity-type topics --entity-name usage.critical.v1 \
  --add-config 'retention.ms=600000,message.timestamp.type=LogAppendTime'

go run ./cmd/lag-exporter \
  --brokers localhost:29092 --group telemetry-clickhouse-v1 \
  --topic usage.critical.v1 --safety-margin 2m \
  --interval 15s --timeout 10s --listen 127.0.0.1:9108
curl -s http://127.0.0.1:9108/metrics
curl -s http://127.0.0.1:9108/status
```

When the monitored group has **no committed offset**, the exporter intentionally reports sampling failure rather than treating its backlog as zero. Do not override this with a fabricated offset. Endpoint is loopback only by default: production network exposure requires TLS/authentication and explicit access control.

## Metrics and alert policy

- `llm_telemetry_lag_scrape_success`: 1 for a fully supported sample; 0 otherwise
- `llm_telemetry_kafka_lag_offsets`: Kafka log-end minus committed offset, **not guaranteed record count**
- `llm_telemetry_kafka_committed_offset`, `llm_telemetry_kafka_log_start_offset`
- `llm_telemetry_kafka_oldest_unconsumed_age_seconds`
- `llm_telemetry_kafka_retention_headroom_seconds`: estimated, not a deletion deadline
- `llm_telemetry_kafka_retention_risk{status="..."}`: current status gauge
- `llm_telemetry_kafka_data_gap_offsets`: group committed position already earlier than broker's earliest retained offset

Rules are in `deployments/prometheus/alert-rules.yml` and include Warning, Critical, Data Gap and Sample Failure. A minimum-warning interval is defined by the configured safety margin (see `internal/lagrisk`); alerts are not guaranteed to be delivered until a Prometheus instance scrapes the exporter and an Alertmanager receiver is configured. Validate rules locally:

```bash
docker run --rm -v "$PWD/deployments/prometheus:/rules:ro" \
  --entrypoint /bin/promtool prom/prometheus:v3.5.0 check rules /rules/alert-rules.yml
docker run --rm -v "$PWD/deployments/prometheus:/rules:ro" \
  --entrypoint /bin/promtool prom/prometheus:v3.5.0 test rules /rules/alert-rules.test.yml
```

## Real CI experiments

`make m3e-lag-smoke` runs an isolated Kafka + ClickHouse Compose with two one-partition Topics, both with `LogAppendTime` and **60-second effective retention**.

**Actual group-offset sampling:** produce 20 events, commit 8; exporter independently sees Kafka lag 12, effective retention 60 seconds and oldest unconsumed broker time. Warning/critical rule-input states are demonstrated using two explicitly **different** safety-margin policies; after consuming remaining 12 the state becomes no_backlog and the risk is removed. This is not a longitudinal 60-second simulated time progression.

**Controlled ClickHouse slowdown:** an integration-only HTTP gate holds the second of four 8-event ClickHouse batches *before upstream insertion*. While it is blocked, Kafka's coordinator reports next committed offset 8 and lag 24; no subsequent commit callback is allowed. The gate is released and 24 remaining events are written; the consumer reaches offset 32 and zero lag, and ClickHouse raw and trusted event counts are 32 without integrity conflict. It records catch-up wall time and speed for that exact run. This is not a sustained performance test.

[Initial real CI evidence](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43906791190): both real tests PASS; downloadable `m3e-live-kafka-lag-evidence` JSON artifact includes two independent reports. The isolated micro-benchmark measured approximately 115 events/s for 24 pending events after gate release. Do not use this number for production capacity planning.

## Boundaries

- Per-sample metadata/group-offset/log reads are **sequential**, not a Kafka-wide atomic snapshot. Rebalances, truncation, retention changes and leader movement can produce transient failures; fail closed and retry on the next tick.
- A Kafka append-time timestamp is required, but segment-level retention, log cleanup timing and effective policy changes make headroom **an estimate**. Retention risk is not a reliable clock for exact deletion.
- Kafka group offset `-1` means no confirmed commit and must not be transformed into zero. For some consumer groups with unused partitions this means the whole sample is unavailable; per-partition monitoring is a future refinement.
- The test-only HTTP gate is not a production traffic filter; no privileged failure-injection interface was added to the actual Worker.
- The CI uses developer single-broker RF=1. It complements, but does not replace, the three-broker RF3/minISR2 experiments in PR #12.
- Alertmanager routing, sustained throughput/P95/P99, multi-cluster collection, SASL/TLS, and independent production monitoring deploy remain future work.

## Final CI evidence

[PR #17 final M3e CI run](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43907781516) validated both real Kafka monitoring and gated ClickHouse recovery on the same code commit as [Go race/vet/build](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43907781298) and [M1 1k/100k](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43907781622). The Prometheus image's `promtool` reported **4 valid alert rules** and all firing/recovery unit cases PASS. Artifact `m3e-live-kafka-lag-evidence` contains separate actual source-offset and slow-write throughput reports. All **11** PR-head Actions workflows passed; change merged as `049a3d8`.
