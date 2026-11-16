# M3e-P0 — Kafka committed-offset and retention-headroom monitor

The `cmd/lag-monitor` process reads **actual Kafka consumer group committed offsets**, partition log start/end, the timestamp of the first available pending record, and **effective topic retention.ms** via Kafka's DescribeConfigs API. It does not join the monitored group, acknowledge records or modify offsets.

The topic must use `message.timestamp.type=LogAppendTime`, because arbitrary producer CreateTime can invalidate retention-age estimates. Missing/uncommitted group offset, unavailable broker coordinator, unset/infinite/invalid retention, missing timestamp and unknown partition data fail the scrape.

```bash
go run ./cmd/lag-monitor --brokers localhost:29092 \
  --topic usage.critical.v1 --group telemetry-clickhouse-v1 \
  --partition 0 --listen 127.0.0.1:9098 --interval 5s \
  --safety-margin 2m
curl -fsS http://127.0.0.1:9098/metrics
curl -fsS http://127.0.0.1:9098/status
```

Metrics include `telemetry_kafka_monitor_up`, `telemetry_kafka_monitor_error`, offset distance, committed/end/start offset, headroom seconds, retention configuration and one-hot `telemetry_kafka_retention_state{state=...}`. A failed poll **removes previous status metrics** and exports `telemetry_kafka_monitor_up 0`: a stale "healthy" gauge is never emitted as current truth.

`deployments/prometheus/m3e-alerts.yml` defines unknown-monitor, warning, critical, expiry-risk and committed-before-log-start alerts. The matching `m3e-alerts.test.yml` provides rule-engine firing and clear-down tests using the actual `promtool test rules` binary. An authenticated Prometheus/Alertmanager deployment and external pager delivery are not part of this slice yet.

Reproduce a **real broker** observation (not mocked Kafka offsets):

```bash
bash scripts/m3e-monitor-smoke.sh
```

The integration writes five broker-ACKed sealed events, uses a real Kafka group to commit the first two, checks lag distance three and 600-second topic retention, scrapes the exporter, then commits the other three and confirms lag zero and alert resolution. Kafka offset distance is not an exact event count in the presence of compaction/offset gaps.

Remaining M3e work: bounded ClickHouse throttling, catch-up throughput measurement and operational scrape/alert delivery integration. This monitor is not a billing or exactly-once system. Secure exposure with TLS/auth and network policy in production.
