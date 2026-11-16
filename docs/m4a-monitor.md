# M4a — Kafka lag/retention monitor and controlled ClickHouse throttling

## Architecture and authority

The read-only `cmd/lag-monitor` is **not the consumer** and never commits offsets or charges tokens. It reads Kafka's *broker-confirmed* group committed offset, actual log-start/log-end offsets, the effective Topic retention configuration and the oldest pending broker append timestamp. It passes that snapshot to `internal/lagrisk` and serves Prometheus and JSON.

```text
Kafka OffsetFetch + partition log boundaries + DescribeConfigs + oldest LogAppendTime
               |
        internal/lagrisk.Assess
               |
       /metrics · /status · /healthz
               |
  Prometheus rule evaluation -> optional Alertmanager (not deployed by this PR)
```

### Local run

```bash
make infra-up && make kafka-topics
make consume                         # terminal A
make relay                           # terminal B
make ledger-ingest                   # terminal C
make monitor                         # terminal D
curl -fsS http://127.0.0.1:9194/status
curl -fsS http://127.0.0.1:9194/metrics
```

Default flags: `--topic usage.critical.v1 --group telemetry-clickhouse-v1 --partition 0 --interval 5s --timeout 4s --safety-margin 2h --listen 127.0.0.1:9194`.

**Important:** Kafka cannot supply a trustworthy timestamp-based retention-risk estimate from arbitrary producer `CreateTime` values. The developer bootstrap sets `usage.critical.v1` to **LogAppendTime** and finite 7-day retention for **new topics only**. Existing topics will not be changed by `--if-not-exists` and the monitor refuses to label them healthy if their effective Topic settings are incompatible. Kafka's reported `retention.ms` may reflect an inherited broker default; operators must review production policy before changing Topic config.

The exporter monitors **one partition per process**. The developer topic has six partitions, so a single default process does **not** represent the entire group. For multiple partitions, run separate instances with distinct `--partition` and `--listen` addresses. Do not treat one partition's healthy output as cluster-wide health.

The exporter currently supports the local Kafka PLAINTEXT setup only; production security requires SASL/TLS, secret management, network isolation, and an authenticated monitoring deployment.

### Endpoints and metrics

- `GET /metrics`: text-format Prometheus gauges for committed/log offsets, offset-distance lag, oldest pending age, estimated retention headroom, warning/critical/data-gap conditions, observation health, stale state and failure count.
- `GET /status`: structured JSON observation with reason and Kafka source coordinates. The latest known sample is retained for investigation but marked unhealthy after an error/stale condition.
- `GET /healthz`: 503 if no successful sample, last attempt failed, or the last success is older than 3 scrape intervals.

Prometheus [alert rules](../deployments/observability/retention-alerts.yml) cover unavailable/stale collector, warning, critical and committed-before-log-start. The alerts are scoped to the same `job,instance` scrape labels to avoid cross-instance coupling. The example job should be named `llm-telemetry-lag-monitor`; scrape each exporter instance individually. Rule-unit tests simulate alert firing **and resolution**, but deployment into a live Prometheus/Alertmanager installation and notification delivery are outside this PR.

```bash
docker run --rm --entrypoint=/bin/promtool \
  -v "$PWD/deployments/observability:/rules:ro" prom/prometheus:v3.4.0 \
  check rules /rules/retention-alerts.yml

docker run --rm --entrypoint=/bin/promtool \
  -v "$PWD/deployments/observability:/rules:ro" prom/prometheus:v3.4.0 \
  test rules /rules/retention-alerts.test.yml
```

## Real limited-throughput experiment

```bash
make m4a-smoke
# Evidence: /tmp/m4a-monitor-report.json
```

A real Kafka group consumes **24 broker-ACKed sealed events** in batches of 8. A test-only HTTP proxy holds ClickHouse's second batch for at least 1.2 seconds **before forwarding it**. While blocked, a separate KafkaSource reads the group's confirmed committed offset **8** (last processed 7), log end **24**, offset-distance lag **16**, and the oldest still-pending broker timestamp. No source offset may advance before the corresponding ClickHouse ACK.

After release, the consumer commits offsets **16** and **24**, the monitor reports `no_backlog` / lag 0, and real ClickHouse shows 24 physical/24 trusted events with 0 hash conflicts. The CI report measures catch-up time and throughput of the 16 remaining small events, and exports broker-derived retention headroom.

[Initial real CI](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43901243415): PASS; measured **162.31 records/second** on this one micro workload. It is not a production p95/p99, saturation curve, SLA/SLO, or HA guarantee. The final PR-head run is authoritative for the merge gate.

## Safety boundaries

- Kafka committed offset vs log end is an offset **distance**, not an exact number of available messages. Offsets can have holes and compaction/transactions change visibility.
- Retention headroom derived from append timestamps and `retention.ms` is **an estimate**, not a guaranteed deletion time; Kafka removes segments and broker settings can change.
- Different metadata fetches are **not one atomic broker snapshot**. Conflicting or missing inputs fail closed; avoid irreversible decisions from one transient reading.
- The test-only HTTP proxy is not ClickHouse native rate limiting. It validates backpressure/ACK safety under *downstream latency*, not ClickHouse's internal load-shedding policy.
- Financial authority remains PostgreSQL `usage_ledger`. The monitor does not reconcile charging or repair ClickHouse. Never assert exactly-once or absolute zero-loss.
