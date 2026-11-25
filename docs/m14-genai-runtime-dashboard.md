# M14.2 GenAI receiver and durable worker dashboard

Import `deployments/grafana/dashboards/genai-runtime.json` from Grafana
Dashboards → New → Import and select an authenticated Prometheus data
source. This is an importable dashboard, not a Grafana deployment or a
production SLO claim.

## Scrape prerequisites

The worker dashboard expects the existing local Compose scrape job
`genai-observation-worker` at port 9109 from
`deployments/prometheus/prometheus.yml`. Start the base observability
stack and the worker overlay as documented in
`docs/m11-durable-http-exporter.md`. The overlay binds host port 19109
to loopback only; do not expose it publicly.

The receiver exports `/metrics` on its OTLP listener (default 4318).
The Kubernetes example annotates that listener for Prometheus discovery.
The dashboard selects receiver targets by `:4318` instance suffix because
the Kubernetes discovery job name is cluster-policy dependent. Confirm
your Prometheus discovery policy honours the pod scrape annotations and
that only the intended receiver target uses this port. For a custom
scrape port or a stricter multi-cluster setup, adapt the selector and
verify it against the actual target labels before importing.

## Panels and metric contracts

Receiver: target scrape status, process metric, admission request rate,
5xx failure ratio, and mean admission latency.

Worker: scrape target, queue occupancy, pending/terminal depth, oldest
pending age, age since the last successful delivery, ACK/retry/terminal
outcome rates, retry ratio, mean HTTP export latency, and operational
error rate.

The code exports summary count/sum, not histogram buckets or quantiles.
Therefore the latency panels are means; do not interpret them as p95/p99.
The receiver's `/readyz` probes PostgreSQL, but the existing Prometheus
receiver metric reports process health, not database readiness. A successful
metrics scrape must not be interpreted as a successful admission or DB ping.

Worker queue gauges retain their last values when a DB stats read fails.
The dashboard gates queue depth, occupancy and oldest-age panels on
`llm_telemetry_genai_worker_queue_length_available == 1`, and on the
same worker target being up. Missing samples are displayed as unknown,
never converted to zero. Last-delivery age is absent until a successful
delivery timestamp exists. No tenant, model, trace or request labels are
added.

## Query verification

The dedicated `m14-genai-dashboard-promql` workflow runs the exact
dashboard expressions with the pinned Prometheus 3.5.0 `promtool`
against deterministic fixtures. The fixtures include receiver/worker
target loss, stale queue samples, recovery, queue occupancy, retry ratio,
and mean-latency calculations.

```sh
tmp="$(mktemp -d)"
python3 scripts/m14_genai_dashboard_promql_fixture.py > "$tmp/dashboard.test.yml"
chmod 755 "$tmp"
chmod 644 "$tmp/dashboard.test.yml"
docker run --rm -v "$tmp:/rules:ro" --entrypoint /bin/promtool \
  prom/prometheus:v3.5.0 test rules /rules/dashboard.test.yml
rm -rf "$tmp"
```

Remaining: live Grafana import/browser acceptance, receiver DB-readiness
scraping, alert drill-down links, and production deployment/SLO validation.
