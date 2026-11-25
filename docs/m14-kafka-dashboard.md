# M14.1 Kafka operator dashboard

The preferred deployment is the M14.4 file-provisioned Grafana profile
(`docs/m14-grafana-provisioning.md`). The dashboard JSON uses the stable
Prometheus datasource UID `prometheus`; a manual import requires a datasource
with that UID. This dashboard definition alone does not deploy Grafana.

The dashboard expects the existing `lag-exporter` scrape job
(`deployments/prometheus/prometheus.yml`). Use the M3e and M4 documented
operator stack for a populated Kafka topic with actual group commits.
Use an operator-only data source and authenticated Grafana; the panel
filters do not enforce tenant isolation. Never expose the developer
monitoring ports publicly.

Seven panels distinguish Prometheus scrape reachability, exporter sample
success, offset lag, estimated retention headroom, oldest pending age,
potential data gap and active retention-risk state. Partition curves
include instance, group, topic and partition. The risk gauge includes
status. All five partition metrics require both target up and successful
sample from that same job and instance. Missing samples are not converted
to zero, including when there is no backlog and headroom is undefined.
Historical curves may show previous valid samples; gaps are not bridged.
The two health panels use instant queries rather than previous good values.

## Query contract test

```sh
tmp="$(mktemp -d)"
python3 scripts/m14_dashboard_promql_fixture.py > "$tmp/dashboard.test.yml"
docker run --rm -v "$tmp:/rules:ro" --entrypoint /bin/promtool \
  prom/prometheus:v3.5.0 test rules /rules/dashboard.test.yml
rm -rf "$tmp"
```

The fixture executes the actual dashboard expressions against four
synthetic evaluation points: healthy backlog, failed exporter sample
with stale partition data, unreachable exporter with a previous
successful sample, and healthy no-backlog recovery. This tests PromQL
semantics rather than a browser or live Grafana deployment.

Remaining: real Grafana provisioning/import verification, live data
retrieval and browser acceptance, GenAI dashboards, and authoritative
usage/cost reporting. No production HA or SLO claim is made.
