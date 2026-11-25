#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."

export GRAFANA_ADMIN_USER="${GRAFANA_ADMIN_USER:-telemetry-ci}"
: "${GRAFANA_ADMIN_PASSWORD:?set a disposable Grafana smoke password}"
project="telemetry-m14-live-${GITHUB_RUN_ID:-local}-${RANDOM}"
compose=(docker compose -p "$project"
  -f deployments/docker-compose.yml
  -f deployments/docker-compose-observability.yml
  -f deployments/docker-compose-grafana.yml)
tmp="$(mktemp -d)"
cleanup() {
  local rc=$?
  if ((rc != 0)); then
    "${compose[@]}" ps >&2 || true
    "${compose[@]}" logs --tail=100 kafka lag-exporter prometheus grafana >&2 || true
  fi
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$tmp"
  return "$rc"
}
trap cleanup EXIT

"${compose[@]}" config > "$tmp/compose.yml"
"${compose[@]}" build lag-exporter
"${compose[@]}" up -d kafka
for _ in $(seq 1 90); do
  if "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --create --if-not-exists \
  --topic m4.alert.e2e.v1 --partitions 1 --replication-factor 1 \
  --config retention.ms=120000 --config message.timestamp.type=LogAppendTime

go run ./scripts/fixtures/m4-alert-fixture.go --mode seed
"${compose[@]}" up -d alert-webhook alertmanager lag-exporter prometheus grafana

ready=0
for _ in $(seq 1 90); do
  if curl -fsS http://127.0.0.1:19090/-/ready >/dev/null 2>&1 && \
     curl -fsS http://127.0.0.1:13000/api/health >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done
[[ "$ready" == 1 ]] || { echo "Prometheus/Grafana readiness timeout" >&2; exit 1; }

curl -fsS -u "$GRAFANA_ADMIN_USER:$GRAFANA_ADMIN_PASSWORD" \
  http://127.0.0.1:13000/api/dashboards/uid/llm-telemetry-kafka-operator \
  > "$tmp/dashboard.json"

python3 - "$tmp/dashboard.json" <<'PY'
import json
import sys
import time
import urllib.parse
import urllib.request

dashboard = json.load(open(sys.argv[1]))["dashboard"]
assert dashboard["uid"] == "llm-telemetry-kafka-operator"
panel = next(p for p in dashboard["panels"] if p["id"] == 3)
expr = panel["targets"][0]["expr"]

def query(promql):
    url = "http://127.0.0.1:19090/api/v1/query?" + urllib.parse.urlencode({"query": promql})
    with urllib.request.urlopen(url, timeout=5) as response:
        payload = json.load(response)
    assert payload["status"] == "success", payload
    return payload["data"]["result"]

deadline = time.monotonic() + 120
while time.monotonic() < deadline:
    scrape = query('llm_telemetry_lag_scrape_success{job="lag-exporter"}')
    lag = query('llm_telemetry_kafka_lag_offsets{job="lag-exporter"}')
    dashboard_lag = query(expr)
    if (scrape and float(scrape[0]["value"][1]) == 1 and
        lag and int(float(lag[0]["value"][1])) == 12 and
        dashboard_lag and int(float(dashboard_lag[0]["value"][1])) == 12):
        print(json.dumps({
            "scrape_success": scrape[0]["value"][1],
            "observed_lag_offsets": lag[0]["value"][1],
            "dashboard_panel": panel["title"],
            "dashboard_query_value": dashboard_lag[0]["value"][1],
        }, indent=2))
        print("PASS: real Kafka -> lag exporter -> Prometheus -> provisioned Grafana dashboard")
        break
    time.sleep(2)
else:
    raise SystemExit("live Kafka lag metric or dashboard PromQL did not become available")
PY
