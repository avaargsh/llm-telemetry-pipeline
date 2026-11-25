#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."

: "${GRAFANA_ADMIN_PASSWORD:?set a non-default Grafana admin password}"
export GRAFANA_ADMIN_USER="${GRAFANA_ADMIN_USER:-telemetry-admin}"
project="m14-grafana-${GITHUB_RUN_ID:-local}-${RANDOM}"
compose=(docker compose -p "$project"
  -f deployments/docker-compose.yml
  -f deployments/docker-compose-observability.yml
  -f deployments/docker-compose-grafana.yml)
tmp="$(mktemp -d)"
cleanup() {
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$tmp"
}
trap cleanup EXIT

"${compose[@]}" config > "$tmp/compose.yml"
"${compose[@]}" up -d --no-deps grafana

ready=0
for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:13000/api/health > "$tmp/health.json"; then
    ready=1
    break
  fi
  sleep 2
done
if [[ "$ready" != 1 ]]; then
  "${compose[@]}" logs grafana >&2
  echo "Grafana did not become healthy" >&2
  exit 1
fi

unauthorized="$(curl -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:13000/api/datasources)"
[[ "$unauthorized" == 401 ]] || { echo "anonymous datasource API returned HTTP $unauthorized" >&2; exit 1; }

fetch_provisioned() {
  local uid="$1" output="$2"
  for _ in $(seq 1 30); do
    if curl -fsS -u "$GRAFANA_ADMIN_USER:$GRAFANA_ADMIN_PASSWORD" \
      "http://127.0.0.1:13000/api/dashboards/uid/$uid" > "$output"; then
      return 0
    fi
    sleep 2
  done
  "${compose[@]}" logs grafana >&2
  echo "dashboard $uid was not provisioned" >&2
  return 1
}

curl -fsS -u "$GRAFANA_ADMIN_USER:$GRAFANA_ADMIN_PASSWORD" \
  http://127.0.0.1:13000/api/datasources/uid/prometheus > "$tmp/datasource.json"
fetch_provisioned llm-telemetry-kafka-operator "$tmp/kafka.json"
fetch_provisioned llm-telemetry-genai-runtime "$tmp/genai.json"

python3 - "$tmp" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
datasource = json.loads((root / "datasource.json").read_text())
assert datasource["uid"] == "prometheus"
assert datasource["type"] == "prometheus"
assert datasource["url"] == "http://prometheus:9090"
for filename, uid, expected_panels in (
    ("kafka.json", "llm-telemetry-kafka-operator", 7),
    ("genai.json", "llm-telemetry-genai-runtime", 14),
):
    report = json.loads((root / filename).read_text())
    dashboard = report["dashboard"]
    assert dashboard["uid"] == uid
    assert len(dashboard["panels"]) == expected_panels
    for panel in dashboard["panels"]:
        assert panel["datasource"]["uid"] == "prometheus"
        for target in panel["targets"]:
            assert target["datasource"]["uid"] == "prometheus"
print("PASS: authenticated Grafana API, provisioned Prometheus datasource and both dashboards")
PY
