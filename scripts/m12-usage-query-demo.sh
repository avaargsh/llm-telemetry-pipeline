#!/usr/bin/env bash
# Disposable local demonstration of the actual usage query binary.
# Creates and removes only this run's Compose project and volumes.
set -euo pipefail
cd "$(dirname "$0")/.."
for tool in go docker curl python3 openssl; do
  command -v "$tool" >/dev/null || { echo "missing prerequisite: $tool" >&2; exit 1; }
done
docker compose version >/dev/null
export COMPOSE_PROJECT_NAME="llmtelemetry-query-demo-$$-$(openssl rand -hex 4)"
compose=(docker compose -f deployments/docker-compose.yml)
tmp="$(mktemp -d)"
api_pid=''
cleanup() {
  local code=$?
  trap - EXIT
  if [[ -n "$api_pid" ]]; then
    kill "$api_pid" 2>/dev/null || true
    wait "$api_pid" 2>/dev/null || true
  fi
  if (( code != 0 )); then
    echo "usage query demo failed (exit $code)" >&2
    [[ ! -f "$tmp/api.log" ]] || tail -30 "$tmp/api.log" >&2
  fi
  if ! "${compose[@]}" down -v --remove-orphans >/dev/null; then
    echo "cleanup failed; inspect Compose project $COMPOSE_PROJECT_NAME" >&2
    code=1
  fi
  rm -rf "$tmp"
  exit "$code"
}
trap cleanup EXIT
pg() {
  "${compose[@]}" exec -T postgres psql -X -v ON_ERROR_STOP=1 -U telemetry -d telemetry "$@"
}
"${compose[@]}" up -d postgres
for i in $(seq 1 60); do
  if "${compose[@]}" exec -T postgres pg_isready -h 127.0.0.1 -U telemetry -d telemetry >/dev/null 2>&1 &&
    [[ "$(pg -tAc "SELECT to_regclass('public.usage_query_scope') IS NOT NULL" 2>/dev/null)" == t ]]; then
    break
  fi
  if (( i == 60 )); then echo 'PostgreSQL migration readiness timeout' >&2; exit 1; fi
  sleep 2
done
[[ "$(pg -tAc 'SELECT count(*) FROM public.usage_ledger')" == 0 ]]
go build -o "$tmp/generator" ./cmd/event-generator
go build -o "$tmp/ingest" ./cmd/usage-ledger-ingest
go build -o "$tmp/query" ./cmd/usage-query-api
"$tmp/generator" --count 16 --seed m12-demo --start 2026-11-21T00:00:00Z > "$tmp/events.ndjson"
for i in 1 2; do
  "$tmp/ingest" --dsn 'postgres://telemetry:localdev-only@127.0.0.1:15432/telemetry?sslmode=disable' < "$tmp/events.ndjson"
done
[[ "$(pg -tAc 'SELECT count(*) FROM public.usage_ledger')" == 16 ]]
[[ "$(pg -tAc 'SELECT count(*) FROM public.event_outbox WHERE published_at IS NULL')" == 16 ]]
password="$(openssl rand -hex 32)"
token="$(openssl rand -hex 32)"
# Fixed disposable role and scope; password consists exclusively of hex.
# No external identifiers are interpolated into SQL.
pg >/dev/null <<SQL
CREATE ROLE m12_demo_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '$password';
GRANT llm_telemetry_usage_query_reader TO m12_demo_reader;
INSERT INTO public.usage_query_scope_bindings(login_role,tenant_id,project_id)
VALUES ('m12_demo_reader','tenant_001','project_001');
SQL
USAGE_QUERY_DATABASE_URL="postgres://m12_demo_reader:$password@127.0.0.1:15432/telemetry?sslmode=disable" \
  USAGE_QUERY_TOKEN="$token" USAGE_QUERY_TENANT_ID=tenant_001 USAGE_QUERY_PROJECT_ID=project_001 \
  "$tmp/query" --listen 127.0.0.1:18086 > "$tmp/api.log" 2>&1 &
api_pid=$!
base='http://127.0.0.1:18086/v1/usage-summary'
window='from=2026-11-21T00:00:00Z&to=2026-11-21T02:00:00Z'
request() {
  local url="$1" expected="$2" output="$3" auth="${4:-yes}"
  local -a args=(--silent --show-error --max-time 8 --output "$output" --write-out '%{http_code}')
  if [[ "$auth" == yes ]]; then args+=(--header "Authorization: Bearer $token"); fi
  local status
  status="$(curl "${args[@]}" "$url")"
  if [[ "$status" != "$expected" ]]; then
    echo "expected HTTP $expected, received $status" >&2
    return 1
  fi
}
for i in $(seq 1 40); do
  if ! kill -0 "$api_pid" 2>/dev/null; then echo 'query API exited before ready' >&2; exit 1; fi
  if request "$base?$window" 200 "$tmp/summary.json" 2>/dev/null; then break; fi
  if (( i == 40 )); then echo 'query API readiness timeout' >&2; exit 1; fi
  sleep 1
done
request "$base/models?$window" 200 "$tmp/models.json"
request "$base/series?$window&granularity=hour" 200 "$tmp/series.json"
request "$base?$window" 401 "$tmp/error.json" no
request "$base?$window&project_id=project_002" 400 "$tmp/error.json"
python3 - "$tmp" <<'PY'
import json
import pathlib
import sys
root = pathlib.Path(sys.argv[1])
def read(name):
    return json.loads((root / name).read_text())
expected = {"request_count": 2, "prompt_tokens": "408", "completion_tokens": "58", "total_tokens": "466"}
assert read("summary.json") == expected
assert read("models.json") == [{"model": "Qwen3-32B", **expected}]
assert read("series.json") == [
    {"bucket_start": "2026-11-21T00:00:00Z", **expected},
    {"bucket_start": "2026-11-21T01:00:00Z", "request_count": 0,
     "prompt_tokens": "0", "completion_tokens": "0", "total_tokens": "0"},
]
print(json.dumps({"summary": read("summary.json"), "models": read("models.json"),
                  "series": read("series.json")}, indent=2))
PY
pg -c "DELETE FROM public.usage_query_scope_bindings WHERE login_role='m12_demo_reader'" >/dev/null
for url in "$base?$window" "$base/models?$window" "$base/series?$window&granularity=hour"; do
  request "$url" 503 "$tmp/error.json"
  python3 - "$tmp/error.json" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    assert json.load(f) == {"error": "ledger_query_unavailable"}
PY
done
echo 'PASS: scoped usage demo, duplicate replay, all three APIs, scope revocation; Kafka remains offline'
