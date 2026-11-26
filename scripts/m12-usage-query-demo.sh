#!/usr/bin/env bash
# Disposable local demonstration of the actual usage query binary.
# Creates and removes only this run's Compose project and volumes.
set -euo pipefail
cd "$(dirname "$0")/.."
for tool in go docker curl python3 openssl; do
  command -v "$tool" >/dev/null || { echo "missing prerequisite: $tool" >&2; exit 1; }
done
# Keep the usual defaults while allowing another local stack to retain its ports.
export TELEMETRY_POSTGRES_HOST_PORT="${TELEMETRY_POSTGRES_HOST_PORT:-15432}"
query_port="${USAGE_QUERY_DEMO_PORT:-18086}"
validate_port() {
  local label="$1" port="$2"
  if [[ ! "$port" =~ ^[1-9][0-9]{0,4}$ ]] || (( port > 65535 )); then
    echo "$label must be a TCP port from 1 to 65535" >&2
    exit 1
  fi
}
validate_port TELEMETRY_POSTGRES_HOST_PORT "$TELEMETRY_POSTGRES_HOST_PORT"
validate_port USAGE_QUERY_DEMO_PORT "$query_port"
if [[ "$TELEMETRY_POSTGRES_HOST_PORT" == "$query_port" ]]; then
  echo 'PostgreSQL and usage query demo ports must differ' >&2
  exit 1
fi
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
    [[ "$(pg -tAc "SELECT to_regclass('public.usage_query_scope') IS NOT NULL AND to_regclass('public.price_version_registry') IS NOT NULL" 2>/dev/null)" == t ]]; then
    break
  fi
  if (( i == 60 )); then echo 'PostgreSQL migration readiness timeout' >&2; exit 1; fi
  sleep 2
done
[[ "$(pg -tAc 'SELECT count(*) FROM public.usage_ledger')" == 0 ]]
go build -o "$tmp/generator" ./cmd/event-generator
go build -o "$tmp/ingest" ./cmd/usage-ledger-ingest
go build -o "$tmp/query" ./cmd/usage-query-api
go build -o "$tmp/cost" ./cmd/usage-cost-estimate
"$tmp/generator" --count 16 --seed m12-demo --start 2026-11-21T00:00:00Z > "$tmp/events.ndjson"
for i in 1 2; do
  "$tmp/ingest" --dsn "postgres://telemetry:localdev-only@127.0.0.1:$TELEMETRY_POSTGRES_HOST_PORT/telemetry?sslmode=disable" < "$tmp/events.ndjson"
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
USAGE_QUERY_DATABASE_URL="postgres://m12_demo_reader:$password@127.0.0.1:$TELEMETRY_POSTGRES_HOST_PORT/telemetry?sslmode=disable" \
  USAGE_QUERY_TOKEN="$token" USAGE_QUERY_TENANT_ID=tenant_001 USAGE_QUERY_PROJECT_ID=project_001 \
  "$tmp/query" --listen "127.0.0.1:$query_port" > "$tmp/api.log" 2>&1 &
api_pid=$!
base="http://127.0.0.1:$query_port/v1/usage-summary"
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
# Fictional rates change between the two scoped events (0ms and 80ms).
cat > "$tmp/schedule.json" <<'JSON'
[
  {"from":"2026-11-21T00:00:00Z","to":"2026-11-21T00:00:00.040Z","table":{"version":"demo-v1","currency":"USD","rates":[{"model":"Qwen3-32B","input_per_million":"2.5","output_per_million":"10"}]}},
  {"from":"2026-11-21T00:00:00.040Z","to":"2026-11-21T02:00:00Z","table":{"version":"demo-v2","currency":"USD","rates":[{"model":"Qwen3-32B","input_per_million":"5","output_per_million":"20"}]}}
]
JSON
historical_cost() {
  USAGE_QUERY_DATABASE_URL="postgres://m12_demo_reader:$password@127.0.0.1:$TELEMETRY_POSTGRES_HOST_PORT/telemetry?sslmode=disable" \
    USAGE_QUERY_TENANT_ID=tenant_001 USAGE_QUERY_PROJECT_ID=project_001 \
    "$tmp/cost" --schedule "$tmp/schedule.json" --from 2026-11-21T00:00:00Z --to 2026-11-21T02:00:00Z "$@"
}
historical_cost > "$tmp/history.json"
python3 - "$tmp/history.json" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    report = json.load(f)
assert report["tenant_id"] == "tenant_001"
assert report["project_id"] == "project_001"
assert report["currency"] == "USD"
assert report["total_cost"] == "0.002450000000"
assert len(report["windows"]) == 2
assert [w["estimate"]["price_version"] for w in report["windows"]] == ["demo-v1", "demo-v2"]
assert [w["estimate"]["total_cost"] for w in report["windows"]] == ["0.000750000000", "0.001700000000"]
assert report["windows"][0]["to"] == report["windows"][1]["from"]
print(json.dumps(report, indent=2))
PY
# Register only synthetic test fixtures in this disposable database.
# The unverified report supplies content digests, not trusted production rates.
python3 - "$tmp/history.json" > "$tmp/register.sql" <<'PY'
import json, re, sys
with open(sys.argv[1]) as f:
    report = json.load(f)
assert "price_registry_verified" not in report
for window in report["windows"]:
    estimate = window["estimate"]
    version, fp = estimate["price_version"], estimate["price_fingerprint"]
    assert version in ("demo-v1", "demo-v2")
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", fp)
    print("INSERT INTO public.price_version_registry(tenant_id,project_id,price_version,price_fingerprint) VALUES ('tenant_001','project_001','%s','%s');" % (version, fp))
PY
# Missing versions must fail before any estimate is emitted.
if historical_cost --verify-price-registry > "$tmp/missing.json" 2>"$tmp/missing.err"; then
  echo 'unregistered historical cost unexpectedly succeeded' >&2
  exit 1
fi
[[ ! -s "$tmp/missing.json" ]]
grep -Fxq 'registered price version not found' "$tmp/missing.err"
pg < "$tmp/register.sql" >/dev/null
historical_cost --verify-price-registry > "$tmp/verified.json"
python3 - "$tmp" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
legacy = json.loads((root / "history.json").read_text())
verified = json.loads((root / "verified.json").read_text())
assert verified.pop("price_registry_verified") is True
assert verified == legacy
# Change price content without changing the registered version.
schedule = json.loads((root / "schedule.json").read_text())
schedule[0]["table"]["rates"][0]["input_per_million"] = "3"
(root / "tampered-schedule.json").write_text(json.dumps(schedule))
PY
# Use the same verified option with a different schedule, without mutating the registry.
cp "$tmp/schedule.json" "$tmp/original-schedule.json"
cp "$tmp/tampered-schedule.json" "$tmp/schedule.json"
if historical_cost --verify-price-registry > "$tmp/mismatch.json" 2>"$tmp/mismatch.err"; then
  echo 'mismatched historical cost unexpectedly succeeded' >&2
  exit 1
fi
[[ ! -s "$tmp/mismatch.json" ]]
grep -Fxq 'registered price version fingerprint mismatch' "$tmp/mismatch.err"
mv "$tmp/original-schedule.json" "$tmp/schedule.json"
echo 'PASS: real verified cost CLI, missing version, fingerprint mismatch, legacy parity'
pg -c "DELETE FROM public.usage_query_scope_bindings WHERE login_role='m12_demo_reader'" >/dev/null
if historical_cost > "$tmp/revoked-cost.json" 2>"$tmp/revoked-cost.err"; then
  echo 'revoked historical cost query unexpectedly succeeded' >&2
  exit 1
fi
[[ ! -s "$tmp/revoked-cost.json" ]]
if historical_cost --verify-price-registry > "$tmp/revoked-verified.json" 2>"$tmp/revoked-verified.err"; then
  echo 'revoked verified cost query unexpectedly succeeded' >&2
  exit 1
fi
[[ ! -s "$tmp/revoked-verified.json" ]]
echo 'PASS: historical scoped cost CLI, exact price transition and revoked binding'
for url in "$base?$window" "$base/models?$window" "$base/series?$window&granularity=hour"; do
  request "$url" 503 "$tmp/error.json"
  python3 - "$tmp/error.json" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    assert json.load(f) == {"error": "ledger_query_unavailable"}
PY
done
echo 'PASS: scoped usage demo, duplicate replay, all three APIs, scope revocation; Kafka remains offline'
