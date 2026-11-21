#!/usr/bin/env bash
# Isolated, single-host M5 performance smoke/profile.
# No SLO claim without actual JSON evidence at the exact test HEAD.
set -Eeuo pipefail
cd "$(dirname "$0")/.."
count="${M5_COUNT:-128}"
concurrency="${M5_CONCURRENCY:-4}"
partitions="${M5_PARTITIONS:-6}"
batch="${M5_BATCH_SIZE:-32}"
relay_timeout="${M5_RELAY_BATCH_TIMEOUT:-100ms}"
relay_limit="${M5_RELAY_CLAIM_LIMIT:-5}"
timeout="${M5_VISIBILITY_TIMEOUT:-90}"
output="${M5_OUTPUT:-artifacts/m5-performance.json}"
if [[ "$output" != /* ]]; then output="$PWD/$output"; fi
if ! [[ "$count" =~ ^[1-9][0-9]*$ && "$concurrency" =~ ^[1-9][0-9]*$ && "$partitions" =~ ^[1-9][0-9]*$ && "$batch" =~ ^[1-9][0-9]*$ ]]; then
  echo "invalid positive M5 matrix parameter" >&2; exit 2
fi
if (( count > 10000 || concurrency > 128 || partitions > 128 || batch > 10000 )); then
  echo "M5 matrix parameter exceeds bounded test range" >&2;exit 2
fi

export COMPOSE_PROJECT_NAME="llmtelemetry-m5-${GITHUB_RUN_ID:-local}-$$"
compose=(docker compose -f deployments/docker-compose.yml)
tmp="$(mktemp -d)"
api_pid='' ; relay_pid='' ; worker_pid=''
cleanup() {
  code=$?
  trap - EXIT
  for pid in "$api_pid" "$relay_pid" "$worker_pid"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  if (( code != 0 )); then
    echo "M5 performance smoke failed (exit $code)" >&2
    for file in "$tmp/api.log" "$tmp/relay.log" "$tmp/worker.log"; do
      [[ ! -f "$file" ]] || tail -70 "$file" >&2
    done
    "${compose[@]}" logs --tail 40 postgres kafka clickhouse >&2 || true
  fi
  "${compose[@]}" down -v >/dev/null 2>&1 || true
  rm -rf "$tmp"
  exit "$code"
}
trap cleanup EXIT

pg_ready() {
  "${compose[@]}" exec -T postgres \
    psql -U telemetry -d telemetry -tAc 'SELECT count(*) FROM usage_ledger' >/dev/null 2>&1
}
kafka_ready() {
  "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 --list >/dev/null 2>&1
}
ch_ready() {
  curl -fsS --max-time 3 -u telemetry:localdev-only \
    'http://127.0.0.1:18123/?database=telemetry' --data-binary 'SELECT 1' >/dev/null
}
"${compose[@]}" up -d
for i in $(seq 1 90); do
  if pg_ready && kafka_ready && ch_ready; then break; fi
  if (( i == 90 )); then echo "Postgres, Kafka or ClickHouse unavailable" >&2;exit 1; fi
  sleep 2
done

for topic in usage.critical.v1 inference.logs.v1 events.dlq.v1; do
  "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1 >/dev/null
done
go build -o "$tmp/generator" ./cmd/event-generator
go build -o "$tmp/api" ./cmd/usage-ingest-api
go build -o "$tmp/relay" ./cmd/outbox-relay
go build -o "$tmp/worker" ./cmd/ingest-worker

seed="perf-$(openssl rand -hex 10)"
start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
"$tmp/generator" --count "$count" --seed "$seed" --start "$start" > "$tmp/events.jsonl"
tokens=()
credentials='['
for i in $(seq 0 7); do
  token="$(openssl rand -hex 32)"
  tokens+=("$token")
  printf -v binding '{"token":"%s","tenant_id":"tenant_%03d","project_id":"project_%03d"}' \
    "$token" "$((i % 4+1))" "$((i+1))"
  if (( i > 0 )); then credentials+=','; fi
  credentials+="$binding"
done
credentials+=']'

DATABASE_URL='postgres://telemetry:localdev-only@localhost:15432/telemetry?sslmode=disable' \
  USAGE_INGEST_CREDENTIALS_JSON="$credentials" \
  "$tmp/api" --listen 127.0.0.1:18085 > "$tmp/api.log" 2>&1 &
api_pid=$!
"$tmp/relay" --worker "m5-${GITHUB_RUN_ID:-local}-$" \
  --claim-limit "$relay_limit" --kafka-batch-timeout "$relay_timeout" > "$tmp/relay.log" 2>&1 &
relay_pid=$!
"$tmp/worker" --batch-size "$batch" --group "m5-${GITHUB_RUN_ID:-local}-$$" \
  --fetch-wait 250ms > "$tmp/worker.log" 2>&1 &
worker_pid=$!

for i in $(seq 1 40); do
  if curl -fsS --max-time 2 'http://127.0.0.1:18085/healthz' >/dev/null 2>&1; then break; fi
  if ! kill -0 "$api_pid" 2>/dev/null; then echo "Ingestion API exited before ready" >&2;exit 1;fi
  if (( i == 40 )); then echo "Ingestion API not ready" >&2;exit 1;fi
  sleep 1
done

mkdir -p "$(dirname "$output")"
slo_args=()
[[ -z "${M5_MAX_ACK_P95_MS:-}" ]] || slo_args+=(--max-ack-p95-ms "$M5_MAX_ACK_P95_MS")
[[ -z "${M5_MAX_VISIBLE_P95_MS:-}" ]] || slo_args+=(--max-visible-p95-ms "$M5_MAX_VISIBLE_P95_MS")
[[ -z "${M5_MAX_DRAIN_SECONDS:-}" ]] || slo_args+=(--max-drain-seconds "$M5_MAX_DRAIN_SECONDS")
CLICKHOUSE_PASSWORD=localdev-only USAGE_INGEST_CREDENTIALS_JSON="$credentials" \
python3 scripts/m5_benchmark.py \
  --events-file "$tmp/events.jsonl" --seed "$seed" \
  --concurrency "$concurrency" --partitions "$partitions" \
  --batch-size "$batch" --visibility-timeout "$timeout" \
  --relay-claim-limit "$relay_limit" --relay-batch-timeout "$relay_timeout" \
  --pid "api:$api_pid" --pid "relay:$relay_pid" --pid "consumer:$worker_pid" \
  "${slo_args[@]}" --output "$output"
echo "M5 correctness report: $output"
echo "WARNING: single-node, local development credentials, RF=1, and CI contention are NOT production SLO evidence"
