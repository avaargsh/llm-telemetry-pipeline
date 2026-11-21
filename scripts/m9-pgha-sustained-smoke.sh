#!/usr/bin/env bash
# M9 disposable real PostgreSQL synchronous-standby + sustained HTTP → Kafka → CH.
# Manual promotion, explicit API/Relay reconnect. Never use on production DBs.
set -Eeuo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME="llmtelemetry-m9-${GITHUB_RUN_ID:-local}-$$"
compose=(docker compose -f deployments/docker-compose.yml -f deployments/docker-compose-pg-ha.yml)
tmp="$(mktemp -d)"
output="${M9_OUTPUT_DIR:-artifacts/m9-pgha-sustained}"
[[ "$output" == /* ]] || output="$PWD/$output"
mkdir -p "$output"
rate="${M9_TARGET_RPS:-25}"
duration="${M9_DURATION_SECONDS:-180}"
concurrency="${M9_CONCURRENCY:-32}"
fault_at="${M9_FAULT_AT_SECONDS:-35}"
[[ "$rate" =~ ^[1-9][0-9]*$ && "$duration" =~ ^[1-9][0-9]*$ &&
   "$concurrency" =~ ^[1-9][0-9]*$ && "$fault_at" =~ ^[1-9][0-9]*$ ]] || {
  echo "M9 only accepts bounded positive integer profile values" >&2; exit 2;
}
(( rate <= 50 && duration >= 45 && duration <= 600 && rate * duration <= 10000 &&
   rate * duration >= 1000 && concurrency <= 128 && fault_at + 25 < duration )) || {
  echo "M9 fault must fall well within sustained 1000..10000-event profile" >&2; exit 2;
}
count=$((rate * duration))
primary_dsn='postgres://telemetry:localdev-only@127.0.0.1:25432/telemetry?sslmode=disable'
standby_dsn='postgres://telemetry:localdev-only@127.0.0.1:25433/telemetry?sslmode=disable'
api_pid='' relay_pid='' worker_pid='' client_pid='' hold_pid='' stats_pid=''
cleanup() {
  rc=$?
  trap - EXIT
  for pid in "$client_pid" "$hold_pid" "$api_pid" "$relay_pid" "$worker_pid" "$stats_pid"; do
    [[ -z "$pid" ]] || { kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; }
  done
  if (( rc != 0 ));then
    echo "M9 real fault experiment FAILED (exit=$rc)" >&2
    for log in "$tmp/api.log" "$tmp/relay.log" "$tmp/worker.log" "$tmp/client.log";do
      [[ ! -f "$log" ]] || tail -35 "$log" >&2
    done
    "${compose[@]}" ps >&2 || true
    "${compose[@]}" logs --tail 30 pg-primary pg-standby kafka clickhouse >&2 || true
  fi
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$tmp"
  exit "$rc"
}
trap cleanup EXIT
pgsql() {
  "${compose[@]}" exec -T "$1" psql -U telemetry -d telemetry -v ON_ERROR_STOP=1 -tAc "$2"
}
ready() {
  local service="$1" query="$2" expected="$3" attempts="${4:-100}"
  for _ in $(seq 1 "$attempts");do
    if [[ "$(pgsql "$service" "$query" 2>/dev/null)" == "$expected" ]];then return 0;fi
    sleep 2
  done
  echo "M9 PostgreSQL state not ready: $service" >&2
  return 1
}
mark_event() {
  python3 - "$output/fault-events.jsonl" "$1" <<'PY'
import datetime,json,pathlib,sys,time
line={"event":sys.argv[2],"monotonic_seconds":time.monotonic(),
      "utc":datetime.datetime.now(datetime.timezone.utc).isoformat()}
with pathlib.Path(sys.argv[1]).open("a",encoding="utf-8") as f:
    f.write(json.dumps(line,sort_keys=True)+"\n")
    f.flush()
PY
}
# Establish the existing M7 synchronous physical standby before starting traffic.
"${compose[@]}" up -d pg-primary
ready pg-primary 'SELECT count(*) FROM usage_ledger' 0
"${compose[@]}" up -d pg-standby
ready pg-standby 'SELECT pg_is_in_recovery()' t 120
ready pg-primary "SELECT count(*) FROM pg_stat_replication WHERE application_name='standby1' AND state='streaming'" 1
pgsql pg-primary "ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (standby1)'" >/dev/null
pgsql pg-primary "ALTER SYSTEM SET synchronous_commit='remote_apply'" >/dev/null
pgsql pg-primary 'SELECT pg_reload_conf()' >/dev/null
ready pg-primary "SELECT count(*) FROM pg_stat_replication WHERE application_name='standby1' AND sync_state='sync'" 1 50
ready pg-primary 'SHOW synchronous_commit' remote_apply 50
"${compose[@]}" up -d kafka clickhouse
for i in $(seq 1 100);do
  if "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list >/dev/null 2>&1 &&
     curl -fsS --max-time 2 -u telemetry:localdev-only 'http://127.0.0.1:18123/?database=telemetry' --data-binary 'SELECT 1' >/dev/null;then break;fi
  if (( i == 100 ));then echo 'Kafka/ClickHouse failed startup' >&2;exit 1;fi
  sleep 2
done
for topic in usage.critical.v1 inference.logs.v1 events.dlq.v1;do
  "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 --create --if-not-exists \
    --topic "$topic" --partitions 6 --replication-factor 1 >/dev/null
done
go build -o "$tmp/generator" ./cmd/event-generator
go build -o "$tmp/api" ./cmd/usage-ingest-api
go build -o "$tmp/relay" ./cmd/outbox-relay
go build -o "$tmp/worker" ./cmd/ingest-worker
go build -o "$tmp/pgha-probe" ./cmd/pgha-probe
seed="m9-$(openssl rand -hex 8)"
hold_seed="hold-$(openssl rand -hex 8)"
"$tmp/generator" --count "$count" --seed "$seed" \
  --start "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$tmp/events.jsonl"
credentials='['
for i in $(seq 0 7);do
  token="$(openssl rand -hex 32)"
  printf -v entry '{"token":"%s","tenant_id":"tenant_%03d","project_id":"project_%03d"}' \
    "$token" "$((i % 4 + 1))" "$((i + 1))"
  if (( i > 0 ));then credentials+=',';fi
  credentials+="$entry"
done
credentials+=']'
start_api() {
  local dsn="$1" log="$2"
  DATABASE_URL="$dsn" USAGE_INGEST_CREDENTIALS_JSON="$credentials" \
    "$tmp/api" --listen 127.0.0.1:18085 >"$log" 2>&1 &
  api_pid=$!
  for i in $(seq 1 60);do
    if curl -fsS --max-time 2 http://127.0.0.1:18085/healthz >/dev/null 2>&1;then return 0;fi
    kill -0 "$api_pid" 2>/dev/null || { echo "M9 API startup failed" >&2;return 1; }
    sleep 1
  done
  echo "M9 API health timeout" >&2;return 1
}
start_relay() {
  local dsn="$1" id="$2" log="$3"
  "$tmp/relay" --dsn "$dsn" --worker "$id" \
    --kafka-batch-timeout 5ms >"$log" 2>&1 &
  relay_pid=$!
}
start_api "$primary_dsn" "$tmp/api.log"
start_relay "$primary_dsn" "m9-primary-$$" "$tmp/relay.log"
"$tmp/worker" --batch-size 64 --group "m9-$$" --fetch-wait 250ms >"$tmp/worker.log" 2>&1 &
worker_pid=$!
# Resource snapshots, independent from HTTP and SQL timing.
(
  echo 'sampled_at_utc,container,cpu_percent,mem_usage'
  while true;do
    sampled="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    for name in pg-primary pg-standby kafka clickhouse;do
      cid=$("${compose[@]}" ps -q "$name" 2>/dev/null || true)
      [[ -z "$cid" ]] || docker stats --no-stream \
         --format "${sampled},{{.Name}},{{.CPUPerc}},{{.MemUsage}}" "$cid" 2>/dev/null || true
    done
    sleep 5
  done
) >"$output/docker-stats.csv" 2>/dev/null &
stats_pid=$!
M9_EXACT_HEAD="$(git rev-parse HEAD)" CLICKHOUSE_PASSWORD=localdev-only \
USAGE_INGEST_CREDENTIALS_JSON="$credentials" \
python3 scripts/m9_failover_client.py load --events-file "$tmp/events.jsonl" \
  --seed "$seed" --target-rps "$rate" --duration-seconds "$duration" \
  --concurrency "$concurrency" --fault-events "$output/fault-events.jsonl" \
  --start-marker "$tmp/load.started" --output "$output/client.json" \
  >"$tmp/client.log" 2>&1 &
client_pid=$!
for _ in $(seq 1 30);do
  [[ ! -f "$tmp/load.started" ]] || break
  kill -0 "$client_pid" 2>/dev/null || { echo 'M9 client failed to start' >&2;exit 1; }
  sleep 1
done
[[ -f "$tmp/load.started" ]] || { echo 'M9 no paced-load start marker' >&2;exit 1; }
sleep "$fault_at"
kill -0 "$client_pid" 2>/dev/null || { echo 'M9 client stopped before SIGKILL' >&2;exit 1; }
# Hold an actual uncommitted Ledger+Outbox transaction during ongoing requests.
"$tmp/pgha-probe" --phase hold --seed "$hold_seed" --dsn "$primary_dsn" \
    --hold-ready "$tmp/hold.ready" >"$tmp/hold.log" 2>&1 &
hold_pid=$!
for _ in $(seq 1 30);do
  [[ ! -f "$tmp/hold.ready" ]] || break
  kill -0 "$hold_pid" 2>/dev/null || { echo 'M9 uncommitted transaction exited early' >&2;exit 1; }
  sleep 1
done
[[ -f "$tmp/hold.ready" ]] || { echo 'M9 missing live transaction' >&2;exit 1; }
# Fail closed unless synchronous replication is genuinely still active.
[[ "$(pgsql pg-primary 'SHOW synchronous_commit')" == remote_apply ]]
[[ "$(pgsql pg-primary "SELECT count(*) FROM pg_stat_replication WHERE application_name='standby1' AND sync_state='sync'")" == 1 ]]
mark_event fault_injected
"${compose[@]}" kill -s SIGKILL pg-primary
wait "$hold_pid" 2>/dev/null || true
hold_pid=''
[[ -z "$("${compose[@]}" ps --status running -q pg-primary)" ]] || { echo 'old primary still running' >&2;exit 1; }
if timeout 3 bash -c '</dev/tcp/127.0.0.1/25432' >/dev/null 2>&1;then
  echo 'old primary not isolated; refusing promotion' >&2;exit 1
fi
mark_event primary_isolated
[[ "$(pgsql pg-standby 'SELECT pg_is_in_recovery()')" == t ]] || exit 1
"${compose[@]}" exec -T -u postgres pg-standby pg_ctl -D /var/lib/postgresql/data promote
ready pg-standby 'SELECT pg_is_in_recovery()' f 60
mark_event promoted
# Explicit manual client/Relay endpoint change: NO automatic HA claim.
kill "$api_pid" "$relay_pid" 2>/dev/null || true
wait "$api_pid" "$relay_pid" 2>/dev/null || true
api_pid='' relay_pid=''
start_api "$standby_dsn" "$tmp/api-promoted.log"
start_relay "$standby_dsn" "m9-promoted-$$" "$tmp/relay-promoted.log"
[[ "$(pgsql pg-standby 'SELECT pg_is_in_recovery()')" == f ]] || exit 1
mark_event api_ready
wait "$client_pid"
client_pid=''
# Trusted projection is already complete; allow the real Relay to persist ACK marks.
for _ in $(seq 1 90);do
  published="$(pgsql pg-standby 'SELECT count(*) FROM event_outbox WHERE published_at IS NOT NULL')"
  [[ "$published" != "$count" ]] || break
  sleep 1
done
[[ "$published" == "$count" ]] || { echo "outbox not fully published: $published/$count" >&2;exit 1; }
ledger_count="$(pgsql pg-standby 'SELECT count(*) FROM usage_ledger')"
outbox_count="$(pgsql pg-standby 'SELECT count(*) FROM event_outbox')"
held_count="$(pgsql pg-standby "SELECT count(*) FROM usage_ledger WHERE request_id LIKE '${hold_seed}_req_%'")"
# Private synthetic IDs/hashes never leave disposable runner tempdir/artifacts.
"${compose[@]}" exec -T pg-standby psql -U telemetry -d telemetry \
  -v ON_ERROR_STOP=1 -c "COPY (
    SELECT l.event_id::text,l.payload_hash::text,
           e.event_payload->>'payload_hash',e.published_at IS NOT NULL
    FROM usage_ledger l LEFT JOIN event_outbox e ON e.event_id=l.event_id
    ORDER BY l.event_id
  ) TO STDOUT WITH CSV" >"$tmp/ledger.csv"
python3 scripts/m9_failover_client.py verify \
  --events-file "$tmp/events.jsonl" --seed "$seed" \
  --ledger-csv "$tmp/ledger.csv" --ledger-count "$ledger_count" \
  --outbox-count "$outbox_count" --held-uncommitted-rows "$held_count" \
  --fault-events "$output/fault-events.jsonl" --client-report "$output/client.json" \
  --exact-head "$(git rev-parse HEAD)" --output "$output/summary.json"
echo 'M9 combined PGHA + steady-load experiment PASS (only for controlled CI fault envelope)'
