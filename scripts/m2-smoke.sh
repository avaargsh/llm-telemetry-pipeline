#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f deployments/docker-compose.yml)
worker_pid=''
relay_pid=''
cleanup() {
  code=$?
  [[ -z "$worker_pid" ]] || { kill "$worker_pid" 2>/dev/null || true; wait "$worker_pid" 2>/dev/null || true; }
  [[ -z "$relay_pid" ]] || { kill "$relay_pid" 2>/dev/null || true; wait "$relay_pid" 2>/dev/null || true; }
  if (( code != 0 )); then
    tail -100 /tmp/m2-consumer.log /tmp/m2-relay.log 2>/dev/null || true
    "${compose[@]}" ps || true
    "${compose[@]}" logs --tail 50 kafka postgres clickhouse || true
  fi
  "${compose[@]}" down -v || true
}
trap cleanup EXIT
"${compose[@]}" up -d
kafka_ready() {
  "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 --list >/dev/null 2>&1
}
postgres_count() {
  "${compose[@]}" exec -T postgres psql -U telemetry -d telemetry -tAc "$1" | tr -d '[:space:]'
}
clickhouse_query() {
  curl -fsS -u telemetry:localdev-only \
    'http://localhost:18123/?database=telemetry' --data-binary "$1" | tr -d '[:space:]'
}
for n in $(seq 1 90); do
  if kafka_ready && [[ "$(postgres_count 'SELECT count(*) FROM usage_ledger' 2>/dev/null)" == 0 ]] &&
    [[ "$(clickhouse_query 'SELECT 1 FORMAT TabSeparatedRaw' 2>/dev/null)" == 1 ]]; then break; fi
  if (( n==90 )); then echo 'services not ready' >&2;exit 1;fi
  sleep 2
done
bash scripts/bootstrap-topics.sh
go build -o /tmp/m2-generator ./cmd/event-generator
go build -o /tmp/m2-ledger ./cmd/usage-ledger-ingest
go build -o /tmp/m2-consumer ./cmd/ingest-worker
go build -o /tmp/m2-relay ./cmd/outbox-relay

# PostgreSQL is the durable source; Kafka is intentionally unavailable.
"${compose[@]}" stop kafka
/tmp/m2-generator --count 100 --seed "m2-${GITHUB_RUN_ID:-local}" \
    --start 2026-11-16T11:00:00Z | /tmp/m2-ledger
ledger_before=$(postgres_count 'SELECT count(*) FROM usage_ledger')
outbox_before=$(postgres_count 'SELECT count(*) FROM event_outbox WHERE published_at IS NULL')
echo "Kafka offline: ledger=$ledger_before pending_outbox=$outbox_before"
[[ "$ledger_before" == 100 && "$outbox_before" == 100 ]] || exit 1
"${compose[@]}" start kafka
for n in $(seq 1 90); do
  if kafka_ready;then break;fi
  if (( n==90 ));then echo 'Kafka restart failure' >&2;exit 1;fi
  sleep 2
done

/tmp/m2-consumer --batch-size 50 > /tmp/m2-consumer.log 2>&1 &
worker_pid=$!
/tmp/m2-relay --worker "ci-${GITHUB_RUN_ID:-local}" > /tmp/m2-relay.log 2>&1 &
relay_pid=$!
for n in $(seq 1 90); do
  done_count=$(postgres_count 'SELECT count(*) FROM event_outbox WHERE published_at IS NOT NULL')
  distinct_count=$(clickhouse_query 'SELECT uniqExact(event_id) FROM inference_events_raw FORMAT TabSeparatedRaw')
  echo "M2 recovery: published=$done_count distinct_clickhouse=$distinct_count"
  if [[ "$done_count" == 100 && "$distinct_count" == 100 ]];then break;fi
  if ! kill -0 "$worker_pid" 2>/dev/null || ! kill -0 "$relay_pid" 2>/dev/null; then echo 'worker or relay exited' >&2;exit 1;fi
  if (( n==90 ));then echo 'recovery timeout' >&2;exit 1;fi
  sleep 2
done

# Replay of the exact immutable event IDs must not create another charge.
/tmp/m2-generator --count 100 --seed "m2-${GITHUB_RUN_ID:-local}" \
    --start 2026-11-16T11:00:00Z | /tmp/m2-ledger
ledger_after=$(postgres_count 'SELECT count(*) FROM usage_ledger')
outbox_after=$(postgres_count 'SELECT count(*) FROM event_outbox')
if [[ "$ledger_after" != 100 || "$outbox_after" != 100 ]];then
    echo "duplicate charge or outbox created: ledger=$ledger_after outbox=$outbox_after" >&2;exit 1
fi
echo "PASS M2: Kafka outage stored 100 ledger+outbox, replayed to Kafka/ClickHouse, duplicate input created no new charges"
