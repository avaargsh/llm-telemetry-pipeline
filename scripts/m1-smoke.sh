#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f deployments/docker-compose.yml)
seed="ci-m1-${GITHUB_RUN_ID:-local}"
count="${M1_COUNT:-1000}"
if [[ ! "$count" =~ ^[0-9]+$ ]] || (( count < 1 || count > 100000 )); then echo "M1_COUNT must be 1..100000" >&2; exit 2; fi
batch_size=100
if (( count > 1000 )); then batch_size=1000; fi
worker_pid=''
cleanup() {
  local code=$?
  if [[ -n "$worker_pid" ]]; then kill "$worker_pid" 2>/dev/null || true; wait "$worker_pid" 2>/dev/null || true; fi
  if [[ "$code" -ne 0 ]]; then
    echo '=== worker log ===' >&2; tail -100 /tmp/llm-telemetry-worker.log >&2 || true
    echo '=== containers ===' >&2; "${compose[@]}" ps >&2 || true
    "${compose[@]}" logs --tail=60 kafka clickhouse >&2 || true
  fi
  "${compose[@]}" down -v || true
}
trap cleanup EXIT

"${compose[@]}" up -d
for attempt in $(seq 1 90); do
  if "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
       --bootstrap-server kafka:19092 --list >/dev/null 2>&1; then break; fi
  container_id=$("${compose[@]}" ps -aq kafka)
  if [[ -n "$container_id" ]]; then
    state=$(docker inspect --format '{{.State.Status}}' "$container_id" 2>/dev/null || true)
    if [[ "$state" == "exited" || "$state" == "dead" ]]; then
      echo "Kafka container exited before readiness" >&2
      exit 1
    fi
  fi
  if [[ "$attempt" -eq 90 ]]; then echo 'Kafka readiness timeout' >&2; exit 1; fi
  sleep 2
done
bash scripts/bootstrap-topics.sh

clickhouse() {
  curl --fail --silent --show-error -u telemetry:localdev-only \
    'http://127.0.0.1:18123/?database=telemetry' --data-binary "$1"
}
for attempt in $(seq 1 90); do
  if clickhouse 'SELECT 1 FORMAT TabSeparatedRaw' >/dev/null 2>&1; then break; fi
  if [[ "$attempt" -eq 90 ]]; then echo 'ClickHouse readiness timeout' >&2; exit 1; fi
  sleep 2
done

go run ./cmd/ingest-worker --batch-size "$batch_size" > /tmp/llm-telemetry-worker.log 2>&1 &
worker_pid=$!
go run ./cmd/event-generator --count "$count" --seed "$seed" --start '2026-11-16T11:00:00Z' | go run ./cmd/event-producer --batch-size "$batch_size"

for attempt in $(seq 1 90); do
  read -r raw_rows unique_events < <(clickhouse 'SELECT count(), uniqExact(event_id) FROM inference_events_raw FORMAT TabSeparatedRaw' | tr '\t' ' ')
  echo "M1 sample: raw_rows=${raw_rows:-0} unique_events=${unique_events:-0}"
  if [[ "${unique_events:-0}" == "$count" ]]; then
    if (( raw_rows < count )); then echo 'Fewer raw rows than unique events' >&2; exit 1; fi
    trusted_count=$(clickhouse 'SELECT count() FROM inference_events_dedup_v1 FORMAT TabSeparatedRaw' | tr -d '[:space:]')
    integrity_count=$(clickhouse 'SELECT count() FROM inference_events_integrity_v1 FORMAT TabSeparatedRaw' | tr -d '[:space:]')
    if [[ "$trusted_count" != "$count" || "$integrity_count" != 0 ]]; then
      echo "Analytical projection mismatch: expected=$count trusted=$trusted_count integrity=$integrity_count" >&2
      exit 1
    fi
    echo "PASS: ${unique_events} unique usage events, ${raw_rows} raw rows, ${trusted_count} trusted analytical events"
    exit 0
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then echo 'Consumer stopped prematurely' >&2; exit 1; fi
  if [[ "$attempt" -eq 90 ]]; then echo "${count}-event verification timed out" >&2; exit 1; fi
  sleep 2
done
