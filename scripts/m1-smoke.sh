#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f deployments/docker-compose.yml)
seed="ci-m1-${GITHUB_RUN_ID:-local}"
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

go run ./cmd/ingest-worker --batch-size 100 > /tmp/llm-telemetry-worker.log 2>&1 &
worker_pid=$!
go run ./cmd/event-generator --count 1000 --seed "$seed" --start '2026-11-16T11:00:00Z' | go run ./cmd/event-producer --batch-size 100

for attempt in $(seq 1 90); do
  read -r raw_rows unique_events < <(clickhouse 'SELECT count(), uniqExact(event_id) FROM inference_events_raw FORMAT TabSeparatedRaw' | tr '\t' ' ')
  echo "M1 sample: raw_rows=${raw_rows:-0} unique_events=${unique_events:-0}"
  if [[ "${unique_events:-0}" == 1000 ]]; then
    if (( raw_rows < 1000 )); then echo 'Fewer raw rows than unique events' >&2; exit 1; fi
    echo "PASS: ${unique_events} unique usage events, ${raw_rows} raw rows"
    exit 0
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then echo 'Consumer stopped prematurely' >&2; exit 1; fi
  if [[ "$attempt" -eq 90 ]]; then echo '1000-event verification timed out' >&2; exit 1; fi
  sleep 2
done
