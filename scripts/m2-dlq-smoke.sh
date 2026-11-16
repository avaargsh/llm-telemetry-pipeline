#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f deployments/docker-compose.yml)
worker_pid=''
cleanup() {
  code=$?
  [[ -z "$worker_pid" ]] || { kill "$worker_pid" 2>/dev/null || true; wait "$worker_pid" 2>/dev/null || true; }
  if (( code != 0 )); then
    tail -100 /tmp/m2-dlq-worker.log 2>/dev/null || true
    "${compose[@]}" logs --tail 50 kafka clickhouse || true
  fi
  "${compose[@]}" down -v || true
}
trap cleanup EXIT
"${compose[@]}" up -d kafka clickhouse
for n in $(seq 1 90); do
  if "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list >/dev/null 2>&1; then break; fi
  if (( n==90 ));then echo 'Kafka not ready' >&2;exit 1;fi
  sleep 2
done
bash scripts/bootstrap-topics.sh
ch() {
  curl -fsS -u telemetry:localdev-only 'http://localhost:18123/?database=telemetry' --data-binary "$1" | tr -d '[:space:]'
}
for n in $(seq 1 90);do
  if [[ "$(ch 'SELECT 1 FORMAT TabSeparatedRaw' 2>/dev/null)" == 1 ]];then break;fi
  if (( n==90 ));then echo 'ClickHouse not ready' >&2;exit 1;fi
  sleep 2
done

go build -o /tmp/m2-dlq-consumer ./cmd/ingest-worker
go build -o /tmp/m2-dlq-generator ./cmd/event-generator
go build -o /tmp/m2-dlq-producer ./cmd/event-producer
group="m2-dlq-${GITHUB_RUN_ID:-local}"
/tmp/m2-dlq-consumer --group "$group" --batch-size 10 --fetch-wait 500ms > /tmp/m2-dlq-worker.log 2>&1 &
worker_pid=$!

/tmp/m2-dlq-generator --count 1 --seed "$group" --start 2026-11-16T11:00:00Z | /tmp/m2-dlq-producer
printf '{definitely-not-json}\n' | "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:19092 --topic usage.critical.v1

for n in $(seq 1 50);do
  actual=$(ch 'SELECT uniqExact(event_id) FROM inference_events_raw FORMAT TabSeparatedRaw')
  if [[ "$actual" == 1 ]];then break;fi
  if ! kill -0 "$worker_pid" 2>/dev/null;then echo 'consumer crashed' >&2;exit 1;fi
  if (( n==50 ));then echo 'valid message not ingested' >&2;exit 1;fi
  sleep 2
done

raw=$("${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 --topic events.dlq.v1 --from-beginning --timeout-ms 15000 \
  --max-messages 1 2>/dev/null)
printf '%s\n' "$raw" | python3 -c '
import sys,json,base64
msg=json.load(sys.stdin)
assert msg["source_topic"]=="usage.critical.v1",msg
assert msg["source_offset"]>=0,msg
assert base64.b64decode(msg["raw_payload"])==b"{definitely-not-json}",msg
print("PASS DLQ source identity and original payload preserved")
'
echo "PASS M2b: valid event visible in ClickHouse, poison message acknowledged in Kafka DLQ"
