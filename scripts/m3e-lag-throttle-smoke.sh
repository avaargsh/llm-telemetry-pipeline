#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f deployments/docker-compose.yml)
cleanup(){
  local code=$?
  if ((code!=0));then
    "${compose[@]}" ps >&2 || true
    "${compose[@]}" logs --tail=75 kafka clickhouse >&2 || true
  fi
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT
"${compose[@]}" up -d kafka clickhouse
for i in $(seq 1 90);do
  if "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 --list >/dev/null 2>&1;then break;fi
  if ((i==90));then echo 'Kafka startup timeout' >&2;exit 1;fi
  sleep 2
done
for topic in m3e.monitor.v1 m3e.throttle.v1;do
  "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1 \
    --config retention.ms=60000 --config message.timestamp.type=LogAppendTime
done
for i in $(seq 1 90);do
  if curl -fsS -u telemetry:localdev-only \
    'http://localhost:18123/?database=telemetry' --data-binary 'SELECT 1' >/dev/null 2>&1;then break;fi
  if ((i==90));then echo 'ClickHouse startup timeout' >&2;exit 1;fi
  sleep 2
done
export TEST_KAFKA_LAG=1 TEST_M3E_THROTTLE=1
export TEST_KAFKA_BROKER=localhost:29092 TEST_CLICKHOUSE_HTTP=http://localhost:18123
export M3E_LAG_REPORT_PATH="${M3E_LAG_REPORT_PATH:-/tmp/m3e-lag-report.json}"
export M3E_THROTTLE_REPORT_PATH="${M3E_THROTTLE_REPORT_PATH:-/tmp/m3e-throttle-report.json}"
go test -count=1 -timeout 5m -v ./internal/lagmonitor \
 -run '^TestRealKafkaGroupOffsetsAndEffectiveRetentionMetrics$'
go test -count=1 -timeout 5m -v ./internal/ingest \
 -run '^TestRealClickHouseThrottleAndCatchUp$'
python3 - <<'PY'
import json,os
a=json.load(open(os.environ["M3E_LAG_REPORT_PATH"]))
assert a["broker_observed_lag_offsets"]==12 and a["effective_retention_seconds"]==60,a
assert a["initial_committed_offset"]==8 and a["final_committed_offset"]==20,a
assert a["warning_fired"] and a["critical_fired_under_59s_margin"] and a["alert_resolved_after_catchup"],a
assert a["recovery_records"]==12 and a["recovery_seconds"]>0,a
b=json.load(open(os.environ["M3E_THROTTLE_REPORT_PATH"]))
assert b["producer_acks"]==32 and b["blocked_second_batch_offset"]==8,b
assert b["recovered_committed_offset"]==32,b
assert b["raw_events"]==32 and b["trusted_events"]==32 and b["integrity_conflicts"]==0,b
assert b["observed_lag_offsets"]==24 and b["warning_during_block"] and b["alert_resolved_after_catchup"],b
assert b["catchup_seconds"]>0 and b["catchup_events_per_second"]>0,b
print("PASS M3e live broker telemetry: lag warning/critical/resolved; throttled ClickHouse 24 pending records safely caught up")
PY
