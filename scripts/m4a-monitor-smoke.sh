#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f deployments/docker-compose.yml)
cleanup(){
  local rc=$?
  if ((rc!=0));then
    echo '=== M4a monitor/slow-CH failure diagnostics ===' >&2
    "${compose[@]}" ps >&2 || true
    "${compose[@]}" logs --tail=100 kafka clickhouse >&2 || true
  fi
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${compose[@]}" up -d kafka clickhouse
for i in $(seq 1 90);do
  if "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka:19092 --list >/dev/null 2>&1;then break;fi
  if ((i==90));then echo 'Kafka readiness failed' >&2;exit 1;fi
  sleep 2
done
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --create --if-not-exists \
  --topic m4a.throttle.v1 --partitions 1 --replication-factor 1 \
  --config retention.ms=600000 --config message.timestamp.type=LogAppendTime
for i in $(seq 1 90);do
  if curl -fsS -u telemetry:localdev-only \
    'http://127.0.0.1:18123/?database=telemetry' --data-binary 'SELECT 1' >/dev/null 2>&1;then break;fi
  if ((i==90));then echo 'ClickHouse readiness failed' >&2;exit 1;fi
  sleep 2
done
export TEST_M4A_MONITOR=1
export TEST_KAFKA_BROKER=localhost:29092
export TEST_CLICKHOUSE_HTTP=http://127.0.0.1:18123
export M4A_REPORT_PATH="${M4A_REPORT_PATH:-/tmp/m4a-monitor-report.json}"
go test -count=1 -timeout 4m -v ./internal/lagmonitor \
  -run '^TestRealKafkaLagGrowsUnderClickHouseHTTPThrottleAndRecovers$'
python3 - <<'PY'
import json,os
r=json.load(open(os.environ["M4A_REPORT_PATH"]))
assert r["producer_acks"]==24,r
assert r["committed_before_release"]==8,r
assert r["lag_offset_distance_during_throttle"]==16,r
assert r["committed_after_recovery"]==24,r
assert r["lag_offset_distance_after_recovery"]==0,r
assert r["clickhouse_trusted_events"]==24 and r["clickhouse_hash_conflicts"]==0,r
assert r["recovery_duration_ms"]>0 and r["recovery_throughput_records_per_second"]>0,r
assert r["metrics_up"] is True,r
print("PASS M4a: live Kafka group lag 16 during HTTP throttle, no unsafe commit, catch-up to 0, throughput measured")
PY
