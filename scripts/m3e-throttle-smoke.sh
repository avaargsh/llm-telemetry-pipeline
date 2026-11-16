#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f deployments/docker-compose.yml)
cleanup(){
 code=$?
 if ((code!=0));then "${compose[@]}" logs --tail=70 kafka clickhouse || true;fi
 "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT
"${compose[@]}" up -d kafka clickhouse
for i in $(seq 1 90);do
 if "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list >/dev/null 2>&1;then break;fi
 if ((i==90));then echo 'Kafka readiness failed' >&2;exit 1;fi
 sleep 2
done
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka:19092 --create --if-not-exists \
 --topic m3e.throttle.v1 --partitions 1 --replication-factor 1 \
 --config retention.ms=600000 --config message.timestamp.type=LogAppendTime
for i in $(seq 1 90);do
 if curl -fsS -u telemetry:localdev-only 'http://localhost:18123/?database=telemetry' --data-binary 'SELECT 1' >/dev/null 2>&1;then break;fi
 if ((i==90));then echo 'ClickHouse readiness failed' >&2;exit 1;fi
 sleep 2
done
export TEST_M3E_THROTTLE=1 TEST_KAFKA_BROKER=localhost:29092 TEST_CLICKHOUSE_HTTP=http://localhost:18123
export M3E_THROTTLE_REPORT="${M3E_THROTTLE_REPORT:-/tmp/m3e-throttle-report.json}"
go test -count=1 -timeout 3m -v ./internal/ingest -run '^TestClickHouseBackpressureCatchUp$'
python3 - <<'PY'
import json,os
r=json.load(open(os.environ["M3E_THROTTLE_REPORT"]))
assert r["producer_acks"]==24,r
assert r["first_committed_next_offset"]==8 and r["held_committed_next_offset"]==8,r
assert r["held_lag_offset_distance"]==16,r
assert r["recovery_committed_offsets"]==[15,23],r
assert r["catchup_events"]==16 and r["catchup_events_per_second"]>0,r
assert r["final_committed_next_offset"]==24 and r["final_lag_offset_distance"]==0,r
assert r["raw_rows"]==24 and r["trusted_events"]==24,r
print("PASS M3e slow ClickHouse ACK: fail-closed source offset, measured 16-event catch-up and no data loss in tested scope")
PY
