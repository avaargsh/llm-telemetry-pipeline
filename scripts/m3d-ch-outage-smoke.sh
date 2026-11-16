#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."

compose=(docker compose -f deployments/docker-compose.yml)
cleanup() {
  local rc=$?
  if ((rc!=0));then
    echo '=== M3d failed; isolated Kafka/ClickHouse diagnostic ===' >&2
    "${compose[@]}" ps >&2 || true
    "${compose[@]}" logs --tail=80 kafka clickhouse >&2 || true
  fi
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${compose[@]}" up -d kafka clickhouse
for i in $(seq 1 90); do
  if "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 --list >/dev/null 2>&1;then break;fi
  if ((i==90));then echo 'Kafka did not become ready' >&2;exit 1;fi
  sleep 2
done

# Use actual broker append timestamps, not the user-supplied inference event
# timestamp. This isolates the retention risk assessment from clock skew in
# the synthetic usage event payload.
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --create --if-not-exists \
  --topic m3d.ch.outage.v1 --partitions 1 --replication-factor 1 \
  --config retention.ms=600000 --config message.timestamp.type=LogAppendTime

for i in $(seq 1 90);do
  if curl -fsS -u telemetry:localdev-only 'http://localhost:18123/?database=telemetry' \
    --data-binary 'SELECT 1' >/dev/null 2>&1;then break;fi
  if ((i==90));then echo 'ClickHouse did not become ready' >&2;exit 1;fi
  sleep 2
done

export TEST_M3D_OUTAGE=1
export TEST_KAFKA_BROKER=localhost:29092
export TEST_CLICKHOUSE_HTTP=http://localhost:18123
export M3D_REPORT_PATH="${M3D_REPORT_PATH:-/tmp/m3d-outage-report.json}"

go test -count=1 -timeout 4m -v ./internal/ingest \
  -run '^TestClickHouseOutageMidStreamKeepsCommittedPrefixAndReplays$'

python3 - <<'PY'
import json,os
r=json.load(open(os.environ["M3D_REPORT_PATH"]))
assert r["producer_acks"]==24, r
assert r["initial_committed_offsets"]==[7],r
assert r["recovery_committed_offsets"]==[15,23],r
assert r["raw_rows"]==24 and r["trusted_event_ids"]==24 and r["integrity_conflicts"]==0,r
assert r["fail_closed"] and r["recovered"],r
risk=r["outage_retention_assessment"]
assert risk["lag_offset_distance"]==16 and risk["status"]=="healthy",risk
assert risk["time_to_expiry_seconds"]>risk["safety_margin_seconds"],risk
print("PASS M3d: CH stopped during batch 2; no unsafe offset commit, 16 records replayed, 24 trusted; broker timestamp-based retention headroom measured")
PY
