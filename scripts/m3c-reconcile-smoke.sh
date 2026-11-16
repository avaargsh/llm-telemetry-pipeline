#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f deployments/docker-compose.yml)
"${compose[@]}" up -d postgres clickhouse
for n in $(seq 1 90);do
  if "${compose[@]}" exec -T postgres psql -U telemetry -d telemetry -tAc 'SELECT count(*) FROM usage_ledger' >/dev/null 2>&1 &&
      curl -fsS -u telemetry:localdev-only 'http://localhost:18123/?database=telemetry' \
       --data-binary 'SELECT 1' >/dev/null 2>&1;then break;fi
  if (( n==90 ));then echo 'PostgreSQL/ClickHouse not ready' >&2;exit 1;fi
  sleep 2
done
go test -count=1 -v ./internal/reconcile -run '^TestRealDatabasesReconcileAllStatuses$'
go build -o /tmp/llm-m3c-reconciler ./cmd/reconciler
set +e
POSTGRES_DSN="$TEST_POSTGRES_DSN" CLICKHOUSE_HTTP="$TEST_CLICKHOUSE_HTTP" \
 CLICKHOUSE_USER=telemetry CLICKHOUSE_PASSWORD=localdev-only \
  /tmp/llm-m3c-reconciler --start '2026-11-16T11:00:00Z' \
    --end '2026-11-16T11:01:00Z' --grace 3m --max-records 100 > /tmp/m3c-report.json
result_code=$?
set -e
if (( result_code!=2 ));then
  echo "expected CLI exit 2 on confirmed integrity discrepancies; got $result_code" >&2
  cat /tmp/m3c-report.json >&2 || true
  exit 1
fi
python3 - <<'PY'
import json
r=json.load(open("/tmp/m3c-report.json"))
assert r["result"]=="FAIL",r
assert r["ledger_events"]==7 and r["analytics_events"]==5,r
for kind in ["matched","replayed","pending","in_flight","missing","orphan","conflict","mismatch"]:
  assert r["counts"].get(kind)==1,(kind,r)
assert r["raw_duplicate_copies"]==2,r
assert len(r["findings"])==8,r
print("PASS M3c machine-readable report: ledger=7, ClickHouse=5, 8 classified identities; proper failure exit 2")
PY
