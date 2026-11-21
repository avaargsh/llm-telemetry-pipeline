#!/usr/bin/env bash
# Isolated M6 scheduled reconciliation: real PostgreSQL/ClickHouse, no writes
# beyond existing synthetic test-fixture generation. No production secrets.
set -Eeuo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME="llmtelemetry-m6-${GITHUB_RUN_ID:-local}-$$"
compose=(docker compose -f deployments/docker-compose.yml)
tmp="$(mktemp -d)"
cleanup() {
  local rc=$?
  trap - EXIT
  if (( rc != 0 )); then
    echo "M6 scheduled reconciliation failed ($rc)" >&2
    "${compose[@]}" logs --tail 60 postgres clickhouse >&2 || true
  fi
  "${compose[@]}" down -v >/dev/null 2>&1 || true
  rm -rf "$tmp"
  exit "$rc"
}
trap cleanup EXIT

"${compose[@]}" up -d postgres clickhouse
for i in $(seq 1 90); do
  if "${compose[@]}" exec -T postgres psql -U telemetry -d telemetry \
        -tAc 'SELECT count(*) FROM usage_ledger' >/dev/null 2>&1 &&
     curl -fsS --max-time 4 -u telemetry:localdev-only \
        'http://127.0.0.1:18123/?database=telemetry' \
        --data-binary 'SELECT 1' >/dev/null 2>&1; then
    break
  fi
  if (( i == 90 )); then echo "Postgres/ClickHouse not ready" >&2; exit 1; fi
  sleep 2
done
export TEST_POSTGRES_DSN='postgres://telemetry:localdev-only@127.0.0.1:15432/telemetry?sslmode=disable'
export TEST_CLICKHOUSE_HTTP='http://127.0.0.1:18123'
export POSTGRES_DSN="$TEST_POSTGRES_DSN"
export CLICKHOUSE_HTTP="$TEST_CLICKHOUSE_HTTP"
export CLICKHOUSE_USER='telemetry'
export CLICKHOUSE_PASSWORD='localdev-only'

go test -count=1 -timeout 100s -v ./internal/reconcile -run '^TestRealDatabasesReconcileAllStatuses$'
go build -o "$tmp/reconciler" ./cmd/reconciler

# At as-of 11:16 and 15-minute observation lag, [11:00, 11:01) includes
# real fixture records. It must detect the 8 categories and exit 2 (FAIL).
mkdir -p artifacts
set +e
python3 scripts/m6_scheduled_reconcile.py \
  --as-of '2026-11-16T11:16:00Z' \
  --window-minutes 1 --lag-minutes 15 --max-records 100 \
  --reconciler-bin "$tmp/reconciler" --output artifacts/m6-scheduled-fail.json \
  >"$tmp/reconcile-fail-summary.json"
failed_rc=$?
set -e
if (( failed_rc != 2 )); then
  echo "expected confirmed discrepancy exit 2, got $failed_rc" >&2
  exit 1
fi

# A different bounded minute interval has no data. A real empty window should
# yield PASS, not silently reusing the previous result or scanning all time.
python3 scripts/m6_scheduled_reconcile.py \
  --as-of '2026-11-16T11:17:00Z' \
  --window-minutes 1 --lag-minutes 15 --max-records 100 \
  --reconciler-bin "$tmp/reconciler" --output artifacts/m6-scheduled-empty.json \
  >"$tmp/reconcile-empty-summary.json"

python3 - <<'PY'
import json, pathlib
failed = json.loads(pathlib.Path('artifacts/m6-scheduled-fail.json').read_text())
empty = json.loads(pathlib.Path('artifacts/m6-scheduled-empty.json').read_text())
assert failed['status'] == 'FAIL' and failed['reconciler_exit_code'] == 2, failed
assert failed['ledger_events'] == 7 and failed['analytics_events'] == 5, failed
for kind in ('matched', 'replayed', 'pending', 'in_flight',
             'missing', 'orphan', 'conflict', 'mismatch'):
    assert failed['counts'].get(kind) == 1, (kind, failed)
assert empty['status'] == 'PASS' and empty['reconciler_exit_code'] == 0, empty
assert empty['ledger_events'] == 0 and empty['analytics_events'] == 0, empty
for report in (failed, empty):
    assert report['findings_redacted'] is True
    assert 'findings' not in report
    assert 'localdev-only' not in json.dumps(report)
    assert 'postgres://' not in json.dumps(report)
print('PASS M6: real PG/CH bounded scheduled window, correct FAIL/PASS exits, no raw IDs/passwords in evidence')
PY
