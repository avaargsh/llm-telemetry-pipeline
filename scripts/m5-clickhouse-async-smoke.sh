#!/usr/bin/env bash
# Disposable ClickHouse 25.8 async-buffer/flush ACK semantics experiment.
# Never connect to production; the script destroys only its own Compose project.
set -Eeuo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME="llmtelemetry-m5-async-${GITHUB_RUN_ID:-local}-$$"
compose=(docker compose -f deployments/docker-compose.yml)
cleanup() {
  local rc=$?
  trap - EXIT
  if (( rc != 0 )); then
    echo "M5 ClickHouse async ACK test failed with exit $rc" >&2
    "${compose[@]}" ps >&2 || true
    "${compose[@]}" logs --tail 80 clickhouse >&2 || true
  fi
  "${compose[@]}" down -v >/dev/null 2>&1 || true
  exit "$rc"
}
trap cleanup EXIT
"${compose[@]}" up -d clickhouse

ready=0
for i in $(seq 1 90); do
  if curl -fsS --max-time 3 -u telemetry:localdev-only \
      'http://127.0.0.1:18123/?database=telemetry' --data-binary 'SELECT 1' \
      >/dev/null 2>&1; then ready=1;break;fi
  sleep 2
done
if (( ready != 1 )); then echo "ClickHouse not ready" >&2;exit 1;fi

# Package-level "go test" changes cwd; store evidence at an absolute workspace
# path, otherwise upload-artifact cannot locate it.
if [[ -n "${M5_ASYNC_EVIDENCE_PATH:-}" && "$M5_ASYNC_EVIDENCE_PATH" != /* ]];then
  export M5_ASYNC_EVIDENCE_PATH="$PWD/$M5_ASYNC_EVIDENCE_PATH"
fi
export TEST_CLICKHOUSE_HTTP='http://127.0.0.1:18123'
go test -count=1 -timeout 90s -v ./internal/clickhouse \
  -run '^(TestInsertOverridesUnsafeAsyncEndpointSettings|TestRealClickHouseAsyncACKBoundary)$'
echo "PASS M5: confirmed ClickHouse writer cannot be downgraded; tested real wait=1 vs wait=0 buffer ACKs"
