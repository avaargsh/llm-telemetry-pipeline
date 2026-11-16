#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f deployments/docker-compose.yml)
cleanup(){
  local exit_code=$?
  if (( exit_code != 0 ));then
    "${compose[@]}" ps || true
    "${compose[@]}" logs --tail=100 kafka clickhouse || true
  fi
  "${compose[@]}" down -v || true
}
trap cleanup EXIT
"${compose[@]}" up -d kafka clickhouse
for i in $(seq 1 90);do
  if "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
       --bootstrap-server kafka:19092 --list >/dev/null 2>&1;then break;fi
  if (( i==90 ));then echo 'Kafka readiness timeout' >&2;exit 1;fi
  sleep 2
done
bash scripts/bootstrap-topics.sh
for i in $(seq 1 90);do
  if curl -fsS -u telemetry:localdev-only \
      'http://localhost:18123/?database=telemetry' --data-binary 'SELECT 1' >/dev/null 2>&1;then break;fi
  if (( i==90 ));then echo 'ClickHouse readiness timeout' >&2;exit 1;fi
  sleep 2
done
go test -count=1 -timeout 3m -v ./internal/ingest -run '^TestKafkaCrashWindowsPreserveLogicalUsage$'
