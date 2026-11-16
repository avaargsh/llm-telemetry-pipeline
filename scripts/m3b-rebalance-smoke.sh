#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f deployments/docker-compose.yml)
"${compose[@]}" up -d kafka clickhouse
for i in $(seq 1 90); do
  if "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka:19092 --list >/dev/null 2>&1; then break; fi
  if ((i==90));then echo 'Kafka readiness failed' >&2; exit 1;fi
  sleep 2
done
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --create --if-not-exists \
  --topic m3b.rebalance.v1 --partitions 2 --replication-factor 1
for i in $(seq 1 90); do
  if curl -fsS -u telemetry:localdev-only 'http://localhost:18123/?database=telemetry' \
      --data-binary 'SELECT 1' >/dev/null 2>&1; then break; fi
  if ((i==90));then echo 'ClickHouse readiness failed' >&2; exit 1;fi
  sleep 2
done
go test -count=1 -timeout 4m -v ./internal/ingest \
  -run '^TestRealRebalanceRejectsStaleCommitsAndReplays$'
python3 - <<'PY'
import json, os
path = os.environ["M3B_REPORT_PATH"]
r=json.load(open(path))
assert r["initial_insertions"] == 2
assert r["revoked_inflight_handlers"] == 2
assert r["post_rebalance_commits"] == 2
assert r["worker_b_commits"] >= 1
assert len(r["events"]) == 2
for e in r["events"]:
    assert e["raw_copies"] == 2
    assert e["logical_events"] == 1
    assert e["integrity_conflicts"] == 0
print("PASS M3b: real reassignment, failed stale commits, correct replay and analytics")
PY
