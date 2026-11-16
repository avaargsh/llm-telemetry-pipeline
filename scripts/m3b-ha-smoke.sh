#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f deployments/docker-compose-ha.yml)
cleanup() {
  local rc=$?
  if (( rc != 0 ));then
    echo "HA integration failed; broker state:" >&2
    "${compose[@]}" ps >&2 || true
    "${compose[@]}" logs --tail=100 broker1 broker2 broker3 >&2 || true
  fi
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT
"${compose[@]}" up -d
ready=0
for n in $(seq 1 120);do
  if "${compose[@]}" exec -T broker1 /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server broker1:19092 --list >/dev/null 2>&1; then
    ready=1;break
  fi
  sleep 2
done
if (( ready != 1 ));then echo 'Kafka HA bootstrap timed out' >&2;exit 1;fi

# The Topic's replication factor is 3 and its normal min ISR is 2.
# Retry topic creation during broker registration (before RF=3 is ready).
created=0
for n in $(seq 1 90);do
  if "${compose[@]}" exec -T broker1 /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server broker1:19092 --create --if-not-exists \
      --topic m3b.ha.critical.v1 --partitions 1 --replication-factor 3 \
      --config min.insync.replicas=2 >/dev/null 2>&1;then
    created=1;break
  fi
  sleep 2
done
if (( created != 1 ));then echo 'RF3 topic creation timed out' >&2;exit 1;fi
"${compose[@]}" exec -T broker1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker1:19092 --describe --topic m3b.ha.critical.v1

export TEST_KAFKA_HA=1
export M3B_HA_REPORT_PATH="${M3B_HA_REPORT_PATH:-/tmp/m3b-kafka-ha-report.json}"
go test -count=1 -timeout 9m -v ./internal/kafkaha \
  -run '^TestThreeBrokerLeaderFailoverAndISRFailClosed$'
python3 - <<'PY'
import json,os
r=json.load(open(os.environ["M3B_HA_REPORT_PATH"]))
assert r["replication_factor"]==3 and r["min_insync_replicas"]==2,r
assert r["initial_leader"]!=r["failover_leader"],r
assert r["isr_after_failure"]==2 and r["isr_after_recovery"]==3,r
assert r["insufficient_isr_probe_min_isr"]==3 and r["insufficient_isr_probe_isr"]==2,r
assert r["broker_acknowledged_ids"]==35,r
assert r["readback_records"]>=35 and r["all_acknowledged_verified"] is True,r
assert r["quorum_loss_tested"] is True,r
assert r["acknowledged_ids_verified_after_quorum_restore"] is True,r
assert r["quorum_loss_producer_error"],r
assert "replica" in r["rejected_error"].lower(),r
print("PASS M3b-P1: RF3, leader failover, ISR refusal, quorum-loss no-ACK, 35 ACKed ID readbacks")
PY
