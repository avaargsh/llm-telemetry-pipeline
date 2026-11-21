#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f deployments/docker-compose-ha.yml)
diagnostics="${M3B_KRAFT_DIAGNOSTICS_DIR:-/tmp/m3b-kraft-diagnostics}"
capture_raft() {
  local phase="$1"
  # Best effort: observer failure must not hide the Kafka fault verdict.
  python3 scripts/m3b_kraft_diagnostics.py --phase "$phase" \
    --out-dir "$diagnostics" || true
}
audit_raft() {
  local phase="$1"
  # Failures are evidence, never a reason to mutate broker state. Only a
  # sanitized aggregate (no raw broker logs or directory IDs) is archived.
  python3 scripts/m3b_kraft_audit.py \
    --before "$diagnostics/before_fault.json" \
    --after "$diagnostics/$phase.json" \
    --out "$diagnostics/voter-identity-audit.json" --docker-logs || true
}
cleanup() {
  local rc=$?
  trap - EXIT
  if (( rc != 0 ));then
    echo "HA integration failed; preserving voter identity, quorum and DNS snapshot:" >&2
    capture_raft after_failure >&2
    audit_raft after_failure >&2
    "${compose[@]}" ps >&2 || true
    "${compose[@]}" logs --tail=100 broker1 broker2 broker3 >&2 || true
  fi
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  exit "$rc"
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

# Capture identity/DNS/quorum baseline while all three brokers are healthy.
# Tests retain the original full quorum-loss + RF3/ISR3 recovery assertions.
capture_raft before_fault

export TEST_KAFKA_HA=1
export M3B_HA_REPORT_PATH="${M3B_HA_REPORT_PATH:-/tmp/m3b-kafka-ha-report.json}"
export M3B_RECOVERY_TIMELINE_PATH="${M3B_RECOVERY_TIMELINE_PATH:-/tmp/m3b-kraft-recovery-timeline.json}"
export M3B_EXACT_HEAD="$(git rev-parse HEAD)"
# Never accept leftover JSON from an earlier experiment on the same runner.
rm -f "$M3B_HA_REPORT_PATH" "$M3B_RECOVERY_TIMELINE_PATH" "$diagnostics/voter-identity-audit.json"
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
assert r["stale_bootstrap_probe_error"],r
assert r["rebootstrap_same_event_id_ack"] is True,r
assert r["rebootstrap_live_isr_size"]==2,r
assert r["quorum_isr_timeline_verified"] is True,r
assert r["quorum_loss_tested"] is True,r
assert r["acknowledged_ids_verified_after_quorum_restore"] is True,r
assert r["quorum_loss_producer_error"],r
assert "replica" in r["rejected_error"].lower(),r
print("PASS M3b-P1: RF3, leader failover, ISR refusal, quorum-loss no-ACK, 35 ACKed ID readbacks")
PY
python3 - <<'PY'
import json,os
p=json.load(open(os.environ["M3B_RECOVERY_TIMELINE_PATH"]))
assert p["head_sha"]==os.environ["M3B_EXACT_HEAD"],"timeline must bind to exact tested HEAD"
assert p["result"]=="PASS_EXPERIMENT_ONLY",p
assert len(p["recovery_milestones"])==8,p
durations=p["observed_durations_ms"]
assert set(durations)=={
  "first_restart_to_quorum_status_observed_ms",
  "first_restart_to_isr2_observed_ms",
  "second_restart_to_isr3_observed_ms",
  "two_voter_loss_to_ack_readback_ms",
},durations
assert all(isinstance(x,int) and x>=0 for x in durations.values()),durations
assert p["quorum_status_observed_not_actual_quorum_establishment_instant"] is True
print("PASS M3b recovery: independent quorum status, ISR2, ISR3 and replay evidence",durations)
PY

capture_raft after_success
audit_raft after_success
python3 - <<'PY'
import json,os
p=os.path.join(os.environ.get("M3B_KRAFT_DIAGNOSTICS_DIR","/tmp/m3b-kraft-diagnostics"),"voter-identity-audit.json")
a=json.load(open(p))
assert a["exact_head"]==os.environ["M3B_EXACT_HEAD"],"voter audit must bind to exact code HEAD"
assert a["baseline_phase"]=="before_fault" and a["comparison_phase"]=="after_success",a
assert a["complete_observations"] is True,a
assert a["verdict"]=="NO_MISMATCH_OBSERVED",a
assert a["observed_voter_target_mismatches"]==[],a
assert a["identity_changes"]==[] and a["dns_misroutes"]==[],a
assert a["ip_changed_nodes"]==[],"KRaft controller endpoints moved during stop/start"
print("PASS M3b voter audit: retained node IDs/directory IDs and consistent Docker DNS; no sampled cross-target VOTE mismatch")
PY
