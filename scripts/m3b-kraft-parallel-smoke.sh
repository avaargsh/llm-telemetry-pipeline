#!/usr/bin/env bash
# Isolated parallel-voter KRaft restart fault lab. Not production HA.
set -Eeuo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f deployments/docker-compose-ha.yml)
diagnostics="${M3B_KRAFT_DIAGNOSTICS_DIR:-/tmp/m3b-parallel-kraft-diagnostics}"
export M3B_EXACT_HEAD="$(git rev-parse HEAD)"
export TEST_KAFKA_HA_CONCURRENT=1
export M3B_PARALLEL_REPORT_PATH="${M3B_PARALLEL_REPORT_PATH:-/tmp/m3b-parallel-restart-report.json}"
export M3B_CONTROLLER_SOCKET_RAW_PATH="$diagnostics/controller-peers.jsonl"
export M3B_CONTROLLER_SOCKET_SUMMARY_PATH="$diagnostics/controller-peers-summary.json"
socket_sampler_pid=""
stop_socket_sampler() {
  if [[ -n "$socket_sampler_pid" ]]; then
    kill "$socket_sampler_pid" >/dev/null 2>&1 || true
    wait "$socket_sampler_pid" >/dev/null 2>&1 || true
    socket_sampler_pid=""
  fi
}
verify_socket_evidence() {
  python3 scripts/m3b_controller_sockets.py verify \
    --input "$M3B_CONTROLLER_SOCKET_RAW_PATH" \
    --head "$M3B_EXACT_HEAD" \
    --out "$M3B_CONTROLLER_SOCKET_SUMMARY_PATH"
}
mkdir -p "$diagnostics"
rm -f "$M3B_CONTROLLER_SOCKET_RAW_PATH" "$M3B_CONTROLLER_SOCKET_SUMMARY_PATH"
rm -f "$M3B_PARALLEL_REPORT_PATH" "$diagnostics/before_fault.json" \
  "$diagnostics/after_failure.json" "$diagnostics/after_success.json" \
  "$diagnostics/voter-identity-audit.json"

capture() {
  python3 scripts/m3b_kraft_diagnostics.py --phase "$1" --out-dir "$diagnostics" || true
}
audit() {
  python3 scripts/m3b_kraft_audit.py \
    --before "$diagnostics/before_fault.json" \
    --after "$diagnostics/$1.json" \
    --out "$diagnostics/voter-identity-audit.json" --docker-logs || true
}
cleanup() {
  local rc=$?
  trap - EXIT
  stop_socket_sampler
  if (( rc != 0 )); then
    # Preserve an INCOMPLETE socket summary, not an invented JVM/DNS PASS.
    verify_socket_evidence >&2 || true
    echo "Parallel KRaft voter recovery FAILED; preserving read-only evidence" >&2
    capture after_failure >&2
    audit after_failure >&2
    "${compose[@]}" ps >&2 || true
    "${compose[@]}" logs --no-color --tail 75 broker1 broker2 broker3 >&2 || true
  fi
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  exit "$rc"
}
trap cleanup EXIT
"${compose[@]}" up -d
ready=0
for i in $(seq 1 120); do
  if "${compose[@]}" exec -T broker1 /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server broker1:19092 --list >/dev/null 2>&1; then
    ready=1; break
  fi
  sleep 2
done
if (( ready != 1 )); then echo "parallel KRaft bootstrap timed out" >&2; exit 1; fi
created=0
for i in $(seq 1 90); do
  if "${compose[@]}" exec -T broker1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server broker1:19092 --create --if-not-exists \
    --topic m3b.ha.critical.v1 --partitions 1 --replication-factor 3 \
    --config min.insync.replicas=2 >/dev/null 2>&1; then
    created=1; break
  fi
  sleep 2
done
if (( created != 1 )); then echo "RF3 topic creation timed out" >&2; exit 1; fi
capture before_fault
python3 - <<'PY'
import json,os
p=os.path.join(os.environ.get("M3B_KRAFT_DIAGNOSTICS_DIR","/tmp/m3b-parallel-kraft-diagnostics"),"before_fault.json")
s=json.load(open(p))
assert s["identity_verdict"]=="PASS_IDENTITY", "incomplete pre-fault voter identity / DNS baseline"
PY

# Keep sampling actual kernel TCP remote peers during quorum loss/restart.
# This is namespace-level evidence, NOT proof of ownership by the Kafka JVM.
python3 scripts/m3b_controller_sockets.py sample \
  --head "$M3B_EXACT_HEAD" \
  --out "$M3B_CONTROLLER_SOCKET_RAW_PATH" \
  --interval 2 --duration 540 >/dev/null 2>&1 &
socket_sampler_pid=$!

# No intermediate ISR2 admission before the two restarted voters!
go test -count=1 -timeout 9m -v ./internal/kafkaha \
  -run '^TestThreeBrokerParallelVoterRestart "$M3B_PARALLEL_REPORT_PATH" --head "$M3B_EXACT_HEAD"
capture after_success
audit after_success
python3 - <<'PY'
import json,os
p=os.path.join(os.environ.get("M3B_KRAFT_DIAGNOSTICS_DIR","/tmp/m3b-parallel-kraft-diagnostics"),"voter-identity-audit.json")
r=json.load(open(p))
assert r["exact_head"]==os.environ["M3B_EXACT_HEAD"],"audit bound to wrong code HEAD"
assert r["baseline_phase"]=="before_fault" and r["comparison_phase"]=="after_success"
assert r["complete_observations"] is True, "missing identity, Docker DNS or voter log observations"
assert r["verdict"]=="NO_MISMATCH_OBSERVED",r
assert not r["ip_changed_nodes"] and not r["identity_changes"] and not r["dns_misroutes"],r
assert not r["observed_voter_target_mismatches"],r
print("PASS parallel voter recovery: stable IDs and IP, no sampled cross-target VOTE")
PY

stop_socket_sampler
verify_socket_evidence
python3 scripts/m3b_parallel_proof.py "$M3B_PARALLEL_REPORT_PATH" --head "$M3B_EXACT_HEAD"
capture after_success
audit after_success
python3 - <<'PY'
import json,os
p=os.path.join(os.environ.get("M3B_KRAFT_DIAGNOSTICS_DIR","/tmp/m3b-parallel-kraft-diagnostics"),"voter-identity-audit.json")
r=json.load(open(p))
assert r["exact_head"]==os.environ["M3B_EXACT_HEAD"],"audit bound to wrong code HEAD"
assert r["baseline_phase"]=="before_fault" and r["comparison_phase"]=="after_success"
assert r["complete_observations"] is True, "missing identity, Docker DNS or voter log observations"
assert r["verdict"]=="NO_MISMATCH_OBSERVED",r
assert not r["ip_changed_nodes"] and not r["identity_changes"] and not r["dns_misroutes"],r
assert not r["observed_voter_target_mismatches"],r
print("PASS parallel voter recovery: stable IDs and IP, no sampled cross-target VOTE")
PY
