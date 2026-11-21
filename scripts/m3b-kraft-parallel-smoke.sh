#!/usr/bin/env bash
# Isolated parallel-voter KRaft restart fault lab. Not production HA.
set -Eeuo pipefail
cd "$(dirname "$0")/.."
profile="${M3B_NETWORK_AB_MODE:-pinned}"
compose_file="${M3B_HA_COMPOSE_FILE:-deployments/docker-compose-ha.yml}"
case "$profile:$compose_file" in
  pinned:deployments/docker-compose-ha.yml|dynamic:deployments/docker-compose-ha-dynamic.yml) ;;
  *) echo "invalid A/B control: $profile:$compose_file" >&2; exit 2 ;;
esac
export M3B_HA_COMPOSE_FILE="$compose_file"
compose=(docker compose -f "$compose_file")
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
    # Do not turn a faulty recovery run into a good evidence sample.
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

# Observe actual kernel TCP peers while the original real fault test runs.
# Observed namespace sockets do not establish Java DNS cache ownership.
python3 scripts/m3b_controller_sockets.py sample \
  --compose "$compose_file" \
  --head "$M3B_EXACT_HEAD" \
  --out "$M3B_CONTROLLER_SOCKET_RAW_PATH" \
  --interval 2 --duration 540 >/dev/null 2>&1 &
socket_sampler_pid=$!

# No intermediate ISR2 admission before the two restarted voters!
go test -count=1 -timeout 9m -v ./internal/kafkaha \
  -run '^TestThreeBrokerParallelVoterRestart$'
stop_socket_sampler
verify_socket_evidence
python3 scripts/m3b_parallel_proof.py "$M3B_PARALLEL_REPORT_PATH" --head "$M3B_EXACT_HEAD"
capture after_success
audit after_success
python3 scripts/m3b_network_ab_evidence.py \
  --profile "$profile" --head "$M3B_EXACT_HEAD" \
  --report "$M3B_PARALLEL_REPORT_PATH" \
  --audit "$diagnostics/voter-identity-audit.json" \
  --sockets "$M3B_CONTROLLER_SOCKET_SUMMARY_PATH" \
  --before "$diagnostics/before_fault.json" \
  --after "$diagnostics/after_success.json" \
  --out "$diagnostics/network-ab-comparison.json"
