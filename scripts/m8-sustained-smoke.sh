#!/usr/bin/env bash
# Three independent, duration-controlled open-loop 25 RPS runs.
# The isolated project restarts between runs; this is not a production SLO.
set -Eeuo pipefail
cd "$(dirname "$0")/.."
output_dir="${M8_OUTPUT_DIR:-artifacts/m8-sustained}"
if [[ "$output_dir" != /* ]]; then output_dir="$PWD/$output_dir";fi
mkdir -p "$output_dir"

count="${M8_COUNT:-1125}"
rate="${M8_TARGET_RPS:-25}"
duration="${M8_DURATION_SECONDS:-45}"
concurrency="${M8_CONCURRENCY:-8}"
partitions="${M8_PARTITIONS:-6}"
batch="${M8_BATCH_SIZE:-64}"
relay_batch_timeout="${M8_RELAY_BATCH_TIMEOUT:-5ms}"

for round in 1 2 3;do
  echo "M8 sustained run $round/3 (count=$count target_RPS=$rate seconds=$duration)"
  M5_COUNT="$count" \
  M5_TARGET_RPS="$rate" \
  M5_DURATION_SECONDS="$duration" \
  M5_CONCURRENCY="$concurrency" \
  M5_PARTITIONS="$partitions" \
  M5_BATCH_SIZE="$batch" \
  M5_RELAY_BATCH_TIMEOUT="$relay_batch_timeout" \
  M5_VISIBILITY_TIMEOUT=120 \
  M5_OUTPUT="$output_dir/run-$round.json" \
  M5_DOCKER_STATS_OUTPUT="$output_dir/docker-stats-$round.csv" \
    bash scripts/m5-performance-smoke.sh
done

limits=()
append_limit(){
  local variable="$1" option="$2" value
  value="${!variable:-}"
  if [[ -n "$value" ]];then limits+=("$option" "$value");fi
}
append_limit M8_MAX_ACK_P95_MS --max-ack-p95-ms
append_limit M8_MAX_ACK_P99_MS --max-ack-p99-ms
append_limit M8_MAX_VISIBLE_P95_MS --max-visible-p95-ms
append_limit M8_MAX_VISIBLE_P99_MS --max-visible-p99-ms
append_limit M8_MAX_SCHEDULE_LAG_P99_MS --max-schedule-lag-p99-ms

python3 scripts/m8_slo_eval.py \
  "$output_dir/run-1.json" "$output_dir/run-2.json" "$output_dir/run-3.json" \
  "${limits[@]}" --output "$output_dir/summary.json"
echo 'PASS M8: three >=1000-event steady-load trials, all ACKed and visible; P95/P99 reported with honesty'
