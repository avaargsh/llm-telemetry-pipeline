#!/usr/bin/env bash
# Disposable real PostgreSQL + Kafka ACK/crash-window integration lab.
# Does NOT establish PostgreSQL HA, Kafka exactly-once, or zero-loss guarantees.
set -Eeuo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME="llmtelemetry-m5b-${GITHUB_RUN_ID:-local}-$$"
compose=(docker compose -f deployments/docker-compose.yml)
cleanup() {
  local code=$?
  trap - EXIT
  if (( code != 0 )); then
    echo "M5b ACK/crash-window test failed (exit $code)" >&2
    "${compose[@]}" ps >&2 || true
    "${compose[@]}" logs --tail 70 postgres kafka >&2 || true
  fi
  "${compose[@]}" down -v >/dev/null 2>&1 || true
  exit "$code"
}
trap cleanup EXIT

"${compose[@]}" up -d postgres kafka
for i in $(seq 1 90); do
  if "${compose[@]}" exec -T postgres psql -U telemetry -d telemetry \
       -tAc 'SELECT count(*) FROM usage_ledger' >/dev/null 2>&1 &&
     "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
       --bootstrap-server kafka:19092 --list >/dev/null 2>&1; then
    break
  fi
  if (( i == 90 )); then
    echo 'PostgreSQL/Kafka readiness timeout' >&2
    exit 1
  fi
  sleep 2
done

topic="m5b.relay.ack.crash.v1"
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1

export TEST_POSTGRES_DSN='postgres://telemetry:localdev-only@localhost:15432/telemetry?sslmode=disable'
export TEST_KAFKA_BROKER='localhost:29092'
export TEST_M5B_KAFKA_TOPIC="$topic"

go test -count=1 -timeout 90s -v ./internal/outbox -run '^TestRelayKafkaACKCrashWindowRealBroker$'
echo 'PASS M5b ACK/crash recovery: two Kafka copies, one PG Ledger/Outbox record, stale lease fenced'
