#!/usr/bin/env bash
set -euo pipefail
COMPOSE='deployments/docker-compose.yml'
if [[ ! -f "$COMPOSE" ]]; then
  echo 'Run from repository root' >&2
  exit 1
fi

# Critical usage events use actual broker append timestamps so that the
# retention-age monitor does not rely on untrusted usage payload timestamps.
# --if-not-exists never changes pre-existing topic settings; operators must
# verify/migrate older topics explicitly rather than silently reconfigure them.
docker compose -f "$COMPOSE" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --create --if-not-exists \
  --topic usage.critical.v1 --partitions 6 --replication-factor 1 \
  --config message.timestamp.type=LogAppendTime --config retention.ms=604800000

for topic in inference.logs.v1 events.dlq.v1; do
  docker compose -f "$COMPOSE" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 --create --if-not-exists --topic "$topic" \
    --partitions 6 --replication-factor 1
done

docker compose -f "$COMPOSE" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --list
