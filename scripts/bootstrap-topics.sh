#!/usr/bin/env bash
set -euo pipefail
COMPOSE='deployments/docker-compose.yml'
if [[ ! -f "$COMPOSE" ]]; then
  echo 'Run from repository root' >&2
  exit 1
fi
for topic in usage.critical.v1 inference.logs.v1 events.dlq.v1; do
  docker compose -f "$COMPOSE" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 --create --if-not-exists --topic "$topic" \
    --partitions 6 --replication-factor 1
done
docker compose -f "$COMPOSE" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --list
