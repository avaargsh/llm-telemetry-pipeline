#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f deployments/docker-compose.yml)
cleanup(){
  code=$?
  if ((code!=0));then "${compose[@]}" logs --tail=100 kafka || true;fi
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT
"${compose[@]}" up -d kafka
for i in $(seq 1 90);do
 if "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 --list >/dev/null 2>&1;then break;fi
 if ((i==90));then echo "Kafka not ready" >&2;exit 1;fi
 sleep 2
done
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
 --bootstrap-server kafka:19092 --create --if-not-exists \
 --topic m3e.monitor.v1 --partitions 1 --replication-factor 1 \
 --config retention.ms=600000 --config message.timestamp.type=LogAppendTime
export TEST_M3E_KAFKA=1 TEST_KAFKA_BROKER=localhost:29092
go test -count=1 -timeout 2m -v ./internal/lagmonitor \
 -run '^TestRealKafkaBrokerLagAndRetentionMonitoring$'
docker run --rm --entrypoint=/bin/promtool \
 -v "$(pwd)/deployments/prometheus:/rules:ro" -w /rules \
 prom/prometheus:v3.5.0 check rules m3e-alerts.yml
docker run --rm --entrypoint=/bin/promtool \
 -v "$(pwd)/deployments/prometheus:/rules:ro" -w /rules \
 prom/prometheus:v3.5.0 test rules m3e-alerts.test.yml
echo "PASS M3e: real committed Kafka offsets, live Prometheus exporter and warning/critical/data-gap alert evaluation"
