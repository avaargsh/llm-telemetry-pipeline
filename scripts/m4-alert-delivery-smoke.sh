#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -p telemetry-m4 -f deployments/docker-compose.yml -f deployments/docker-compose-observability.yml)
export M4_ALERT_REPORT_PATH="${M4_ALERT_REPORT_PATH:-/tmp/m4-alert-delivery-report.json}"
diagnose(){
 local rc=$?
 if ((rc!=0));then
   echo "M4 live alert delivery failure; service diagnostics" >&2
   "${compose[@]}" ps >&2 || true
   "${compose[@]}" logs --tail=100 kafka lag-exporter prometheus alertmanager alert-webhook >&2 || true
 fi
 "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap diagnose EXIT

"${compose[@]}" up -d kafka
for i in $(seq 1 90);do
 if "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
   --bootstrap-server kafka:19092 --list >/dev/null 2>&1;then break;fi
 if ((i==90));then echo 'Kafka bootstrap timeout' >&2;exit 1;fi
 sleep 2
done
"${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:19092 --create --if-not-exists \
  --topic m4.alert.e2e.v1 --partitions 1 --replication-factor 1 \
  --config retention.ms=120000 --config message.timestamp.type=LogAppendTime
go run ./scripts/fixtures/m4-alert-fixture.go --mode seed

"${compose[@]}" up -d alert-webhook alertmanager lag-exporter prometheus
for i in $(seq 1 60);do
 if curl -fsS http://127.0.0.1:19090/-/ready >/dev/null 2>&1 &&
    curl -fsS http://127.0.0.1:19093/-/ready >/dev/null 2>&1 &&
    curl -fsS http://127.0.0.1:19099/healthz >/dev/null 2>&1;then break;fi
 if ((i==60));then echo 'Prometheus/Alertmanager receiver readiness failure' >&2;exit 1;fi
 sleep 2
done

# The real alert rule has 'for: 30s'. With 120s effective retention and a
# 50s critical margin, pending data enters Warning and later Critical.
python3 scripts/m4-wait-alert.py --alert KafkaRetentionWarning --status firing --timeout 110
python3 scripts/m4-wait-alert.py --alert KafkaRetentionCritical --status firing --timeout 100
go run ./scripts/fixtures/m4-alert-fixture.go --mode catchup
python3 scripts/m4-wait-alert.py --alert KafkaRetentionCritical --status resolved --timeout 100
python3 scripts/m4-wait-alert.py --alert KafkaRetentionWarning --status resolved --timeout 20

# Exercise real broker unavailability and verify the exporter reports a bad
# sample; alert must reach Alertmanager and resolve on broker restoration.
"${compose[@]}" stop kafka
python3 scripts/m4-wait-alert.py --alert KafkaLagCollectorUnavailable --status firing --timeout 110
"${compose[@]}" start kafka
python3 scripts/m4-wait-alert.py --alert KafkaLagCollectorUnavailable --status resolved --timeout 110

curl -fsS http://127.0.0.1:19099/events > "$M4_ALERT_REPORT_PATH"
python3 scripts/m4-wait-alert.py --validate-report "$M4_ALERT_REPORT_PATH"
echo "PASS M4 live alert delivery: Kafka retention Warning/Critical, broker failure and all resolved via Prometheus→Alertmanager→webhook"
