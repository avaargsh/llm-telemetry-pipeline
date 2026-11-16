#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."
# Compose startup+cleanup is owned by the Go integration test to ensure
# the webhook receiver is listening before Prometheus begins scraping.
export TEST_M4A_ALERT_DELIVERY=1
export M4A_REPORT_PATH="${M4A_REPORT_PATH:-/tmp/m4a-alert-delivery.json}"
go test -count=1 -timeout 9m -v ./internal/alertdelivery \
 -run '^TestRealPrometheusAlertmanagerWebhookFiringAndResolved$'
python3 - <<'PY'
import json,os
r=json.load(open(os.environ["M4A_REPORT_PATH"]))
assert r["firing_and_resolved_verified"] is True,r
for name in r["required_alerts"]:
  for status in ("firing","resolved"):
    assert any(e["alertname"]==name and e["status"]==status and e["severity"]=="critical"
               for e in r["notifications"]),(name,status,r)
assert len(r["required_alerts"])==3,r
assert r["latency_seconds"]["KafkaLagCollectorUnavailable_firing_seconds"]>=25,r
print("PASS M4a: real Prometheus/Alertmanager delivered firing and resolved webhooks for critical retention, data gap and collector outage")
PY
