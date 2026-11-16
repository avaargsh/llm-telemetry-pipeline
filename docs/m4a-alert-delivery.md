# M4a — real Prometheus → Alertmanager → Webhook delivery

**Status:** in review — [PR #22](https://github.com/avaargsh/llm-telemetry-pipeline/pull/22). This slice addresses [Issue #20](https://github.com/avaargsh/llm-telemetry-pipeline/issues/20) without replacing the already merged Kafka lag exporter (PR #17).

## Scope

This test exercises **real Prometheus and Alertmanager processes**, the unmodified `deployments/prometheus/alert-rules.yml` rules, Docker networking, and a real HTTP webhook delivery endpoint.

The metric source is a **test-only Go HTTP fixture**, not live Kafka. Live group-offset collection and actual ClickHouse throttling remain separately proven by the M3e PR #17 Docker integration. This separation makes the delivery-layer failure envelope explicit instead of claiming a production-grade on-call end-to-end test.

## Reproduce

```bash
make m4a-alert-smoke
# Or:
bash scripts/m4a-alert-delivery-smoke.sh
```

The integration test starts its own source/webhook server at `0.0.0.0:19080`, then starts a disposable Prometheus + Alertmanager Compose project. Prometheus scrapes the fixture at `host.docker.internal:19080` using the Docker Linux host-gateway; Alertmanager POSTs to `host.docker.internal:19080/alerts`. Container admin UIs are bound to host loopback ports 19090 and 19093 and are shut down after verification.

The test asserts **two states per alert**, backed by actual received webhook JSON:

| Alert | Input condition | Required evidence |
|---|---|---|
| KafkaRetentionCritical | Kafka risk state critical = 1 | `firing` followed by `resolved` with severity critical |
| KafkaCommittedBeforeLogStart | simulated data-gap distance = 5 | `firing` followed by `resolved` with severity critical |
| KafkaLagCollectorUnavailable | collector sample success = 0 for rule's existing 30s hold | `firing` (after hold) and `resolved` with severity critical |

The captured JSON contains alert identity, status, severity, receive timestamp and delivery latency. The CI `m4a-alertmanager-webhook-e2e` uploads `m4a-alertmanager-webhook-evidence`. No physical Kafka offsets or account credentials are included.

## Boundaries and deployment risks

- This is **delivery to an internal CI receiver**, not SMS/email/Slack/pager/on-call production routing. Actual escalation, alert dedup under repeated notifications, quiet hours, missed-notification retry, TLS, authentication and HA Alertmanager are not demonstrated.
- Real Prometheus rules are evaluated with their existing timing, including the 30-second collector-unavailability hold. A successful alert rule evaluation in isolation is not itself evidence of successful notification; the webhook is the extra evidence gate here.
- The test Go receiver does not expose a production endpoint. Do not bind it publicly or carry test-only insecure Compose defaults into any deployment.
- Source health and retention warnings are still **estimates**; this integration proves the delivery boundary rather than a physical Kafka retention deletion deadline.
- PostgreSQL `usage_ledger` remains authoritative for billing; neither Prometheus nor Alertmanager participates in financial reconciliation.

## Next improvements

Use a trusted on-call receiver and real authenticated clusters; measure missed/retried webhook notifications with receiver outages; build a durable alert audit and SLO dashboard. These remain future work and must not be called completed by this CI scenario.
