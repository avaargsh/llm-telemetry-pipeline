# M4-P0 — real Kafka → Prometheus → Alertmanager delivery

**Status:** merged via [PR #21](https://github.com/avaargsh/llm-telemetry-pipeline/pull/21) as `574a937` after 13/13 combined CI workflows passed. Disposable local/CI profile, not production on-call wiring.

## Architecture and purpose

```text
Kafka broker (actual group commits + append-time retention)
  ↓
read-only lag-exporter (one Topic + Consumer Group)
  ↓ /metrics over Docker-internal port 9108
Prometheus 3.5 (2s scrape + 2s evaluation, actual alert-rules.yml)
  ↓ Alertmanager API (Docker network)
Alertmanager (1s group_wait; send_resolved)
  ↓ HTTP webhook (test-only)
isolated Python event receiver → /events (localhost:19099)
```

The existing worker remains generation-fenced and only commits after a confirmed ClickHouse/DLQ write. The monitor never joins the consumer group, fetches billing data, or commits any offsets. PostgreSQL `usage_ledger` is the only billing authority.

## Reproduce end-to-end

Requirements: Docker Engine with Compose, Go 1.23+, Python 3 and local port availability (19090, 19093, 19099, 29092).

```bash
make m4-alert-e2e
# equivalent to:
bash scripts/m4-alert-delivery-smoke.sh
```

This script builds a **non-root** Go exporter container, creates an isolated single-partition Kafka test Topic with broker `LogAppendTime` and finite 120s retention, publishes 20 records and commits the first eight in a real Kafka group. It starts the Prometheus/Alertmanager overlay only after the group has a valid committed next-offset. It uses the existing **unmodified production alert rules**, including `for: 30s` for Warning and CollectorUnavailable.

The script checks **actual webhook deliveries** in this order:

1. **KafkaRetentionWarning firing** as the oldest pending broker record reaches the configured warning window.
2. **KafkaRetentionCritical firing** as estimated retention headroom reaches the critical margin. Warning also resolves.
3. Commit the remaining Kafka records, proving **Critical resolved** and no backlog.
4. Stop Kafka; the exporter reports `lag_scrape_success=0`. After rule's `for: 30s`, **KafkaLagCollectorUnavailable firing** is delivered.
5. Restart Kafka; successful sampling resumes and **KafkaLagCollectorUnavailable resolved** is delivered.

The test enforces `firing` before `resolved` per alert and stores an evidence report at `/tmp/m4-alert-delivery-report.json`. CI attaches it as `m4-live-alertmanager-delivery`.

The local overlay publishes Prometheus only on `127.0.0.1:19090`, Alertmanager on `127.0.0.1:19093`, and the test-only receiver on `127.0.0.1:19099`. Kafka Lag Exporter has **no host-exposed port**. The receiver is intentionally ephemeral, unauthenticated and in-memory, so it must never be exposed to untrusted networks.

## Safety and operational boundaries

- This proves an end-to-end local notification path with one group, one Topic and one Kafka partition. Real Alertmanager routing to email, PagerDuty, Opsgenie, Feishu or a protected webhook still requires secret management, receiver authorization, TLS, dedup policies and on-call testing.
- Retention headroom is an **estimate**, not a guaranteed broker deletion deadline. The test uses short retention and append timestamps for timing; do not change production Topic timestamp policy just to enable this test.
- Prometheus `KafkaCommittedBeforeLogStart` and collector rules remain tested via `promtool`. The real end-to-end E2E intentionally covers the Warning/Critical transition and CollectorUnavailable recovery; it does not deliberately delete broker segments.
- Neither a sustained throughput/latency benchmark nor a production SLO has been established. 100,000-event correctness and 24-event catch-up micro tests are separate scopes.
- The Compose is **not** a production HA configuration: one Kafka broker, plaintext local network, Docker single host. The separate RF3 fault matrix remains in M3b-P1.

## Final verified evidence

[Live Kafka alert delivery GitHub Actions run 43912506378](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/43912506378) passed on the final merge-tested head `63f1263` and uploaded the `m4-live-alertmanager-delivery` JSON artifact. It verified three distinct real Kafka-triggered alerts (RetentionWarning, RetentionCritical and CollectorUnavailable), each with actual webhook **firing and resolved** deliveries in that order. The complementary [synthetic-metrics M4a test (PR #22)](https://github.com/avaargsh/llm-telemetry-pipeline/pull/22) separately verified data-gap alert delivery.

All 13 PR-head workflows passed, including Go race/vet/build, M1 1k/100k events, M2a/M2b/M2c and M3a/M3b-P0/M3b-P1/M3c/M3d/M3e. **No sustained performance SLO or external on-call delivery** is claimed.


## Alert evaluation-window regression (2026-11-21)

[CI run #44859116042](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/44859116042) observed a **failed** M4 live-warning delivery: `KafkaRetentionWarning` never reached `firing` during the 110-second wait. This is a regression/CI failure, not evidence that the alerting chain is always reliable.

The disposable Topic uses 120s retention and a 50s critical margin. The current early-warning rule is active only while remaining estimated headroom is **50–100 seconds**, a window of approximately **50 seconds**. With `for: 30s` and a 30-second group evaluation interval, unlucky evaluation phase alignment can skip `firing` even when the status passed through Warning. The Prometheus global 2-second evaluation default did not override the group's explicit 30-second interval.

The fix changes only the lag alert **group evaluation interval to 5s**, retaining the 30-second `for` requirement. `promtool` regression data now explicitly models a finite 50-second Warning state, expects the warning to fire at 55s and resolve at 70s. This is a real timing-boundary regression check, not a shorter grace period. Existing live Kafka→webhook tests remain mandatory; deterministic rule tests alone do not prove live notification delivery.

Production deployment must also account for scrape jitter, retention changes, evaluation stalls and Alertmanager notification delays. The 5-second evaluation interval is not a guarantee of real-time notification or a retention deadline.
