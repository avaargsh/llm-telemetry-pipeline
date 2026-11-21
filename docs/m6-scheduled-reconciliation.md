# M6 — Bounded, scheduled PostgreSQL ↔ ClickHouse reconciliation

**Scope:** read-only, auditable scheduled execution of the existing `cmd/reconciler`. The PostgreSQL `usage_ledger` remains the *only billing authority*. The scheduler does **not** write any business state, publish DLQ events, retry unsafe side effects, or send raw event IDs to external alerts.

## Why

The existing reconciler requires `--start` and `--end` manually. A CronJob must choose **one finite UTC event-time window**, wait for a configured observation lag, preserve the three business statuses, and output a safe summary. A Kubernetes Job's success/failure can then be monitored independently by the operator.

At UTC `12:15`, hourly `--window-minutes 60 --lag-minutes 15` selects `[11:00, 12:00)`. Next hourly execution covers `[12:00, 13:00)`. The window is inclusive start, exclusive end, and aligned to a whole UTC minute. The lag is intentionally separate from the reconciler's `--grace` field, which classifies very recent `published_at` timestamps.

- `PASS` → exit **0**, only if reconciliation is complete and has no confirmed conflicts.
- `FAIL` → exit **2**, confirmed missing/orphan/hash-conflict/token-mismatch/missing-Outbox events.
- `INCOMPLETE` → exit **3**, pending Outbox or recently published event still in grace.
- `ERROR` → exit **1**, invalid input, upstream execution error, missing credentials, timeout, malformed result, unbounded scan, or result/exit-code inconsistency.

**Critical:** neither `INCOMPLETE` nor `ERROR` can be reported as PASS. Job-failure alerting must distinguish 1/2/3 in log/receipt; Kubernetes alone records only Job failed/succeeded, not those detailed categories.

## Runbook

Build the trusted read-only reconciler binary:

```bash
go build -o bin/reconciler ./cmd/reconciler
export POSTGRES_DSN='postgres://...'
export CLICKHOUSE_HTTP='https://...'
export CLICKHOUSE_USER='read_only_analytics'
export CLICKHOUSE_PASSWORD='...'
python3 scripts/m6_scheduled_reconcile.py \
  --window-minutes 60 --lag-minutes 15 --max-records 1000 \
  --reconciler-bin ./bin/reconciler \
  --output /tmp/m6-reconciliation.json
```

The scheduler writes an **atomically replaced**, mode-0600 JSON receipt. It contains bounded counts, UTC start/end, status, code, Git SHA/run ID (when CI-supplied), but **no per-event findings, event IDs, payloads, DSNs or credentials**. Logs print only this same redacted JSON. Detailed findings require separately running the underlying reconciler with appropriate access and secure storage.

A controlled backfill may supply `--as-of '2026-11-16T11:16:00Z'`, which replaces the scheduler clock with an explicit timezone-aware timestamp. **Do not use this option in a production CronJob.** The job stops on a 95s child timeout and fails closed.

## Kubernetes deployment

```bash
docker build -f deployments/Dockerfile.m6-scheduled -t REPLACE_WITH_LOCAL_TAG .
# Push to an authorized registry; record its digest and inspect the image.
# Replace image in deployments/k8s/m6-reconcile-cronjob.example.yaml
# Provision telemetry-reconcile-secrets in the target namespace through
# your secret manager, NOT a committed YAML with actual credentials.
kubectl apply -f deployments/k8s/m6-reconcile-cronjob.example.yaml
```

The example runs at 15 minutes past every UTC hour, forbids overlapping jobs, disables automatic retries, uses a non-root filesystem-restricted container, requires a Secret containing `POSTGRES_DSN`, `CLICKHOUSE_HTTP`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, and stores the temporary receipt only in an ephemeral volume. **It is a deployment template, not evidence of a deployed cluster or alert integration.** The image placeholder is deliberately not deployable.

Attach Job-failure monitoring to an independently operated alerting system before relying on it. Standard scheduling can miss runs during cluster outages or deadlines; periodically check for missing schedule slots. Results are not a distributed consistent snapshot; stable event-time cutoffs and follow-up rechecks are required before remediation.

```bash
kubectl get cronjob llm-telemetry-reconciler
kubectl get jobs -l app.kubernetes.io/name=llm-telemetry-reconciler
kubectl logs job/<job-name>
```

## Reproducible acceptance

```bash
python3 -m unittest discover -s scripts -p 'test_m6_scheduled_reconcile.py' -v
bash scripts/m6-reconcile-smoke.sh
```

The real Docker test populates eight verified reconciliation statuses in PostgreSQL+ClickHouse and asserts **FAIL/exit 2** for the fixture's bounded UTC minute; a following empty minute must yield **PASS/exit 0**. CI uploads only sanitized JSON receipts from the exact checked commit. No auto-repair, Kafka ACK or billing functionality is added.

## Limitations

- A finite `max-records` is **not** a full ClickHouse scan-cost cap. Use narrow windows and query budgets. A capped scan must fail, not pass with partial data.
- This solution does not implement secure DLQ redrive, full PostgreSQL HA, ClickHouse replica consistency, data retention governance, or notification delivery.
- Production image and PostgreSQL/CH read-only roles are the deployer's responsibility. Container credentials must be managed externally. Docker registry publication and Kubernetes rollout are not part of this PR.
- A synthetic test fixture is not proof of production scheduled reliability or alert SLOs.
