# M8 — sustained open-loop P95/P99 benchmark and scenario SLO evaluation

The authoritative billing source is **PostgreSQL `usage_ledger`**. These measurements
test HTTP acceptance of a signed finalized event and its first observed presence in
ClickHouse's *trusted analytical projection*. Kafka and ClickHouse may contain
physical replays, and this benchmark never charges based on their raw rows.

## Why the former 128-event burst was insufficient

Burst benchmarks measure a short client saturation episode, not a defined sustained
arrival rate. An event submitted late because the client itself is overloaded can
create **coordinated omission**. Small samples also make P99 very sensitive to a
single point. M8 adds open-loop pacing, schedule-lag measurements, at least 1,000
independent HTTP attempts per trial, and a strict cross-trial profile validator.

## One-command real test

```bash
python3 -m unittest discover -s scripts -p 'test_m8_sustained.py' -v
make m8-sustained-smoke
# or bash scripts/m8-sustained-smoke.sh
```

Default CI matrix is **three independent runs**, each 45 seconds at 25
target requests/s, with 1,125 events, 8 client workers, 6 Kafka partitions,
Consumer batch 64 and experimental Relay Kafka `BatchTimeout=5ms`.
Each run uses a fresh disposable PostgreSQL/Kafka/ClickHouse Compose project,
a new event seed and scoped temporary HTTP credentials. This is a steady
**offered-load profile**, not proof of the maximum sustainable throughput.

Each run produces:
- `run-N.json`: event counts, observed acceptance throughput, client
  schedule-lag P95/P99, HTTP PostgreSQL-ACK P50/P95/P99, first-observed
  ClickHouse visibility P95/P99, drain, partial errors, process CPU/RSS and
  exact Git SHA.
- `docker-stats-N.csv`: sampled PostgreSQL, Kafka and ClickHouse container
  CPU percentages and memory-usage strings. These snapshots are **descriptive**,
  not normalized cross-host capacity limits.
- `summary.json`: validated same-commit/configuration evidence for **all
  three trials**, per-run latency metrics, worst-run P95/P99 and median-run
  P95/P99.

CI uploads these raw receipts as `m8-sustained-p95-p99-evidence`.

## P95/P99 definition and SLO decision

Per-run percentiles use the **nearest-rank method**:
`ceil(N × percentile)`, applied to **successful 201 HTTP ACKs** and their
**first-observed trusted ClickHouse visibility** latencies. ClickHouse
visibility includes bounded 0.5-second polling error; it is not an exact
internal processing timestamp. The report does not pool per-run percentiles
as if they were raw latency samples.

The evaluator requires:
1. All three trials use identical profile and exact Git HEAD, each with
   >=1,000 attempts over >=30 seconds.
2. Every event receives HTTP 201 and becomes visible in the trusted projection;
   no observer-query errors and no missing resource samples.
3. The measured send interval matches the configured interval within 10%,
   the HTTP accepted throughput reaches at least 90% of requested RPS, and
   P99 client scheduling lag is below an explicit hard overload guard.
4. An optional SLO can only **PASS** when all five ceilings are configured
   (HTTP P95/P99, trusted CH visibility P95/P99, schedule-lag P99) and
   **every individual trial** meets each ceiling. Any failed trial fails
   the candidate SLO; a bad/incomplete load is `NOT_EVALUABLE`, never PASS.

No ceilings supplied -> `slo=NOT_SET`, even when correctness passes. This is
the CI default: CI demonstrates functioning instrumentation and a complete
benchmarked service path, **not** a product SLO promise.

Example **illustrative candidate** threshold test (not proven achieved):

```bash
M8_MAX_ACK_P95_MS=500 \\
M8_MAX_ACK_P99_MS=750 \\
M8_MAX_VISIBLE_P95_MS=5000 \\
M8_MAX_VISIBLE_P99_MS=8000 \\
M8_MAX_SCHEDULE_LAG_P99_MS=100 \\
make m8-sustained-smoke
```

The evaluator exits 2 when a complete measured candidate misses any ceiling,
1 if evidence is incomplete/invalid, and 0 for complete `NOT_SET` or
measured `PASS`. Treat `NOT_SET` as **not rated**, never evidence that an
SLO exists. The experiment includes no customer workload and no fault-injection
availability denominator.

## Not a production SLO

The test uses a **single PostgreSQL, single Kafka broker (RF=1), single
ClickHouse and one Consumer/Relay on shared CI hardware**. RPS and tail latency
are workload-, disk-, hardware-, API-credential-count- and configuration-specific.
Kafka RF3/minISR2 and PostgreSQL manual HA have **separate** fault experiments;
their correctness does not imply these sustained latency targets in HA mode.
No hosted/published SLO, multi-AZ RPO/RTO, replica lag, on-call alert or
capacity curve is established.

Before publishing a service SLO, add a controlled hardware environment,
durable Kafka/PG HA profiles, at least three runs per load level, percentile
confidence/uncertainty, error-budget accounting across calendar windows, real
failure injection and verified end-to-end workload tracing.


## Verified first CI profile and candidate regression gate

[Exact-head real M8 baseline run 44865577833](https://github.com/avaargsh/llm-telemetry-pipeline/actions/runs/44865577833)
passed 19/19 full PR workflows, including the three sustained-load trials and
raw ZIP artifact `m8-sustained-p95-p99-evidence`.

| Metric | Trial 1 | Trial 2 | Trial 3 |
| --- | ---: | ---: | ---: |
| HTTP ACKed / trusted visible | 1,125 / 1,125 | 1,125 / 1,125 | 1,125 / 1,125 |
| Accepted events/s | 25.02 | 25.02 | 25.02 |
| HTTP ACK P95 ms | 2.26 | 2.17 | 2.21 |
| HTTP ACK P99 ms | 3.13 | 3.37 | 3.16 |
| CH first-observed P95 ms | 2349.76 | 3382.20 | 3365.57 |
| Drain s | 0.698 | 0.709 | 0.700 |

The separate aggregated report records **worst CH first-observed P99 =
5182.21 ms** and worst client pacing lag P99 = **0.52 ms**.
The original run's `slo=NOT_SET`: passing correctness did not silently
create an SLO.

Based on those observations, the dedicated M8 CI job now defines an **isolated
scenario regression gate** (not a production SLA) at fixed **25 target RPS,
45 seconds/trial, 1,125 events/trial, 3 trials**, and only on the indicated
single-broker/single-PostgreSQL/single-ClickHouse GitHub Runner:

| Per-trial worst-run ceiling | Candidate CI gate |
| --- | ---: |
| HTTP ACK P95 | 500 ms |
| HTTP ACK P99 | 750 ms |
| Trusted CH first-observed visibility P95 | 5000 ms |
| Trusted CH first-observed visibility P99 | 8000 ms |
| Client open-loop scheduling lag P99 | 100 ms |

A pass **requires all three** complete trials to meet all five metrics, plus
the existing data-integrity and offered-load validity checks. A single
missed ceiling is a failed performance CI job. This is a deliberately loose
first regression budget, not an externally promised customer SLO, statistical
confidence interval, or a bound under database/Kafka failure.

To avoid running 3×45-second disposable full stacks for pure documentation
changes, the workflow triggers on relevant ingestion, Relay, ledger/Outbox,
Kafka/ClickHouse Writer, benchmark, Docker, and Go dependency paths, plus
manual workflow dispatch. The integration tests are still available for
an intentional repeat/profile check. Cross-hardware results are not comparable
without normalization.
