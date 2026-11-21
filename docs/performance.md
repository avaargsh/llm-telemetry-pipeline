# M5 — performance measurement and SLO baseline (work in progress)

**Goal:** quantify the usable path `HTTP → PostgreSQL Ledger/Outbox → Kafka → ClickHouse trusted projection`, without confusing HTTP DB commits with downstream delivery. PostgreSQL remains the only billing authority. This is an **isolated performance harness**, not a proven production SLO.

## One-command benchmark

Requirements: Docker + Compose, Go 1.23+, Python 3.10+, openssl, curl and a Linux host for /proc resource snapshots. Execute only in a disposable developer/CI environment: the script creates a unique Docker Compose project and removes **its own** volumes on exit.

```bash
make m5-performance-smoke
# identical to:
M5_COUNT=128 M5_CONCURRENCY=4 M5_PARTITIONS=6 M5_BATCH_SIZE=32 \
  bash scripts/m5-performance-smoke.sh
```

The script starts real PostgreSQL 17, Kafka 4.1 (single broker, RF=1), ClickHouse 25.8, the Go HTTP ingestion server, Relay and Consumer. It creates three topics with exactly `M5_PARTITIONS` partitions (the whole Compose project is fresh), generates unique sealed events and per-scope random temporary credentials, then submits concurrent authenticated requests. The benchmark polls a **trusted de-duplicated analytical view**, records the first time each event becomes visible and writes `artifacts/m5-performance.json`.

CI uses a tiny 128-event **correctness smoke** so results are reproducible and reviewers get raw evidence. It is neither a throughput maximum nor a production-grade statistical performance claim. To compare loads, use **separate clean runs**, for example:

```bash
M5_COUNT=1000 M5_CONCURRENCY=1 M5_PARTITIONS=1 M5_BATCH_SIZE=16 \
  M5_OUTPUT=artifacts/m5-c1-p1-b16.json make m5-performance-smoke
M5_COUNT=1000 M5_CONCURRENCY=8 M5_PARTITIONS=6 M5_BATCH_SIZE=64 \
  M5_OUTPUT=artifacts/m5-c8-p6-b64.json make m5-performance-smoke
M5_COUNT=1000 M5_CONCURRENCY=32 M5_PARTITIONS=12 M5_BATCH_SIZE=250 \
  M5_OUTPUT=artifacts/m5-c32-p12-b250.json make m5-performance-smoke
```

**Important:** Kafka partition count can affect Consumer parallelism, but a single Consumer process cannot fully exploit multiple partitions; also the present Relay uses a five-row claim limit. Results from the matrix describe this exact implementation and bottlenecks. Keep the host CPU/memory and software versions stable before comparing.

## What the report actually measures

| Field | Precise definition | Limitation |
| --- | --- | --- |
| `acceptance_throughput_events_s` | HTTP 201 responses / (last HTTP completion − first request start) | Client-paced; this is not Kafka or billed-token throughput |
| `ack_p50_ms`, `ack_p95_ms`, `ack_p99_ms` | Nearest-rank HTTP request start to 201 completion latency | Includes network, validation, PG transaction/commit |
| `visible_p95_ms`, `visible_p99_ms` | HTTP request start to **first observation** of event in CH trusted projection | Upper-bound observation, sampled at 0.5 s by default; includes polling jitter |
| `drain_seconds` | Maximum CH first-visibility timestamp minus final HTTP ACK, floored at zero | **Not** a Kafka outage recovery time or a percentile |
| `resources` | Maximum /proc-observed RSS MiB and peak sampled CPU% for API/Relay/Consumer | Linux process snapshots, not Kafka/PG/CH container CPU/memory or long-term capacity |
| `correctness` | `PASS_CORRECTNESS` only when all submitted events return 201 and all appear exactly once in trusted CH logical view | Does not prove raw Kafka/CH exactly-once |
| `slo` | `NOT_SET`, `PASS`, `FAIL`, or `NOT_EVALUABLE` | No thresholds means **no SLO pass** |

Benchmark visibility is observed by a Python client on one host clock, not claimed as exact producer-to-consumer processing latency. Published Kafka data may be replayed; trusted CH projection remains **non-authoritative for billing**. `visibility_query_errors` is also reported, not suppressed. The client requires credential mapping for **every** event's tenant and project scope; missing mappings fail closed.

## Candidate SLO contract (NOT an achieved target)

Do **not** treat these as verified project commitments. A candidate, **after** controlling machine size, topic durability, concurrency, data distribution, baseline warm-up and run length, might be:

- HTTP PG acceptance P95 no more than **500 ms** under an agreed bounded rate.
- First CH observable usage P95 no more than **5 seconds**.
- Complete initial 1,000-event burst within **30 seconds after last HTTP ACK**, with no unresolved records.

These values are illustrative **hypotheses** and should be tuned against repeated results. Opt into gating explicitly:

```bash
M5_COUNT=1000 M5_CONCURRENCY=8 M5_PARTITIONS=6 M5_BATCH_SIZE=64 \
  M5_MAX_ACK_P95_MS=500 M5_MAX_VISIBLE_P95_MS=5000 M5_MAX_DRAIN_SECONDS=30 \
  M5_OUTPUT=artifacts/m5-slo-candidate.json make m5-performance-smoke
```

A JSON report with `slo=PASS` applies **only** to its fixed scenario and specific host/commit. `INCOMPLETE` (not all events visible) and `FAIL` (HTTP errors) may **never** be counted as SLO passes.

## Suggested baseline protocol before claiming an SLO

1. Record exact Git SHA, hardware/CPU quota, RAM, disk type, Kafka RF/minISR, ClickHouse mode/version, Go version, partition count, consumer/relay instances and batch size.
2. Warm up or explicitly label a cold start; run 3–5 independent trials per profile. Use sustained duration-controlled tests before publishing an availability or latency guarantee.
3. Compare P50/P95/P99 across acceptance and visibility separately; provide actual client RPS and confidence/variance across trials, not just peak.
4. Record PG commit/Outbox backlog, Kafka partition lag, CH insert errors and container resource pressure over time. Add a separate outage/catch-up workload for **recovery time**; `drain_seconds` here is normal-load drain only.
5. Independently test CH asynchronous inserts with `async_insert=1` and `wait_for_async_insert=0/1`: an ACK with wait disabled is **not** sufficient proof that the analytics row persisted. Never use asynchronous CH ACKs as a billing commit.
6. Store raw JSON artifacts per run, a machine-readable profile matrix and regression thresholds only after the evidence stabilizes.

## Evidence ledger

| Stage | Source | Verified result |
| --- | --- | --- |
| Deterministic percentile/SLO guards | `python3 -m unittest discover -s scripts -p 'test_m5_benchmark.py' -v` | Await exact-head CI |
| Real-stack 128-event smoke | `.github/workflows/m5-performance.yml` | Await exact-head CI |
| 1k/10k high-concurrency matrix | manual controlled profiles above | **Not yet run** |
| ClickHouse async insert 0/1, backlog recovery SLO | separate M5 hardening | **Not yet implemented/verified** |
| Production load SLO | requires real hardware and repeated observations | **Not established** |

Raw results are intentionally **not** prefilled with invented throughput/latency values. Distinguish acceptance correctness, first visibility, and business accounting in all reports.


## Comparing actual observations

Once there are at least two **complete** receipts, generate a Markdown profile comparison without inventing numbers:

```bash
python3 scripts/m5_compare.py \
  artifacts/m5-c1-p1-b16.json artifacts/m5-c8-p6-b64.json \
  --output artifacts/m5-profile-comparison.md
```

The comparator rejects incomplete/failed correctness experiments, missing metrics and invalid JSON. It prints HTTP P95/P99, sampled CH visibility P95/P99, drain, RPS and a throughput factor relative to the first receipt. Different count, concurrency, partition or batch settings are prominently labeled **profiles, not controlled before/after optimization evidence**. For true tuning claims, rerun the same profile and hardware before and after the change, repeat trials and show variation.
