#!/usr/bin/env python3
"""M8 strict multi-run steady-load SLO evaluation. No pooled P99 invention."""
import argparse
import json
import math
import pathlib
import statistics

LATENCY = ("ack_p95_ms", "ack_p99_ms", "visible_p95_ms", "visible_p99_ms",
           "steady_schedule_lag_p99_ms")
PROFILE = ("head_sha", "submitted", "concurrency", "partitions", "consumer_batch_size",
           "relay_claim_limit", "relay_batch_timeout", "steady_target_rps", "steady_duration_seconds")


def number(obj, key, *, allow_zero=True):
    value = obj.get(key)
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value):
        raise ValueError(f"missing or non-finite metric: {key}")
    if value < 0 or (not allow_zero and value <= 0):
        raise ValueError(f"negative or invalid metric: {key}")
    return float(value)


def evaluate(reports, thresholds=None):
    if not isinstance(reports, list) or len(reports) < 3:
        raise ValueError("need at least three independent completed steady-load trials")
    limits = thresholds or {}
    if limits and set(limits) != set(LATENCY):
        raise ValueError("a PASSable SLO must specify all four P95/P99 and scheduler-lag objectives")
    for key, val in limits.items():
        if isinstance(val, bool) or not isinstance(val, (float, int)) or not math.isfinite(val) or val <= 0:
            raise ValueError(f"invalid threshold {key}")
    first = reports[0]
    baseline = tuple(first.get(k) for k in PROFILE)
    if not isinstance(first.get("head_sha"), str) or not first["head_sha"]:
        raise ValueError("exact Git HEAD absent")
    for i, r in enumerate(reports):
        if r.get("schema_version") != 1:
            raise ValueError(f"trial {i}: receipt schema mismatch")
        if tuple(r.get(k) for k in PROFILE) != baseline:
            raise ValueError(f"trial {i}: profile/commit differs from baseline")
        count = r.get("submitted")
        if not isinstance(count, int) or isinstance(count, bool) or count < 1000 or count > 10000:
            raise ValueError(f"trial {i}: insufficient samples")
        if r.get("accepted") != count or r.get("visible") != count or r.get("correctness") != "PASS_CORRECTNESS":
            raise ValueError(f"trial {i}: missing HTTP ACK or trusted CH projection")
        if r.get("visibility_query_errors") != 0:
            raise ValueError(f"trial {i}: visibility observation incomplete")
        rate = number(r, "steady_target_rps", allow_zero=False)
        duration = number(r, "steady_duration_seconds", allow_zero=False)
        if not 30 <= duration <= 1200 or count != math.ceil(rate * duration):
            raise ValueError(f"trial {i}: incorrect or truncated offered load")
        sent_duration = number(r, "steady_observed_send_duration_seconds", allow_zero=False)
        if not .9 * duration <= sent_duration <= 1.1 * duration:
            raise ValueError(f"trial {i}: submission interval deviates from duration")
        if number(r, "acceptance_throughput_events_s", allow_zero=False) < .9 * rate:
            raise ValueError(f"trial {i}: client/server could not sustain the requested rate")
        for key in LATENCY:
            number(r, key)
        if number(r, "steady_schedule_lag_p99_ms") > max(200., 3000. / rate):
            raise ValueError(f"trial {i}: client scheduling overloaded; measured latency unreliable")
        for key in ("api", "relay", "consumer"):
            resource = r.get("resources", {}).get(key)
            if not isinstance(resource, dict) or number(resource, "samples", allow_zero=False) < 1:
                raise ValueError(f"trial {i}: missing service resource snapshots")

    worst = {field: max(float(r[field]) for r in reports) for field in LATENCY}
    med = {field: statistics.median(float(r[field]) for r in reports) for field in LATENCY}
    status = "NOT_SET" if not limits else ("PASS" if all(worst[key] <= limit for key, limit in limits.items()) else "FAIL")
    return {
        "schema_version": 1, "type": "isolated_sustained_profile",
        "head_sha": first["head_sha"], "runs": len(reports),
        "submitted_per_run": first["submitted"],
        "target_rps": first["steady_target_rps"],
        "duration_seconds_per_run": first["steady_duration_seconds"],
        "profile": {k:first[k] for k in PROFILE if k!="head_sha"},
        "correctness": "PASS_CORRECTNESS",
        "slo": status,
        "thresholds": limits,
        "worst_p99_ms": {k:worst[k] for k in ("ack_p99_ms", "visible_p99_ms")},
        "median_p99_ms": {k:med[k] for k in ("ack_p99_ms", "visible_p99_ms")},
        "worst_latency_ms": worst,
        "median_latency_ms": med,
        "per_run": [
            {"seed": r["seed"], "submitted": r["submitted"],
             "acceptance_throughput_events_s":r["acceptance_throughput_events_s"],
             **{field: r[field] for field in LATENCY},
             "drain_seconds": r["drain_seconds"],
             "resources": r["resources"]}
            for r in reports
        ],
        "interpretation": "P95/P99 are nearest-rank per-run first-observed latencies. Worst-of-runs gates; no cross-run pooled percentile or production SLO guarantee.",
    }


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("reports", nargs="+")
    p.add_argument("--output", required=True)
    p.add_argument("--max-ack-p95-ms", type=float)
    p.add_argument("--max-ack-p99-ms", type=float)
    p.add_argument("--max-visible-p95-ms", type=float)
    p.add_argument("--max-visible-p99-ms", type=float)
    p.add_argument("--max-schedule-lag-p99-ms", type=float)
    args=p.parse_args()
    limits = {key: value for key, value in (
        ("ack_p95_ms",args.max_ack_p95_ms),("ack_p99_ms",args.max_ack_p99_ms),
        ("visible_p95_ms",args.max_visible_p95_ms),("visible_p99_ms",args.max_visible_p99_ms),
        ("steady_schedule_lag_p99_ms",args.max_schedule_lag_p99_ms))
        if value is not None}
    try:
        r=evaluate([json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
                    for path in args.reports],limits)
    except (ValueError, OSError, json.JSONDecodeError) as exc:
        p.exit(1, f"FAIL CLOSED: invalid sustained SLO evidence: {exc}\n")
    output=pathlib.Path(args.output)
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(r,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    print(json.dumps({"correctness":r["correctness"],"slo":r["slo"],
                      "runs":r["runs"],"worst_latency_ms":r["worst_latency_ms"]},sort_keys=True))
    if r["slo"]=="FAIL":
        raise SystemExit(2)


if __name__=="__main__":
    main()
