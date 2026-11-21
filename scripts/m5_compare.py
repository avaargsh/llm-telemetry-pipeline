#!/usr/bin/env python3
"""Compare observed M5 JSON receipts; never manufacture missing measurements."""
import argparse
import json
import math
import pathlib


FIELDS = (
    ("acceptance_throughput_events_s", "Accepted/s"),
    ("ack_p95_ms", "HTTP P95 ms"),
    ("ack_p99_ms", "HTTP P99 ms"),
    ("visible_p95_ms", "Visible P95 ms"),
    ("visible_p99_ms", "Visible P99 ms"),
    ("drain_seconds", "Drain s"),
)


def validate(receipt):
    if receipt.get("schema_version") != 1:
        raise ValueError("unsupported M5 receipt schema")
    if receipt.get("correctness") != "PASS_CORRECTNESS":
        raise ValueError("only complete correctness-passing runs can be compared")
    if receipt.get("submitted") != receipt.get("accepted") or receipt.get("accepted") != receipt.get("visible"):
        raise ValueError("incomplete or inconsistent M5 receipt")
    for field, _ in FIELDS:
        value = receipt.get(field)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError(f"missing or invalid metric: {field}")
    if not isinstance(receipt.get("relay_batch_timeout"), str) or not receipt["relay_batch_timeout"].endswith(("ms", "s")):
        raise ValueError("missing relay batch timeout")
    for key in ("submitted", "concurrency", "partitions", "consumer_batch_size", "relay_claim_limit"):
        if not isinstance(receipt.get(key), int) or receipt[key] <= 0:
            raise ValueError(f"invalid profile: {key}")


def markdown(reports):
    if not reports:
        raise ValueError("need at least one M5 receipt")
    for r in reports:
        validate(r)
    first = reports[0]
    header = "| Run | Count | Concurrency | Partitions | Batch | Relay limit | Relay timeout | " + " | ".join(label for _, label in FIELDS) + " | Visible P95 factor |"
    divider = "|" + "|".join("---" for _ in header.strip("|").split("|")) + "|"
    rows = [header, divider]
    for idx, r in enumerate(reports):
        baseline = first["visible_p95_ms"]
        factor = "n/a" if r["visible_p95_ms"] == 0 else f"{baseline/r['visible_p95_ms']:.2f}x"
        vals = [str(idx + 1), str(r["submitted"]), str(r["concurrency"]),
                str(r["partitions"]), str(r["consumer_batch_size"]),
                str(r["relay_claim_limit"]), r["relay_batch_timeout"]]
        vals.extend(f"{r[field]:.2f}" for field, _ in FIELDS)
        vals.append(factor)
        rows.append("| " + " | ".join(vals) + " |")
    profiles = {(r["submitted"], r["concurrency"], r["partitions"],
                 r["consumer_batch_size"], r["relay_claim_limit"], r["relay_batch_timeout"]) for r in reports}
    rows.append("")
    rows.append("Evidence: raw receipts record git SHA and run IDs; only correctness-complete runs are listed.")
    if len(profiles) > 1:
        rows.append("**Caution:** configurations differ; these are profiles, NOT a controlled before/after optimization experiment.")
    else:
        rows.append("**Caution:** identical profile settings alone do not establish comparable hardware or statistically significant improvement.")
    rows.append("ClickHouse visibility is polling-based observation, not a billing commit.")
    return "\n".join(rows) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="+", help="M5 performance JSON artifacts")
    parser.add_argument("--output", help="optional Markdown summary output path")
    args = parser.parse_args()
    data = [json.loads(pathlib.Path(name).read_text(encoding="utf-8")) for name in args.reports]
    rendered = markdown(data)
    if args.output:
        p = pathlib.Path(args.output)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()
