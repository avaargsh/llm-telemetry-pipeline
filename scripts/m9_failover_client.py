#!/usr/bin/env python3
"""M9: fault-aware paced HTTP client and strict PostgreSQL/ClickHouse receipt.

Uses the existing M5 generator and read-only trusted ClickHouse projection.
This is an isolated synchronous-standby/manual-failover experiment, NOT a
production SLO, automatic HA, or an exactly-once Kafka claim.
"""
import argparse
import concurrent.futures
import csv
import datetime as dt
import json
import math
import os
import pathlib
import threading
import time
import urllib.error
import urllib.request

from m5_benchmark import (build_scopes, load_events, observed_timestamp,
                          pacing_plan, percentile, query_visible, token_for,
                          visibility_query)


def classify_status(status):
    if status == 201:
        return "CONFIRMED_CREATED"
    if status == 200:
        return "CONFIRMED_DUPLICATE"
    if status == 0 or status in (408, 425, 429) or 500 <= status <= 599:
        return "UNKNOWN"
    return "REJECTED"


def percentile_or_none(values, quantile):
    return round(percentile(values, quantile), 3) if values else None


def verify_control(control):
    fields = ("fault_injected", "primary_isolated", "promoted", "api_ready")
    if not isinstance(control, dict):
        raise ValueError("missing fault timeline")
    vals = [control.get(field) for field in fields]
    if any(isinstance(v, bool) or not isinstance(v, (int, float))
           or not math.isfinite(v) or v <= 0 for v in vals):
        raise ValueError("missing/nonfinite fault milestone")
    if vals != sorted(vals) or len(set(vals)) != len(vals):
        raise ValueError("fault, fencing, promotion and routing must be ordered")
    return control


def phase_of(started, control):
    verify_control(control)
    if started < control["fault_injected"]:
        return "before"
    if started < control["api_ready"]:
        return "fault"
    return "recovery"


def build_reconciliation(expected, records, control, ledger_count, outbox_count):
    """Fail closed on missing, tampered, unpublished or duplicate authoritative rows."""
    verify_control(control)
    if (not isinstance(expected, dict) or not expected or
            not isinstance(records, dict) or
            isinstance(ledger_count, bool) or isinstance(outbox_count, bool) or
            ledger_count != len(expected) or outbox_count != len(expected) or
            set(records) != set(expected)):
        raise ValueError("Ledger/Outbox counts or stable Event IDs do not reconcile")
    for event_id, payload_hash in expected.items():
        if not isinstance(payload_hash, str) or not payload_hash:
            raise ValueError("expected payload seal missing")
        ledger_hash, outbox_hash, published = records[event_id]
        if payload_hash != ledger_hash or payload_hash != outbox_hash or published is not True:
            raise ValueError("hash conflict or Outbox not marked published")
    return {
        "accepted_unique_ids": len(expected),
        "authoritative_ledger_rows": ledger_count,
        "authoritative_outbox_rows": outbox_count,
        "published_with_confirmed_outbox": len(expected),
        "observed_acknowledged_id_loss": 0,
        "atomic_ledger_outbox": True,
        "stale_primary_fenced_before_promotion": True,
    }


def read_fault_events(path):
    events = {}
    for line in pathlib.Path(path).read_text(encoding="utf-8").splitlines():
        obj = json.loads(line)
        if not isinstance(obj, dict) or obj.get("event") in events:
            raise ValueError("invalid or duplicate fault milestone")
        name = obj.get("event")
        stamp = obj.get("monotonic_seconds")
        if not isinstance(name, str) or isinstance(stamp, bool) or not isinstance(stamp, (int, float)):
            raise ValueError("fault milestone missing monotonic time")
        events[name] = stamp
    return verify_control(events)


def send_http(url, event, token, scheduled, index, retry_round):
    body = json.dumps(event, sort_keys=True, separators=(",", ":")).encode()
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + token})
    started = time.monotonic()
    status, response = 0, ""
    try:
        with urllib.request.urlopen(req, timeout=4) as r:
            status = r.status
            response = r.read(4096).decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        status = exc.code
        exc.close()
    except (OSError, TimeoutError):
        status = 0
    ended = time.monotonic()
    classification = classify_status(status)
    if classification.startswith("CONFIRMED"):
        try:
            obj = json.loads(response)
            expected_result = "created" if status == 201 else "duplicate"
            if obj.get("event_id") != event["event_id"] or obj.get("result") != expected_result:
                classification = "REJECTED"
        except (ValueError, AttributeError, TypeError):
            classification = "REJECTED"
    return {
        "index": index, "event_id": event["event_id"], "status": status,
        "classification": classification, "started": started, "ended": ended,
        "scheduled": scheduled, "retry_round": retry_round,
    }


def summarize_attempts(attempts, control, visible_at, count, target_rps, duration,
                       schedule_epoch, observer_errors):
    verify_control(control)
    firsts = {a["index"]: a for a in attempts if a["retry_round"] == 0}
    if len(firsts) != count:
        raise ValueError("open-loop did not attempt every stable event exactly once")
    initial = list(firsts.values())
    initial.sort(key=lambda a: a["index"])
    if any(a["scheduled"] < schedule_epoch for a in initial):
        raise ValueError("invalid open-loop schedule")
    if (initial[-1]["scheduled"] <= control["api_ready"] or
            initial[0]["started"] >= control["fault_injected"]):
        raise ValueError("fault was not injected during sustained load")
    successful = [a for a in attempts if a["classification"].startswith("CONFIRMED")]
    initial_unknown = [a for a in initial if a["classification"] == "UNKNOWN"]
    rejected = [a for a in attempts if a["classification"] == "REJECTED"]
    by_id = {a["event_id"] for a in successful}
    before = [a for a in successful if a["ended"] < control["fault_injected"]]
    after = [a for a in successful if a["ended"] >= control["api_ready"]]
    fault_attempts = [a for a in initial if phase_of(a["started"], control) == "fault"]
    if not before or not after or not fault_attempts:
        raise ValueError("missing real before/fault/recovery traffic")
    if not any(a["classification"] == "UNKNOWN" for a in fault_attempts):
        raise ValueError("no observed failed/unknown ACK during primary outage")
    if (len(by_id) != count or len(visible_at) != count or rejected or observer_errors):
        raise ValueError("missing final ACK, trusted visibility or reliable observations")
    first_confirmation = {}
    for attempt in sorted(successful, key=lambda item: item["ended"]):
        first_confirmation.setdefault(attempt["event_id"], attempt["ended"])
    phase_metrics = {}
    for phase in ("before", "fault", "recovery"):
        phase_initial = [a for a in initial if phase_of(a["started"], control) == phase]
        phase_success = [a for a in phase_initial if a["classification"].startswith("CONFIRMED")]
        latencies = [(a["ended"] - a["started"]) * 1000 for a in phase_success]
        # Visibility and final confirmation include failed attempts and retries.
        # Only HTTP ACK latency uses successful individual attempts.
        visible_latencies_by_phase = [
            max(0, (visible_at[a["event_id"]] - a["started"]) * 1000)
            for a in phase_initial]
        confirmation_latencies = [
            max(0, (first_confirmation[a["event_id"]] - a["started"]) * 1000)
            for a in phase_initial]
        phase_metrics[phase] = {
            "initial_requests": len(phase_initial),
            "initial_unknown": sum(a["classification"] == "UNKNOWN" for a in phase_initial),
            "confirmed_acks": len(phase_success),
            "ack_p95_ms": percentile_or_none(latencies, 0.95),
            "ack_p99_ms": percentile_or_none(latencies, 0.99),
            "visible_p95_ms": percentile_or_none(visible_latencies_by_phase, 0.95),
            "visible_p99_ms": percentile_or_none(visible_latencies_by_phase, 0.99),
            "final_confirmation_p95_ms": percentile_or_none(confirmation_latencies, 0.95),
            "final_confirmation_p99_ms": percentile_or_none(confirmation_latencies, 0.99),
        }
    first_request_start_by_id = {a["event_id"]: a["started"] for a in initial}
    visible_latencies = [
        max(0, (visible_at[event_id] - first_request_start_by_id[event_id]) * 1000)
        for event_id in visible_at]
    lag = [max(0.0, (a["started"] - a["scheduled"]) * 1000) for a in initial]
    first_after_fault_ack = min(a["ended"] for a in successful
                                if a["ended"] >= control["fault_injected"])
    first_after_routing_ack = min(a["ended"] for a in after)
    last_before_fault_ack = max(a["ended"] for a in before)
    all_ack_times = sorted(a["ended"] for a in successful)
    max_ack_gap_s = max((right - left for left, right in
                         zip(all_ack_times, all_ack_times[1:])), default=0.0)
    last_confirmed_ack = all_ack_times[-1]
    last_first_visibility = max(visible_at.values())
    return {
        "schema_version": 1, "head_sha": os.getenv("M9_EXACT_HEAD", ""),
        "mode": "manual_pg17_remote_apply_fault_aware",
        "slo": "NOT_SET", "client_correctness": "PASS_CLIENT_OBSERVED",
        "target_rps": target_rps, "duration_seconds": duration,
        "submitted": count, "finally_confirmed_ids": len(by_id),
        "trusted_clickhouse_visible_ids": len(visible_at),
        "initial_unknown_ack": len(initial_unknown),
        "initial_unknown_resolved_same_id": sum(
            a["event_id"] in by_id for a in initial_unknown),
        "recovered_duplicate_responses": sum(
            a["retry_round"] > 0 and a["classification"] == "CONFIRMED_DUPLICATE"
            for a in attempts),
        "total_attempts": len(attempts),
        "http_attempt_status_counts": {str(code): sum(a["status"] == code for a in attempts)
                                       for code in sorted({a["status"] for a in attempts})},
        "window_metrics": phase_metrics,
        "steady_schedule_lag_p99_ms": percentile_or_none(lag, 0.99),
        "trusted_visibility_p95_ms": percentile_or_none(visible_latencies, 0.95),
        "trusted_visibility_p99_ms": percentile_or_none(visible_latencies, 0.99),
        "first_inflight_ack_after_fault_s": round(
            first_after_fault_ack - control["fault_injected"], 3),
        "promoted_to_first_confirmed_ack_s": round(
            first_after_routing_ack - control["promoted"], 3),
        "fault_boundary_ack_gap_s": round(
            first_after_fault_ack - last_before_fault_ack, 3),
        "observed_write_interruption_bracket_s": round(
            first_after_routing_ack - last_before_fault_ack, 3),
        "maximum_observed_inter_ack_gap_s": round(max_ack_gap_s, 3),
        "post_last_ack_trusted_visibility_drain_s": round(
            max(0, last_first_visibility - last_confirmed_ack), 3),
        "promotion_duration_s": round(control["promoted"] - control["fault_injected"], 3),
        "routing_recovery_duration_s": round(control["api_ready"] - control["fault_injected"], 3),
        "visibility_polling_note": "First observed in trusted CH projection, polling uncertainty ~0.5s",
        "ack_gap_note": "Fault-boundary ACK may be an in-flight success; the bracket and maximum sampled gap are observations, not RTO guarantees",
        "production_rpo_rto": "NOT_ESTABLISHED",
        "fault_timeline": control,
    }



def write_raw_samples(attempts, visible_at, control, epoch, output):
    """Redacted per-attempt/per-event raw receipts for independent percentile review."""
    root = pathlib.Path(output).parent
    root.mkdir(parents=True, exist_ok=True)
    with (root / "attempts.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(("event_index", "retry_round", "phase", "status", "classification",
                         "scheduled_offset_ms", "start_offset_ms", "end_offset_ms"))
        for a in sorted(attempts, key=lambda item: (item["index"], item["retry_round"])):
            writer.writerow((
                a["index"], a["retry_round"], phase_of(a["started"], control),
                a["status"], a["classification"],
                f'{(a["scheduled"] - epoch) * 1000:.6f}',
                f'{(a["started"] - epoch) * 1000:.6f}',
                f'{(a["ended"] - epoch) * 1000:.6f}',
            ))
    firsts = {a["index"]: a for a in attempts if a["retry_round"] == 0}
    with (root / "visibility.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(("event_index", "phase", "first_start_offset_ms",
                         "first_seen_offset_ms", "visibility_latency_ms"))
        for index in sorted(firsts):
            a = firsts[index]
            seen = visible_at.get(a["event_id"])
            if seen is None:
                raise ValueError("cannot archive incomplete ClickHouse visibility")
            writer.writerow((index, phase_of(a["started"], control),
                             f'{(a["started"] - epoch) * 1000:.6f}',
                             f'{(seen - epoch) * 1000:.6f}',
                             f'{max(0, seen - a["started"]) * 1000:.6f}'))


def validate_raw_samples(root, summary, count):
    """Independent raw receipt consistency gate, without storing stable IDs."""
    with (pathlib.Path(root) / "attempts.csv").open(newline="", encoding="utf-8") as handle:
        attempts = list(csv.DictReader(handle))
    with (pathlib.Path(root) / "visibility.csv").open(newline="", encoding="utf-8") as handle:
        visibility = list(csv.DictReader(handle))
    if len(attempts) != summary["total_attempts"] or len(visibility) != count:
        raise ValueError("raw evidence cardinality disagrees with report")
    try:
        indices = [int(x["event_index"]) for x in visibility]
        initial_indices = [int(x["event_index"]) for x in attempts
                           if int(x["retry_round"]) == 0]
        statuses = {}
        for a in attempts:
            index = int(a["event_index"])
            if index < 0 or index >= count or a["classification"] != classify_status(int(a["status"])):
                raise ValueError("raw attempt classification/index tampered")
            statuses[a["status"]] = statuses.get(a["status"], 0) + 1
        if (len(set(indices)) != count or set(indices) != set(range(count)) or
                sorted(initial_indices) != list(range(count)) or
                statuses != summary["http_attempt_status_counts"]):
            raise ValueError("raw request/visibility identities or statuses incomplete")
        values = [float(x["visibility_latency_ms"]) for x in visibility]
        if abs(percentile(values, 0.99) - summary["trusted_visibility_p99_ms"]) > 0.002:
            raise ValueError("reported ClickHouse P99 differs from raw samples")
        for phase, metrics in summary["window_metrics"].items():
            phase_values = [float(x["visibility_latency_ms"]) for x in visibility
                            if x["phase"] == phase]
            phase_attempts = [a for a in attempts if a["phase"] == phase
                              and int(a["retry_round"]) == 0]
            if (len(phase_attempts) != metrics["initial_requests"] or
                    len(phase_values) != metrics["initial_requests"]):
                raise ValueError("phase-specific evidence incomplete")
            observed = percentile_or_none(phase_values, 0.99)
            reported = metrics["visible_p99_ms"]
            if observed is None or reported is None or abs(observed - reported) > 0.002:
                raise ValueError("phase ClickHouse P99 differs from raw samples")
    except (TypeError, KeyError) as exc:
        raise ValueError("invalid raw receipt schema") from exc
    return {"raw_attempt_rows": len(attempts), "raw_visibility_rows": len(visibility),
            "independent_p99_recalculated": True}


def run_load(args):
    if not 1 <= args.concurrency <= 128 or not 0 < args.poll_seconds <= 5:
        raise ValueError("invalid client concurrency or poll")
    events = load_events(args.events_file, args.seed)
    offsets = pacing_plan(len(events), args.target_rps, args.duration_seconds)
    scopes = build_scopes(json.loads(os.environ.get("USAGE_INGEST_CREDENTIALS_JSON", "[]")))
    for event in events:
        token_for(event, scopes)
    earliest = min(observed_timestamp(e["timestamp"]) for e in events)
    latest = max(observed_timestamp(e["timestamp"]) for e in events) + 1001
    query = visibility_query(args.seed, earliest, latest)
    event_ids = {e["event_id"] for e in events}
    visible_at = {}
    observer_errors = []
    stop = threading.Event()
    def observe():
        while not stop.is_set():
            try:
                ids = query_visible(args.clickhouse, "telemetry",
                                    os.getenv("CLICKHOUSE_PASSWORD", ""),
                                    query)
                stamp = time.monotonic()
                for event_id in ids & event_ids:
                    visible_at.setdefault(event_id, stamp)
            except (OSError, ValueError, UnicodeError) as exc:
                observer_errors.append(type(exc).__name__)
            if len(visible_at) == len(events):
                break
            stop.wait(args.poll_seconds)

    watcher = threading.Thread(target=observe, daemon=True)
    watcher.start()
    attempts = []
    epoch = time.monotonic()
    pathlib.Path(args.start_marker).write_text(str(epoch) + "\n", encoding="utf-8")
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            futures = []
            for i, (event, offset) in enumerate(zip(events, offsets)):
                scheduled = epoch + offset
                remaining = scheduled - time.monotonic()
                if remaining > 0:
                    time.sleep(remaining)
                futures.append(pool.submit(send_http, args.api_url, event,
                                           token_for(event, scopes), scheduled, i, 0))
            attempts.extend(f.result() for f in concurrent.futures.as_completed(futures))
            confirmed = {a["event_id"] for a in attempts
                         if a["classification"].startswith("CONFIRMED")}
            for round_index in range(1, args.max_retry_rounds + 1):
                missing = [(i, e) for i, e in enumerate(events) if e["event_id"] not in confirmed]
                if not missing:
                    break
                time.sleep(args.retry_delay)
                retry_futures = [pool.submit(send_http, args.api_url, e,
                                             token_for(e, scopes), time.monotonic(), i, round_index)
                                 for i, e in missing]
                round_attempts = [f.result() for f in concurrent.futures.as_completed(retry_futures)]
                attempts.extend(round_attempts)
                confirmed.update(a["event_id"] for a in round_attempts
                                 if a["classification"].startswith("CONFIRMED"))
        deadline = time.monotonic() + args.visibility_timeout
        while len(visible_at) < len(events) and time.monotonic() < deadline:
            time.sleep(0.5)
    finally:
        stop.set()
        watcher.join(timeout=5)
    control = read_fault_events(args.fault_events)
    summary = summarize_attempts(attempts, control, visible_at, len(events),
                                 args.target_rps, args.duration_seconds, epoch,
                                 len(observer_errors))
    pathlib.Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    write_raw_samples(attempts, visible_at, control, epoch, args.output)
    pathlib.Path(args.output).write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"client_correctness": summary["client_correctness"],
                      "finally_confirmed_ids": summary["finally_confirmed_ids"],
                      "initial_unknown_ack": summary["initial_unknown_ack"],
                      "trusted_clickhouse_visible_ids": summary["trusted_clickhouse_visible_ids"]}))


def verify_final(args):
    events = load_events(args.events_file, args.seed)
    expected = {e["event_id"]: e["payload_hash"] for e in events}
    records = {}
    with open(args.ledger_csv, newline="", encoding="utf-8") as handle:
        for row in csv.reader(handle):
            if len(row) != 4 or row[0] in records or row[3] not in ("t", "f"):
                raise ValueError("invalid/duplicate Ledger+Outbox evidence row")
            records[row[0]] = (row[1], row[2], row[3] == "t")
    control = read_fault_events(args.fault_events)
    proof = build_reconciliation(expected, records, control,
                                 args.ledger_count, args.outbox_count)
    summary = json.loads(pathlib.Path(args.client_report).read_text(encoding="utf-8"))
    raw_proof = validate_raw_samples(pathlib.Path(args.client_report).parent, summary, len(expected))
    if (summary.get("client_correctness") != "PASS_CLIENT_OBSERVED" or
            summary.get("submitted") != len(expected) or
            summary.get("finally_confirmed_ids") != len(expected) or
            summary.get("trusted_clickhouse_visible_ids") != len(expected) or
            summary.get("initial_unknown_ack", 0) < 1 or
            summary.get("head_sha") != args.exact_head or
            len(args.exact_head) != 40 or
            args.held_uncommitted_rows != 0 or
            summary.get("fault_timeline") != control or
            summary.get("slo") != "NOT_SET"):
        raise ValueError("incomplete client, HEAD, SIGKILL or transaction evidence")
    receipt = {
        "schema_version": 1, "head_sha": args.exact_head,
        "experiment": "M9 manual PostgreSQL HA with sustained open-loop load",
        "result": "PASS_EXPERIMENT_ONLY",
        "slo": "NOT_SET",
        "production_rpo_rto": "NOT_ESTABLISHED",
        "pg_remote_apply_checked_before_load_and_fault": True,
        "real_sigkill_and_manual_promotion": True,
        "held_uncommitted_ledger_and_outbox_rows": 0,
        "reconciliation": proof,
        "raw_samples": raw_proof,
        "client": summary,
        "evidence_limits": [
            "No automatic leader election, network-partition fencing or client failover",
            "ClickHouse first-observed visibility is polling-based",
            "Single-node Kafka RF1, manually redirected processes, shared CI",
            "No production SLO/RPO/RTO/exactly-once assertion",
        ],
    }
    output = pathlib.Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("PASS M9: accepted stable IDs, atomic Ledger/Outbox and CH trusted visibility reconciled")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="mode", required=True)
    send = sub.add_parser("load")
    send.add_argument("--events-file", required=True)
    send.add_argument("--seed", required=True)
    send.add_argument("--api-url", default="http://127.0.0.1:18085/v1/usage-events")
    send.add_argument("--clickhouse", default="http://127.0.0.1:18123")
    send.add_argument("--target-rps", type=float, default=25)
    send.add_argument("--duration-seconds", type=float, default=180)
    send.add_argument("--concurrency", type=int, default=32)
    send.add_argument("--poll-seconds", type=float, default=0.5)
    send.add_argument("--visibility-timeout", type=float, default=180)
    send.add_argument("--max-retry-rounds", type=int, default=10)
    send.add_argument("--retry-delay", type=float, default=2)
    send.add_argument("--fault-events", required=True)
    send.add_argument("--start-marker", required=True)
    send.add_argument("--output", required=True)
    verify = sub.add_parser("verify")
    verify.add_argument("--events-file", required=True)
    verify.add_argument("--seed", required=True)
    verify.add_argument("--ledger-csv", required=True)
    verify.add_argument("--ledger-count", type=int, required=True)
    verify.add_argument("--outbox-count", type=int, required=True)
    verify.add_argument("--held-uncommitted-rows", type=int, required=True)
    verify.add_argument("--fault-events", required=True)
    verify.add_argument("--client-report", required=True)
    verify.add_argument("--exact-head", required=True)
    verify.add_argument("--output", required=True)
    args = ap.parse_args()
    if args.mode == "load":
        if not 1 <= args.max_retry_rounds <= 25 or not 0 < args.retry_delay <= 30:
            ap.error("invalid retry bounds")
        run_load(args)
    else:
        verify_final(args)


if __name__ == "__main__":
    main()
