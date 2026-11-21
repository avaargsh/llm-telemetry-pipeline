#!/usr/bin/env python3
"""M5 isolated, observable usage-ingestion benchmark. Python 3 stdlib only.

Metrics distinguish PostgreSQL HTTP acceptance from eventually visible
ClickHouse *analysis*. No clock-sync claims, invoice totals, or exactly-once.
"""
import argparse
import base64
import concurrent.futures
import datetime as dt
import json
import math
import os
import pathlib
import re
import statistics
import threading
import time
import urllib.error
import urllib.request

SAFE_SEED = re.compile(r"^[a-zA-Z0-9_-]{3,80}$")


def percentile(values, quantile):
    if not values or not 0 < quantile <= 1 or any(not math.isfinite(v) or v < 0 for v in values):
        raise ValueError("non-empty finite nonnegative values and quantile 0<q<=1 required")
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * quantile) - 1)]


def classify(report, thresholds):
    if report["accepted"] != report["submitted"]:
        return "FAIL", "NOT_EVALUABLE"
    if report["visible"] != report["submitted"]:
        return "INCOMPLETE", "NOT_EVALUABLE"
    if not thresholds:
        return "PASS_CORRECTNESS", "NOT_SET"
    if any(report[key] is None or report[key] > value for key, value in thresholds.items()):
        return "PASS_CORRECTNESS", "FAIL"
    return "PASS_CORRECTNESS", "PASS"



def pacing_plan(count, target_rps, duration_seconds):
    """Deterministic monotonic-clock offsets; no coordinated-omission shortcut."""
    if (isinstance(count, bool) or not isinstance(count, int) or
        not isinstance(target_rps, (int, float)) or isinstance(target_rps, bool) or
        not isinstance(duration_seconds, (int, float)) or isinstance(duration_seconds, bool) or
        not math.isfinite(target_rps) or not math.isfinite(duration_seconds) or
        target_rps <= 0 or target_rps > 2000 or
        not 30 <= duration_seconds <= 1200 or
        not 1000 <= count <= 10000):
        raise ValueError("sustained profiles require 1000..10000 samples, 30..1200s, positive bounded rate")
    if count != math.ceil(target_rps * duration_seconds):
        raise ValueError("number of generated sealed events must equal ceil(target RPS × duration)")
    return [i / target_rps for i in range(count)]

def build_scopes(credentials):
    if not isinstance(credentials, list) or not credentials:
        raise ValueError("credential list is required")
    result = {}
    for item in credentials:
        if not isinstance(item, dict):
            raise ValueError("invalid credentials")
        token, tenant, project = item.get("token"), item.get("tenant_id"), item.get("project_id")
        if not isinstance(token, str) or len(token) < 32 or not tenant or not project:
            raise ValueError("invalid scope or secret")
        scope = (tenant, project)
        if scope in result:
            raise ValueError("duplicate tenant/project scope")
        result[scope] = token
    return result


def token_for(event, scopes):
    try:
        return scopes[(event["tenant_id"], event["project_id"])]
    except KeyError as exc:
        raise ValueError("missing scoped credential for an event") from exc


def visibility_query(seed, start_ms, end_ms):
    if not SAFE_SEED.fullmatch(seed) or start_ms < 0 or end_ms <= start_ms:
        raise ValueError("unsafe seed or invalid event-time bounds")
    return (
        "SELECT toString(event_id) FROM inference_events_dedup_v1 "
        f"WHERE event_time >= fromUnixTimestamp64Milli({start_ms}) "
        f"AND event_time < fromUnixTimestamp64Milli({end_ms}) "
        f"AND startsWith(request_id, '{seed}_req_') FORMAT TabSeparatedRaw"
    )


def load_events(path, seed):
    events = []
    seen = set()
    for line in pathlib.Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        event = json.loads(line)
        if not isinstance(event, dict) or "event_id" not in event:
            raise ValueError("event object with event_id required")
        event_id = event["event_id"]
        if event_id in seen or not str(event.get("request_id", "")).startswith(seed + "_req_"):
            raise ValueError("duplicate event_id or event from another benchmark seed")
        seen.add(event_id)
        events.append(event)
    if not 1 <= len(events) <= 10000:
        raise ValueError("require 1..10000 uniquely identified events")
    return events


def observed_timestamp(text):
    value = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("event timestamp must include a timezone")
    return int(value.timestamp() * 1000)


def query_visible(endpoint, user, password, query):
    request = urllib.request.Request(endpoint.rstrip("/") + "/?database=telemetry",
                                     data=query.encode("utf-8"), method="POST")
    request.add_header("Content-Type", "text/plain; charset=utf-8")
    request.add_header("Authorization", "Basic " + base64.b64encode((user + ":" + password).encode()).decode())
    with urllib.request.urlopen(request, timeout=8) as response:
        return set(response.read(2 << 20).decode("utf-8").splitlines())


def send_one(api_url, event, token, started, mutex, scheduled=None):
    event_id = event["event_id"]
    data = json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(api_url, data=data, method="POST",
                                     headers={"Content-Type": "application/json", "Authorization": "Bearer " + token})
    start = time.monotonic()
    lag_ms = max(0.0, (start - scheduled) * 1000) if scheduled is not None else None
    with mutex:
        started[event_id] = start
    status = 0
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            status = response.status
            response.read(4096)
    except urllib.error.HTTPError as exc:
        status = exc.code
        exc.close()
    except (OSError, TimeoutError):
        status = 0
    return event_id, status, start, time.monotonic(), lag_ms


def read_process_stats(pid):
    try:
        parts = pathlib.Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        ticks = (int(parts[11]) + int(parts[12])) / os.sysconf("SC_CLK_TCK")
        resident = int(pathlib.Path(f"/proc/{pid}/statm").read_text().split()[1])
        rss_mib = resident * os.sysconf("SC_PAGE_SIZE") / (1024 * 1024)
        return ticks, rss_mib
    except (OSError, IndexError, ValueError):
        return None


def sample_processes(pids, stop, results):
    last = {}
    while not stop.is_set():
        now = time.monotonic()
        for label, pid in pids.items():
            data = read_process_stats(pid)
            if data is None:
                continue
            cpu_s, rss = data
            entry = results.setdefault(label, {"max_rss_mib": 0.0, "max_cpu_percent_one_core": 0.0, "samples": 0})
            entry["max_rss_mib"] = max(entry["max_rss_mib"], rss)
            entry["samples"] += 1
            if label in last and now > last[label][0]:
                rate = 100.0 * (cpu_s - last[label][1]) / (now - last[label][0])
                entry["max_cpu_percent_one_core"] = max(entry["max_cpu_percent_one_core"], rate)
            last[label] = (now, cpu_s)
        stop.wait(0.25)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--events-file", required=True)
    ap.add_argument("--seed", required=True)
    ap.add_argument("--api-url", default="http://127.0.0.1:18085/v1/usage-events")
    ap.add_argument("--clickhouse", default="http://127.0.0.1:18123")
    ap.add_argument("--clickhouse-user", default="telemetry")
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--target-rps", type=float, help="steady open-loop offered load; requires --duration-seconds")
    ap.add_argument("--duration-seconds", type=float, help="steady offered-load interval; min 30s")
    ap.add_argument("--partitions", type=int, required=True)
    ap.add_argument("--batch-size", type=int, required=True)
    ap.add_argument("--relay-claim-limit", type=int, required=True)
    ap.add_argument("--relay-batch-timeout", required=True)
    ap.add_argument("--poll-seconds", type=float, default=0.5)
    ap.add_argument("--visibility-timeout", type=float, default=90)
    ap.add_argument("--pid", action="append", default=[], help="name:PID to sample /proc CPU+RSS")
    ap.add_argument("--max-ack-p95-ms", type=float)
    ap.add_argument("--max-visible-p95-ms", type=float)
    ap.add_argument("--max-drain-seconds", type=float)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()
    if not SAFE_SEED.fullmatch(args.seed):
        ap.error("seed must contain only ASCII letters/digits/-/_")
    if not 1 <= args.concurrency <= 128 or not 1 <= args.partitions <= 128 or not 1 <= args.batch_size <= 10000:
        ap.error("invalid concurrency, Kafka partitions or consumer batch size")
    if not 1 <= args.relay_claim_limit <= 500 or not re.fullmatch(r"[1-9][0-9]*(?:ms|s)", args.relay_batch_timeout):
        ap.error("invalid Relay claim limit or Kafka batch timeout")
    if not .1 <= args.poll_seconds <= 10 or not 1 <= args.visibility_timeout <= 1200:
        ap.error("invalid visibility polling limits")
    creds = json.loads(os.environ.get("USAGE_INGEST_CREDENTIALS_JSON", "[]"))
    scopes = build_scopes(creds)
    events = load_events(args.events_file, args.seed)
    if (args.target_rps is None) != (args.duration_seconds is None):
        ap.error("--target-rps and --duration-seconds must both be set")
    try:
        planned_offsets = (pacing_plan(len(events), args.target_rps, args.duration_seconds)
                           if args.target_rps is not None else None)
    except ValueError as exc:
        ap.error(str(exc))
    for item in events:
        token_for(item, scopes)
    start_ms = min(observed_timestamp(e["timestamp"]) for e in events)
    end_ms = max(observed_timestamp(e["timestamp"]) for e in events) + 1001
    sql = visibility_query(args.seed, start_ms, end_ms)
    pids = {}
    for item in args.pid:
        label, _, number = item.partition(":")
        if not label or not number.isdigit() or label in pids:
            ap.error("pid must be unique name:positive-integer")
        pids[label] = int(number)
    thresholds = {}
    for arg_key, metric in [("max_ack_p95_ms", "ack_p95_ms"),
                            ("max_visible_p95_ms", "visible_p95_ms"),
                            ("max_drain_seconds", "drain_seconds")]:
        limit = getattr(args, arg_key)
        if limit is not None:
            if not math.isfinite(limit) or limit <= 0:
                ap.error("SLO limits must be finite positive numbers")
            thresholds[metric] = limit

    started = {}
    visible_at = {}
    mutex = threading.Lock()
    finish = threading.Event()
    resource_stop = threading.Event()
    resource_metrics = {}
    observed_errors = 0
    deadline = [float("inf")]

    def observer():
        nonlocal observed_errors
        while True:
            try:
                ids = query_visible(args.clickhouse, args.clickhouse_user,
                                    os.environ.get("CLICKHOUSE_PASSWORD", ""), sql)
                seen_at = time.monotonic()
                with mutex:
                    for event_id in ids:
                        if event_id in started and event_id not in visible_at:
                            visible_at[event_id] = seen_at
            except (OSError, UnicodeError, ValueError):
                observed_errors += 1
            if finish.is_set() and (len(visible_at) == len(events) or time.monotonic() >= deadline[0]):
                return
            time.sleep(args.poll_seconds)

    resources = threading.Thread(target=sample_processes, args=(pids, resource_stop, resource_metrics), daemon=True)
    monitor = threading.Thread(target=observer, daemon=True)
    resources.start()
    monitor.start()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            if planned_offsets is None:
                futures = [pool.submit(send_one, args.api_url, event, token_for(event, scopes), started, mutex)
                           for event in events]
            else:
                futures = []
                epoch = time.monotonic()
                for event, offset in zip(events, planned_offsets):
                    scheduled = epoch + offset
                    remaining = scheduled - time.monotonic()
                    if remaining > 0:
                        time.sleep(remaining)
                    futures.append(pool.submit(send_one, args.api_url, event,
                                               token_for(event, scopes), started, mutex, scheduled))
            responses = [future.result() for future in concurrent.futures.as_completed(futures)]
        last_ack = max(r[3] for r in responses)
        deadline[0] = time.monotonic() + args.visibility_timeout
        finish.set()
        monitor.join(args.visibility_timeout + args.poll_seconds + 10)
        if monitor.is_alive():
            raise RuntimeError("visibility observer did not terminate")
    finally:
        resource_stop.set()
        resources.join(timeout=3)

    accepted = [r for r in responses if r[1] == 201]
    accepted_by_id = {r[0]: r for r in accepted}
    visible_ids = accepted_by_id.keys() & visible_at.keys()
    ack_ms = [(r[3] - r[2]) * 1000 for r in accepted]
    visible_ms = [max(0, (visible_at[event_id] - accepted_by_id[event_id][2]) * 1000)
                  for event_id in visible_ids]
    window_s = max(0.001, max(r[3] for r in responses) - min(r[2] for r in responses))
    drain_s = max(0, max((visible_at[x] for x in visible_ids), default=last_ack) - last_ack)
    summary = {
        "schema_version": 1,
        "head_sha": os.environ.get("GITHUB_SHA", "local"),
        "run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        "seed": args.seed,
        "submitted": len(events),
        "accepted": len(accepted),
        "visible": len(visible_ids),
        "http_status_counts": {str(code): sum(x[1] == code for x in responses)
                               for code in sorted(set(x[1] for x in responses))},
        "concurrency": args.concurrency,
        "partitions": args.partitions,
        "consumer_batch_size": args.batch_size,
        "relay_claim_limit": args.relay_claim_limit,
        "relay_batch_timeout": args.relay_batch_timeout,
        "acceptance_throughput_events_s": round(len(accepted) / window_s, 2),
        "ack_p50_ms": round(percentile(ack_ms, .50), 2) if ack_ms else None,
        "ack_p95_ms": round(percentile(ack_ms, .95), 2) if ack_ms else None,
        "ack_p99_ms": round(percentile(ack_ms, .99), 2) if ack_ms else None,
        "visible_p95_ms": round(percentile(visible_ms, .95), 2) if visible_ms else None,
        "visible_p99_ms": round(percentile(visible_ms, .99), 2) if visible_ms else None,
        "drain_seconds": round(drain_s, 3),
        "visibility_poll_seconds": args.poll_seconds,
        "visibility_query_errors": observed_errors,
        "steady_target_rps": args.target_rps,
        "steady_duration_seconds": args.duration_seconds,
        "steady_observed_send_duration_seconds": (round(max(r[2] for r in responses) - min(r[2] for r in responses), 3)
                                                  if planned_offsets is not None else None),
        "steady_schedule_lag_p95_ms": (round(percentile([r[4] for r in responses], .95), 2)
                                     if planned_offsets is not None else None),
        "steady_schedule_lag_p99_ms": (round(percentile([r[4] for r in responses], .99), 2)
                                     if planned_offsets is not None else None),
        "resources": resource_metrics,
        "candidate_slo": thresholds,
    }
    correctness, slo = classify(summary, thresholds)
    summary["correctness"] = correctness
    summary["slo"] = slo
    output = pathlib.Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({k: summary[k] for k in
                      ("correctness", "slo", "submitted", "accepted", "visible",
                       "acceptance_throughput_events_s", "ack_p95_ms",
                       "ack_p99_ms", "visible_p95_ms", "drain_seconds")}, sort_keys=True))
    if correctness != "PASS_CORRECTNESS" or slo == "FAIL":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
