#!/usr/bin/env python3
"""M6 read-only reconciliation scheduler: fixed UTC windows, fail-closed receipts.

Requires the existing cmd/reconciler binary. This wrapper NEVER repairs ledger,
replays Kafka, sends data to a third party, or treats INCOMPLETE as success.
"""
import argparse
import datetime as dt
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile

UTC = dt.timezone.utc
ALLOWED_STATUSES = {"PASS": 0, "FAIL": 2, "INCOMPLETE": 3}
CRITICAL = {"missing", "orphan", "conflict", "mismatch", "outbox_missing"}
TRANSIENT = {"pending", "in_flight"}
KNOWN = CRITICAL | TRANSIENT | {"matched", "replayed"}


def select_window(now, window_minutes, lag_minutes):
    """Select inclusive start, exclusive end, aligned to minute boundaries."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("timezone-aware current instant required")
    if not isinstance(window_minutes, int) or not 1 <= window_minutes <= 1440:
        raise ValueError("window must be 1..1440 minutes")
    if not isinstance(lag_minutes, int) or not 5 <= lag_minutes <= 1440:
        raise ValueError("observation lag must be 5..1440 minutes")
    # Align to stable UTC epoch slots, not the actual CronJob start minute.
    # Otherwise a 4-minute cluster scheduling delay shifts every boundary
    # and creates overlaps/gaps even if the CronJob runs hourly.
    eligible = now.astimezone(UTC) - dt.timedelta(minutes=lag_minutes)
    elapsed_minutes = int(eligible.timestamp() // 60)
    closed_slot_minutes = (elapsed_minutes // window_minutes) * window_minutes
    end = dt.datetime.fromtimestamp(closed_slot_minutes * 60, tz=UTC)
    start = end - dt.timedelta(minutes=window_minutes)
    return start, end


def _parsed_timestamp(value):
    if not isinstance(value, str):
        raise ValueError("missing UTC timestamp")
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp without timezone")
    return parsed.astimezone(UTC)


def sanitize_report(report, start, end, returncode):
    """Validate upstream report/exit status before emitting redacted evidence."""
    if not isinstance(report, dict) or report.get("schema_version") != 1:
        raise ValueError("invalid report schema")
    if _parsed_timestamp(report.get("start")) != start or _parsed_timestamp(report.get("end")) != end:
        raise ValueError("reconciler returned a different window")
    status = report.get("result")
    if status not in ALLOWED_STATUSES or returncode != ALLOWED_STATUSES[status]:
        raise ValueError("report status and reconciler process exit code conflict")
    counts = report.get("counts")
    if not isinstance(counts, dict) or any(
        k not in KNOWN or isinstance(v, bool) or not isinstance(v, int) or v < 0
        for k, v in counts.items()
    ):
        raise ValueError("invalid finding counts")
    for key in ("ledger_events", "analytics_events"):
        value = report.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("invalid source event totals")
    derived = ("FAIL" if any(counts.get(k, 0) for k in CRITICAL)
               else "INCOMPLETE" if any(counts.get(k, 0) for k in TRANSIENT)
               else "PASS")
    if status != derived:
        raise ValueError("result disagrees with categorized findings")
    # Never include event IDs, tenant/project IDs, raw DLQ payloads, or
    # error strings originating in a database's response.
    return {
        "status": status,
        "schema_version": 1,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "ledger_events": report["ledger_events"],
        "analytics_events": report["analytics_events"],
        "counts": counts,
        "reconciler_exit_code": returncode,
        "findings_redacted": True,
    }


def _atomic_receipt(path, receipt):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    candidate = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=".m6-",
            suffix=".json", delete=False,
        ) as stream:
            candidate = stream.name
            os.chmod(candidate, 0o600)
            json.dump(receipt, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(candidate, path)
    finally:
        if candidate and os.path.exists(candidate):
            os.unlink(candidate)


def run(*, now, window_minutes, lag_minutes, max_records, grace,
        reconciler_bin, output, environ):
    """Return 0 PASS, 2 FAIL, 3 INCOMPLETE, 1 for an unsafe/unknown outcome."""
    start, end = select_window(now, window_minutes, lag_minutes)
    receipt = {
        "schema_version": 1,
        "status": "ERROR",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "window_minutes": window_minutes,
        "observation_lag_minutes": lag_minutes,
        "head_sha": environ.get("GITHUB_SHA", "local"),
        "run_id": environ.get("GITHUB_RUN_ID", "local"),
        "findings_redacted": True,
    }
    try:
        if not 1 <= max_records <= 10000:
            raise ValueError("max-records must be 1..10000")
        if not re.fullmatch(r"(?:0|[1-9][0-9]*)(?:s|m|h)", grace):
            raise ValueError("grace must be a bounded duration such as 3m")
        if not environ.get("POSTGRES_DSN") or not environ.get("CLICKHOUSE_HTTP"):
            raise ValueError("POSTGRES_DSN and CLICKHOUSE_HTTP are required")
        command = [
            reconciler_bin, "--start", start.isoformat(),
            "--end", end.isoformat(), "--grace", grace,
            "--max-records", str(max_records),
        ]
        finished = subprocess.run(
            command, env=dict(environ), capture_output=True, text=True,
            timeout=95, check=False,
        )
        if len(finished.stdout) > 8 * 1024 * 1024:
            raise ValueError("reconciler response exceeds evidence size ceiling")
        data = json.loads(finished.stdout)
        clean = sanitize_report(data, start, end, finished.returncode)
        receipt.update(clean)
        # Only known exit semantics can affect CI / Kubernetes Job status.
        result_code = finished.returncode
    except (ValueError, TypeError, json.JSONDecodeError):
        receipt["status"] = "ERROR"
        receipt["error_class"] = "invalid_reconciliation_result_or_configuration"
        result_code = 1
    except subprocess.TimeoutExpired:
        receipt["status"] = "ERROR"
        receipt["error_class"] = "reconciler_timeout"
        result_code = 1
    except OSError:
        receipt["status"] = "ERROR"
        receipt["error_class"] = "reconciler_launch_or_file_error"
        result_code = 1
    _atomic_receipt(output, receipt)
    # This JSON is deliberately summary-only: no DSN, credentials or event IDs.
    print(json.dumps(receipt, sort_keys=True))
    return result_code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window-minutes", type=int, default=60)
    parser.add_argument("--lag-minutes", type=int, default=15)
    parser.add_argument("--max-records", type=int, default=1000)
    parser.add_argument("--grace", default="3m")
    parser.add_argument("--reconciler-bin", default="/usr/local/bin/reconciler")
    parser.add_argument("--output", default="/tmp/m6-reconciliation.json")
    parser.add_argument("--as-of", help="explicit historical UTC RFC3339 instant for controlled backfill/testing")
    args = parser.parse_args()
    if args.as_of:
        try:
            now = _parsed_timestamp(args.as_of)
        except ValueError:
            parser.error("--as-of must be timezone-aware RFC3339")
    else:
        now = dt.datetime.now(UTC)
    try:
        code = run(
            now=now, window_minutes=args.window_minutes,
            lag_minutes=args.lag_minutes, max_records=args.max_records,
            grace=args.grace, reconciler_bin=args.reconciler_bin,
            output=args.output, environ=os.environ,
        )
    except ValueError as exc:
        # Never display external credentials on a bad argument.
        parser.error(str(exc))
    raise SystemExit(code)


if __name__ == "__main__":
    main()
