#!/usr/bin/env python3
"""Poll an actual Alertmanager webhook receiver; validate firing AND resolution."""
import argparse
import json
import pathlib
import sys
import time
import urllib.error
import urllib.request

URL = "http://127.0.0.1:19099/events"
NAMES = ("KafkaRetentionWarning", "KafkaRetentionCritical", "KafkaLagCollectorUnavailable")

def receive():
    with urllib.request.urlopen(URL, timeout=3) as res:
        payload = json.load(res)
    if payload.get("schema_version") != 1:
        raise ValueError("invalid receiver evidence format")
    return payload

def validate(data):
    deliveries = data.get("deliveries", [])
    for name in NAMES:
        stages = [a["status"] for a in deliveries if a.get("alertname") == name]
        if "firing" not in stages or "resolved" not in stages:
            raise AssertionError(f"missing firing/resolved notifications for {name}: {stages}")
        if stages.index("firing") >= stages.index("resolved"):
            raise AssertionError(f"{name} resolved before firing: {stages}")
    if len(deliveries) < 6:
        raise AssertionError(f"insufficient Alertmanager delivery events: {deliveries}")
    print("PASS: received all 3 actual Alertmanager alerts firing and resolved in order")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--alert", choices=NAMES)
    p.add_argument("--status", choices=("firing","resolved"))
    p.add_argument("--timeout", type=int, default=90)
    p.add_argument("--validate-report")
    a = p.parse_args()
    if a.validate_report:
        validate(json.loads(pathlib.Path(a.validate_report).read_text()))
        return
    if not a.alert or not a.status:
        p.error("--alert and --status are required except in --validate-report mode")
    deadline = time.monotonic() + a.timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            data = receive()
            if any(x.get("alertname") == a.alert and x.get("status") == a.status
                   for x in data["deliveries"]):
                print(f"PASS Alertmanager webhook delivered {a.alert} status={a.status}")
                return
        except (OSError, ValueError, KeyError) as exc:
            last_error = str(exc)
        time.sleep(1)
    raise SystemExit(f"FAIL: no {a.alert} status={a.status} within {a.timeout}s; {last_error}")

if __name__ == "__main__":
    main()
