#!/usr/bin/env python3
"""Fail-closed verifier for the *real* dual-controller restart receipt.

This verifier never manufactures a success record. It consumes one signed-event
Kafka HA test's JSON and rejects partial/metadata-only results.
"""
import argparse
import json
import pathlib


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(value, name):
    require(type(value) is int and value >= 0, "invalid integer for " + name)
    return value


def verify(receipt, expected_head):
    require(isinstance(receipt, dict), "missing experiment receipt")
    require(bool(expected_head) and expected_head != "UNBOUND", "missing exact HEAD")
    require(receipt.get("schema_version") == 1, "unsupported schema")
    require(receipt.get("scenario") == "two_voter_parallel_restart", "wrong experiment")
    require(receipt.get("result") == "PASS_EXPERIMENT_ONLY", "partial experiment cannot pass")
    require(receipt.get("exact_head") == expected_head, "tested HEAD mismatch")
    leader = number(receipt.get("initial_leader"), "initial_leader")
    require(1 <= leader <= 3, "invalid original data leader")
    stopped = receipt.get("stopped_voter_ids")
    require(type(stopped) is list and len(stopped) == 2
            and all(type(x) is int and 1 <= x <= 3 for x in stopped)
            and len(set(stopped)) == 2 and leader not in stopped,
            "must stop both nonleader controller voters")
    require(receipt.get("single_compose_restart_request") is True,
            "not a concurrent restart command")
    require(receipt.get("no_ack_during_quorum_loss") is True,
            "a quorum-loss write was ACKed or not verified")
    require(receipt.get("quorum_loss_probe_outcome") == "UNKNOWN_NO_ACK",
            "ambiguous probe must not be treated as absent or accepted")
    require(receipt.get("quorum_status_observed") is True, "no real quorum CLI observation")
    qleader = number(receipt.get("quorum_leader_id"), "quorum_leader_id")
    require(1 <= qleader <= 3, "invalid quorum leader")
    number(receipt.get("quorum_high_watermark"), "quorum_high_watermark")
    for key in ("initial_replication_factor", "initial_isr",
                "recovered_replication_factor", "recovered_isr"):
        require(number(receipt.get(key), key) == 3, "RF3/ISR3 gate incomplete: " + key)
    require(number(receipt.get("acknowledged_ids"), "acknowledged_ids") == 35,
            "insufficient previously ACKed events")
    require(number(receipt.get("readback_records"), "readback_records") >= 35,
            "ACKed event readback incomplete")
    require(receipt.get("all_acknowledged_verified") is True,
            "broker readback proof absent")
    observed = receipt.get("timings_ms")
    require(isinstance(observed, dict), "missing monotonic timing observations")
    keys = ("restart_to_quorum_status_observed", "restart_to_isr3_observed",
            "restart_to_ack_readback")
    require(set(observed) == set(keys), "unexpected/missing recovery measurements")
    times = [number(observed[key], key) for key in keys]
    require(times == sorted(times), "recovery stages out of order")
    return {
        "verdict": "PASS_EXPERIMENT_ONLY",
        "exact_head": expected_head,
        "stopped_voter_ids": stopped,
        "timings_ms": observed,
        "scope": "disposable_combined_kraft_lab_no_production_RTO",
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("receipt", type=pathlib.Path)
    ap.add_argument("--head", required=True)
    args = ap.parse_args()
    try:
        proof = verify(json.loads(args.receipt.read_text(encoding="utf-8")), args.head)
    except (ValueError, OSError, UnicodeError) as exc:
        ap.exit(2, f"FAIL CLOSED dual-controller recovery evidence: {exc}\n")
    print(json.dumps(proof, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
