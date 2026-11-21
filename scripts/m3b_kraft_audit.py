#!/usr/bin/env python3
"""Read-only KRaft voter identity/DNS audit for disposable three-broker CI.

Compares persisted metadata and Docker DNS snapshots, classifies a bounded
Kafka log tail, never exports raw logs, payloads, credentials or directory IDs.
This cannot inspect JVM DNS caches or prove the causal root cause.
"""
import argparse
import collections
import json
import os
import pathlib
import re
import subprocess
import tempfile

NODES = ("broker1", "broker2", "broker3")
PERSISTED_FIELDS = ("node.id", "cluster.id", "directory.id")
VOTER_CROSS_TARGET = re.compile(
    r"\[RaftManager id=(\d+)\].*?A replica (\d+) sent a voter key"
    r".*?ReplicaKey\(id=(\d+),.*?doesn't match the local key",
    re.IGNORECASE,
)
COMPOSE = ("docker", "compose", "-f", "deployments/docker-compose-ha.yml")


def _complete_snapshot(receipt, expected_phase):
    if not isinstance(receipt, dict) or receipt.get("schema_version") != 1:
        return False
    if receipt.get("phase") not in expected_phase or receipt.get("identity_verdict") != "PASS_IDENTITY":
        return False
    nodes = receipt.get("nodes")
    if not isinstance(nodes, dict) or set(nodes) != set(NODES):
        return False
    for name in NODES:
        node = nodes[name]
        if not isinstance(node, dict) or node.get("errors"):
            return False
        meta = node.get("meta") or {}
        if not all(meta.get(key) for key in PERSISTED_FIELDS):
            return False
        if meta.get("node.id") != name[-1] or not node.get("container_ipv4"):
            return False
        if not all((node.get("dns_from_node") or {}).get(other) for other in NODES):
            return False
    return True


def parse_voter_target_mismatch(logs):
    """Return only normalized cross-target Kafka VOTE mismatch counts.

    Same receiver/target ID and unrelated warnings are not cross-target proof.
    This parser never surfaces persistent IDs or raw log text.
    """
    counts = collections.Counter()
    for line in logs.splitlines():
        match = VOTER_CROSS_TARGET.search(line)
        if not match:
            continue
        receiver, sender, target = (int(p) for p in match.groups())
        if all(1 <= p <= 3 for p in (receiver, sender, target)) and receiver != target:
            counts[(sender, receiver, target)] += 1
    return [
        {"sender_id": sender, "receiver_id": receiver,
         "target_id": target, "count": count}
        for (sender, receiver, target), count in sorted(counts.items())
    ]


def analyze(baseline, later, logs, logs_observed, exact_head):
    base_ok = _complete_snapshot(baseline, {"before_fault"})
    later_ok = _complete_snapshot(later, {"after_success", "after_failure"})
    complete = base_ok and later_ok and logs_observed
    identity_changes, ip_changed, dns_misroutes = [], [], []
    if isinstance(baseline, dict) and isinstance(later, dict):
        base_nodes, later_nodes = baseline.get("nodes") or {}, later.get("nodes") or {}
        for name in NODES:
            old, new = base_nodes.get(name) or {}, later_nodes.get(name) or {}
            old_meta, new_meta = old.get("meta") or {}, new.get("meta") or {}
            for key in PERSISTED_FIELDS:
                if old_meta.get(key) and new_meta.get(key) and old_meta[key] != new_meta[key]:
                    identity_changes.append({"node": name, "field": key})
            if (old.get("container_ipv4") and new.get("container_ipv4")
                    and old["container_ipv4"] != new["container_ipv4"]):
                ip_changed.append(name)
            for target in NODES:
                actual = (later_nodes.get(target) or {}).get("container_ipv4")
                resolved = (new.get("dns_from_node") or {}).get(target)
                if actual and resolved and resolved != actual:
                    dns_misroutes.append({"observer": name, "target": target})
    mismatches = parse_voter_target_mismatch(logs) if logs_observed else []
    if mismatches:
        verdict = "VOTER_TARGET_ID_MISMATCH_OBSERVED"
    elif identity_changes or dns_misroutes:
        verdict = "IDENTITY_OR_DNS_DRIFT_OBSERVED"
    elif not complete:
        verdict = "INCOMPLETE"
    else:
        verdict = "NO_MISMATCH_OBSERVED"
    return {
        "schema_version": 1, "exact_head": exact_head,
        "baseline_phase": baseline.get("phase") if isinstance(baseline, dict) else None,
        "comparison_phase": later.get("phase") if isinstance(later, dict) else None,
        "verdict": verdict, "complete_observations": bool(complete),
        "logs_observed": bool(logs_observed),
        "observed_voter_target_mismatches": mismatches,
        "identity_changes": identity_changes,
        "ip_changed_nodes": ip_changed,
        "dns_misroutes": dns_misroutes,
        "scope": "bounded_log_tail_and_docker_dns_not_jvm_cache_not_causal_proof",
    }


def load_receipt(path):
    try:
        result = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
        return result if isinstance(result, dict) else None
    except (OSError, ValueError, UnicodeError):
        return None


def docker_log_sample():
    try:
        proc = subprocess.run([*COMPOSE, "logs", "--no-color", "--tail", "500", *NODES],
                              capture_output=True, text=True, check=False, timeout=25)
        if proc.returncode != 0:
            return "", False
        return proc.stdout[-2_000_000:], True
    except (OSError, subprocess.TimeoutExpired, UnicodeError):
        return "", False


def persist_atomic(out, result):
    target = pathlib.Path(out)
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    candidate = None
    try:
        with tempfile.NamedTemporaryFile("w", dir=target.parent,
                                         prefix=".m3b-voter-audit-", delete=False) as f:
            candidate = f.name
            os.chmod(candidate, 0o600)
            json.dump(result, f, indent=2, sort_keys=True)
            f.write("\n")
        os.replace(candidate, target)
    finally:
        if candidate and os.path.exists(candidate):
            os.unlink(candidate)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--before", required=True)
    ap.add_argument("--after", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--docker-logs", action="store_true")
    ap.add_argument("--log-file", help="offline replay of debug log samples")
    opts = ap.parse_args()
    if opts.docker_logs == bool(opts.log_file):
        ap.error("provide exactly one of --docker-logs or --log-file")
    if opts.docker_logs:
        logs, observed = docker_log_sample()
    else:
        try:
            logs = pathlib.Path(opts.log_file).read_text(encoding="utf-8")[-2_000_000:]
            observed = True
        except (OSError, UnicodeError):
            logs, observed = "", False
    result = analyze(load_receipt(opts.before), load_receipt(opts.after),
                     logs, observed, os.getenv("M3B_EXACT_HEAD", "UNBOUND"))
    persist_atomic(opts.out, result)
    print(json.dumps({key: result[key] for key in (
        "verdict", "complete_observations", "observed_voter_target_mismatches",
        "identity_changes", "ip_changed_nodes", "dns_misroutes", "exact_head"
    )}, sort_keys=True))
    # Only diagnostic classification; never mutates Kafka or grants a HA pass.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
