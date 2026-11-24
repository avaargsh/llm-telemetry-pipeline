#!/usr/bin/env python3
"""Diagnostic-only KRaft snapshot. No broker mutation, storage format or resets.

Captures per-broker persisted node/cluster/directory ID, each container's
Docker IPv4 and DNS view, and KRaft quorum status. No environment variables,
credentials, raw events or broker log text are written to the JSON receipt.
"""
import argparse
import datetime as dt
import ipaddress
import json
import os
import pathlib
import re
import subprocess
import tempfile

NODES = ("broker1", "broker2", "broker3")
KAFKA_IMAGE_REF = "apache/kafka:4.1.0@sha256:bff074a5d0051dbc0bbbcd25b045bb1fe84833ec0d3c7c965d1797dd289ec88f"
KAFKA_IMAGE_REF_PATTERN = re.compile(r"apache/kafka:[0-9]+\.[0-9]+\.[0-9]+@sha256:[0-9a-f]{64}\Z")
META_KEYS = frozenset(("node.id", "cluster.id", "directory.id"))
JAVA_RUNTIME_KEYS = frozenset((
    "java.vendor", "java.version", "java.runtime.version",
    "java.vm.name", "java.vm.vendor", "java.vm.version", "os.arch",
))
QUORUM_KEYS = frozenset(("ClusterId", "LeaderId", "LeaderEpoch",
                         "HighWatermark", "CurrentVoters", "CurrentObservers",
                         "MaxFollowerLag", "MaxFollowerLagTimeMs"))
COMPOSE = ("docker", "compose", "-f", os.getenv("M3B_HA_COMPOSE_FILE", "deployments/docker-compose-ha.yml"))


def expected_kafka_image_ref():
    """Return the selected immutable Kafka image reference, or empty if invalid."""
    image_ref = os.getenv("M3B_EXPECTED_KAFKA_IMAGE_REF", KAFKA_IMAGE_REF)
    return image_ref if KAFKA_IMAGE_REF_PATTERN.fullmatch(image_ref) else ""


def parse_properties(raw):
    parsed = {}
    for line in raw.splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip() in META_KEYS:
            parsed[key.strip()] = value.strip()
    return parsed


def parse_java_properties(raw):
    """Return only bounded, allowlisted Java runtime identity properties."""
    parsed = {}
    for line in raw.splitlines():
        key, sep, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if (sep and key in JAVA_RUNTIME_KEYS and value and len(value) <= 128
                and all(ord(char) >= 32 for char in value)):
            parsed[key] = value
    return parsed


def parse_quorum(raw):
    parsed = {}
    for line in raw.splitlines():
        key, sep, value = line.partition(":")
        if sep and key.strip() in QUORUM_KEYS:
            parsed[key.strip()] = value.strip()[:2048]
    return parsed


def assess(nodes):
    findings = []
    expected_ips = {name: nodes.get(name, {}).get("container_ipv4", "")
                    for name in NODES}
    cluster = None
    directory_seen = set()
    image_ids = set()
    java_runtime_ids = set()
    expected_image_ref = expected_kafka_image_ref()
    if not expected_image_ref:
        findings.append("missing or invalid expected Kafka image reference")
    for name in NODES:
        n = nodes.get(name, {})
        expected = int(name[-1])
        meta = n.get("meta") or {}
        if not all(meta.get(k) for k in META_KEYS):
            findings.append(f"{name}: missing Kafka node identity")
        else:
            if meta["node.id"] != str(expected):
                findings.append(f"{name}: persisted node.id mismatch")
            if cluster is None:
                cluster = meta["cluster.id"]
            elif meta["cluster.id"] != cluster:
                findings.append(f"{name}: cluster.id differs across nodes")
            directory_id = meta["directory.id"]
            if directory_id in directory_seen:
                findings.append(f"{name}: duplicate Kafka directory.id")
            directory_seen.add(directory_id)
        if not expected_ips[name]:
            findings.append(f"{name}: no Docker container IPv4")
        if expected_image_ref and n.get("image_ref") != expected_image_ref:
            findings.append(f"{name}: Kafka image reference differs from expected digest-qualified image")
        image_id = n.get("image_id", "")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
            findings.append(f"{name}: missing Docker image ID")
        else:
            image_ids.add(image_id)
        java_runtime = n.get("java_runtime")
        if (not isinstance(java_runtime, dict)
                or not all(isinstance(java_runtime.get(key), str)
                           and java_runtime[key].strip()
                           for key in JAVA_RUNTIME_KEYS)):
            findings.append(f"{name}: missing or incomplete Java runtime identity")
        else:
            java_runtime_ids.add(tuple(java_runtime[key] for key in sorted(JAVA_RUNTIME_KEYS)))
        for destination in NODES:
            resolved = n.get("dns_from_node", {}).get(destination, "")
            if not resolved:
                findings.append(f"{name}: dns {destination} unavailable")
            elif expected_ips[destination] and resolved != expected_ips[destination]:
                findings.append(f"{name}: dns {destination} does not match container IPv4")
    if len(image_ids) > 1:
        findings.append("Kafka image IDs differ across brokers")
    if len(java_runtime_ids) > 1:
        findings.append("Java runtime identities differ across brokers")
    return findings


def command(argv, timeout=9):
    """No shell, no raw command stderr in receipts."""
    try:
        proc = subprocess.run(argv, capture_output=True, text=True,
                              check=False, timeout=timeout)
        if proc.returncode == 0:
            return proc.stdout[:16384], ""
        return "", "exit_nonzero"
    except subprocess.TimeoutExpired:
        return "", "timeout"
    except OSError:
        return "", "launch_failure"


def container_ipv4(service):
    cid, err = command([*COMPOSE, "ps", "-q", service], timeout=5)
    if err or not cid.strip():
        return "", err or "missing_container"
    output, err = command(
        ["docker", "inspect", "--format", "{{json .NetworkSettings.Networks}}",
         cid.strip()], timeout=5)
    if err:
        return "", err
    try:
        networks = json.loads(output)
        ips = sorted({n.get("IPAddress", "") for n in networks.values()
                      if n.get("IPAddress")})
        return (ips[0], "") if len(ips) == 1 else ("", "missing_or_multiple_ipv4")
    except (TypeError, ValueError, AttributeError):
        return "", "invalid_docker_inspect_json"


def container_image_identity(service):
    cid, err = command([*COMPOSE, "ps", "-q", service], timeout=5)
    if err or not cid.strip():
        return "", "", err or "missing_container"
    output, err = command(
        ["docker", "inspect", "--format", "{{.Config.Image}}|{{.Image}}",
         cid.strip()], timeout=5)
    if err:
        return "", "", err
    image_ref, separator, image_id = output.strip().partition("|")
    if not separator or not image_ref or not image_id:
        return "", "", "invalid_image_identity"
    return image_ref, image_id, ""


def java_runtime_identity(service):
    """Read sanitized JVM properties from the running broker container."""
    try:
        proc = subprocess.run(
            [*COMPOSE, "exec", "-T", service, "java",
             "-XshowSettings:properties", "-version"],
            capture_output=True, text=True, errors="replace",
            check=False, timeout=8,
        )
        if proc.returncode != 0:
            return {}, "exit_nonzero"
        identity = parse_java_properties(proc.stdout + "\n" + proc.stderr)
        if not all(identity.get(key) for key in JAVA_RUNTIME_KEYS):
            return identity, "missing_runtime_properties"
        return identity, ""
    except subprocess.TimeoutExpired:
        return {}, "timeout"
    except OSError:
        return {}, "launch_failure"


def run_context():
    return {
        "exact_head": os.getenv("M3B_EXACT_HEAD", ""),
        "run_id": os.getenv("GITHUB_RUN_ID", ""),
        "run_attempt": os.getenv("GITHUB_RUN_ATTEMPT", ""),
    }


def snapshot(phase):
    nodes = {}
    for node in NODES:
        number = int(node[-1])
        n = {"expected_id": number, "errors": []}
        n["container_ipv4"], err = container_ipv4(node)
        if err:
            n["errors"].append("container_ipv4:" + err)
        n["image_ref"], n["image_id"], err = container_image_identity(node)
        if err:
            n["errors"].append("image_identity:" + err)
        n["java_runtime"], err = java_runtime_identity(node)
        if err:
            n["errors"].append("java_runtime:" + err)
        meta, err = command([*COMPOSE, "exec", "-T", node,
                             "cat", "/tmp/kraft-combined-logs/meta.properties"])
        n["meta"] = parse_properties(meta)
        if err:
            n["errors"].append("meta_properties:" + err)
        quorum, err = command([*COMPOSE, "exec", "-T", node,
                               "/opt/kafka/bin/kafka-metadata-quorum.sh",
                               "--bootstrap-server", f"{node}:19092",
                               "describe", "--status"], timeout=12)
        n["quorum"] = parse_quorum(quorum)
        if err:
            n["errors"].append("metadata_quorum:" + err)
        resolved = {}
        for target in NODES:
            stdout, err = command([*COMPOSE, "exec", "-T", node,
                                   "getent", "hosts", target], timeout=5)
            found = ""
            if not err:
                for line in stdout.splitlines():
                    value = line.split()[0] if line.split() else ""
                    try:
                        if ipaddress.ip_address(value).version == 4:
                            found = value
                            break
                    except ValueError:
                        continue
            resolved[target] = found
            if err:
                n["errors"].append(f"dns_{target}:{err}")
        n["dns_from_node"] = resolved
        nodes[node] = n

    findings = assess(nodes)
    incomplete = any(n["errors"] for n in nodes.values())
    return {
        "schema_version": 1,
        "phase": phase,
        "git_sha": os.getenv("GITHUB_SHA", "local"),
        **run_context(),
        "captured_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "identity_verdict": ("FAIL_IDENTITY" if findings else
                             "INCOMPLETE" if incomplete else "PASS_IDENTITY"),
        "findings": findings,
        "nodes": nodes,
        "scope": "diagnostic_only_not_HA_proof",
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--phase", required=True)
    ap.add_argument("--out-dir", default="/tmp/m3b-kraft-diagnostics")
    args = ap.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]{2,50}", args.phase):
        ap.error("phase must be a safe short label")
    receipt = snapshot(args.phase)
    folder = pathlib.Path(args.out_dir)
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = folder / f"{args.phase}.json"
    candidate = None
    try:
        with tempfile.NamedTemporaryFile("w", dir=folder, prefix=".m3b-",
                                         suffix=".json", delete=False) as stream:
            candidate = stream.name
            os.chmod(candidate, 0o600)
            json.dump(receipt, stream, indent=2, sort_keys=True)
            stream.write("\n")
        os.replace(candidate, target)
    finally:
        if candidate and os.path.exists(candidate):
            os.unlink(candidate)
    # Print only vetted status fields. This survives CI failures even when
    # the compressed artifact cannot be retrieved by a reviewer.
    compact = {
        "phase": args.phase,
        "identity_verdict": receipt["identity_verdict"],
        "findings": receipt["findings"],
        "node_status": {
            name: {
                "docker_ipv4": node["container_ipv4"],
                "kafka_image_ref": node["image_ref"],
                "docker_image_id": node["image_id"],
                "java_runtime": node["java_runtime"],
                "persisted_node_id": node["meta"].get("node.id"),
                "directory_id": node["meta"].get("directory.id"),
                "quorum_leader_id": node["quorum"].get("LeaderId"),
                "quorum_high_watermark": node["quorum"].get("HighWatermark"),
                "errors": node["errors"],
            } for name, node in receipt["nodes"].items()
        },
        "receipt": str(target),
    }
    print(json.dumps(compact, sort_keys=True))
    # Evidence collection is non-interfering: the HA test, not the
    # observation tool, decides whether an experiment has passed.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
