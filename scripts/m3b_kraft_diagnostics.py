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
DEFAULT_KAFKA_IMAGE_REF = "apache/kafka:4.1.0@sha256:bff074a5d0051dbc0bbbcd25b045bb1fe84833ec0d3c7c965d1797dd289ec88f"
KAFKA_IMAGE_REF = DEFAULT_KAFKA_IMAGE_REF
META_KEYS = frozenset(("node.id", "cluster.id", "directory.id"))
QUORUM_KEYS = frozenset(("ClusterId", "LeaderId", "LeaderEpoch",
                         "HighWatermark", "CurrentVoters", "CurrentObservers",
                         "MaxFollowerLag", "MaxFollowerLagTimeMs"))
COMPOSE = ("docker", "compose", "-f", os.getenv("M3B_HA_COMPOSE_FILE", "deployments/docker-compose-ha.yml"))


def parse_properties(raw):
    parsed = {}
    for line in raw.splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip() in META_KEYS:
            parsed[key.strip()] = value.strip()
    return parsed


def parse_quorum(raw):
    parsed = {}
    for line in raw.splitlines():
        key, sep, value = line.partition(":")
        if sep and key.strip() in QUORUM_KEYS:
            parsed[key.strip()] = value.strip()[:2048]
    return parsed


def expected_image_ref():
    return os.getenv("M3B_KAFKA_IMAGE", DEFAULT_KAFKA_IMAGE_REF)


def image_reference_identity(image_ref):
    match = re.fullmatch(r"apache/kafka:(\d+\.\d+\.\d+)@sha256:([0-9a-f]{64})",
                         image_ref or "")
    if not match:
        return "", ""
    return match.group(1), "apache/kafka@sha256:" + match.group(2)


def parse_java_identity(raw):
    allowed = {
        "java.runtime.version": "java_runtime_version",
        "java.vendor": "java_vendor",
        "java.vm.name": "java_vm_name",
    }
    result = {}
    for line in raw.splitlines():
        key, separator, value = line.partition("=")
        if separator and key.strip() in allowed:
            result[allowed[key.strip()]] = value.strip()[:160]
    return result


def container_runtime_identity(service):
    kafka, err = command([*COMPOSE, "exec", "-T", service,
                          "/opt/kafka/bin/kafka-topics.sh", "--version"], timeout=10)
    if err:
        return {}, "kafka_version:" + err
    match = re.search(r"(?m)^\s*(\d+\.\d+\.\d+)\s*$", kafka)
    if not match:
        return {}, "invalid_kafka_version"
    java, err = command([*COMPOSE, "exec", "-T", service,
                         "java", "-XshowSettings:properties", "-version"],
                        timeout=10, include_stderr=True)
    if err:
        return {}, "java_identity:" + err
    identity = parse_java_identity(java)
    required = {"java_runtime_version", "java_vendor", "java_vm_name"}
    if set(identity) != required or not all(identity.values()):
        return {}, "incomplete_java_identity"
    identity["kafka_version"] = match.group(1)
    return identity, ""


def assess(nodes):
    findings = []
    expected_ips = {name: nodes.get(name, {}).get("container_ipv4", "")
                    for name in NODES}
    expected_ref = expected_image_ref()
    expected_version, expected_repo_digest = image_reference_identity(expected_ref)
    if not expected_version:
        findings.append("configured Kafka image must use an exact version and sha256 digest")
    cluster = None
    directory_seen = set()
    image_ids, repo_digests, platforms = set(), set(), set()
    kafka_versions, java_identities = set(), set()
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
        if n.get("image_ref") != expected_ref:
            findings.append(f"{name}: Kafka image reference differs from configured pinned digest")
        repo_digest = n.get("repo_digest", "")
        if repo_digest != expected_repo_digest:
            findings.append(f"{name}: actual repo digest differs from requested image digest")
        else:
            repo_digests.add(repo_digest)
        image_id = n.get("image_id", "")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
            findings.append(f"{name}: missing Docker image ID")
        else:
            image_ids.add(image_id)
        platform = n.get("platform", "")
        if not re.fullmatch(r"linux/(?:amd64|arm64)", platform):
            findings.append(f"{name}: missing or unsupported Docker image platform")
        else:
            platforms.add(platform)
        kafka_version = n.get("kafka_version", "")
        if kafka_version != expected_version:
            findings.append(f"{name}: Kafka runtime version does not match pinned image tag")
        elif kafka_version:
            kafka_versions.add(kafka_version)
        java_identity = tuple(n.get(key, "") for key in (
            "java_runtime_version", "java_vendor", "java_vm_name"))
        if not all(java_identity):
            findings.append(f"{name}: incomplete Java runtime identity")
        else:
            java_identities.add(java_identity)
        for destination in NODES:
            resolved = n.get("dns_from_node", {}).get(destination, "")
            if not resolved:
                findings.append(f"{name}: dns {destination} unavailable")
            elif expected_ips[destination] and resolved != expected_ips[destination]:
                findings.append(f"{name}: dns {destination} does not match container IPv4")
    if len(image_ids) > 1:
        findings.append("Kafka image IDs differ across brokers")
    if len(repo_digests) > 1:
        findings.append("Kafka repo digests differ across brokers")
    if len(platforms) > 1:
        findings.append("Kafka image platforms differ across brokers")
    if len(kafka_versions) > 1:
        findings.append("Kafka binary versions differ across brokers")
    if len(java_identities) > 1:
        findings.append("Java runtime identity differs across brokers")
    return findings


def command(argv, timeout=9, include_stderr=False):
    """No shell; stderr is included only for allowlisted runtime-version parsing."""
    try:
        proc = subprocess.run(argv, capture_output=True, text=True,
                              check=False, timeout=timeout)
        if proc.returncode == 0:
            output = proc.stdout + (proc.stderr if include_stderr else "")
            return output[:16384], ""
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
        return "", "", "", "", err or "missing_container"
    output, err = command(
        ["docker", "inspect", "--format",
         "{{.Config.Image}}|{{.Image}}|{{.Os}}/{{.Architecture}}",
         cid.strip()], timeout=5)
    if err:
        return "", "", "", "", err
    fields = output.strip().split("|")
    if len(fields) != 3 or not all(fields):
        return "", "", "", "", "invalid_image_identity"
    image_ref, image_id, platform = fields
    repo_output, err = command(
        ["docker", "image", "inspect", "--format", "{{json .RepoDigests}}",
         image_id], timeout=5)
    if err:
        return "", "", "", "", "repo_digest:" + err
    try:
        repo_digests = json.loads(repo_output)
    except (TypeError, ValueError):
        return "", "", "", "", "invalid_repo_digests"
    _, expected_repo_digest = image_reference_identity(expected_image_ref())
    matches = [item for item in repo_digests
               if isinstance(item, str) and item == expected_repo_digest]
    if len(matches) != 1:
        return "", "", "", "", "requested_digest_not_present_in_repo_digests"
    return image_ref, image_id, matches[0], platform, ""


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
        (n["image_ref"], n["image_id"], n["repo_digest"],
         n["platform"], err) = container_image_identity(node)
        if err:
            n["errors"].append("image_identity:" + err)
        runtime, err = container_runtime_identity(node)
        n.update(runtime)
        if err:
            n["errors"].append("runtime_identity:" + err)
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
                "kafka_repo_digest": node["repo_digest"],
                "docker_image_id": node["image_id"],
                "platform": node["platform"],
                "kafka_version": node["kafka_version"],
                "java_runtime_version": node["java_runtime_version"],
                "java_vendor": node["java_vendor"],
                "java_vm_name": node["java_vm_name"],
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
