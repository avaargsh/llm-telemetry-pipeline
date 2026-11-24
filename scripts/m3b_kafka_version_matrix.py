"""Fail-closed summary of the same dynamic KRaft fault across pinned Kafka 4.1.x images.

The comparison is observational CI evidence only. The Kafka image also carries
its JDK/base image, so this matrix cannot isolate Kafka-version causality.
"""
import argparse
import ipaddress
import json
import os
import re
import tempfile
from pathlib import Path

NODES = {"broker1", "broker2", "broker3"}
VERSIONS = {
    "4.1.0": {
        "image_ref": "apache/kafka:4.1.0@sha256:bff074a5d0051dbc0bbbcd25b045bb1fe84833ec0d3c7c965d1797dd289ec88f",
        "repo_digest": "apache/kafka@sha256:bff074a5d0051dbc0bbbcd25b045bb1fe84833ec0d3c7c965d1797dd289ec88f",
    },
    "4.1.2": {
        "image_ref": "apache/kafka:4.1.2@sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e",
        "repo_digest": "apache/kafka@sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e",
    },
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def _exposure(row):
    exposure = row.get("endpoint_exposure")
    changed = row.get("ip_changed_nodes")
    reassigned = row.get("reassigned_addresses")
    require(isinstance(changed, list)
            and all(isinstance(node, str) and node in NODES for node in changed)
            and len(set(changed)) == len(changed)
            and isinstance(reassigned, list), "invalid endpoint exposure fields")
    if exposure == "ADDRESS_REASSIGNMENT_OBSERVED":
        require(len(changed) == 2 and len(reassigned) == 2,
                "exposed trial must prove two changed voters and two reassigned addresses")
        addresses, before_owners, after_owners = set(), set(), set()
        for item in reassigned:
            require(isinstance(item, dict), "invalid reassigned address row")
            ip, before, after = item.get("ipv4"), item.get("before_owner"), item.get("after_owner")
            require(isinstance(ip, str) and before in changed and after in changed and before != after,
                    "invalid reassignment ownership")
            address = ipaddress.IPv4Address(ip)
            require(address not in addresses and before not in before_owners and after not in after_owners,
                    "duplicate reassignment address or owner")
            addresses.add(address)
            before_owners.add(before)
            after_owners.add(after)
        require(before_owners == set(changed) and after_owners == set(changed),
                "reassignment does not cover both changed voters")
        return True
    if exposure == "NO_ADDRESS_CHANGE_OBSERVED":
        require(not changed and not reassigned, "no-change receipt contains endpoint changes")
        return False
    if exposure == "ADDRESS_CHANGE_WITHOUT_REASSIGNMENT":
        require(bool(changed) and not reassigned,
                "address-change receipt has contradictory reassignment details")
        return False
    raise ValueError("unknown endpoint exposure classification")


def _classify_trial(head, version, row):
    require(version in VERSIONS, "unexpected Kafka version")
    require(isinstance(row, dict) and row.get("schema_version") == 1
            and row.get("exact_head") == head and row.get("profile") == "dynamic"
            and row.get("causal_root_cause_proven") is False,
            "wrong exact-head/profile/schema or unsupported causality claim")
    expected = VERSIONS[version]
    require(row.get("image_ref") == expected["image_ref"]
            and row.get("repo_digest") == expected["repo_digest"],
            "Kafka image reference or actual repo digest does not match pinned version")
    require(type(row.get("image_id")) is str
            and re.fullmatch(r"sha256:[0-9a-f]{64}", row["image_id"]) is not None,
            "missing runtime Docker image ID")
    require(row.get("platform") == "linux/amd64",
            "version comparison requires the same linux/amd64 platform")
    require(row.get("kafka_version") == version,
            "Kafka binary version does not match pinned image tag")
    for key in ("java_runtime_version", "java_vendor", "java_vm_name"):
        require(isinstance(row.get(key), str) and row[key],
                "missing runtime JDK identity")
    count = row.get("cross_target_voter_request_count")
    require(type(count) is int and count >= 0, "invalid cross-target voter request count")
    exposed = _exposure(row)
    result = row.get("result")
    if result == "RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED":
        require(row.get("acknowledged_readback_verified") is True
                and type(row.get("initial_ack_count")) is int
                and row["initial_ack_count"] == 35,
                "recovered trial lacks 35/35 ACK readback")
        outcome = "recovered"
    elif result == "FAILED_RUN_RECOVERY_UNVERIFIED":
        require(row.get("acknowledged_readback_verified") is False
                and row.get("kafka_report_result") in {"INCOMPLETE", "PASS_EXPERIMENT_ONLY"}
                and count > 0,
                "failed trial does not match the expected exposed dynamic failure signature")
        outcome = "failed"
    else:
        raise ValueError("unknown or incomplete Kafka recovery outcome")
    return {"exposed": exposed, "outcome": outcome}


def verify(head, reports, expected_trials=3, minimum_exposed=2):
    require(isinstance(head, str) and re.fullmatch(r"[0-9a-f]{40}", head) is not None,
            "invalid exact HEAD")
    require(type(expected_trials) is int and expected_trials >= 1,
            "invalid expected trial count")
    require(type(minimum_exposed) is int and 1 <= minimum_exposed <= expected_trials,
            "invalid minimum exposed trial count")
    expected_slots = {(version, trial) for version in VERSIONS
                      for trial in range(1, expected_trials + 1)}
    require(isinstance(reports, dict) and set(reports) == expected_slots,
            "missing or unexpected version/trial slots")

    summary = {}
    for version in VERSIONS:
        exposed = inconclusive = recovered = failed = 0
        java_versions, java_vendors, java_vms = set(), set(), set()
        outcomes = []
        for trial in range(1, expected_trials + 1):
            row = reports[(version, trial)]
            classified = _classify_trial(head, version, row)
            java_versions.add(row["java_runtime_version"])
            java_vendors.add(row["java_vendor"])
            java_vms.add(row["java_vm_name"])
            if classified["exposed"]:
                exposed += 1
                if classified["outcome"] == "recovered":
                    recovered += 1
                else:
                    failed += 1
            else:
                inconclusive += 1
            outcomes.append({
                "trial": trial,
                "exposure": row["endpoint_exposure"],
                "outcome": classified["outcome"],
                "cross_target_voter_request_count": row["cross_target_voter_request_count"],
            })
        require(exposed >= minimum_exposed,
                f"Kafka {version}: insufficient independently exposed trials")
        require(len(java_versions) == len(java_vendors) == len(java_vms) == 1,
                f"Kafka {version}: runtime JDK identity changed across repeated trials")
        summary[version] = {
            **VERSIONS[version],
            "kafka_version": version,
            "java_runtime_version": next(iter(java_versions)),
            "java_vendor": next(iter(java_vendors)),
            "java_vm_name": next(iter(java_vms)),
            "exposed_trials": exposed,
            "inconclusive_trials": inconclusive,
            "recovered": recovered,
            "failed": failed,
            "outcomes": outcomes,
        }

    return {
        "schema_version": 1,
        "exact_head": head,
        "result": "VERSION_COMPARISON_EVIDENCE_COMPLETE",
        "versions": summary,
        "java_runtime_versions": {
            version: [summary[version]["java_runtime_version"]]
            for version in VERSIONS
        },
        "jdk_changed_with_image_version": (
            summary["4.1.0"]["java_runtime_version"] !=
            summary["4.1.2"]["java_runtime_version"]
        ),
        "causal_root_cause_proven": False,
        "production_ha_proven": False,
        "scope": (
            "same isolated Docker dynamic two-voter restart fault on pinned Kafka 4.1.0 and 4.1.2; "
            "the image update also changes its JDK/base image and does not isolate Kafka-version causality"
        ),
    }


def load_trial_receipts(directory, expected_trials=3):
    root = Path(directory)
    files = sorted(root.rglob("network-ab-comparison.json"))
    expected_artifacts = {
        f"m3b-kafka-version-{version}-trial-{trial}"
        for version in VERSIONS for trial in range(1, expected_trials + 1)
    }
    artifact_dirs = {
        next((parent for parent in path.parents if parent.name in expected_artifacts), None)
        for path in files
    }
    require(len(files) == len(expected_artifacts)
            and None not in artifact_dirs
            and {path.name for path in artifact_dirs} == expected_artifacts,
            "expected one comparison receipt from each Kafka-version trial artifact")
    reports = {}
    for path in files:
        artifact_dir = next((parent for parent in path.parents
                             if parent.name in expected_artifacts), None)
        require(artifact_dir is not None, "comparison receipt is outside a named trial artifact")
        match = re.fullmatch(r"m3b-kafka-version-(4\.1\.[02])-trial-(\d+)",
                             artifact_dir.name)
        require(match is not None, "invalid Kafka version trial artifact name")
        version, trial = match.group(1), int(match.group(2))
        slot = (version, trial)
        require(slot not in reports, "duplicate Kafka version trial artifact")
        reports[slot] = json.loads(path.read_text(encoding="utf-8"))
    return reports


def write_atomic(path, data):
    output = Path(path)
    output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    candidate = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=output.parent,
                                         prefix=".m3b-version-", delete=False) as stream:
            candidate = stream.name
            os.chmod(candidate, 0o600)
            json.dump(data, stream, indent=2, sort_keys=True)
            stream.write("\n")
        os.replace(candidate, output)
    finally:
        if candidate and os.path.exists(candidate):
            os.unlink(candidate)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--head", required=True)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--expected-trials", type=int, default=3)
    parser.add_argument("--minimum-exposed", type=int, default=2)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    try:
        rows = load_trial_receipts(args.directory, args.expected_trials)
        result = verify(args.head, rows, args.expected_trials, args.minimum_exposed)
        write_atomic(args.out, result)
    except (OSError, UnicodeError, ValueError, TypeError, AttributeError, KeyError) as exc:
        parser.exit(2, f"Kafka version comparison failed closed: {type(exc).__name__}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
