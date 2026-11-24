"""Aggregate independent TTL-treated trials without counting unexposed runs."""
import argparse
import ipaddress
import json
import re
from pathlib import Path

NODES = {"broker1", "broker2", "broker3"}


def _validate_exposure(doc):
    changed = doc.get("ip_changed_nodes")
    reassigned = doc.get("reassigned_addresses")
    if (not isinstance(changed, list)
            or any(not isinstance(node, str) or node not in NODES for node in changed)
            or len(set(changed)) != len(changed)
            or not isinstance(reassigned, list)):
        raise ValueError("invalid endpoint lists")

    exposure = doc.get("endpoint_exposure")
    if exposure == "ADDRESS_REASSIGNMENT_OBSERVED":
        if len(changed) != 2 or len(reassigned) != 2:
            raise ValueError("expected exactly two changed voters and two reassigned addresses")
        addresses, before_owners, after_owners = set(), set(), set()
        for row in reassigned:
            if not isinstance(row, dict):
                raise ValueError("invalid reassignment row")
            ip, old, new = row.get("ipv4"), row.get("before_owner"), row.get("after_owner")
            if not isinstance(ip, str) or not isinstance(old, str) or not isinstance(new, str):
                raise ValueError("invalid reassignment fields")
            address = ipaddress.IPv4Address(ip)
            if (old not in changed or new not in changed or old == new
                    or address in addresses or old in before_owners or new in after_owners):
                raise ValueError("reassignment owners or addresses are inconsistent")
            addresses.add(address)
            before_owners.add(old)
            after_owners.add(new)
        if before_owners != set(changed) or after_owners != set(changed):
            raise ValueError("reassignment does not cover both changed voters")
        return True

    if exposure == "NO_ADDRESS_CHANGE_OBSERVED":
        if changed or reassigned:
            raise ValueError("no-address-change receipt contains endpoint changes")
        return False

    if exposure == "ADDRESS_CHANGE_WITHOUT_REASSIGNMENT":
        if not changed or reassigned:
            raise ValueError("address-change-without-reassignment receipt is contradictory")
        return False

    raise ValueError("unknown endpoint exposure classification")


def verify(head, reports, expected_trials=3, minimum_exposed=2):
    if not isinstance(head, str) or re.fullmatch(r"[0-9a-f]{40}", head) is None:
        raise ValueError("invalid exact HEAD")
    if type(expected_trials) is not int or expected_trials < 1:
        raise ValueError("invalid expected trial count")
    if type(minimum_exposed) is not int or not 1 <= minimum_exposed <= expected_trials:
        raise ValueError("invalid minimum exposed trial count")
    if not isinstance(reports, list) or len(reports) != expected_trials:
        raise ValueError("wrong number of TTL trial receipts")

    exposed = inconclusive = 0
    for doc in reports:
        if not isinstance(doc, dict) or doc.get("exact_head") != head:
            raise ValueError("TTL receipt bound to wrong HEAD")
        if type(doc.get("schema_version")) is not int or doc["schema_version"] != 1:
            raise ValueError("unsupported TTL receipt schema")
        if (doc.get("profile") != "dynamic-ttl-zero"
                or doc.get("result") != "RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED"):
            raise ValueError("TTL trial did not verify Kafka recovery")
        if (type(doc.get("initial_ack_count")) is not int or doc["initial_ack_count"] != 35
                or doc.get("acknowledged_readback_verified") is not True):
            raise ValueError("TTL trial did not verify all 35 ACKed records")
        if (type(doc.get("cross_target_voter_request_count")) is not int
                or doc["cross_target_voter_request_count"] != 0):
            raise ValueError("TTL trial contains voter mismatch or invalid count")
        if (type(doc.get("controller_socket_samples")) is not int
                or doc["controller_socket_samples"] < 3):
            raise ValueError("TTL trial lacks controller socket samples")
        if doc.get("causal_root_cause_proven") is not False:
            raise ValueError("TTL trial makes an unsupported causality claim")

        if _validate_exposure(doc):
            exposed += 1
        else:
            inconclusive += 1

    if exposed < minimum_exposed:
        raise ValueError("insufficient independently exposed TTL trials")
    if exposed + inconclusive != expected_trials:
        raise ValueError("TTL trial accounting mismatch")
    return {
        "exact_head": head,
        "result": "TTL_TREATMENT_EXPOSURE_GATE_PASS",
        "exposed_recovery_trials": exposed,
        "inconclusive_unexposed_trials": inconclusive,
        "required_exposed_trials": minimum_exposed,
        "total_trials": expected_trials,
        "causal_root_cause_proven": False,
        "scope": "CI_Docker_TTL_treatment_evidence_not_production_HA_or_root_cause_proof",
    }


def load_trial_receipts(directory, expected_trials=3):
    root = Path(directory)
    files = sorted(root.rglob("network-ab-comparison.json"))
    expected_artifacts = {
        f"m3b-network-ab-identity-dynamic-ttl-zero-trial-{trial}"
        for trial in range(1, expected_trials + 1)
    }
    if (len(files) != expected_trials
            or {path.parent.name for path in files} != expected_artifacts):
        raise ValueError("expected one receipt from each uniquely named TTL trial artifact")
    return [json.loads(path.read_text(encoding="utf-8")) for path in files]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--head", required=True)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--expected-trials", type=int, default=3)
    parser.add_argument("--minimum-exposed", type=int, default=2)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    try:
        docs = load_trial_receipts(args.directory, args.expected_trials)
        result = verify(args.head, docs, args.expected_trials, args.minimum_exposed)
        output = Path(args.out)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except (OSError, UnicodeError, ValueError, TypeError, AttributeError) as exc:
        parser.exit(2, f"TTL exposure aggregate failed closed: {type(exc).__name__}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
