"""Fail-closed aggregate for same-fault Kafka 4.1.x runtime comparisons."""
import importlib.util
import pathlib
import tempfile
import unittest
import json

spec = importlib.util.spec_from_file_location(
    "version_matrix", pathlib.Path(__file__).with_name("m3b_kafka_version_matrix.py")
)
matrix = importlib.util.module_from_spec(spec)
spec.loader.exec_module(matrix)

HEAD = "a" * 40


def report(version, result="FAILED_RUN_RECOVERY_UNVERIFIED", exposure="ADDRESS_REASSIGNMENT_OBSERVED"):
    image = matrix.VERSIONS[version]
    if exposure == "ADDRESS_REASSIGNMENT_OBSERVED":
        changed = ["broker2", "broker3"]
        reassigned = [
            {"ipv4": "172.29.242.12", "before_owner": "broker2", "after_owner": "broker3"},
            {"ipv4": "172.29.242.13", "before_owner": "broker3", "after_owner": "broker2"},
        ]
    elif exposure == "NO_ADDRESS_CHANGE_OBSERVED":
        changed, reassigned = [], []
    else:
        changed, reassigned = ["broker2"], []
    doc = {
        "schema_version": 1,
        "exact_head": HEAD,
        "profile": "dynamic",
        "image_ref": image["image_ref"],
        "repo_digest": image["repo_digest"],
        "image_id": "sha256:" + ("b" if version == "4.1.0" else "c") * 64,
        "platform": "linux/amd64",
        "kafka_version": version,
        "java_runtime_version": "21.0.8+9" if version == "4.1.0" else "21.0.10+7",
        "java_vendor": "Eclipse Adoptium",
        "java_vm_name": "OpenJDK 64-Bit Server VM",
        "endpoint_exposure": exposure,
        "ip_changed_nodes": changed,
        "reassigned_addresses": reassigned,
        "cross_target_voter_request_count": 1000 if result == "FAILED_RUN_RECOVERY_UNVERIFIED" else 0,
        "causal_root_cause_proven": False,
        "acknowledged_readback_verified": result == "RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED",
    }
    if result == "RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED":
        doc["initial_ack_count"] = 35
    else:
        doc["result"] = result
        doc["kafka_report_result"] = "INCOMPLETE"
    if result == "RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED":
        doc["result"] = result
        doc["initial_ack_count"] = 35
    return doc


def six_trials():
    rows = {}
    for version in matrix.VERSIONS:
        for trial in (1, 2, 3):
            result = ("FAILED_RUN_RECOVERY_UNVERIFIED" if version == "4.1.0"
                      else "RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED")
            rows[(version, trial)] = report(version, result)
    return rows


class KafkaVersionMatrixTests(unittest.TestCase):
    def test_same_dynamic_fault_is_summarized_without_causal_claim(self):
        result = matrix.verify(HEAD, six_trials())
        self.assertEqual(result["result"], "VERSION_COMPARISON_EVIDENCE_COMPLETE")
        self.assertEqual(result["versions"]["4.1.0"]["exposed_trials"], 3)
        self.assertEqual(result["versions"]["4.1.0"]["failed"], 3)
        self.assertEqual(result["versions"]["4.1.2"]["exposed_trials"], 3)
        self.assertEqual(result["versions"]["4.1.2"]["recovered"], 3)
        self.assertEqual(result["java_runtime_versions"], {
            "4.1.0": ["21.0.8+9"], "4.1.2": ["21.0.10+7"]
        })
        self.assertFalse(result["causal_root_cause_proven"])
        self.assertFalse(result["production_ha_proven"])

    def test_wrong_head_is_rejected(self):
        rows = six_trials()
        rows[("4.1.2", 2)]["exact_head"] = "b" * 40
        with self.assertRaises(ValueError):
            matrix.verify(HEAD, rows)

    def test_wrong_digest_or_image_tag_is_rejected(self):
        for field, value in (
            ("repo_digest", "apache/kafka@sha256:" + "d" * 64),
            ("image_ref", "apache/kafka:4.1.2"),
            ("kafka_version", "4.1.0"),
        ):
            rows = six_trials()
            rows[("4.1.2", 1)][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                matrix.verify(HEAD, rows)

    def test_missing_or_inconsistent_jdk_identity_is_rejected(self):
        rows = six_trials()
        rows[("4.1.0", 1)].pop("java_runtime_version")
        with self.assertRaises(ValueError):
            matrix.verify(HEAD, rows)
        rows = six_trials()
        rows[("4.1.2", 2)]["java_runtime_version"] = "21.0.11+1"
        with self.assertRaises(ValueError):
            matrix.verify(HEAD, rows)

    def test_unexposed_trials_do_not_count_toward_minimum(self):
        rows = six_trials()
        rows[("4.1.2", 2)] = report("4.1.2", "RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED",
                                   "NO_ADDRESS_CHANGE_OBSERVED")
        rows[("4.1.2", 3)] = report("4.1.2", "RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED",
                                   "ADDRESS_CHANGE_WITHOUT_REASSIGNMENT")
        with self.assertRaises(ValueError):
            matrix.verify(HEAD, rows)

    def test_unknown_failure_cannot_be_counted_as_expected_dynamic_failure(self):
        rows = six_trials()
        rows[("4.1.0", 1)]["kafka_report_result"] = "UNKNOWN"
        with self.assertRaises(ValueError):
            matrix.verify(HEAD, rows)

    def test_missing_trial_slot_is_rejected(self):
        rows = six_trials()
        rows.pop(("4.1.2", 3))
        with self.assertRaises(ValueError):
            matrix.verify(HEAD, rows)

    def test_loader_requires_exactly_one_receipt_per_version_trial_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            for version in matrix.VERSIONS:
                for trial in (1, 2, 3):
                    artifact = root / f"m3b-kafka-version-{version}-trial-{trial}"
                    diagnostics = artifact / "m3b-parallel-kraft-diagnostics"
                    diagnostics.mkdir(parents=True)
                    (diagnostics / "network-ab-comparison.json").write_text(
                        json.dumps(report(version)), encoding="utf-8"
                    )
            loaded = matrix.load_trial_receipts(root)
            self.assertEqual(set(loaded), {
                (version, trial) for version in matrix.VERSIONS for trial in (1, 2, 3)
            })


if __name__ == "__main__":
    unittest.main()
