"""M3b diagnostic evidence contract; all cases use synthetic Docker/KRaft data."""
import importlib.util
import os
import pathlib
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "diagnostics", pathlib.Path(__file__).with_name("m3b_kraft_diagnostics.py")
)
diagnostics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diagnostics)


def node(num, ip):
    return {
        "expected_id": num,
        "container_ipv4": ip,
        "image_ref": diagnostics.KAFKA_IMAGE_REF,
        "image_id": "sha256:" + "a" * 64,
        "meta": {
            "node.id": str(num),
            "cluster.id": "test-only-cluster",
            "directory.id": f"unique-directory-{num}",
        },
        "dns_from_node": {
            f"broker{i}": f"172.22.0.{i + 10}" for i in (1, 2, 3)
        },
        "quorum": {
            "LeaderId": "2",
            "HighWatermark": "36",
        },
        "errors": [],
    }


class KafkaMetadataValidationTests(unittest.TestCase):
    def test_run_context_uses_checked_out_head_and_github_attempt(self):
        with patch.dict(os.environ, {"M3B_EXACT_HEAD": "a" * 40,
                                     "GITHUB_RUN_ID": "12345",
                                     "GITHUB_RUN_ATTEMPT": "2"}):
            receipt = diagnostics.run_context()
        self.assertEqual(receipt, {"exact_head": "a" * 40,
                                   "run_id": "12345",
                                   "run_attempt": "2"})
    def test_meta_properties_accepts_only_known_identity_fields(self):
        data = """version=1
cluster.id=test-only-cluster
node.id=2
directory.id=unique-directory-2
other-secret=never-print
"""
        self.assertEqual(diagnostics.parse_properties(data), {
            "cluster.id": "test-only-cluster",
            "node.id": "2",
            "directory.id": "unique-directory-2",
        })

    def test_valid_nodes_have_no_identity_or_dns_mismatch(self):
        nodes = {f"broker{i}": node(i, f"172.22.0.{i+10}") for i in (1, 2, 3)}
        self.assertEqual(diagnostics.assess(nodes), [])

    def test_mutable_kafka_tag_is_rejected(self):
        nodes = {f"broker{i}": node(i, f"172.22.0.{i+10}") for i in (1, 2, 3)}
        nodes["broker2"]["image_ref"] = "apache/kafka:4.1.0"
        findings = diagnostics.assess(nodes)
        self.assertIn("broker2: Kafka image reference differs from pinned digest", findings)

    def test_missing_runtime_image_id_fails_closed(self):
        nodes = {f"broker{i}": node(i, f"172.22.0.{i+10}") for i in (1, 2, 3)}
        nodes["broker1"].pop("image_id")
        findings = diagnostics.assess(nodes)
        self.assertIn("broker1: missing Docker image ID", findings)

    def test_runtime_image_ids_must_match_across_brokers(self):
        nodes = {f"broker{i}": node(i, f"172.22.0.{i+10}") for i in (1, 2, 3)}
        nodes["broker3"]["image_id"] = "sha256:" + "b" * 64
        findings = diagnostics.assess(nodes)
        self.assertIn("Kafka image IDs differ across brokers", findings)

    def test_crossed_dns_detected_even_when_broker_process_alive(self):
        nodes = {f"broker{i}": node(i, f"172.22.0.{i+10}") for i in (1, 2, 3)}
        nodes["broker1"]["dns_from_node"]["broker2"] = nodes["broker3"]["container_ipv4"]
        findings = diagnostics.assess(nodes)
        self.assertIn("broker1: dns broker2 does not match container IPv4", findings)

    def test_duplicate_directory_id_and_wrong_node_id_are_reported(self):
        nodes = {f"broker{i}": node(i, f"172.22.0.{i+10}") for i in (1, 2, 3)}
        nodes["broker2"]["meta"]["directory.id"] = nodes["broker1"]["meta"]["directory.id"]
        nodes["broker3"]["meta"]["node.id"] = "2"
        findings = diagnostics.assess(nodes)
        self.assertIn("broker2: duplicate Kafka directory.id", findings)
        self.assertIn("broker3: persisted node.id mismatch", findings)

    def test_missing_metadata_and_cross_cluster_fail_closed(self):
        nodes = {f"broker{i}": node(i, f"172.22.0.{i+10}") for i in (1, 2, 3)}
        nodes["broker2"]["meta"] = {}
        nodes["broker3"]["meta"]["cluster.id"] = "other-cluster"
        findings = diagnostics.assess(nodes)
        self.assertIn("broker2: missing Kafka node identity", findings)
        self.assertIn("broker3: cluster.id differs across nodes", findings)

    def test_parse_quorum_keeps_only_safe_status_fields(self):
        raw = """ClusterId: good
LeaderId: 2
LeaderEpoch: 7
HighWatermark: 19
CurrentVoters: [1,2,3]
PotentialSecret: never-print
"""
        self.assertEqual(diagnostics.parse_quorum(raw), {
            "ClusterId": "good",
            "LeaderId": "2",
            "LeaderEpoch": "7",
            "HighWatermark": "19",
            "CurrentVoters": "[1,2,3]",
        })


if __name__ == "__main__":
    unittest.main()
