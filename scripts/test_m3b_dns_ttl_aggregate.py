import json
import tempfile
import unittest
from pathlib import Path

from m3b_dns_ttl_aggregate import load_trial_receipts, verify

HEAD = "a" * 40


def receipt(exposed=True):
    return {
        "schema_version": 1,
        "exact_head": HEAD,
        "profile": "dynamic-ttl-zero",
        "result": "RECOVERY_VERIFIED_ENDPOINTS_DYNAMIC_OBSERVED",
        "endpoint_exposure": "ADDRESS_REASSIGNMENT_OBSERVED" if exposed else "NO_ADDRESS_CHANGE_OBSERVED",
        "ip_changed_nodes": ["broker1", "broker2"] if exposed else [],
        "reassigned_addresses": [
            {"ipv4": "172.18.0.2", "before_owner": "broker1", "after_owner": "broker2"},
            {"ipv4": "172.18.0.3", "before_owner": "broker2", "after_owner": "broker1"},
        ] if exposed else [],
        "acknowledged_readback_verified": True,
        "initial_ack_count": 35,
        "cross_target_voter_request_count": 0,
        "controller_socket_samples": 12,
        "causal_root_cause_proven": False,
    }


class TTLExposureAggregateTests(unittest.TestCase):
    def test_two_exposed_and_one_unexposed(self):
        result = verify(HEAD, [receipt(), receipt(), receipt(False)])
        self.assertEqual(result["exposed_recovery_trials"], 2)
        self.assertEqual(result["inconclusive_unexposed_trials"], 1)

    def test_one_exposed_is_insufficient(self):
        with self.assertRaises(ValueError):
            verify(HEAD, [receipt(), receipt(False), receipt(False)])

    def test_boolean_false_is_not_a_zero_vote_count(self):
        rows = [receipt(), receipt(), receipt()]
        rows[1]["cross_target_voter_request_count"] = False
        with self.assertRaises(ValueError):
            verify(HEAD, rows)

    def test_socket_and_schema_evidence_are_required(self):
        for key, value in (("controller_socket_samples", 0), ("schema_version", 2)):
            rows = [receipt(), receipt(), receipt()]
            rows[1][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                verify(HEAD, rows)

    def test_exposed_reassignment_must_be_exact_and_consistent(self):
        mutations = [
            ("ip_changed_nodes", ["broker1"]),
            ("reassigned_addresses", [receipt()["reassigned_addresses"][0]]),
            ("reassigned_addresses", [receipt()["reassigned_addresses"][0]] * 2),
            ("reassigned_addresses", [{"ipv4": "not-an-ip", "before_owner": "broker1", "after_owner": "broker2"}] * 2),
            ("reassigned_addresses", [
                {"ipv4": "172.18.0.2", "before_owner": "broker1", "after_owner": "broker2"},
                {"ipv4": "172.18.0.3", "before_owner": "broker1", "after_owner": "broker2"},
            ]),
        ]
        for key, value in mutations:
            rows = [receipt(), receipt(), receipt()]
            rows[1][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                verify(HEAD, rows)

    def test_unexposed_labels_must_match_endpoint_details(self):
        for label, changed in (
            ("NO_ADDRESS_CHANGE_OBSERVED", ["broker1"]),
            ("ADDRESS_CHANGE_WITHOUT_REASSIGNMENT", []),
        ):
            rows = [receipt(), receipt(False), receipt()]
            rows[1]["endpoint_exposure"] = label
            rows[1]["ip_changed_nodes"] = changed
            with self.subTest(label=label), self.assertRaises(ValueError):
                verify(HEAD, rows)

    def test_wrong_head_profile_and_trial_count_rejected(self):
        rows = [receipt(), receipt(), receipt()]
        rows[1]["exact_head"] = "b" * 40
        with self.assertRaises(ValueError):
            verify(HEAD, rows)
        rows = [receipt(), receipt(), receipt()]
        rows[1]["profile"] = "dynamic"
        with self.assertRaises(ValueError):
            verify(HEAD, rows)
        with self.assertRaises(ValueError):
            verify(HEAD, [receipt(), receipt()])

    def test_invalid_thresholds_rejected(self):
        with self.assertRaises(ValueError):
            verify(HEAD, [receipt(), receipt(), receipt()], minimum_exposed=0)
        with self.assertRaises(ValueError):
            verify(HEAD, [receipt(), receipt(), receipt()], minimum_exposed=4)

    def test_artifact_loader_requires_unique_trial_slots(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for trial in (1, 2, 3):
                folder = root / f"m3b-network-ab-identity-dynamic-ttl-zero-trial-{trial}"
                folder.mkdir()
                (folder / "network-ab-comparison.json").write_text(json.dumps(receipt()))
            self.assertEqual(len(load_trial_receipts(root)), 3)
            (root / "m3b-network-ab-identity-dynamic-ttl-zero-trial-2" / "network-ab-comparison.json").unlink()
            with self.assertRaises(ValueError):
                load_trial_receipts(root)


if __name__ == "__main__":
    unittest.main()
