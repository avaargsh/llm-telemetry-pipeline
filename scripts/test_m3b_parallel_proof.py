"""Fail-closed oracle for a *parallel* restart of two KRaft controllers."""
import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location(
    "m3b_parallel_proof", pathlib.Path(__file__).with_name("m3b_parallel_proof.py"))
proof = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proof)


def good_receipt():
    return {
        "schema_version": 1,
        "scenario": "two_voter_parallel_restart",
        "result": "PASS_EXPERIMENT_ONLY",
        "exact_head": "abc123",
        "initial_leader": 1,
        "initial_replication_factor": 3,
        "initial_isr": 3,
        "stopped_voter_ids": [2, 3],
        "single_compose_restart_request": True,
        "no_ack_during_quorum_loss": True,
        "quorum_loss_probe_outcome": "UNKNOWN_NO_ACK",
        "quorum_status_observed": True,
        "quorum_leader_id": 1,
        "quorum_high_watermark": 19,
        "recovered_replication_factor": 3,
        "recovered_isr": 3,
        "acknowledged_ids": 35,
        "readback_records": 35,
        "all_acknowledged_verified": True,
        "timings_ms": {
            "restart_to_quorum_status_observed": 16000,
            "restart_to_isr3_observed": 22000,
            "restart_to_ack_readback": 25000,
        },
    }


class ParallelRecoveryReceiptTests(unittest.TestCase):
    def test_full_valid_receipt_passes(self):
        self.assertEqual(proof.verify(good_receipt(), "abc123")["verdict"],
                         "PASS_EXPERIMENT_ONLY")

    def test_missing_or_stale_head_is_not_accepted(self):
        for head in ("wrong", "", None):
            with self.subTest(head=head), self.assertRaises(ValueError):
                proof.verify(good_receipt(), head)

    def test_does_not_accept_missing_or_contradictory_safety_proofs(self):
        cases = [
            ("initial_isr", 2), ("initial_replication_factor", 2),
            ("recovered_isr", 2), ("recovered_replication_factor", 2),
            ("acknowledged_ids", 34), ("readback_records", 34),
            ("all_acknowledged_verified", False),
            ("no_ack_during_quorum_loss", False),
            ("quorum_loss_probe_outcome", "ACKED"),
            ("quorum_status_observed", False),
            ("quorum_leader_id", 0), ("quorum_high_watermark", -1),
            ("single_compose_restart_request", False),
            ("result", "INCOMPLETE"),
        ]
        for name, value in cases:
            with self.subTest(name=name), self.assertRaises(ValueError):
                item = good_receipt()
                item[name] = value
                proof.verify(item, "abc123")

    def test_rejects_wrong_stopped_voter_membership(self):
        for stopped in ([1, 2], [2, 2], [2], [2, 3, 1], [0, 3]):
            with self.subTest(stopped=stopped), self.assertRaises(ValueError):
                item = good_receipt()
                item["stopped_voter_ids"] = stopped
                proof.verify(item, "abc123")

    def test_time_order_and_type_are_strict(self):
        for key, value in [
            ("restart_to_quorum_status_observed", -1),
            ("restart_to_isr3_observed", 15000),
            ("restart_to_ack_readback", 20000),
            ("restart_to_isr3_observed", "22000"),
            ("restart_to_ack_readback", True),
        ]:
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                item = good_receipt()
                item["timings_ms"][key] = value
                proof.verify(item, "abc123")

    def test_no_substituting_incomplete_receipt_or_unbound_head(self):
        for change in ({"exact_head": "UNBOUND"}, {"scenario": "staged_restart"},
                       {"schema_version": 9}, {"result": "PASS"}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                item = good_receipt()
                item.update(change)
                proof.verify(item, "abc123")

    def test_rejects_metadata_only_without_acked_readback(self):
        item = good_receipt()
        item["readback_records"] = 0
        item["all_acknowledged_verified"] = False
        with self.assertRaises(ValueError):
            proof.verify(item, "abc123")


if __name__ == "__main__":
    unittest.main()
