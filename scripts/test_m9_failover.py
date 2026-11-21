#!/usr/bin/env python3
"""M9 contract tests: fail closed on ambiguous ACK, partial ledgers and absent fault evidence."""
import unittest
from m9_failover_client import classify_status, phase_of, percentile_or_none, build_reconciliation


class M9FailoverContractTests(unittest.TestCase):
    def test_ambiguous_http_response_is_not_an_ack(self):
        for status in (0, 408, 429, 500, 502, 503, 504):
            self.assertEqual(classify_status(status), "UNKNOWN")
        self.assertEqual(classify_status(201), "CONFIRMED_CREATED")
        self.assertEqual(classify_status(200), "CONFIRMED_DUPLICATE")
        for status in (400, 401, 403, 409, 422):
            self.assertEqual(classify_status(status), "REJECTED")

    def test_clock_windows_are_half_open(self):
        control = {"fault_injected": 50.0, "primary_isolated": 52.0,
                   "promoted": 55.0, "api_ready": 58.0}
        self.assertEqual(phase_of(49.9, control), "before")
        self.assertEqual(phase_of(50.0, control), "fault")
        self.assertEqual(phase_of(57.9, control), "fault")
        self.assertEqual(phase_of(58.0, control), "recovery")

    def test_empty_window_has_no_made_up_p99(self):
        self.assertIsNone(percentile_or_none([], 0.99))
        self.assertEqual(percentile_or_none([3, 5, 9, 2], 0.99), 9)

    def test_missing_promotion_and_fencing_fail_closed(self):
        records = {"id-1": ("hash1", "hash1", True)}
        expected = {"id-1": "hash1"}
        good = {"fault_injected": 50.0, "primary_isolated": 52.0,
                "promoted": 55.0, "api_ready": 58.0}
        with self.assertRaises(ValueError):
            build_reconciliation(expected, records, dict(good, api_ready=None), 1, 1)
        with self.assertRaises(ValueError):
            build_reconciliation(expected, records, dict(good, primary_isolated=55.0), 1, 1)

    def test_rejects_missing_outbox_hash_and_missing_published_ack(self):
        expected = {"id-1": "hash1", "id-2": "hash2"}
        stamps = {"fault_injected": 50.0, "primary_isolated": 52.0,
                  "promoted": 55.0, "api_ready": 58.0}
        with self.assertRaises(ValueError):
            build_reconciliation(expected, {"id-1": ("hash1", "hash1", True)},
                                 stamps, 2, 2)
        with self.assertRaises(ValueError):
            build_reconciliation(expected, {"id-1": ("hash1", "hash1", True),
                                            "id-2": ("hash2", "wrong", True)},
                                 stamps, 2, 2)
        with self.assertRaises(ValueError):
            build_reconciliation(expected, {"id-1": ("hash1", "hash1", True),
                                            "id-2": ("hash2", "hash2", False)},
                                 stamps, 2, 2)

    def test_one_authoritative_ledger_outbox_per_stable_id(self):
        expected = {"id-1": "hash1", "id-2": "hash2"}
        records = {"id-1": ("hash1", "hash1", True),
                   "id-2": ("hash2", "hash2", True)}
        stamps = {"fault_injected": 50.0, "primary_isolated": 52.0,
                  "promoted": 55.0, "api_ready": 58.0}
        result = build_reconciliation(expected, records, stamps, 2, 2)
        self.assertEqual(result["accepted_unique_ids"], 2)
        self.assertEqual(result["published_with_confirmed_outbox"], 2)
        self.assertEqual(result["observed_acknowledged_id_loss"], 0)


if __name__ == "__main__":
    unittest.main()
