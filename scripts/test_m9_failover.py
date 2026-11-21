#!/usr/bin/env python3
"""M9 contract tests: fail closed on ambiguous ACK, partial ledgers and absent fault evidence."""
import pathlib
import tempfile
import unittest
from m9_failover_client import (classify_status, phase_of, percentile_or_none,
                                build_reconciliation, summarize_attempts,
                                write_raw_samples, validate_raw_samples,
                                eligible_early_retries, api_recovered_marker_seen)


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


    def test_fault_window_includes_retried_acks_and_is_raw_recomputable(self):
        control = {"fault_injected": 50.0, "primary_isolated": 52.0,
                   "promoted": 55.0, "api_ready": 58.0}
        def attempt(index, event_id, status, classification, started, ended,
                    retry_round=0):
            return {"index": index, "event_id": event_id, "status": status,
                    "classification": classification, "started": started,
                    "ended": ended, "scheduled": started, "retry_round": retry_round}
        attempts = [
            attempt(0, "id-1", 201, "CONFIRMED_CREATED", 49.0, 49.001),
            attempt(1, "id-2", 503, "UNKNOWN", 51.2, 51.3),
            attempt(2, "id-3", 201, "CONFIRMED_CREATED", 60.0, 60.1),
            attempt(1, "id-2", 201, "CONFIRMED_CREATED", 60.4, 60.5, 1),
        ]
        visible = {"id-1": 49.4, "id-2": 61.0, "id-3": 60.5}
        summary = summarize_attempts(attempts, control, visible, 3, 25, 120, 48.0, 0)
        self.assertEqual(summary["window_metrics"]["fault"]["initial_unknown"], 1)
        self.assertEqual(summary["window_metrics"]["fault"]["visible_p99_ms"], 9800.0)
        self.assertAlmostEqual(summary["observed_write_interruption_bracket_s"], 11.099)
        self.assertAlmostEqual(summary["maximum_observed_inter_ack_gap_s"], 11.099)
        self.assertEqual(summary["post_last_ack_trusted_visibility_drain_s"], 0.5)
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "client.json"
            write_raw_samples(attempts, visible, control, 48.0, path)
            self.assertEqual(validate_raw_samples(directory, summary, 3)["raw_attempt_rows"], 4)
            self.assertNotIn("id-2", (path.parent / "attempts.csv").read_text())
            self.assertNotIn("id-2", (path.parent / "visibility.csv").read_text())
            bad = path.parent / "visibility.csv"
            bad.write_text(bad.read_text().replace("9800.000000", "1800.000000"))
            with self.assertRaises(ValueError):
                validate_raw_samples(directory, summary, 3)


    def test_early_retries_follow_recovery_marker_without_changing_identity(self):
        completed = [(1, {"classification": "UNKNOWN", "event_id": "stable-1"}),
                     (2, {"classification": "CONFIRMED_CREATED", "event_id": "stable-2"}),
                     (3, {"classification": "REJECTED", "event_id": "stable-3"})]
        self.assertEqual(eligible_early_retries(completed, set()), [1])
        self.assertEqual(eligible_early_retries(completed, {1}), [])
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "fault-events.jsonl"
            self.assertFalse(api_recovered_marker_seen(path))
            path.write_text('{"event": "fault_injected"}\\n'.replace('\\n', '\n'))
            self.assertFalse(api_recovered_marker_seen(path))
            with path.open("a") as f:
                f.write('{"event": "api_ready"}' + "\n")
            self.assertTrue(api_recovered_marker_seen(path))


if __name__ == "__main__":
    unittest.main()
