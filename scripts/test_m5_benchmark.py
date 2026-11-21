"""Deterministic, dependency-free M5 reporting tests (not performance claims)."""
import importlib.util
import pathlib
import unittest


spec = importlib.util.spec_from_file_location("m5_perf", pathlib.Path(__file__).with_name("m5_benchmark.py"))
perf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(perf)


class PerformanceMathTests(unittest.TestCase):
    def test_nearest_rank_percentiles_are_not_averages(self):
        self.assertEqual(perf.percentile([1, 5, 10, 20], 0.50), 5)
        self.assertEqual(perf.percentile([1, 5, 10, 20], 0.95), 20)
        self.assertEqual(perf.percentile([1, 5, 10, 20], 0.99), 20)

    def test_refuse_missing_and_invalid_latency_samples(self):
        with self.assertRaises(ValueError):
            perf.percentile([], 0.95)
        with self.assertRaises(ValueError):
            perf.percentile([10, -1], 0.99)
        with self.assertRaises(ValueError):
            perf.percentile([1], 0)

    def test_slo_is_unrated_when_thresholds_missing(self):
        report = dict(submitted=2, accepted=2, visible=2,
                      ack_p95_ms=200.0, visible_p95_ms=550.0,
                      drain_seconds=0.1)
        self.assertEqual(perf.classify(report, {}), ("PASS_CORRECTNESS", "NOT_SET"))
        self.assertEqual(perf.classify(report, {"ack_p95_ms": 250.0}), ("PASS_CORRECTNESS", "PASS"))

    def test_failed_and_incomplete_cannot_pass_slo(self):
        base = dict(submitted=2, accepted=2, visible=2,
                    ack_p95_ms=200.0, visible_p95_ms=550.0,
                    drain_seconds=0.1)
        self.assertEqual(perf.classify({**base, "accepted": 1}, {"ack_p95_ms": 500}), ("FAIL", "NOT_EVALUABLE"))
        self.assertEqual(perf.classify({**base, "visible": 1}, {"ack_p95_ms": 500}), ("INCOMPLETE", "NOT_EVALUABLE"))
        self.assertEqual(perf.classify(base, {"visible_p95_ms": 500}), ("PASS_CORRECTNESS", "FAIL"))

    def test_scope_credentials_required_for_every_event(self):
        creds = [{"token": "x" * 64, "tenant_id": "tenant_001", "project_id": "project_001"}]
        scopes = perf.build_scopes(creds)
        self.assertEqual(perf.token_for({"tenant_id": "tenant_001", "project_id": "project_001"}, scopes), "x" * 64)
        with self.assertRaises(ValueError):
            perf.token_for({"tenant_id": "tenant_002", "project_id": "project_001"}, scopes)
        with self.assertRaises(ValueError):
            perf.build_scopes([creds[0], creds[0]])

    def test_sql_request_prefix_rejects_injection(self):
        self.assertIn("startsWith", perf.visibility_query("perf-abc_123", 1700000000000, 1700000001000))
        with self.assertRaises(ValueError):
            perf.visibility_query("x'); DROP TABLE event_outbox; --", 0, 1000)


if __name__ == "__main__":
    unittest.main()
