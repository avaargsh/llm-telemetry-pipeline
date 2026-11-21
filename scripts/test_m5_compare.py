import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location("compare", pathlib.Path(__file__).with_name("m5_compare.py"))
compare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compare)


def receipt():
    return dict(
        schema_version=1, correctness="PASS_CORRECTNESS", slo="NOT_SET",
        submitted=100, accepted=100, visible=100,
        concurrency=4, partitions=6, consumer_batch_size=32,
        relay_claim_limit=5, relay_batch_timeout="100ms",
        acceptance_throughput_events_s=25.0, ack_p95_ms=20.0,
        ack_p99_ms=30.0, visible_p95_ms=750.0,
        visible_p99_ms=890.0, drain_seconds=1.0,
        head_sha="abc", run_id="local"
    )


class CompareTests(unittest.TestCase):
    def test_comparison_uses_measurements_and_speedup(self):
        baseline = receipt()
        tuned = {**baseline, "acceptance_throughput_events_s": 50.0, "visible_p95_ms": 375.0,
                 "relay_batch_timeout": "5ms"}
        text = compare.markdown([baseline, tuned])
        self.assertIn("2.00x", text)
        self.assertIn("Visible P95 ms", text)

    def test_fail_closed_on_incomplete_or_missing_metrics(self):
        for candidate in [
            {**receipt(), "visible": 99},
            {**receipt(), "correctness": "INCOMPLETE"},
            {**receipt(), "ack_p95_ms": None},
        ]:
            with self.assertRaises(ValueError):
                compare.markdown([candidate])

    def test_different_profile_is_labeled_noncomparable(self):
        other = {**receipt(), "relay_batch_timeout": "5ms"}
        self.assertIn("NOT a controlled before/after", compare.markdown([receipt(), other]))


if __name__ == "__main__":
    unittest.main()
