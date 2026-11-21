"""M8 paced-load/P95/P99 SLO acceptance contract. No external services."""
import importlib.util
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parent
def read_module(name, file):
    spec=importlib.util.spec_from_file_location(name, ROOT / file)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

perf=read_module("m8perf","m5_benchmark.py")
sl=read_module("m8slo","m8_slo_eval.py")


class PacedLoadTests(unittest.TestCase):
    def test_pacing_plan_is_evenly_spaced_and_covers_duration(self):
        p=perf.pacing_plan(1200,20,60)
        self.assertEqual(len(p),1200)
        self.assertAlmostEqual(p[0],0)
        self.assertAlmostEqual(p[1],0.05)
        self.assertAlmostEqual(p[-1],59.95)

    def test_mismatched_sample_size_and_invalid_rate_fail_closed(self):
        for count,rate,duration in [(599,20,30),(1201,20,60),(1200,0,60),
                                    (1200,-20,60),(1200,float("nan"),60),
                                    (1200,20,29),(1200,20,1201)]:
            with self.subTest(count=count,rate=rate,duration=duration):
                with self.assertRaises(ValueError):
                    perf.pacing_plan(count,rate,duration)

    def test_percentile_sampling_requires_sufficient_observations(self):
        self.assertEqual(perf.percentile(list(range(1,1001)),.99),990)


class SustainedSLORuns(unittest.TestCase):
    def report(self,run,**changes):
        receipt=dict(schema_version=1,head_sha="abc123",
                     seed=f"perf-test-{run}",submitted=1125,accepted=1125,visible=1125,
                     correctness="PASS_CORRECTNESS",slo="NOT_SET",
                     steady_target_rps=25.0,steady_duration_seconds=45.0,
                     steady_observed_send_duration_seconds=44.96,
                     steady_schedule_lag_p99_ms=6.0,
                     acceptance_throughput_events_s=24.95,
                     visibility_query_errors=0,
                     concurrency=8,partitions=6,consumer_batch_size=64,
                     relay_claim_limit=5,relay_batch_timeout="5ms",
                     ack_p95_ms=75,ack_p99_ms=110,visible_p95_ms=2500,
                     visible_p99_ms=4200,drain_seconds=0.5,
                     resources={"api":{"samples":10},"relay":{"samples":10},"consumer":{"samples":10}})
        receipt.update(changes)
        return receipt

    def test_no_thresholds_is_not_a_claimed_slo_pass(self):
        receipt=sl.evaluate([self.report(i) for i in range(3)],None)
        self.assertEqual(receipt["slo"],"NOT_SET")
        self.assertEqual(receipt["correctness"],"PASS_CORRECTNESS")
        self.assertEqual(receipt["worst_p99_ms"]["visible_p99_ms"],4200)

    def test_all_three_windows_must_pass_requested_p95_p99(self):
        metrics={"ack_p95_ms":200,"ack_p99_ms":300,
                 "visible_p95_ms":4500,"visible_p99_ms":5000,
                 "steady_schedule_lag_p99_ms":50}
        self.assertEqual(sl.evaluate([self.report(i) for i in range(3)],metrics)["slo"],"PASS")
        slow=self.report(2,visible_p99_ms=6000)
        self.assertEqual(sl.evaluate([self.report(0),self.report(1),slow],metrics)["slo"],"FAIL")

    def test_insufficient_events_or_trials_never_pass(self):
        with self.assertRaises(ValueError):
            sl.evaluate([self.report(0),self.report(1)],{"ack_p95_ms":200})
        bad=self.report(1,submitted=128,accepted=128,visible=128)
        with self.assertRaises(ValueError):
            sl.evaluate([self.report(0),bad,self.report(2)],{"ack_p95_ms":200})

    def test_missing_visibility_or_client_overload_never_pass(self):
        incomplete=self.report(1,visible=1124,correctness="INCOMPLETE")
        with self.assertRaises(ValueError):
            sl.evaluate([self.report(0),incomplete,self.report(2)],{"ack_p95_ms":200})
        late=self.report(1,steady_schedule_lag_p99_ms=550,
                         acceptance_throughput_events_s=18)
        with self.assertRaises(ValueError):
            sl.evaluate([self.report(0),late,self.report(2)],{"ack_p95_ms":200})

    def test_different_profiles_or_git_sha_do_not_mix(self):
        mismatches=(dict(head_sha="other"),dict(partitions=12),
                    dict(relay_batch_timeout="100ms"),dict(steady_target_rps=30))
        for changed in mismatches:
            with self.subTest(changed=changed):
                with self.assertRaises(ValueError):
                    sl.evaluate([self.report(0),self.report(1,**changed),self.report(2)],None)

    def test_query_failures_or_missing_resources_are_not_valid_samples(self):
        for changed in (dict(visibility_query_errors=2),
                        dict(resources={}),
                        dict(ack_p99_ms=None),
                        dict(steady_duration_seconds=20)):
            with self.subTest(changed=changed):
                with self.assertRaises(ValueError):
                    sl.evaluate([self.report(0),self.report(1,**changed),self.report(2)],None)

    def test_percentiles_are_not_pooled_and_worst_window_controls_slo(self):
        runs=[self.report(0,visible_p95_ms=2100,visible_p99_ms=4100),
              self.report(1,visible_p95_ms=2500,visible_p99_ms=4400),
              self.report(2,visible_p95_ms=2300,visible_p99_ms=4200)]
        x=sl.evaluate(runs,None)
        self.assertEqual(x["worst_p99_ms"]["visible_p99_ms"],4400)
        self.assertEqual(x["per_run"][0]["visible_p99_ms"],4100)
        self.assertEqual(x["median_p99_ms"]["visible_p99_ms"],4200)


if __name__=="__main__":
    unittest.main()
