"""M6 scheduling contract: bounded UTC windows and fail-closed CI-safe evidence."""
import datetime as dt
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import m6_scheduled_reconcile as sched

UTC = dt.timezone.utc

class WindowTests(unittest.TestCase):
    def test_inclusive_exclusive_utc_time_window_is_closed_before_lag(self):
        start, end = sched.select_window(
            dt.datetime(2026, 11, 21, 12, 37, 51, tzinfo=UTC), 60, 15)
        self.assertEqual(start.isoformat(), "2026-11-21T11:00:00+00:00")
        self.assertEqual(end.isoformat(), "2026-11-21T12:00:00+00:00")

    def test_timezone_aware_inputs_normalized_to_utc(self):
        est = dt.timezone(dt.timedelta(hours=-5))
        a, b = sched.select_window(dt.datetime(2026, 11, 21, 7, 37, 59, tzinfo=est), 30, 10)
        self.assertEqual(a.isoformat(), "2026-11-21T11:30:00+00:00")
        self.assertEqual(b.isoformat(), "2026-11-21T12:00:00+00:00")

    def test_late_cronjob_start_does_not_shift_reconciliation_event_time_slot(self):
        on_time = sched.select_window(
            dt.datetime(2026, 11, 21, 12, 15, tzinfo=UTC), 60, 15)
        late = sched.select_window(
            dt.datetime(2026, 11, 21, 12, 19, 58, tzinfo=UTC), 60, 15)
        self.assertEqual(on_time, late)
        self.assertEqual(on_time[0].isoformat(), "2026-11-21T11:00:00+00:00")
        self.assertEqual(on_time[1].isoformat(), "2026-11-21T12:00:00+00:00")

    def test_contiguous_slots_across_hourly_schedules(self):
        slot1 = sched.select_window(dt.datetime(2026, 11, 21, 12, 15, tzinfo=UTC), 60, 15)
        slot2 = sched.select_window(dt.datetime(2026, 11, 21, 13, 15, tzinfo=UTC), 60, 15)
        self.assertEqual(slot1[1], slot2[0])

    def test_unsafe_window_and_lag_rejected(self):
        for win, lag in [(0, 15), (60, 0), (60, 4), (1441, 15), (30, 1441), (-2, 15)]:
            with self.assertRaises(ValueError):
                sched.select_window(dt.datetime(2026, 11, 21, 12, 0, tzinfo=UTC), win, lag)
        with self.assertRaises(ValueError):
            sched.select_window(dt.datetime(2026, 11, 21, 12, 0), 60, 15)

class ReportTests(unittest.TestCase):
    def setUp(self):
        self.start = dt.datetime(2026, 11, 21, 11, 0, tzinfo=UTC)
        self.end = dt.datetime(2026, 11, 21, 12, 0, tzinfo=UTC)

    def report(self, result="FAIL"):
        counts = {"matched": 7, "missing": 0, "pending": 0}
        if result == "FAIL":
            counts = {"matched": 6, "missing": 1, "pending": 0}
        elif result == "INCOMPLETE":
            counts = {"matched": 6, "missing": 0, "pending": 1}
        return {
            "schema_version": 1, "start": self.start.isoformat(),
            "end": self.end.isoformat(), "result": result,
            "ledger_events": 7, "analytics_events": 5,
            "counts": counts,
            "findings": [{"event_id": "sensitive-id", "reason": "private"}]
        }

    def test_verified_summary_keeps_counts_but_drops_identifiers(self):
        evidence = sched.sanitize_report(self.report(), self.start, self.end, 2)
        self.assertEqual(evidence["status"], "FAIL")
        self.assertEqual(evidence["counts"]["missing"], 1)
        self.assertNotIn("findings", evidence)
        self.assertNotIn("sensitive-id", json.dumps(evidence))

    def test_fail_closed_when_exit_code_result_disagree(self):
        with self.assertRaises(ValueError):
            sched.sanitize_report(self.report("FAIL"), self.start, self.end, 0)
        with self.assertRaises(ValueError):
            sched.sanitize_report(self.report("PASS"), self.start, self.end, 2)

    def test_rejects_untrusted_wrong_window_and_incomplete_reports(self):
        for mutation in [
            {"end": "2026-11-21T12:23:00+00:00"},
            {"schema_version": 9},
            {"counts": {"missing": -3}},
            {"ledger_events": -1},
            {"result": "UNKNOWN"},
        ]:
            with self.assertRaises(ValueError):
                sched.sanitize_report({**self.report(), **mutation}, self.start, self.end, 2)

    def test_runner_preserves_nonzero_fail_and_incomplete_exit_codes(self):
        for status, code in [("PASS", 0), ("FAIL", 2), ("INCOMPLETE", 3)]:
            with self.subTest(status=status), tempfile.TemporaryDirectory() as temp:
                path = pathlib.Path(temp) / "evidence.json"
                output = json.dumps(self.report(status))
                finished = subprocess.CompletedProcess(args=["reconciler"], returncode=code, stdout=output, stderr="")
                with mock.patch.object(sched.subprocess, "run", return_value=finished) as spawn:
                    rc = sched.run(
                        now=dt.datetime(2026, 11, 21, 12, 37, tzinfo=UTC),
                        window_minutes=60, lag_minutes=15, max_records=100,
                        grace="3m", reconciler_bin="/bin/fake-reconciler",
                        output=path, environ={
                            "POSTGRES_DSN": "secret-dsn-value", "CLICKHOUSE_HTTP": "http://ch:8123",
                        })
                self.assertEqual(rc, code)
                saved = json.loads(path.read_text())
                self.assertEqual(saved["status"], status)
                self.assertNotIn("secret-dsn-value", json.dumps(saved))
                self.assertNotIn("sensitive-id", json.dumps(saved))
                command = spawn.call_args.args[0]
                self.assertIn("--start", command)
                self.assertIn("--end", command)
                self.assertNotIn("secret-dsn-value", command)

    def test_invalid_json_and_process_timeout_never_emit_pass(self):
        with tempfile.TemporaryDirectory() as temp:
            path = pathlib.Path(temp) / "evidence.json"
            bad = subprocess.CompletedProcess(["reconciler"], 0, stdout="{not-json}", stderr="secret-dsn-value")
            with mock.patch.object(sched.subprocess, "run", return_value=bad):
                rc = sched.run(
                    now=dt.datetime(2026, 11, 21, 12, 37, tzinfo=UTC),
                    window_minutes=60, lag_minutes=15, max_records=100,
                    grace="3m", reconciler_bin="/fake", output=path,
                    environ={"POSTGRES_DSN": "secret-dsn-value", "CLICKHOUSE_HTTP": "http://ch"})
                self.assertEqual(rc, 1)
                self.assertEqual(json.loads(path.read_text())["status"], "ERROR")
            with mock.patch.object(sched.subprocess, "run", side_effect=subprocess.TimeoutExpired("reconciler", 95)):
                rc = sched.run(
                    now=dt.datetime(2026, 11, 21, 12, 37, tzinfo=UTC),
                    window_minutes=60, lag_minutes=15, max_records=100,
                    grace="3m", reconciler_bin="/fake", output=path,
                    environ={"POSTGRES_DSN": "secret-dsn-value", "CLICKHOUSE_HTTP": "http://ch"})
                self.assertEqual(rc, 1)
                self.assertEqual(json.loads(path.read_text())["status"], "ERROR")

    def test_missing_credentials_fail_closed_and_never_launch(self):
        with tempfile.TemporaryDirectory() as temp:
            path = pathlib.Path(temp) / "evidence.json"
            with mock.patch.object(sched.subprocess, "run") as spawn:
                rc = sched.run(
                    now=dt.datetime(2026, 11, 21, 12, 37, tzinfo=UTC),
                    window_minutes=60, lag_minutes=15, max_records=100,
                    grace="3m", reconciler_bin="/fake", output=path, environ={})
                self.assertEqual(rc, 1)
                spawn.assert_not_called()

if __name__ == "__main__":
    unittest.main()
