import unittest
from pathlib import Path

from ci_workflow_policy import audit_workflows

GOOD = '''name: gate
on:
  push:
    branches: [master]
  pull_request:
  workflow_dispatch:
concurrency:
  group: ${{ github.workflow }}-${{ github.event_name == 'pull_request' && github.ref || github.run_id }}
  cancel-in-progress: ${{ github.event_name == 'pull_request' }}
jobs:
  test:
    runs-on: ubuntu-latest
'''


class WorkflowPolicyTests(unittest.TestCase):
    def audit(self, source):
        return audit_workflows({'test.yml': source})

    def test_canonical_policy_passes(self):
        self.assertEqual(self.audit(GOOD), [])

    def test_feature_push_and_unfiltered_push_are_rejected(self):
        for replacement in ('branches: [master, feature/**]', 'branches: ["**"]', 'paths: [src/**]'):
            with self.subTest(replacement=replacement):
                self.assertTrue(self.audit(GOOD.replace('branches: [master]', replacement)))

    def test_pr_ref_and_run_identity_are_required(self):
        for replacement in ('github.base_ref', 'github.head_ref', 'github.sha', 'github.run_id'):
            with self.subTest(replacement=replacement):
                self.assertTrue(self.audit(GOOD.replace('github.ref', replacement)))
        self.assertTrue(self.audit(GOOD.replace('|| github.run_id', '|| github.ref')))
        self.assertTrue(self.audit(GOOD.replace("cancel-in-progress: ${{ github.event_name == 'pull_request' }}", 'cancel-in-progress: true')))

    def test_workflow_names_are_case_insensitively_unique(self):
        self.assertTrue(audit_workflows({'a.yml': GOOD, 'b.yml': GOOD.replace('name: gate', 'name: GATE')}))

    def test_missing_and_ambiguous_sections_are_rejected(self):
        self.assertTrue(self.audit(GOOD.replace('  pull_request:\n', '')))
        self.assertTrue(self.audit(GOOD.replace('  pull_request:\n', '  pull_request_target:\n')))
        self.assertTrue(self.audit(GOOD + 'concurrency: {}\n'))
        self.assertTrue(self.audit(GOOD.replace('    runs-on:', '    concurrency: shared\n    runs-on:')))
        self.assertTrue(audit_workflows({}))

    def test_existing_path_filtered_and_scheduled_workflows_are_allowed(self):
        source = GOOD.replace('  pull_request:\n', '  pull_request:\n    paths:\n      - "internal/**"\n  schedule:\n    - cron: "17 3 * * 1"\n')
        self.assertEqual(self.audit(source), [])


class ExactCheckoutTests(unittest.TestCase):
    def test_live_alerts_checks_exact_head_before_running_tests(self):
        path = Path(__file__).resolve().parent.parent / '.github' / 'workflows' / 'm4-alertmanager-e2e.yml'
        source = path.read_text(encoding='utf-8')
        expected = '''    steps:
      - uses: actions/checkout@v4
        with:
          ref: ${{ github.event.pull_request.head.sha || github.sha }}
      - name: Verify exact tested HEAD
        env:
          EXPECTED_SHA: ${{ github.event.pull_request.head.sha || github.sha }}
        run: test "$(git rev-parse HEAD)" = "$EXPECTED_SHA"
      - uses: actions/setup-go@v5'''
        self.assertEqual(source.count('uses: actions/checkout@'), 1)
        self.assertIn(expected, source)


if __name__ == '__main__':
    unittest.main()
