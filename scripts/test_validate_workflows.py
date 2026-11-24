import tempfile
import unittest
from pathlib import Path

from validate_workflows import validate

GROUP = "group: ${{ github.workflow }}-${{ github.event_name == 'pull_request' && github.ref || github.run_id }}"
CANCEL = "cancel-in-progress: ${{ github.event_name == 'pull_request' }}"


def workflow(pr='  pull_request:\n    paths-ignore:\n      - ".github/workflows/**"', push=""):
    events = pr + ("\n" + push if push else "") + "\n  workflow_dispatch:"
    return f"name: test\non:\n{events}\nconcurrency:\n  {GROUP}\n  {CANCEL}\njobs:\n  test:\n    runs-on: ubuntu-latest\n"


class WorkflowPolicyTests(unittest.TestCase):
    def test_paths_ignore_policy_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "m1.yml").write_text(workflow(push='  push:\n    branches: [master]\n    paths-ignore:\n      - ".github/workflows/**"'))
            self.assertEqual(validate(tmp), [])

    def test_negative_path_pattern_passes(self):
        pr='  pull_request:\n    paths:\n      - "internal/**"\n      - "!.github/workflows/**"'
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "m1.yml").write_text(workflow(pr=pr))
            self.assertEqual(validate(tmp), [])

    def test_unfiltered_pull_request_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "m1.yml").write_text(workflow(pr="  pull_request:"))
            self.assertTrue(any("workflow-only" in e for e in validate(tmp)))

    def test_push_without_workflow_ignore_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "m1.yml").write_text(workflow(push="  push:\n    branches: [master]"))
            self.assertTrue(any("push must exclude" in e for e in validate(tmp)))

    def test_missing_concurrency_is_rejected(self):
        content=workflow().replace("concurrency:\n  "+GROUP+"\n  "+CANCEL+"\n","")
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "m1.yml").write_text(content)
            self.assertTrue(any("concurrency" in e for e in validate(tmp)))


if __name__ == "__main__":
    unittest.main()
