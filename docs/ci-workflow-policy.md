# CI workflow queue policy

Tracking: #50. Existing workflows use a workflow-scoped PR-ref group and cancel only superseded pull request runs. A master push, manual run or scheduled run uses its own run ID. Distinct PR refs and unique workflow names prevent group collisions by design.

The repository guard is a strict canonical-format lint (not a general YAML parser or a substitute for hosted scheduler evidence). Run locally:

    python3 -m unittest discover -s scripts -p test_ci_workflow_policy.py
    python3 scripts/ci_workflow_policy.py

The existing Go validation workflow runs both commands after exact-HEAD checkout. The guard rejects duplicate/case-insensitive workflow names, missing PR triggers, unsupported trigger declarations, feature-branch push triggers, changed canonical concurrency expressions, and additional job-level concurrency declarations. Existing path filters, manual/scheduled runs, and job/test bodies are preserved. No new workflow is created.

Historical same-PR scheduler evidence: superseded KRaft run 45330162996 was cancelled and successor 45330945875 launched the full matrix. Keep original #28 RED evidence (run 45310687164). This check does not prove cross-PR scheduler isolation, branch protection, or cancellation of legacy queue entries. Do not close #50 without evidence of the remaining live isolation check or an explicit maintainer decision. Do not create repeated commits simply to provoke CI or cancel current-head/master runs.
