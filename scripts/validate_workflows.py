#!/usr/bin/env python3
"""Validate GitHub Actions workflow trigger and concurrency policy."""
from pathlib import Path
import sys

GROUP = "group: ${{ github.workflow }}-${{ github.event_name == 'pull_request' && github.ref || github.run_id }}"
CANCEL = "cancel-in-progress: ${{ github.event_name == 'pull_request' }}"
WORKFLOW_GLOB = ".github/workflows/**"
NEGATIVE_WORKFLOW_GLOB = "!.github/workflows/**"
VALIDATOR = "ci-workflow-validation.yml"


def _section(lines, key, indent=0):
    prefix = " " * indent + key + ":"
    start = next((i for i, line in enumerate(lines) if line == prefix), None)
    if start is None:
        return []
    end = len(lines)
    for i in range(start + 1, len(lines)):
        line = lines[i]
        if indent == 0 and line and not line.startswith(" "):
            end = i
            break
        if indent == 2 and line.startswith("  ") and not line.startswith("    ") and line.strip():
            end = i
            break
    return lines[start:end]


def _has_glob(block, value):
    return any(value in line for line in block)


def validate(root):
    root = Path(root)
    files = sorted(p for p in root.iterdir() if p.suffix in (".yml", ".yaml"))
    errors = []
    if not files:
        return ["no workflow files found"]
    for path in files:
        lines = path.read_text(encoding="utf-8").splitlines()
        text = "\n".join(lines)
        if "concurrency:" not in lines:
            errors.append(f"{path.name}: missing top-level concurrency")
        if GROUP not in text or CANCEL not in text:
            errors.append(f"{path.name}: incorrect PR-scoped concurrency policy")
        on_block = _section(lines, "on", indent=0)
        if not on_block or "  workflow_dispatch:" not in on_block:
            errors.append(f"{path.name}: workflow_dispatch must remain available")
        if path.name == VALIDATOR:
            for event in ("pull_request", "push"):
                block = _section(on_block, event, indent=2)
                if not _has_glob(block, WORKFLOW_GLOB):
                    errors.append(f"{path.name}: {event} must validate workflow-file changes")
            continue
        pr = _section(on_block, "pull_request", indent=2)
        if not pr:
            errors.append(f"{path.name}: missing pull_request trigger")
        elif any(line == "    paths:" for line in pr):
            if any(line == "    paths-ignore:" for line in pr) or not _has_glob(pr, NEGATIVE_WORKFLOW_GLOB):
                errors.append(f"{path.name}: pull_request paths must exclude workflow-only changes")
        elif any(line == "    paths-ignore:" for line in pr):
            if not _has_glob(pr, WORKFLOW_GLOB):
                errors.append(f"{path.name}: pull_request paths-ignore must exclude workflow files")
        else:
            errors.append(f"{path.name}: pull_request must avoid running integration jobs for workflow-only changes")
        push = _section(on_block, "push", indent=2)
        if push:
            if any(line == "    paths:" for line in push):
                if any(line == "    paths-ignore:" for line in push) or not _has_glob(push, NEGATIVE_WORKFLOW_GLOB):
                    errors.append(f"{path.name}: push paths must exclude workflow-only changes")
            elif not any(line == "    paths-ignore:" for line in push) or not _has_glob(push, WORKFLOW_GLOB):
                errors.append(f"{path.name}: push must exclude workflow-only changes")
    return errors


def main():
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".github/workflows")
    errors = validate(root)
    if errors:
        for error in errors:
            print("FAIL:", error, file=sys.stderr)
        return 1
    count = len(list(root.glob("*.yml"))) + len(list(root.glob("*.yaml")))
    print(f"PASS: validated workflow trigger and concurrency policy for {count} workflows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
