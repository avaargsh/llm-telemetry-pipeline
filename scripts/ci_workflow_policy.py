"""Lint the repository's canonical Actions trigger/concurrency declarations.

This is deliberately a strict format check, not a general YAML parser or a
substitute for observing GitHub's scheduler. Only canonical block syntax is
accepted in the guarded declarations; job definitions are not parsed.
"""
import re
import sys
from pathlib import Path

GROUP = "  group: ${{ github.workflow }}-${{ github.event_name == 'pull_request' && github.ref || github.run_id }}"
CANCEL = "  cancel-in-progress: ${{ github.event_name == 'pull_request' }}"
TOP = re.compile(r"^([A-Za-z_][A-Za-z0-9_-]*):(?:[ \t]*(.*))?$")
EVENT = re.compile(r"^  ([a-z_]+):[ \t]*$")
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9 _.\-]*")
ALLOWED = {"pull_request", "push", "workflow_dispatch", "schedule"}


def audit_workflows(sources):
    errors = []
    names = {}
    if not sources:
        return ["no workflows found"]
    for path, source in sorted(sources.items()):
        def fail(message):
            errors.append(f"{path}: {message}")

        sections = {}
        key = None
        for raw in source.splitlines():
            line = raw.rstrip()
            if not line or line.lstrip().startswith('#'):
                continue
            if not line[0].isspace():
                match = TOP.fullmatch(line)
                if not match:
                    fail("unsupported top-level workflow declaration")
                    key = None
                    continue
                key, value = match.groups()
                if key in sections:
                    fail(f"duplicate top-level key {key}")
                sections[key] = [value or ""]
            elif key is not None:
                sections[key].append(line)

        name = sections.get('name', [])
        if len(name) != 1 or not NAME.fullmatch(name[0]):
            fail("explicit canonical workflow name required")
        else:
            folded = name[0].casefold()
            if folded in names:
                fail(f"workflow name collides with {names[folded]}")
            names[folded] = path

        if sections.get('concurrency') != ['', GROUP, CANCEL]:
            fail("expected workflow-scoped PR-ref cancellation and unique non-PR run identity")
        # Additional job-level groups could reintroduce cross-run cancellation.
        if any(re.match(r"^\s+(?:concurrency|['\"]concurrency['\"]):", line) for line in sections.get('jobs', [])):
            fail("job-level concurrency requires a separate policy review")

        events = {}
        current = None
        trigger = sections.get('on', [])
        if not trigger or trigger[0] != '':
            fail("canonical block trigger declaration required")
        for line in trigger[1:]:
            match = EVENT.fullmatch(line)
            if match:
                current = match.group(1)
                if current in events:
                    fail(f"duplicate trigger {current}")
                events[current] = []
            elif current is None or not line.startswith('    '):
                fail("unsupported trigger declaration")
            else:
                events[current].append(line)
        if 'pull_request' not in events:
            fail("pull_request trigger required")
        if set(events) - ALLOWED:
            fail("unexpected trigger: " + ', '.join(sorted(set(events) - ALLOWED)))
        if 'push' in events:
            block = events['push']
            fields = [line for line in block if line.startswith('    ') and not line.startswith('     ')]
            if fields.count('    branches: [master]') != 1 or any(line != '    branches: [master]' and line != '    paths:' for line in fields) or fields.count('    paths:') > 1:
                fail("push must target only master (optional paths filter allowed)")
    return errors


def main():
    root = Path(__file__).resolve().parent.parent / '.github' / 'workflows'
    files = sorted((*root.glob('*.yml'), *root.glob('*.yaml')))
    sources = {str(path.relative_to(root)): path.read_text(encoding='utf-8') for path in files}
    errors = audit_workflows(sources)
    if errors:
        print('\n'.join(errors), file=sys.stderr)
        return 1
    print(f'PASS: canonical trigger/concurrency policy for {len(files)} workflows')
    return 0


if __name__ == '__main__':
    sys.exit(main())
