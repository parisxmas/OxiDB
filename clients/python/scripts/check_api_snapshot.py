#!/usr/bin/env python3
"""
CI gate for Phase 3 of ADR-0003. Re-runs the snapshot generator and diffs
against the committed api/v1.json. Exits non-zero on any mismatch so a PR
that changes the 1.0 stable surface fails CI unless the maintainer
intentionally regenerates the snapshot (and labels the PR appropriately).

Run:
    cd python && python3 scripts/check_api_snapshot.py

Update on purpose:
    cd python && python3 scripts/generate_api_snapshot.py > api/v1.json
"""
from __future__ import annotations

import difflib
import json
import subprocess
import sys
from pathlib import Path


def main() -> int:
    here = Path(__file__).resolve().parent
    repo = here.parent
    snapshot_path = repo / "api" / "v1.json"
    if not snapshot_path.exists():
        print(f"error: {snapshot_path} does not exist — run generate_api_snapshot.py first")
        return 2

    generator = here / "generate_api_snapshot.py"
    proc = subprocess.run(
        [sys.executable, str(generator)],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        print("error: snapshot generator failed:")
        print(proc.stderr)
        return proc.returncode

    current = snapshot_path.read_text()
    regenerated = proc.stdout

    if current == regenerated:
        print(f"ok: {snapshot_path.relative_to(repo.parent)} matches generator output")
        return 0

    diff = difflib.unified_diff(
        current.splitlines(keepends=True),
        regenerated.splitlines(keepends=True),
        fromfile=f"committed: {snapshot_path.name}",
        tofile="regenerated",
    )
    sys.stdout.writelines(diff)
    print()
    print("FAIL: the Python client's public surface changed.")
    print("If the change was intentional and the 1.0 stable surface is being")
    print("revved, run:")
    print(f"    cd python && python3 {generator.relative_to(repo)} > api/v1.json")
    print("and commit the new snapshot. Otherwise revert the surface change.")
    print("See docs/PHASE3-SDK-FREEZE.md for the policy.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
