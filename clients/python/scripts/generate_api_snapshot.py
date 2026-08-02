#!/usr/bin/env python3
"""
Generate api/v1.json — a structural snapshot of the Python client's public
surface. Used by check_api_snapshot.py in CI to fail PRs that change the
1.0 stable surface without an explicit version bump.

Phase 3 of ADR-0003. See docs/PHASE3-SDK-FREEZE.md for the pattern this
script demonstrates; the other 9 Tier-A clients each get an equivalent.

Run:
    cd python && python3 scripts/generate_api_snapshot.py > api/v1.json

CI usage: scripts/check_api_snapshot.py exits non-zero if the regenerated
JSON differs from the committed file.
"""
from __future__ import annotations

import inspect
import json
import sys
from pathlib import Path
from typing import Any

# Make `import oxidb` resolve to the source tree, not whatever pip thinks.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import oxidb  # noqa: E402


def _is_public(name: str) -> bool:
    """Underscore-prefixed names are private by Python convention."""
    return not name.startswith("_") or name in ("__init__", "__enter__", "__exit__")


def _signature(obj: Any) -> str | None:
    try:
        sig = inspect.signature(obj)
    except (TypeError, ValueError):
        return None
    # Strip `self` so the surface is comparable across renames.
    params = []
    for p in sig.parameters.values():
        if p.name == "self":
            continue
        # Default values may be arbitrary objects; render conservatively.
        default = (
            "<no-default>"
            if p.default is inspect.Parameter.empty
            else repr(p.default)
        )
        kind = p.kind.name
        annotation = (
            "<no-annotation>"
            if p.annotation is inspect.Parameter.empty
            else _annotation_str(p.annotation)
        )
        params.append(
            {
                "name": p.name,
                "kind": kind,
                "annotation": annotation,
                "default": default,
            }
        )
    return_annotation = (
        "<no-annotation>"
        if sig.return_annotation is inspect.Signature.empty
        else _annotation_str(sig.return_annotation)
    )
    return {"params": params, "return": return_annotation}


def _annotation_str(ann: Any) -> str:
    # `inspect` returns class objects, strings, or typing objects depending on
    # source. Normalise to a stable string form.
    if isinstance(ann, str):
        return ann
    return getattr(ann, "__qualname__", repr(ann))


def _class_snapshot(cls: type) -> dict:
    bases = [b.__name__ for b in cls.__bases__ if b is not object]
    methods = {}
    for name, member in inspect.getmembers(cls):
        if not _is_public(name):
            continue
        if not (inspect.isfunction(member) or inspect.ismethod(member)):
            continue
        # Skip methods inherited from object / Exception with no override.
        if getattr(member, "__qualname__", "").startswith(("object.", "Exception.", "BaseException.")):
            continue
        methods[name] = _signature(member)
    return {"bases": bases, "methods": methods}


def main() -> None:
    surface: dict[str, Any] = {
        "module": oxidb.__name__,
        "schema_version": 1,
        "classes": {},
        "functions": {},
    }
    for name, member in inspect.getmembers(oxidb):
        if not _is_public(name):
            continue
        if inspect.isclass(member) and member.__module__ == oxidb.__name__:
            surface["classes"][name] = _class_snapshot(member)
        elif inspect.isfunction(member) and member.__module__ == oxidb.__name__:
            surface["functions"][name] = _signature(member)

    print(json.dumps(surface, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
