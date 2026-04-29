#!/usr/bin/env python3
"""
Run a battery of full-text search queries against the OxiDB server and
verify that results land in the right book. Three corpora were uploaded:

    alice_*    — Alice's Adventures in Wonderland
    pride_*    — Pride and Prejudice
    sherlock_* — The Adventures of Sherlock Holmes

Each test query has an expected dominant prefix so we can assert that
the BM25 ranking returns the right corpus first.
"""
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "python"))
import oxidb  # type: ignore

BUCKET = "ftstests"
HOST = os.environ.get("OXIDB_HOST", "127.0.0.1")
PORT = int(os.environ.get("OXIDB_PORT", "14888"))

# (query, expected dominant prefix among top hits, min hits required)
EXPECTED = [
    ("Holmes",         "sherlock_", 5),
    ("Watson",         "sherlock_", 5),
    ("Baker Street",   "sherlock_", 3),
    ("Darcy",          "pride_",    5),
    ("Bingley",        "pride_",    5),
    ("Elizabeth",      "pride_",    3),
    ("Cheshire",       "alice_",    1),
    ("rabbit",         "alice_",    3),
    ("Queen Hearts",   "alice_",    3),
]


def expect_top_prefix(client, query, prefix, min_hits, top_k=10) -> bool:
    results = client.search(query, bucket=BUCKET, limit=top_k)
    if not results:
        print(f"  ✗ {query!r}: 0 results")
        return False
    top = results[0]
    dominant = sum(1 for r in results if r["key"].startswith(prefix))
    ok = top["key"].startswith(prefix) and dominant >= min_hits
    mark = "✓" if ok else "✗"
    print(
        f"  {mark} {query!r}: top={top['key']} score={top['score']:.3f} "
        f"({dominant}/{len(results)} from {prefix})"
    )
    return ok


def expect_no_results(client, query) -> bool:
    """Stop words must filter to nothing once tokenized."""
    results = client.search(query, bucket=BUCKET, limit=10)
    ok = len(results) == 0
    mark = "✓" if ok else "✗"
    print(f"  {mark} {query!r}: {len(results)} results (expected 0)")
    return ok


def expect_highlight(client, query, expected_prefix) -> bool:
    """Verify the new search-with-highlight path returns <mark> snippets."""
    payload = {
        "cmd": "search",
        "query": query,
        "bucket": BUCKET,
        "limit": 3,
        "highlight": {"snippet_chars": 100, "max_snippets": 2},
    }
    results = client._checked(payload)
    if not results:
        print(f"  ✗ {query!r}: 0 results")
        return False
    top = results[0]
    snippets = top.get("highlights") or []
    has_mark = any("<mark>" in s for s in snippets)
    ok = top["key"].startswith(expected_prefix) and has_mark
    mark = "✓" if ok else "✗"
    print(
        f"  {mark} {query!r} (highlight): top={top['key']} "
        f"snippets={len(snippets)} mark={has_mark}"
    )
    if snippets:
        first = snippets[0].replace("\n", " ")
        first = first[:120] + ("..." if len(first) > 120 else "")
        print(f"      → {first}")
    return ok


def main() -> int:
    client = oxidb.OxiDbClient(host=HOST, port=PORT, timeout=15)
    print(f"[search] -> {HOST}:{PORT} bucket={BUCKET}", flush=True)

    # FTS indexing happens asynchronously after upload. The server flushes
    # every OXIDB_FTS_FLUSH_INTERVAL_MS (run.sh sets a low value), but we
    # still need to wait for all 100 PDF/DOCX extractions to drain.
    target = 100
    deadline = time.time() + 60
    last_count = -1
    while time.time() < deadline:
        results = client.search("the OR a OR Holmes OR Darcy OR rabbit", bucket=BUCKET, limit=200)
        # Stop words 'the'/'a' filter out, leaving the OR chain. Until
        # most files are indexed the count keeps growing; once it stabilizes
        # we know the worker drained.
        if len(results) >= target * 0.95 and len(results) == last_count:
            break
        last_count = len(results)
        time.sleep(0.5)
    print(f"[search] index ready: {last_count} indexed docs", flush=True)

    passed = failed = 0
    print("\n--- Ranking tests ---")
    for query, prefix, min_hits in EXPECTED:
        if expect_top_prefix(client, query, prefix, min_hits):
            passed += 1
        else:
            failed += 1

    print("\n--- Stop-word filtering ---")
    for sw in ["the", "and", "of"]:
        if expect_no_results(client, sw):
            passed += 1
        else:
            failed += 1

    print("\n--- Highlight integration ---")
    for query, prefix in [("Holmes", "sherlock_"), ("Darcy", "pride_"), ("rabbit", "alice_")]:
        if expect_highlight(client, query, prefix):
            passed += 1
        else:
            failed += 1

    total = passed + failed
    print(f"\n=== {passed}/{total} PASS ===")
    client.close()
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
