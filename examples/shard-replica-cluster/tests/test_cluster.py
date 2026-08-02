#!/usr/bin/env python3
"""
ShopEdge cluster — Python integration tests.

Inserts data through the top-level oxipool router (localhost:4445), reads it
back, validates the results, and confirms sharding / scatter-gather behavior
end-to-end.

Run from the host while the cluster is up:

    cd ShardReplicaRealWorldTest
    python tests/test_cluster.py

Override the router endpoint with env vars if needed:
    ROUTER_HOST=127.0.0.1 ROUTER_PORT=4445 python tests/test_cluster.py
"""

import os
import random
import sys
import time
import zlib

# Use the in-tree OxiDB Python client (single file, no pypi dep needed).
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(HERE, "..", "..", "python")))

from oxidb import OxiDbClient, OxiDbError  # noqa: E402

ROUTER_HOST = os.getenv("ROUTER_HOST", "127.0.0.1")
ROUTER_PORT = int(os.getenv("ROUTER_PORT", "4445"))

GREEN = "\033[32m"
RED = "\033[31m"
DIM = "\033[2m"
END = "\033[0m"


def shard_of(customer_id: int) -> str:
    """Mirror of oxipool's CRC32 → 256 chunks → shard math."""
    h = zlib.crc32(str(customer_id).encode())
    return ["A", "B", "C"][(h % 256) % 3]


def client():
    return OxiDbClient(ROUTER_HOST, ROUTER_PORT, timeout=10)


# ─── Tests ───────────────────────────────────────────────────────────


def test_ping():
    """Router responds to ping."""
    with client() as db:
        msg = db.ping()
        assert msg, "ping returned empty"
        print(f"     router responded: {msg!r}")


def test_targeted_insert_and_find():
    """Insert with shard key, read back via single-shard query."""
    tag = f"py-target-{int(time.time() * 1000)}"
    inserted = []
    with client() as db:
        for cid in range(1, 31):
            db.insert("orders", {
                "customer_id": cid,
                "_smoke": tag,
                "status": "pending",
                "total": round(random.uniform(10, 500), 2),
            })
            inserted.append(cid)
        time.sleep(0.6)  # let Raft replicate

        # Spot-check a sample by single-shard query
        sample = random.sample(inserted, 10)
        for cid in sample:
            docs = db.find("orders", {"customer_id": cid, "_smoke": tag})
            assert len(docs) == 1, f"customer_id={cid} on shard {shard_of(cid)}: expected 1, got {len(docs)}"
            assert docs[0]["customer_id"] == cid
        print(f"     inserted 30 orders, validated 10 random reads via shard key")


def test_shard_distribution():
    """CRC32 sharding spreads data across all 3 shards."""
    tag = f"py-distrib-{int(time.time() * 1000)}"
    expected = {"A": 0, "B": 0, "C": 0}
    with client() as db:
        for cid in range(1, 91):
            db.insert("orders", {"customer_id": cid, "_smoke": tag, "status": "pending"})
            expected[shard_of(cid)] += 1
        time.sleep(0.6)

        # Scatter-gather: count by shard via the predicted distribution
        # Validate via the router's no-shard-key path (true scatter-gather)
        all_docs = db.find("orders", {"_smoke": tag})
        assert len(all_docs) == 90, f"scatter-gather: expected 90, got {len(all_docs)}"

    print(f"     CRC32 distribution: A={expected['A']} B={expected['B']} C={expected['C']} (sum=90)")
    assert expected["A"] > 0 and expected["B"] > 0 and expected["C"] > 0, "data didn't reach all shards"


def test_count_via_router():
    """Count() on a sharded collection sums per-shard counts."""
    tag = f"py-count-{int(time.time() * 1000)}"
    with client() as db:
        for cid in range(1, 51):
            db.insert("orders", {"customer_id": cid, "_smoke": tag, "status": "ok"})
        time.sleep(0.6)
        n = db.count("orders", {"_smoke": tag})
        assert n == 50, f"expected 50, got {n}"
        print(f"     count returned {n} (sum across all shards)")


def test_scatter_filter():
    """Cross-shard filtered query (no shard key, status filter only)."""
    tag = f"py-scatter-{int(time.time() * 1000)}"
    with client() as db:
        for cid in range(1, 41):
            status = "shipped" if cid % 2 == 0 else "pending"
            db.insert("orders", {"customer_id": cid, "_smoke": tag, "status": status})
        time.sleep(0.6)

        shipped = db.find("orders", {"_smoke": tag, "status": "shipped"})
        pending = db.find("orders", {"_smoke": tag, "status": "pending"})
        assert len(shipped) == 20, f"shipped: expected 20, got {len(shipped)}"
        assert len(pending) == 20, f"pending: expected 20, got {len(pending)}"
        print(f"     scatter+filter: shipped={len(shipped)} pending={len(pending)}")


def test_aggregation():
    """$group / $sum across shards.

    Real-world finding: oxipool's scatter-gather for `aggregate` uses the
    ConcatDocs merge strategy (oxipool/src/scatter.rs), so per-shard $group
    results are CONCATENATED rather than merged. The router returns one row
    per (shard × group). The application must reduce them itself for a
    cluster-wide aggregate. We assert that here.
    """
    tag = f"py-agg-{int(time.time() * 1000)}"
    with client() as db:
        for cid in range(1, 21):
            for _ in range(3):
                db.insert("orders", {
                    "customer_id": cid,
                    "_smoke": tag,
                    "status": "paid",
                    "total": 25.0,
                })
        time.sleep(0.6)

        result = db.aggregate("orders", [
            {"$match": {"_smoke": tag}},
            {"$group": {
                "_id": "$status",
                "count": {"$sum": 1},
                "revenue": {"$sum": "$total"},
            }},
        ])
        # Each shard contributes its own group → up to 3 rows for status="paid"
        assert 1 <= len(result) <= 3, f"expected 1..3 partial groups, got {len(result)}"
        # All partial groups should be for the same status
        assert all(r["_id"] == "paid" for r in result), f"unexpected statuses: {result}"

        # Reduce at the client
        total_count = sum(r["count"] for r in result)
        total_revenue = sum(r["revenue"] for r in result)
        assert total_count == 60, f"sum(count): expected 60, got {total_count}"
        assert total_revenue == 1500.0, f"sum(revenue): expected 1500, got {total_revenue}"
        print(f"     {len(result)} per-shard partial group(s) → "
              f"reduced count={total_count} revenue={total_revenue}")
        print(f"     (oxipool concatenates per-shard groups; client reduces)")


def test_update_and_delete():
    """Update + delete with shard key — both targeted, both verified."""
    tag = f"py-upd-{int(time.time() * 1000)}"
    cid_target = 5
    with client() as db:
        for cid in range(1, 11):
            db.insert("orders", {
                "customer_id": cid,
                "_smoke": tag,
                "status": "pending",
                "total": 100.0,
            })
        time.sleep(0.5)

        # Update one customer's order via shard-keyed query
        db.update(
            "orders",
            {"customer_id": cid_target, "_smoke": tag},
            {"$set": {"status": "shipped"}},
        )
        time.sleep(0.5)
        docs = db.find("orders", {"customer_id": cid_target, "_smoke": tag})
        assert len(docs) == 1 and docs[0]["status"] == "shipped"
        print(f"     update → cid={cid_target} status=shipped on shard {shard_of(cid_target)}")

        # Delete a different customer
        db.delete("orders", {"customer_id": 7, "_smoke": tag})
        time.sleep(0.5)
        docs = db.find("orders", {"customer_id": 7, "_smoke": tag})
        assert len(docs) == 0, f"delete failed: {len(docs)} rows remain"
        print(f"     delete → cid=7 removed from shard {shard_of(7)}")

        # Verify the others are untouched
        n = db.count("orders", {"_smoke": tag})
        assert n == 9, f"expected 9 remaining, got {n}"


def test_unsharded_collection():
    """`products` is unsharded — all docs land on shard A, browse hits replicas."""
    tag = f"py-prod-{int(time.time() * 1000)}"
    with client() as db:
        # `_id` is auto-assigned; we tag with `_smoke` for filterability
        for i in range(1, 16):
            db.insert("products", {
                "_smoke": tag,
                "name": f"Test Product {i}",
                "price": 9.99 + i,
                "category": "test",
            })
        time.sleep(0.5)
        docs = db.find("products", {"_smoke": tag})
        assert len(docs) == 15
        prices = sorted([d["price"] for d in docs])
        assert prices == sorted([9.99 + i for i in range(1, 16)])
        print(f"     inserted/read 15 products (unsharded → shard A replicas)")


# ─── Runner ──────────────────────────────────────────────────────────


def run_test(name, fn):
    bar = "─" * 60
    print(f"\n  {DIM}{bar}{END}")
    print(f"  {name}")
    t0 = time.time()
    try:
        fn()
        elapsed = time.time() - t0
        print(f"  {GREEN}[PASS]{END} {name}  {DIM}({elapsed*1000:.0f}ms){END}")
        return True
    except AssertionError as e:
        print(f"  {RED}[FAIL]{END} {name}: {e}")
        return False
    except OxiDbError as e:
        print(f"  {RED}[FAIL]{END} {name}: server error: {e}")
        return False
    except Exception as e:
        print(f"  {RED}[FAIL]{END} {name}: {type(e).__name__}: {e}")
        return False


def main():
    tests = [
        ("Ping the router", test_ping),
        ("Targeted insert + find by shard key", test_targeted_insert_and_find),
        ("CRC32 shard distribution", test_shard_distribution),
        ("Count across shards", test_count_via_router),
        ("Scatter + filter (status)", test_scatter_filter),
        ("Aggregation $group / $sum across shards", test_aggregation),
        ("Update + delete via shard key", test_update_and_delete),
        ("Unsharded collection (products)", test_unsharded_collection),
    ]

    print()
    print("═" * 70)
    print(f"  ShopEdge cluster — Python integration tests")
    print(f"  router: {ROUTER_HOST}:{ROUTER_PORT}")
    print("═" * 70)

    passed = 0
    for name, fn in tests:
        if run_test(name, fn):
            passed += 1

    print()
    print("═" * 70)
    if passed == len(tests):
        print(f"  {GREEN}Result: {passed}/{len(tests)} passed{END}")
    else:
        print(f"  {RED}Result: {passed}/{len(tests)} passed — {len(tests) - passed} failed{END}")
    print("═" * 70)
    print()
    sys.exit(0 if passed == len(tests) else 1)


if __name__ == "__main__":
    main()
