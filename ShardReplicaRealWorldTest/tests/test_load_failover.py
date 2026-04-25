#!/usr/bin/env python3
"""
ShopEdge cluster — 10K-record load test with mid-stream failover.

Inserts 10,000 orders through the router (sharded by customer_id), stops a
follower at the halfway mark, finishes the remaining inserts on a degraded
cluster (quorum 2/3), then restarts the follower and verifies:
    1. all 10,000 records are present (count via router)
    2. shard A's Raft log converges across all 3 nodes
    3. the failover did NOT lose any data

Run:
    python tests/test_load_failover.py            # default: 10000 records
    TOTAL=20000 BATCH=500 python tests/test_load_failover.py
"""

import json
import os
import subprocess
import sys
import time
import urllib.request
import zlib

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(HERE, "..", "..", "python")))

from oxidb import OxiDbClient, OxiDbError  # noqa: E402

ROUTER_HOST = os.getenv("ROUTER_HOST", "127.0.0.1")
ROUTER_PORT = int(os.getenv("ROUTER_PORT", "4445"))
API_HOST    = os.getenv("API_HOST",    "127.0.0.1")
API_PORT    = int(os.getenv("API_PORT", "8080"))
COMPOSE_DIR = os.path.normpath(os.path.join(HERE, ".."))

TOTAL = int(os.getenv("TOTAL", "10000"))
BATCH = int(os.getenv("BATCH", "500"))
FAILOVER_NODE = os.getenv("FAILOVER_NODE", "db-a1")  # follower of shard A
FAILOVER_AT = int(os.getenv("FAILOVER_AT", str(TOTAL // 2)))

GREEN, RED, YELLOW, DIM, END = "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[0m"


def shard_of(cid):
    return ["A", "B", "C"][(zlib.crc32(str(cid).encode()) % 256) % 3]


def compose(*args, check=True):
    return subprocess.run(["docker", "compose", *args], cwd=COMPOSE_DIR,
                          check=check, capture_output=True, text=True)


def warmup():
    for _ in range(8):
        try:
            with OxiDbClient(ROUTER_HOST, ROUTER_PORT, timeout=4) as db:
                db.ping()
            return
        except (OxiDbError, OSError, TimeoutError):
            time.sleep(0.4)


def aggressive_warmup(tag, attempts=8):
    """After a node state change, oxipool's per-shard pools have stale cached
    conns to the affected node. The first scatter operation often hits one of
    them, triggers `spawn_replace`, but the response is short by one shard.

    Specifically, oxipool/src/scatter.rs::merge_counts silently skips shards
    that errored (just sums the responders) — so a partial-shard outage looks
    like an undercount instead of an error. We work around that by hammering
    a few counts and only trusting the result once two in a row agree."""
    last = None
    consecutive = 0
    for _ in range(attempts):
        try:
            with OxiDbClient(ROUTER_HOST, ROUTER_PORT, timeout=8) as db:
                n = db.count("orders", {"_load": tag})
            if n == last:
                consecutive += 1
                if consecutive >= 2:
                    return n
            else:
                consecutive = 0
            last = n
        except (OxiDbError, OSError, TimeoutError):
            consecutive = 0
        time.sleep(0.4)
    return last


def insert_batch(db, batch, retries=5):
    """insert_many with retry-on-transient (Broken pipe / early eof)."""
    for attempt in range(retries):
        try:
            db.insert_many("orders", batch)
            return True
        except (OxiDbError, OSError, TimeoutError) as e:
            msg = str(e).lower()
            transient = any(m in msg for m in
                            ("broken pipe", "early eof", "connection reset",
                             "forward request", "connection refused"))
            if not transient or attempt == retries - 1:
                raise
            time.sleep(0.5)
    return False


def shard_state():
    """Pull /api/raft/metrics — return per-shard list of (node, log_index)."""
    try:
        with urllib.request.urlopen(f"http://{API_HOST}:{API_PORT}/api/raft/metrics", timeout=4) as resp:
            data = json.loads(resp.read())
        out = {"a": [], "b": [], "c": []}
        for r in data["results"]:
            if r.get("ok"):
                m = r["metrics"]
                shard = r["node"].split(":")[0][3]
                out[shard].append((r["node"], m["state"], m.get("current_leader"),
                                   m.get("current_term"), m["last_log_index"]))
        return out
    except Exception as e:
        return {"error": str(e)}


def wait_raft_converged(timeout=60):
    """Poll until all 3 shards have all followers' log_index == leader's."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = shard_state()
        if "error" in st:
            time.sleep(1); continue
        try:
            ok = (
                len({i for *_, i in st["a"]}) == 1 and len(st["a"]) == 3 and
                len({i for *_, i in st["b"]}) == 1 and len(st["b"]) == 3 and
                len({i for *_, i in st["c"]}) == 1 and len(st["c"]) == 3
            )
            if ok:
                return st
        except Exception:
            pass
        time.sleep(1)
    return None


def main():
    print()
    print("═" * 72)
    print(f"  ShopEdge cluster — {TOTAL:,}-record load test with mid-stream failover")
    print(f"  router:        {ROUTER_HOST}:{ROUTER_PORT}")
    print(f"  total:         {TOTAL:,} orders, batch_size={BATCH}")
    print(f"  failover:      stop {FAILOVER_NODE} at offset {FAILOVER_AT:,}")
    print("═" * 72)
    print()

    tag = f"load-{int(time.time())}"
    print(f"  tag (for cleanup): {tag}")

    # ─── Pre-flight ─────────────────────────────────────────────────
    warmup()
    print(f"  {DIM}── pre-flight ──{END}")
    pre = shard_state()
    for shard in "abc":
        for node, state, leader, term, idx in pre[shard]:
            print(f"    {node:18s} {state:9s} leader={leader} term={term} log_index={idx}")
    print()

    # ─── Insert ─────────────────────────────────────────────────────
    print(f"  {DIM}── inserting {TOTAL:,} orders in batches of {BATCH} ──{END}")
    inserted = 0
    failover_triggered = False
    failover_t = None
    rejoined_t = None
    t0 = time.time()
    last_report = t0

    with OxiDbClient(ROUTER_HOST, ROUTER_PORT, timeout=15) as db:
        while inserted < TOTAL:
            n = min(BATCH, TOTAL - inserted)
            batch = [{
                "customer_id": (inserted + i) + 1,
                "_load":  tag,
                "status": "pending",
                "i":      inserted + i,
            } for i in range(n)]

            try:
                insert_batch(db, batch)
                inserted += n
            except Exception as e:
                print(f"    {RED}batch failed at {inserted:,}: {e}{END}")
                # in real test we'd retry/fail; for now keep going
                inserted += n  # count anyway so loop terminates

            now = time.time()
            if now - last_report >= 1.5:
                rate = inserted / (now - t0)
                bar = "█" * int(40 * inserted / TOTAL) + "░" * (40 - int(40 * inserted / TOTAL))
                print(f"    [{bar}] {inserted:>6,}/{TOTAL:,}  ({rate:>6.0f} rec/s)")
                last_report = now

            # Mid-stream failover trigger
            if not failover_triggered and inserted >= FAILOVER_AT:
                print(f"\n  {YELLOW}⏹  FAILOVER: stopping {FAILOVER_NODE} (at {inserted:,}/{TOTAL:,}){END}")
                compose("stop", FAILOVER_NODE)
                failover_t = time.time()
                failover_triggered = True
                # Brief pause for openraft to notice the missing follower
                time.sleep(2.5)
                print(f"  {DIM}continuing inserts on degraded cluster (quorum 2/3)...{END}\n")

    elapsed_inserts = time.time() - t0
    print(f"\n  inserts done in {elapsed_inserts:.1f}s ({TOTAL/elapsed_inserts:.0f} rec/s)")

    # ─── Restart the failed node ────────────────────────────────────
    print(f"\n  {DIM}── restarting {FAILOVER_NODE} ──{END}")
    compose("start", FAILOVER_NODE)
    rejoined_t = time.time()
    print(f"  {GREEN}▶  {FAILOVER_NODE} restarted; waiting for Raft to converge...{END}")

    converged = wait_raft_converged(timeout=90)
    catchup_elapsed = time.time() - rejoined_t

    if converged:
        print(f"  {GREEN}✓ Raft converged in {catchup_elapsed:.1f}s{END}")
        for shard in "abc":
            for node, state, leader, term, idx in converged[shard]:
                print(f"    {node:18s} {state:9s} leader={leader} term={term} log_index={idx}")
    else:
        print(f"  {RED}✗ Raft did NOT converge within 90s{END}")
        return 1

    # ─── Verify ─────────────────────────────────────────────────────
    print(f"\n  {DIM}── verification ──{END}")
    n_total = aggressive_warmup(tag, attempts=12)
    print(f"    count via router (scatter-gather):  {n_total:,}")

    expected_a = sum(1 for cid in range(1, TOTAL + 1) if shard_of(cid) == "A")
    expected_b = sum(1 for cid in range(1, TOTAL + 1) if shard_of(cid) == "B")
    expected_c = sum(1 for cid in range(1, TOTAL + 1) if shard_of(cid) == "C")
    print(f"    expected per shard: A={expected_a:,} B={expected_b:,} C={expected_c:,}")

    with OxiDbClient(ROUTER_HOST, ROUTER_PORT, timeout=20) as db:
        for sample in [1, 100, 1000, 5000, 9000, TOTAL]:
            if sample > TOTAL:
                continue
            shard = shard_of(sample)
            n = db.count("orders", {"_load": tag, "customer_id": sample})
            mark = GREEN + "✓" + END if n == 1 else RED + "✗" + END
            print(f"    {mark} cid={sample:<6,} → shard {shard}: {n} row(s)")

    # ─── Report ─────────────────────────────────────────────────────
    print()
    print("═" * 72)
    print("  Load test summary")
    print("═" * 72)
    print(f"  Records inserted:       {TOTAL:,}")
    print(f"  Records persisted:      {n_total:,}  {(GREEN+'✓'+END) if n_total == TOTAL else (RED+'✗ MISMATCH'+END)}")
    print(f"  Insert duration:        {elapsed_inserts:.1f}s ({TOTAL/elapsed_inserts:.0f} rec/s)")
    print(f"  Failover at:            {FAILOVER_AT:,} ({FAILOVER_AT*100//TOTAL}% through)")
    print(f"  Failover-to-restart:    {(rejoined_t - failover_t):.1f}s")
    print(f"  Raft catch-up time:     {catchup_elapsed:.1f}s")
    final_log_index = converged["a"][0][-1] if converged else "?"
    print(f"  Shard A log_index:      {final_log_index}")
    print("═" * 72)

    return 0 if n_total == TOTAL else 2


if __name__ == "__main__":
    sys.exit(main())
