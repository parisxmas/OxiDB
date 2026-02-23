#!/usr/bin/env python3
"""
OxiDB Memory Stress Test
=========================
Opens 100 simultaneous connections and hammers the server with concurrent
inserts, updates, queries, deletes, aggregations, and transactions.
Reports server RSS memory before and after to detect leaks.

Prerequisites:
    - oxidb-server running on 127.0.0.1:4444
    - Python 3.8+

Usage:
    python examples/python/memory_stress_test.py [--host HOST] [--port PORT] [--rounds ROUNDS]
"""

import argparse
import os
import random
import signal
import string
import subprocess
import sys
import threading
import time

# Add parent so we can import the client library
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
from oxidb import OxiDbClient, OxiDbError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NUM_CONNECTIONS = 100
DOCS_PER_WORKER = 200
COLLECTION = "memtest"
AGG_COLLECTION = "memtest_agg"


def random_string(length=12):
    return "".join(random.choices(string.ascii_lowercase, k=length))


def random_doc(worker_id, seq):
    return {
        "worker": worker_id,
        "seq": seq,
        "name": random_string(),
        "email": f"{random_string(8)}@example.com",
        "age": random.randint(18, 90),
        "score": round(random.uniform(0.0, 100.0), 2),
        "tags": [random_string(5) for _ in range(random.randint(1, 5))],
        "active": random.choice([True, False]),
        "payload": random_string(200),  # ~200 bytes of filler per doc
    }


def get_server_pid(port):
    """Find the oxidb-server PID listening on the given port."""
    try:
        out = subprocess.check_output(
            ["lsof", "-ti", f":{port}"], stderr=subprocess.DEVNULL, text=True
        )
        pids = set(out.strip().split("\n"))
        for pid in pids:
            pid = pid.strip()
            if not pid:
                continue
            try:
                cmdline = subprocess.check_output(
                    ["ps", "-p", pid, "-o", "comm="], text=True
                ).strip()
                if "oxidb" in cmdline.lower():
                    return int(pid)
            except subprocess.CalledProcessError:
                continue
    except subprocess.CalledProcessError:
        pass
    return None


def get_rss_kb(pid):
    """Get RSS of a process in KB (macOS & Linux)."""
    try:
        out = subprocess.check_output(
            ["ps", "-o", "rss=", "-p", str(pid)], text=True
        )
        return int(out.strip())
    except (subprocess.CalledProcessError, ValueError):
        return None


def fmt_mem(kb):
    if kb is None:
        return "N/A"
    if kb >= 1024:
        return f"{kb / 1024:.1f} MB"
    return f"{kb} KB"


# ---------------------------------------------------------------------------
# Worker routines — each runs on its own connection + thread
# ---------------------------------------------------------------------------

class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.inserts = 0
        self.finds = 0
        self.updates = 0
        self.deletes = 0
        self.aggregates = 0
        self.transactions = 0
        self.errors = 0

    def add(self, **kwargs):
        with self.lock:
            for k, v in kwargs.items():
                setattr(self, k, getattr(self, k) + v)

    def summary(self):
        return (
            f"  inserts:      {self.inserts}\n"
            f"  finds:        {self.finds}\n"
            f"  updates:      {self.updates}\n"
            f"  deletes:      {self.deletes}\n"
            f"  aggregates:   {self.aggregates}\n"
            f"  transactions: {self.transactions}\n"
            f"  errors:       {self.errors}"
        )

    @property
    def total(self):
        return self.inserts + self.finds + self.updates + self.deletes + self.aggregates + self.transactions


def worker_insert(client, worker_id, stats, rounds, col):
    """Bulk insert documents."""
    for r in range(rounds):
        batch = [random_doc(worker_id, r * 10 + i) for i in range(10)]
        try:
            client.insert_many(col, batch)
            stats.add(inserts=len(batch))
        except Exception as e:
            stats.add(errors=1)


def worker_find(client, worker_id, stats, rounds, col):
    """Run various query patterns."""
    queries = [
        {"worker": worker_id},
        {"age": {"$gte": 50}},
        {"active": True},
        {"score": {"$lt": 30.0}},
        {"$or": [{"age": {"$lt": 25}}, {"score": {"$gte": 80}}]},
    ]
    for r in range(rounds):
        q = queries[r % len(queries)]
        try:
            client.find(col, q, limit=20)
            stats.add(finds=1)
        except Exception:
            stats.add(errors=1)
        # find_one
        try:
            client.find_one(col, q)
            stats.add(finds=1)
        except Exception:
            stats.add(errors=1)
        # count
        try:
            client.count(col, q)
            stats.add(finds=1)
        except Exception:
            stats.add(errors=1)


def worker_update(client, worker_id, stats, rounds, col):
    """Run update operations with various operators."""
    for r in range(rounds):
        try:
            client.update(
                col,
                {"worker": worker_id, "active": True},
                {"$inc": {"score": 0.1}, "$set": {"updated": True}},
            )
            stats.add(updates=1)
        except Exception:
            stats.add(errors=1)
        try:
            client.update_one(
                col,
                {"worker": worker_id},
                {"$set": {"name": random_string()}, "$inc": {"age": 1}},
            )
            stats.add(updates=1)
        except Exception:
            stats.add(errors=1)


def worker_delete(client, worker_id, stats, rounds, col):
    """Delete a fraction of own documents."""
    for r in range(rounds):
        try:
            client.delete_one(col, {"worker": worker_id, "seq": r})
            stats.add(deletes=1)
        except Exception:
            stats.add(errors=1)


def worker_aggregate(client, worker_id, stats, rounds, col):
    """Run aggregation pipelines."""
    pipelines = [
        [
            {"$match": {"worker": worker_id}},
            {"$group": {"_id": "$active", "avg_score": {"$avg": "$score"}, "cnt": {"$sum": 1}}},
        ],
        [
            {"$match": {"age": {"$gte": 30}}},
            {"$group": {"_id": "$worker", "max_score": {"$max": "$score"}}},
            {"$sort": {"max_score": -1}},
            {"$limit": 5},
        ],
        [
            {"$match": {"worker": worker_id}},
            {"$group": {"_id": None, "total": {"$sum": "$score"}, "count": {"$sum": 1}}},
        ],
    ]
    for r in range(rounds):
        try:
            client.aggregate(col, pipelines[r % len(pipelines)])
            stats.add(aggregates=1)
        except Exception:
            stats.add(errors=1)


def worker_transaction(client, worker_id, stats, rounds, col):
    """Run transactional insert+update pairs."""
    for r in range(rounds):
        try:
            with client.transaction():
                client.insert(col, {"worker": worker_id, "tx_round": r, "amount": 100})
                client.update_one(
                    col,
                    {"worker": worker_id, "tx_round": r},
                    {"$set": {"amount": 200, "committed": True}},
                )
            stats.add(transactions=1)
        except Exception:
            stats.add(errors=1)


def worker_mixed(client, worker_id, stats, rounds, col):
    """Interleave all operation types on one connection."""
    for r in range(rounds):
        # insert
        try:
            client.insert(col, random_doc(worker_id, 10000 + r))
            stats.add(inserts=1)
        except Exception:
            stats.add(errors=1)
        # query
        try:
            client.find(col, {"worker": worker_id}, limit=5, sort={"score": -1})
            stats.add(finds=1)
        except Exception:
            stats.add(errors=1)
        # update
        try:
            client.update_one(col, {"worker": worker_id}, {"$inc": {"score": 1}})
            stats.add(updates=1)
        except Exception:
            stats.add(errors=1)
        # aggregate
        try:
            client.aggregate(col, [
                {"$match": {"worker": worker_id}},
                {"$count": "total"},
            ])
            stats.add(aggregates=1)
        except Exception:
            stats.add(errors=1)
        # delete one
        try:
            client.delete_one(col, {"worker": worker_id, "seq": 10000 + r})
            stats.add(deletes=1)
        except Exception:
            stats.add(errors=1)


# Map of role -> (function, rounds_multiplier)
WORKER_ROLES = [
    ("insert",      worker_insert,      1),
    ("find",        worker_find,        1),
    ("update",      worker_update,      1),
    ("delete",      worker_delete,      1),
    ("aggregate",   worker_aggregate,   1),
    ("transaction", worker_transaction, 1),
    ("mixed",       worker_mixed,       1),
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_one_round(host, port, rounds, server_pid, round_num):
    """Run a single stress-test round. Returns (rss_before, rss_after, stats, elapsed)."""
    collection = f"{COLLECTION}_r{round_num}"

    # ---- Cleanup from prior runs ----
    setup = OxiDbClient(host, port, timeout=10.0)
    try:
        setup.drop_collection(collection)
    except OxiDbError:
        pass
    setup.create_collection(collection)
    setup.create_index(collection, "worker")
    setup.create_index(collection, "age")
    setup.create_index(collection, "active")
    setup.create_index(collection, "score")
    setup.create_composite_index(collection, ["worker", "seq"])
    setup.close()

    # ---- Seed data ----
    seed = OxiDbClient(host, port, timeout=30.0)
    for w in range(NUM_CONNECTIONS):
        batch = [random_doc(w, s) for s in range(20)]
        seed.insert_many(collection, batch)
    seed.close()

    # ---- Snapshot memory BEFORE ----
    rss_before = get_rss_kb(server_pid) if server_pid else None

    # ---- Open 100 connections ----
    clients = []
    for i in range(NUM_CONNECTIONS):
        try:
            c = OxiDbClient(host, port, timeout=30.0)
            clients.append(c)
        except Exception as e:
            print(f"    [!] Connection {i} failed: {e}")

    # ---- Assign roles and run ----
    stats = Stats()
    threads = []

    for i, client in enumerate(clients):
        role_name, role_fn, rounds_mult = WORKER_ROLES[i % len(WORKER_ROLES)]
        t = threading.Thread(
            target=role_fn,
            args=(client, i, stats, rounds * rounds_mult, collection),
            name=f"r{round_num}-worker-{i}-{role_name}",
            daemon=True,
        )
        threads.append(t)

    t0 = time.monotonic()
    for t in threads:
        t.start()

    while any(t.is_alive() for t in threads):
        time.sleep(2)
        elapsed = time.monotonic() - t0
        rss_now = get_rss_kb(server_pid) if server_pid else None
        mem_str = f" | RSS: {fmt_mem(rss_now)}" if rss_now else ""
        print(f"    [{elapsed:5.1f}s] ops: {stats.total:,} (err: {stats.errors}){mem_str}")

    for t in threads:
        t.join(timeout=30)

    elapsed = time.monotonic() - t0

    # ---- Close connections ----
    for c in clients:
        try:
            c.close()
        except Exception:
            pass

    # ---- Drop collection to free server memory ----
    cleanup = OxiDbClient(host, port, timeout=10.0)
    try:
        cleanup.drop_collection(collection)
    except OxiDbError:
        pass
    cleanup.close()

    time.sleep(2)  # let server settle

    # ---- Snapshot memory AFTER (post-cleanup) ----
    rss_after = get_rss_kb(server_pid) if server_pid else None

    return rss_before, rss_after, stats, elapsed, len(clients)


def main():
    parser = argparse.ArgumentParser(description="OxiDB memory stress test")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4444)
    parser.add_argument("--rounds", type=int, default=20,
                        help="Rounds per worker (higher = more ops)")
    parser.add_argument("--iterations", type=int, default=3,
                        help="Number of full stress-test iterations for leak detection")
    args = parser.parse_args()

    host, port, rounds, iterations = args.host, args.port, args.rounds, args.iterations

    # ---- Find server PID ----
    server_pid = get_server_pid(port)
    if server_pid:
        print(f"[*] Found oxidb-server PID: {server_pid}")
    else:
        print(f"[!] Could not find oxidb-server PID on port {port} — memory stats will be unavailable")

    # ---- Verify connectivity ----
    try:
        probe = OxiDbClient(host, port, timeout=5.0)
        probe.ping()
        probe.close()
    except Exception as e:
        print(f"[!] Cannot connect to oxidb-server at {host}:{port}: {e}")
        sys.exit(1)

    rss_baseline = get_rss_kb(server_pid) if server_pid else None
    print(f"[*] Server baseline RSS: {fmt_mem(rss_baseline)}")
    print(f"[*] Running {iterations} iterations x {NUM_CONNECTIONS} connections x {rounds} rounds\n")

    round_results = []

    for it in range(1, iterations + 1):
        print(f"{'=' * 60}")
        print(f"  ITERATION {it}/{iterations}")
        print(f"{'=' * 60}")

        rss_before, rss_after, stats, elapsed, num_conns = run_one_round(
            host, port, rounds, server_pid, it
        )

        round_results.append({
            "iteration": it,
            "rss_before": rss_before,
            "rss_after": rss_after,
            "ops": stats.total,
            "errors": stats.errors,
            "elapsed": elapsed,
            "connections": num_conns,
            "inserts": stats.inserts,
            "finds": stats.finds,
            "updates": stats.updates,
            "deletes": stats.deletes,
            "aggregates": stats.aggregates,
            "transactions": stats.transactions,
        })

        print(f"  Connections: {num_conns} | Ops: {stats.total:,} in {elapsed:.1f}s "
              f"({stats.total / elapsed:,.0f} ops/sec) | Errors: {stats.errors}")
        print(f"    inserts={stats.inserts} finds={stats.finds} updates={stats.updates} "
              f"deletes={stats.deletes} agg={stats.aggregates} tx={stats.transactions}")
        print(f"  Memory: {fmt_mem(rss_before)} -> {fmt_mem(rss_after)} (after drop)")
        print()

    # ---- Final Report ----
    print(f"\n{'=' * 60}")
    print(f"  MEMORY LEAK ANALYSIS")
    print(f"{'=' * 60}")
    print(f"  {'Iter':<6} {'Before':<12} {'After Drop':<12} {'Delta':<12} {'Ops':>8} {'Err':>5}")
    print(f"  {'-'*55}")

    after_values = []
    for r in round_results:
        before_str = fmt_mem(r["rss_before"])
        after_str = fmt_mem(r["rss_after"])
        if r["rss_before"] and r["rss_after"]:
            delta = r["rss_after"] - r["rss_before"]
            delta_str = f"{'+'if delta >= 0 else ''}{fmt_mem(abs(delta))}"
            after_values.append(r["rss_after"])
        else:
            delta_str = "N/A"
        print(f"  {r['iteration']:<6} {before_str:<12} {after_str:<12} {delta_str:<12} "
              f"{r['ops']:>8,} {r['errors']:>5}")

    print(f"  {'-'*55}")

    if rss_baseline:
        rss_final = get_rss_kb(server_pid) if server_pid else None
        print(f"\n  Server baseline:   {fmt_mem(rss_baseline)}")
        print(f"  Server final:      {fmt_mem(rss_final)}")
        if rss_final:
            total_delta = rss_final - rss_baseline
            print(f"  Total delta:       {'+'if total_delta >= 0 else ''}{fmt_mem(abs(total_delta))}")

    # Leak detection: check if post-drop RSS keeps growing across iterations
    if len(after_values) >= 2:
        growth = after_values[-1] - after_values[0]
        per_iter = growth / (len(after_values) - 1)
        print(f"\n  Post-drop RSS growth across iterations: {'+'if growth >= 0 else ''}{fmt_mem(abs(growth))}")
        print(f"  Per-iteration growth: {'+'if per_iter >= 0 else ''}{per_iter:.0f} KB")
        print()
        if per_iter > 500:  # > 500 KB growth per iteration after cleanup
            print(f"  [LEAK] Memory grows by ~{per_iter:.0f} KB per iteration after cleanup!")
            print(f"         This suggests a memory leak — data is not freed on drop.")
        elif per_iter > 100:
            print(f"  [NOTICE] Slight memory growth ({per_iter:.0f} KB/iter) — may be")
            print(f"           allocator fragmentation or retained metadata.")
        else:
            print(f"  [OK] No significant leak detected. Post-cleanup RSS is stable.")

    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
