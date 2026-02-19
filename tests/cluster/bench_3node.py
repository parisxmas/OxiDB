#!/usr/bin/env python3
"""
OxiDB 3-Node Cluster Performance Benchmark

Measures throughput & latency across a 3-node Raft cluster:
  1. Cluster bootstrap & leader election
  2. Bulk insert (1M documents via HAProxy)
  3. Replication convergence time
  4. Single-node vs HAProxy read throughput
  5. Index creation on replicated data
  6. Query performance (simple, range, compound, aggregation)
  7. Write throughput (single inserts & updates)
  8. Concurrent read/write mixed workload
  9. Leader failover & recovery
 10. Post-failover write & read performance

Usage:
    cd tests/cluster
    ./run_bench.sh
"""

import os
import sys
import json
import socket
import struct
import subprocess
import time
import random
import threading
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
from oxidb import OxiDbClient, OxiDbError

# -- Configuration -----------------------------------------------------------

HAPROXY_PORT = 5500
NODE_PORTS = {1: 5501, 2: 5502, 3: 5503}
COLLECTION = "bench"
DOCKER_PROJECT = "oxidb-3node"

TOTAL_DOCS = int(os.environ.get("BENCH_DOCS", 200_000))
BATCH_SIZE = int(os.environ.get("BENCH_BATCH", 2_000))
NUM_INSERT_THREADS = int(os.environ.get("BENCH_THREADS", 4))
SOCKET_TIMEOUT = 120.0
QUERY_RUNS = 5  # median of N for query benchmarks

# Data generation
STATUSES = ["completed", "pending", "cancelled", "refunded", "shipped"]
CATEGORIES = ["electronics", "clothing", "books", "home", "sports",
              "toys", "food", "beauty", "auto", "garden"]
COUNTRIES = ["TR", "US", "DE", "GB", "FR", "JP", "BR", "IN", "CA", "AU"]
DATE_START = datetime(2022, 1, 1, tzinfo=timezone.utc)
DATE_RANGE_SECS = 3 * 365 * 86400  # 3 years


# -- Helpers ------------------------------------------------------------------

class Timer:
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self
    def __exit__(self, *a):
        self.elapsed = time.perf_counter() - self.t0
        self.ms = self.elapsed * 1000


def log(msg):
    print(f"[bench] {msg}", flush=True)


def header(title):
    w = 72
    print(flush=True)
    print(f"  {'=' * w}")
    print(f"  {title:^{w}}")
    print(f"  {'=' * w}")
    print(flush=True)


def section(title):
    print(flush=True)
    print(f"  --- {title} ---")
    print(flush=True)


def wait_for_port(host, port, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection((host, port), timeout=2)
            s.close()
            return
        except OSError:
            time.sleep(0.5)
    raise TimeoutError(f"Port {host}:{port} not reachable after {timeout}s")


def connect_node(nid):
    return OxiDbClient("127.0.0.1", NODE_PORTS[nid], timeout=SOCKET_TIMEOUT)


def connect_haproxy():
    return OxiDbClient("127.0.0.1", HAPROXY_PORT, timeout=SOCKET_TIMEOUT)


def raft_metrics(client):
    try:
        resp = client._request({"cmd": "raft_metrics"})
        if resp.get("ok"):
            return resp["data"]
    except Exception:
        pass
    return None


def wait_for_leader(node_ids, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        for nid in node_ids:
            try:
                with connect_node(nid) as c:
                    m = raft_metrics(c)
                    if m and m.get("state") == "Leader":
                        return nid, m
            except Exception:
                pass
        time.sleep(0.5)
    raise TimeoutError(f"No leader found among {node_ids} after {timeout}s")


def wait_for_replication(node_ids, collection, expected, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        counts = {}
        ok = True
        for nid in node_ids:
            try:
                with connect_node(nid) as c:
                    cnt = c.count(collection)
                    counts[nid] = cnt
                    if cnt != expected:
                        ok = False
            except Exception as e:
                counts[nid] = f"err: {e}"
                ok = False
        if ok:
            return counts
        time.sleep(0.5)
    raise TimeoutError(f"Replication not converged after {timeout}s. Counts: {counts}")


def wait_for_haproxy(timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with connect_haproxy() as c:
                m = raft_metrics(c)
                if m and m.get("state") == "Leader":
                    return m
        except Exception:
            pass
        time.sleep(0.5)
    raise TimeoutError(f"HAProxy not routing to leader after {timeout}s")


def docker_stop_node(nid):
    container = f"{DOCKER_PROJECT}-oxidb-node{nid}-1"
    subprocess.run(["docker", "stop", "-t", "1", container],
                   check=True, capture_output=True, text=True)


def generate_batch(start_id, count, rng):
    docs = []
    for i in range(count):
        oid = start_id + i
        offset = rng.randint(0, DATE_RANGE_SECS)
        created = DATE_START + timedelta(seconds=offset)
        docs.append({
            "order_id": oid,
            "customer_id": rng.randint(1, 200_000),
            "amount": round(rng.uniform(5.0, 5000.0), 2),
            "status": rng.choice(STATUSES),
            "category": rng.choice(CATEGORIES),
            "country": rng.choice(COUNTRIES),
            "created_at": created.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "priority": rng.randint(1, 5),
        })
    return docs


# -- Progress tracking -------------------------------------------------------

_progress_lock = threading.Lock()
_docs_inserted = 0

def _update_progress(n):
    global _docs_inserted
    with _progress_lock:
        _docs_inserted += n

def _show_progress(total, t0):
    global _docs_inserted
    with _progress_lock:
        done = _docs_inserted
    elapsed = time.perf_counter() - t0
    rate = done / elapsed if elapsed > 0 else 0
    pct = done / total * 100
    bar_len = 40
    filled = int(bar_len * done / total)
    bar = "#" * filled + "-" * (bar_len - filled)
    sys.stdout.write(
        f"\r    [{bar}] {pct:5.1f}%  {done:>10,}/{total:,}  {rate:,.0f} docs/s  "
    )
    sys.stdout.flush()


def insert_worker(tid, batches):
    rng = random.Random(42 + tid)
    client = OxiDbClient("127.0.0.1", HAPROXY_PORT, timeout=SOCKET_TIMEOUT)
    try:
        for start_id, count in batches:
            docs = generate_batch(start_id, count, rng)
            for attempt in range(5):
                try:
                    client.insert_many(COLLECTION, docs)
                    _update_progress(count)
                    break
                except Exception:
                    if attempt == 4:
                        raise
                    time.sleep(1 + attempt)
                    try:
                        client.close()
                    except Exception:
                        pass
                    client = OxiDbClient("127.0.0.1", HAPROXY_PORT, timeout=SOCKET_TIMEOUT)
    finally:
        client.close()


# -- Query runner -------------------------------------------------------------

def run_query(label, fn, runs=QUERY_RUNS):
    times = []
    result_count = 0
    for _ in range(runs):
        with Timer() as t:
            result = fn()
        times.append(t.ms)
        if isinstance(result, list):
            result_count = len(result)
        elif isinstance(result, dict):
            result_count = result.get("count", result.get("modified", 0))
        elif isinstance(result, int):
            result_count = result
    median = statistics.median(times)
    return label, median, result_count


# -- Report collector ---------------------------------------------------------

class Report:
    def __init__(self):
        self.results = []  # (section, label, value, unit)
        self._section = ""

    def set_section(self, s):
        self._section = s

    def add(self, label, value, unit=""):
        self.results.append((self._section, label, value, unit))

    def print_summary(self):
        header("PERFORMANCE REPORT SUMMARY")
        cur_sec = ""
        print(f"  {'Metric':<52s} {'Value':>14s} {'Unit':<10s}")
        print(f"  {'-'*52} {'-'*14} {'-'*10}")
        for sec, label, value, unit in self.results:
            if sec != cur_sec:
                cur_sec = sec
                print(f"\n  [{sec}]")
            if isinstance(value, float):
                print(f"    {label:<50s} {value:>14,.1f} {unit:<10s}")
            else:
                print(f"    {label:<50s} {str(value):>14s} {unit:<10s}")
        print()


# =============================================================================
# MAIN BENCHMARK
# =============================================================================

def main():
    report = Report()

    header("OxiDB 3-Node Cluster Performance Benchmark")
    log(f"Documents:       {TOTAL_DOCS:,}")
    log(f"Batch size:      {BATCH_SIZE:,}")
    log(f"Insert threads:  {NUM_INSERT_THREADS}")
    log(f"Query runs:      {QUERY_RUNS}")
    log(f"Date:            {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # =========================================================================
    # Phase 1: Cluster Bootstrap
    # =========================================================================
    report.set_section("Cluster Bootstrap")
    section("Phase 1: Cluster Bootstrap")

    log("Waiting for nodes ...")
    for nid, port in NODE_PORTS.items():
        wait_for_port("127.0.0.1", port, timeout=60)
        with connect_node(nid) as c:
            c.ping()
        log(f"  Node {nid} (port {port}) up")

    with Timer() as t_bootstrap:
        with connect_node(1) as c:
            resp = c._request({"cmd": "raft_init"})
            assert resp.get("ok"), f"raft_init failed: {resp}"
        time.sleep(1)
        for nid in [2, 3]:
            with connect_node(1) as c:
                resp = c._request({
                    "cmd": "raft_add_learner",
                    "node_id": nid,
                    "addr": f"oxidb-node{nid}:4445",
                })
                assert resp.get("ok"), f"add_learner({nid}) failed: {resp}"
        time.sleep(1)
        with connect_node(1) as c:
            resp = c._request({"cmd": "raft_change_membership", "members": [1, 2, 3]})
            assert resp.get("ok"), f"change_membership failed: {resp}"
        leader_id, _ = wait_for_leader([1, 2, 3], timeout=15)
        wait_for_haproxy(timeout=15)

    log(f"  Cluster bootstrapped in {t_bootstrap.ms:.0f} ms, leader: node {leader_id}")
    report.add("Bootstrap time", t_bootstrap.ms, "ms")
    report.add("Initial leader", str(leader_id))

    # =========================================================================
    # Phase 2: Bulk Insert (1M docs)
    # =========================================================================
    report.set_section("Bulk Insert")
    section(f"Phase 2: Bulk Insert ({TOTAL_DOCS:,} documents through HAProxy)")

    total_batches_list = []
    for b in range(TOTAL_DOCS // BATCH_SIZE):
        total_batches_list.append((b * BATCH_SIZE, BATCH_SIZE))
    remainder = TOTAL_DOCS % BATCH_SIZE
    if remainder > 0:
        total_batches_list.append((len(total_batches_list) * BATCH_SIZE, remainder))

    thread_batches = [[] for _ in range(NUM_INSERT_THREADS)]
    for i, batch in enumerate(total_batches_list):
        thread_batches[i % NUM_INSERT_THREADS].append(batch)

    global _docs_inserted
    _docs_inserted = 0
    t0 = time.perf_counter()

    with ThreadPoolExecutor(max_workers=NUM_INSERT_THREADS) as pool:
        futures = [pool.submit(insert_worker, tid, thread_batches[tid])
                   for tid in range(NUM_INSERT_THREADS)]
        while not all(f.done() for f in futures):
            _show_progress(TOTAL_DOCS, t0)
            time.sleep(0.5)
        for f in futures:
            f.result()

    insert_elapsed = time.perf_counter() - t0
    _show_progress(TOTAL_DOCS, t0)
    print()

    insert_rate = TOTAL_DOCS / insert_elapsed
    log(f"  Insert complete: {insert_elapsed:.1f}s ({insert_rate:,.0f} docs/sec)")
    report.add("Total documents inserted", TOTAL_DOCS, "docs")
    report.add("Insert time", insert_elapsed, "sec")
    report.add("Insert throughput", insert_rate, "docs/sec")

    # Verify count on leader
    with connect_haproxy() as c:
        leader_count = c.count(COLLECTION)
    log(f"  Leader count: {leader_count:,}")
    assert leader_count == TOTAL_DOCS

    # =========================================================================
    # Phase 3: Replication Convergence
    # =========================================================================
    report.set_section("Replication")
    section("Phase 3: Replication Convergence")

    t0_repl = time.perf_counter()
    wait_for_replication([1, 2, 3], COLLECTION, TOTAL_DOCS, timeout=120)
    repl_time = (time.perf_counter() - t0_repl) * 1000

    log(f"  All 3 nodes converged in {repl_time:.0f} ms")
    report.add("Replication convergence", repl_time, "ms")

    # Per-node count verification
    for nid in [1, 2, 3]:
        with connect_node(nid) as c:
            cnt = c.count(COLLECTION)
        log(f"  Node {nid}: {cnt:,} docs")

    # =========================================================================
    # Phase 4: Index Creation
    # =========================================================================
    report.set_section("Index Creation")
    section("Phase 4: Index Creation on 1M Documents")

    index_specs = [
        ("field", "created_at"),
        ("field", "status"),
        ("field", "category"),
        ("field", "country"),
        ("field", "amount"),
        ("field", "priority"),
        ("field", "customer_id"),
        ("composite", ["status", "created_at"]),
        ("composite", ["category", "country"]),
    ]

    with connect_haproxy() as c:
        for idx_type, field in index_specs:
            with Timer() as t:
                if idx_type == "field":
                    c.create_index(COLLECTION, field)
                else:
                    c.create_composite_index(COLLECTION, field)
            label = field if isinstance(field, str) else "+".join(field)
            log(f"  {idx_type:>10s} index: {label:<30s} {t.ms:>8.1f} ms")
            report.add(f"Index: {label}", t.ms, "ms")

    # =========================================================================
    # Phase 5: Read Performance — Leader vs Followers vs HAProxy
    # =========================================================================
    report.set_section("Read Performance")
    section("Phase 5: Read Performance (median of 5 runs)")

    # Allow index replication to settle
    time.sleep(2)

    queries = [
        ("count(*)",
         lambda c: c.count(COLLECTION)),
        ("count(status=completed)",
         lambda c: c.count(COLLECTION, {"status": "completed"})),
        ("find(status=pending, limit=100)",
         lambda c: c.find(COLLECTION, {"status": "pending"}, limit=100)),
        ("find(amount 100-500, limit=100)",
         lambda c: c.find(COLLECTION, {"$and": [{"amount": {"$gte": 100}}, {"amount": {"$lte": 500}}]}, limit=100)),
        ("find(created_at 1-month range, limit=100)",
         lambda c: c.find(COLLECTION, {"$and": [
             {"created_at": {"$gte": "2023-06-01T00:00:00Z"}},
             {"created_at": {"$lt": "2023-07-01T00:00:00Z"}}
         ]}, limit=100)),
        ("find(country=TR + 2024, limit=100)",
         lambda c: c.find(COLLECTION, {"$and": [
             {"country": "TR"},
             {"created_at": {"$gte": "2024-01-01T00:00:00Z"}}
         ]}, limit=100)),
        ("find_one(order_id=500000)",
         lambda c: c.find_one(COLLECTION, {"order_id": 500000})),
    ]

    # Test on all read targets
    read_targets = {
        "HAProxy (leader)": connect_haproxy,
    }
    # Add follower nodes
    for nid in [1, 2, 3]:
        label = f"Node {nid}"
        if nid == leader_id:
            label += " (leader)"
        else:
            label += " (follower)"
        read_targets[label] = lambda n=nid: connect_node(n)

    print(f"  {'Query':<44s}", end="")
    for target_name in read_targets:
        print(f" {target_name:>18s}", end="")
    print("  (ms)")
    print(f"  {'-'*44}", end="")
    for _ in read_targets:
        print(f" {'-'*18}", end="")
    print()

    for q_label, q_fn in queries:
        print(f"  {q_label:<44s}", end="")
        for target_name, conn_fn in read_targets.items():
            times = []
            for _ in range(QUERY_RUNS):
                with conn_fn() as c:
                    with Timer() as t:
                        q_fn(c)
                    times.append(t.ms)
            med = statistics.median(times)
            print(f" {med:>18.2f}", end="")
            report.add(f"{q_label} [{target_name}]", med, "ms")
        print()

    # =========================================================================
    # Phase 6: Aggregation Performance
    # =========================================================================
    report.set_section("Aggregation")
    section("Phase 6: Aggregation Performance (median of 5 runs)")

    agg_queries = [
        ("Group by status, count",
         [{"$group": {"_id": "$status", "n": {"$sum": 1}}}]),
        ("Group by category, sum amount",
         [{"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
          {"$sort": {"total": -1}}]),
        ("Match 2023 + group by country",
         [{"$match": {"$and": [
             {"created_at": {"$gte": "2023-01-01T00:00:00Z"}},
             {"created_at": {"$lt": "2024-01-01T00:00:00Z"}}
         ]}},
          {"$group": {"_id": "$country", "n": {"$sum": 1}}},
          {"$sort": {"n": -1}}]),
        ("Match status=completed + group cat, avg",
         [{"$match": {"status": "completed"}},
          {"$group": {"_id": "$category", "avg_amt": {"$avg": "$amount"}, "cnt": {"$sum": 1}}},
          {"$sort": {"avg_amt": -1}}]),
        ("Top 5 countries by revenue",
         [{"$group": {"_id": "$country", "revenue": {"$sum": "$amount"}}},
          {"$sort": {"revenue": -1}},
          {"$limit": 5}]),
    ]

    print(f"  {'Pipeline':<48s} {'Time(ms)':>10s} {'Results':>10s}")
    print(f"  {'-'*48} {'-'*10} {'-'*10}")

    with connect_haproxy() as c:
        for label, pipeline in agg_queries:
            _, med, cnt = run_query(label, lambda p=pipeline: c.aggregate(COLLECTION, p))
            print(f"  {label:<48s} {med:>10.2f} {cnt:>10,}")
            report.add(label, med, "ms")

    # =========================================================================
    # Phase 7: Write Performance (single + update)
    # =========================================================================
    report.set_section("Write Performance")
    section("Phase 7: Write Performance")

    # Single inserts
    N_SINGLE = 1000
    with connect_haproxy() as c:
        rng = random.Random(99)
        with Timer() as t_single:
            for i in range(N_SINGLE):
                c.insert(COLLECTION, {
                    "order_id": TOTAL_DOCS + i,
                    "customer_id": rng.randint(1, 200_000),
                    "amount": round(rng.uniform(5.0, 5000.0), 2),
                    "status": "new",
                    "category": rng.choice(CATEGORIES),
                    "country": rng.choice(COUNTRIES),
                    "created_at": "2025-01-01T00:00:00Z",
                    "priority": 3,
                })
    single_rate = N_SINGLE / t_single.elapsed
    log(f"  Single inserts: {N_SINGLE} in {t_single.ms:.0f} ms ({single_rate:,.0f} ops/sec)")
    report.add("Single insert throughput", single_rate, "ops/sec")

    # update_one
    N_UPDATES = 1000
    with connect_haproxy() as c:
        rng = random.Random(77)
        with Timer() as t_update:
            for i in range(N_UPDATES):
                oid = rng.randint(0, TOTAL_DOCS - 1)
                c.update_one(COLLECTION, {"order_id": oid}, {"$set": {"status": "updated"}})
    update_rate = N_UPDATES / t_update.elapsed
    log(f"  update_one:     {N_UPDATES} in {t_update.ms:.0f} ms ({update_rate:,.0f} ops/sec)")
    report.add("update_one throughput", update_rate, "ops/sec")

    # delete_one
    N_DELETES = 500
    with connect_haproxy() as c:
        with Timer() as t_delete:
            for i in range(N_DELETES):
                c.delete_one(COLLECTION, {"order_id": TOTAL_DOCS + i})
    delete_rate = N_DELETES / t_delete.elapsed
    log(f"  delete_one:     {N_DELETES} in {t_delete.ms:.0f} ms ({delete_rate:,.0f} ops/sec)")
    report.add("delete_one throughput", delete_rate, "ops/sec")

    # =========================================================================
    # Phase 8: Concurrent Mixed Workload
    # =========================================================================
    report.set_section("Mixed Workload")
    section("Phase 8: Concurrent Mixed Workload (10 threads, 10s)")

    MIXED_DURATION = 10  # seconds
    MIXED_THREADS = 10
    _mixed_ops = {"reads": 0, "writes": 0, "errors": 0}
    _mixed_lock = threading.Lock()
    _mixed_stop = threading.Event()

    def mixed_worker(tid):
        rng = random.Random(200 + tid)
        try:
            c = OxiDbClient("127.0.0.1", HAPROXY_PORT, timeout=30.0)
        except Exception:
            with _mixed_lock:
                _mixed_ops["errors"] += 1
            return
        try:
            while not _mixed_stop.is_set():
                try:
                    op = rng.random()
                    if op < 0.6:  # 60% reads
                        c.find(COLLECTION, {"status": rng.choice(STATUSES)}, limit=10)
                        with _mixed_lock:
                            _mixed_ops["reads"] += 1
                    elif op < 0.8:  # 20% count
                        c.count(COLLECTION, {"category": rng.choice(CATEGORIES)})
                        with _mixed_lock:
                            _mixed_ops["reads"] += 1
                    elif op < 0.95:  # 15% update
                        c.update_one(COLLECTION,
                                     {"order_id": rng.randint(0, TOTAL_DOCS - 1)},
                                     {"$inc": {"priority": 1}})
                        with _mixed_lock:
                            _mixed_ops["writes"] += 1
                    else:  # 5% insert
                        c.insert(COLLECTION, {
                            "order_id": TOTAL_DOCS + 10000 + rng.randint(0, 999999),
                            "status": "mixed",
                            "category": rng.choice(CATEGORIES),
                            "country": rng.choice(COUNTRIES),
                            "amount": round(rng.uniform(1, 100), 2),
                            "created_at": "2025-01-15T00:00:00Z",
                            "priority": 1,
                        })
                        with _mixed_lock:
                            _mixed_ops["writes"] += 1
                except Exception:
                    with _mixed_lock:
                        _mixed_ops["errors"] += 1
        finally:
            c.close()

    threads = [threading.Thread(target=mixed_worker, args=(tid,)) for tid in range(MIXED_THREADS)]
    for th in threads:
        th.start()
    time.sleep(MIXED_DURATION)
    _mixed_stop.set()
    for th in threads:
        th.join(timeout=10)

    total_mixed = _mixed_ops["reads"] + _mixed_ops["writes"]
    mixed_rate = total_mixed / MIXED_DURATION
    log(f"  Duration:    {MIXED_DURATION}s")
    log(f"  Total ops:   {total_mixed:,}")
    log(f"  Reads:       {_mixed_ops['reads']:,}")
    log(f"  Writes:      {_mixed_ops['writes']:,}")
    log(f"  Errors:      {_mixed_ops['errors']:,}")
    log(f"  Throughput:  {mixed_rate:,.0f} ops/sec")
    report.add("Mixed total ops", total_mixed, "ops")
    report.add("Mixed reads", _mixed_ops["reads"], "ops")
    report.add("Mixed writes", _mixed_ops["writes"], "ops")
    report.add("Mixed errors", _mixed_ops["errors"], "ops")
    report.add("Mixed throughput", mixed_rate, "ops/sec")

    # =========================================================================
    # Phase 9: Leader Failover
    # =========================================================================
    report.set_section("Failover")
    section(f"Phase 9: Leader Failover (killing node {leader_id})")

    # Get pre-failover count
    with connect_haproxy() as c:
        pre_count = c.count(COLLECTION)
    log(f"  Pre-failover doc count: {pre_count:,}")

    t0_failover = time.perf_counter()
    docker_stop_node(leader_id)
    log(f"  Node {leader_id} stopped")

    survivor_ids = [nid for nid in [1, 2, 3] if nid != leader_id]

    new_leader_id, _ = wait_for_leader(survivor_ids, timeout=30)
    election_ms = (time.perf_counter() - t0_failover) * 1000
    log(f"  New leader elected: node {new_leader_id} in {election_ms:.0f} ms")
    report.add("Leader election time", election_ms, "ms")

    wait_for_haproxy(timeout=15)
    haproxy_ms = (time.perf_counter() - t0_failover) * 1000
    log(f"  HAProxy failover complete: {haproxy_ms:.0f} ms total")
    report.add("HAProxy failover time", haproxy_ms, "ms")

    # =========================================================================
    # Phase 10: Post-Failover Performance
    # =========================================================================
    report.set_section("Post-Failover")
    section("Phase 10: Post-Failover Performance (2-node cluster)")

    # Post-failover bulk insert
    POST_INSERT = max(10_000, TOTAL_DOCS // 20)
    log(f"  Inserting {POST_INSERT:,} documents through HAProxy ...")
    with connect_haproxy() as c:
        rng = random.Random(42)
        with Timer() as t_post:
            for start in range(0, POST_INSERT, BATCH_SIZE):
                count = min(BATCH_SIZE, POST_INSERT - start)
                docs = generate_batch(TOTAL_DOCS + 100_000 + start, count, rng)
                c.insert_many(COLLECTION, docs)
    post_rate = POST_INSERT / t_post.elapsed
    log(f"  Post-failover insert: {t_post.elapsed:.1f}s ({post_rate:,.0f} docs/sec)")
    report.add("Post-failover insert rate", post_rate, "docs/sec")

    # Post-failover queries
    log("  Post-failover queries:")
    with connect_haproxy() as c:
        post_queries = [
            ("count(*) post-failover", lambda: c.count(COLLECTION)),
            ("find(status=pending, limit=100) post-failover",
             lambda: c.find(COLLECTION, {"status": "pending"}, limit=100)),
            ("Agg: group by status post-failover",
             lambda: c.aggregate(COLLECTION, [
                 {"$group": {"_id": "$status", "n": {"$sum": 1}}}
             ])),
        ]
        for label, fn in post_queries:
            _, med, cnt = run_query(label, fn)
            log(f"    {label:<48s} {med:>8.2f} ms  ({cnt:,} results)")
            report.add(label, med, "ms")

    # Verify replication on survivors
    log("  Verifying replication on survivors ...")
    with connect_haproxy() as c:
        expected = c.count(COLLECTION)
    wait_for_replication(survivor_ids, COLLECTION, expected, timeout=60)
    log(f"  Both survivors have {expected:,} documents")
    report.add("Final document count", expected, "docs")

    # =========================================================================
    # REPORT
    # =========================================================================
    report.print_summary()

    log("BENCHMARK COMPLETE")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"\nFAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
