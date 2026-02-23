#!/usr/bin/env python3
"""
OxiDB vs MongoDB — 1M document side-by-side benchmark.

Both databases run on localhost:
  - OxiDB:   port 4444
  - MongoDB: port 27018
"""

import sys
import os
import time
import random
import threading
import subprocess
from concurrent.futures import ThreadPoolExecutor

import psutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
from oxidb import OxiDbClient
from pymongo import MongoClient, ASCENDING

HOST = "127.0.0.1"
OXIDB_PORT = 4444
MONGO_PORT = 27018
TOTAL_DOCS = 1_000_000
BATCH_SIZE = 5_000
NUM_THREADS = 8
COLLECTION = "bench_1m"
SOCKET_TIMEOUT = 300.0

STATUSES = ["completed", "pending", "cancelled", "refunded"]
CATEGORIES = ["electronics", "clothing", "books", "home", "sports",
              "toys", "food", "beauty", "automotive", "garden"]
COUNTRIES = ["TR", "US", "DE", "GB", "FR", "JP", "BR", "IN", "CA", "AU"]


# ── Memory tracking ──────────────────────────────────────────────────

def _fmt_mb(nbytes):
    """Format bytes as MB string."""
    return f"{nbytes / (1024 * 1024):.1f} MB"


def _find_pid(name_substring):
    """Find the first process whose name or cmdline contains the substring."""
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            cmdline = " ".join(proc.info["cmdline"] or [])
            if name_substring in cmdline:
                return proc.info["pid"]
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None


def get_memory(pid):
    """Return RSS in bytes for a given PID, or None."""
    if pid is None:
        return None
    try:
        return psutil.Process(pid).memory_info().rss
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None


def get_mongo_data_size(mdb, collection_name):
    """Return MongoDB collection storage size via collStats."""
    try:
        stats = mdb.command("collStats", collection_name)
        return stats.get("storageSize", 0) + stats.get("totalIndexSize", 0)
    except Exception:
        return 0


def get_oxidb_data_size(data_dir, collection_name):
    """Return OxiDB on-disk size (.dat + .wal + .idx + .fidx + .cidx files).
    The server stores data under <data_dir>/oxidb/ (default database)."""
    total = 0
    # Try both the base dir and the default 'oxidb' sub-database
    for sub in ["", "oxidb"]:
        search_dir = os.path.join(data_dir, sub) if sub else data_dir
        if not os.path.isdir(search_dir):
            continue
        for fname in os.listdir(search_dir):
            if fname.startswith(collection_name + ".") or fname.startswith(collection_name + "_"):
                fpath = os.path.join(search_dir, fname)
                if os.path.isfile(fpath):
                    total += os.path.getsize(fpath)
    return total


OXIDB_DATA_DIR = os.environ.get("OXIDB_DATA", "./oxidb_data")


def generate_batch(start_id, count, rng):
    docs = []
    for i in range(count):
        oid = start_id + i
        docs.append({
            "order_id": oid,
            "customer_id": rng.randint(1, 100_000),
            "amount": round(rng.uniform(5.0, 5000.0), 2),
            "status": rng.choice(STATUSES),
            "category": rng.choice(CATEGORIES),
            "country": rng.choice(COUNTRIES),
            "priority": rng.randint(1, 5),
        })
    return docs


class Timer:
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.elapsed = time.perf_counter() - self.t0
        self.ms = self.elapsed * 1000


# ── Progress ──────────────────────────────────────────────────────────

_lock = threading.Lock()
_inserted = 0


def _bump(n):
    global _inserted
    with _lock:
        _inserted += n


def _progress(total, t0, label):
    global _inserted
    with _lock:
        done = _inserted
    elapsed = time.perf_counter() - t0
    rate = done / elapsed if elapsed > 0 else 0
    pct = done / total * 100
    filled = int(40 * done / total)
    bar = "█" * filled + "░" * (40 - filled)
    sys.stdout.write(
        f"\r  {label} [{bar}] {pct:5.1f}%  {done:>10,}/{total:,}  {rate:,.0f} docs/s  "
    )
    sys.stdout.flush()


# ── Insert workers ────────────────────────────────────────────────────

def oxi_worker(tid, batches):
    c = OxiDbClient(HOST, OXIDB_PORT, timeout=SOCKET_TIMEOUT)
    rng = random.Random(42 + tid)
    try:
        for start_id, count in batches:
            docs = generate_batch(start_id, count, rng)
            c.insert_many(COLLECTION, docs)
            _bump(count)
    finally:
        c.close()


def mongo_worker(tid, batches):
    c = MongoClient(HOST, MONGO_PORT)
    db = c["benchmark"]
    rng = random.Random(42 + tid)
    try:
        for start_id, count in batches:
            docs = generate_batch(start_id, count, rng)
            db[COLLECTION].insert_many(docs)
            _bump(count)
    finally:
        c.close()


def run_inserts(worker_fn, label):
    global _inserted
    _inserted = 0

    total_batches = TOTAL_DOCS // BATCH_SIZE
    remainder = TOTAL_DOCS % BATCH_SIZE
    all_batches = [(b * BATCH_SIZE, BATCH_SIZE) for b in range(total_batches)]
    if remainder > 0:
        all_batches.append((total_batches * BATCH_SIZE, remainder))

    thread_batches = [[] for _ in range(NUM_THREADS)]
    for i, batch in enumerate(all_batches):
        thread_batches[i % NUM_THREADS].append(batch)

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as pool:
        futures = [pool.submit(worker_fn, tid, thread_batches[tid]) for tid in range(NUM_THREADS)]
        while not all(f.done() for f in futures):
            _progress(TOTAL_DOCS, t0, label)
            time.sleep(0.3)
        for f in futures:
            f.result()

    elapsed = time.perf_counter() - t0
    _progress(TOTAL_DOCS, t0, label)
    print()
    return elapsed


# ── Query benchmark ───────────────────────────────────────────────────

def bench(label, oxi_fn, mongo_fn, runs=3):
    oxi_times = []
    mongo_times = []
    for _ in range(runs):
        with Timer() as t:
            oxi_result = oxi_fn()
        oxi_times.append(t.ms)
        with Timer() as t:
            mongo_result = mongo_fn()
        mongo_times.append(t.ms)

    oxi_best = min(oxi_times)
    mongo_best = min(mongo_times)
    ratio = mongo_best / oxi_best if oxi_best > 0 else 0
    winner = "OxiDB" if oxi_best <= mongo_best else "MongoDB"

    # result count
    if isinstance(oxi_result, list):
        cnt = len(oxi_result)
    elif isinstance(oxi_result, int):
        cnt = oxi_result
    else:
        cnt = 0

    w_color = "\033[92m" if winner == "OxiDB" else "\033[93m"
    print(f"  {label:<48s} {oxi_best:>10.1f}ms {mongo_best:>10.1f}ms  {w_color}{ratio:>6.2f}x {winner}\033[0m  ({cnt:,} results)")
    return {"test": label, "oxidb_ms": oxi_best, "mongo_ms": mongo_best, "ratio": ratio, "winner": winner}


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print()
    print("  \033[1m╔══════════════════════════════════════════════════════════════════════════════╗\033[0m")
    print("  \033[1m║              OxiDB vs MongoDB — 1M Document Benchmark                        ║\033[0m")
    print("  \033[1m╚══════════════════════════════════════════════════════════════════════════════╝\033[0m")
    print()
    print(f"  Documents:  {TOTAL_DOCS:,}")
    print(f"  Batch size: {BATCH_SIZE:,}   Threads: {NUM_THREADS}")
    print(f"  OxiDB:      {HOST}:{OXIDB_PORT}")
    print(f"  MongoDB:    {HOST}:{MONGO_PORT}")
    print(f"  Date:       {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    oxi = OxiDbClient(HOST, OXIDB_PORT, timeout=SOCKET_TIMEOUT)
    mongo = MongoClient(HOST, MONGO_PORT)
    mdb = mongo["benchmark"]

    # Discover server PIDs for memory tracking
    oxi_pid = _find_pid("oxidb-server")
    mongo_pid = _find_pid("mongod")
    print(f"  PIDs:       OxiDB={oxi_pid}  MongoDB={mongo_pid}")
    print()

    # Cleanup
    try:
        oxi.drop_collection(COLLECTION)
    except Exception:
        pass
    mdb.drop_collection(COLLECTION)

    # Baseline memory
    oxi_mem_before = get_memory(oxi_pid)
    mongo_mem_before = get_memory(mongo_pid)

    # ── Phase 1: Insert 1M ────────────────────────────────────────────
    print("  \033[1m── Phase 1: Insert 1,000,000 documents ──\033[0m")
    print()

    oxi_insert_time = run_inserts(oxi_worker, "OxiDB  ")
    oxi_rate = TOTAL_DOCS / oxi_insert_time

    mongo_insert_time = run_inserts(mongo_worker, "MongoDB")
    mongo_rate = TOTAL_DOCS / mongo_insert_time

    print()
    insert_ratio = mongo_insert_time / oxi_insert_time if oxi_insert_time > 0 else 0
    insert_winner = "OxiDB" if oxi_insert_time <= mongo_insert_time else "MongoDB"
    w_color = "\033[92m" if insert_winner == "OxiDB" else "\033[93m"
    print(f"  OxiDB:   {oxi_insert_time:.2f}s ({oxi_rate:,.0f} docs/s)")
    print(f"  MongoDB: {mongo_insert_time:.2f}s ({mongo_rate:,.0f} docs/s)")
    print(f"  Winner:  {w_color}{insert_winner} ({insert_ratio:.2f}x)\033[0m")
    print()

    # Memory after inserts
    oxi_mem_after_insert = get_memory(oxi_pid)
    mongo_mem_after_insert = get_memory(mongo_pid)

    # Verify counts
    oxi_count = oxi.count(COLLECTION)
    mongo_count = mdb[COLLECTION].count_documents({})
    print(f"  Verified: OxiDB={oxi_count:,}  MongoDB={mongo_count:,}")
    print()

    # ── Phase 2: Create indexes ───────────────────────────────────────
    print("  \033[1m── Phase 2: Create indexes ──\033[0m")
    print()
    print(f"  {'Index':<35s} {'OxiDB':>10s} {'MongoDB':>10s}  Winner")
    print(f"  {'─'*35} {'─'*10} {'─'*10}  {'─'*8}")

    for field in ["status", "category", "country", "amount", "order_id"]:
        with Timer() as t1:
            oxi.create_index(COLLECTION, field)
        with Timer() as t2:
            mdb[COLLECTION].create_index(field)
        ratio = t2.ms / t1.ms if t1.ms > 0 else 0
        winner = "OxiDB" if t1.ms <= t2.ms else "MongoDB"
        w_color = "\033[92m" if winner == "OxiDB" else "\033[93m"
        print(f"  {field:<35s} {t1.ms:>9.1f}ms {t2.ms:>9.1f}ms  {w_color}{ratio:.2f}x {winner}\033[0m")

    print()

    # ── Phase 3: Query benchmark ──────────────────────────────────────
    print("  \033[1m── Phase 3: Queries (best of 3 runs, 1M docs) ──\033[0m")
    print()
    print(f"  {'Query':<48s} {'OxiDB':>10s}  {'MongoDB':>10s}  {'Ratio':>7s} Winner")
    print(f"  {'─'*48} {'─'*10}  {'─'*10}  {'─'*7} {'─'*8}")

    results = []

    # Find queries
    results.append(bench("Find: status=completed",
        lambda: oxi.find(COLLECTION, {"status": "completed"}),
        lambda: list(mdb[COLLECTION].find({"status": "completed"}))))

    results.append(bench("Find: category=electronics",
        lambda: oxi.find(COLLECTION, {"category": "electronics"}),
        lambda: list(mdb[COLLECTION].find({"category": "electronics"}))))

    results.append(bench("Find: amount > 4000",
        lambda: oxi.find(COLLECTION, {"amount": {"$gt": 4000}}),
        lambda: list(mdb[COLLECTION].find({"amount": {"$gt": 4000}}))))

    results.append(bench("Find: country=TR + status=completed",
        lambda: oxi.find(COLLECTION, {"country": "TR", "status": "completed"}),
        lambda: list(mdb[COLLECTION].find({"country": "TR", "status": "completed"}))))

    results.append(bench("Find: priority >= 4",
        lambda: oxi.find(COLLECTION, {"priority": {"$gte": 4}}),
        lambda: list(mdb[COLLECTION].find({"priority": {"$gte": 4}}))))

    results.append(bench("Find: $or country TR|US",
        lambda: oxi.find(COLLECTION, {"$or": [{"country": "TR"}, {"country": "US"}]}),
        lambda: list(mdb[COLLECTION].find({"$or": [{"country": "TR"}, {"country": "US"}]}))))

    results.append(bench("Find: $in category [books,food,toys]",
        lambda: oxi.find(COLLECTION, {"category": {"$in": ["books", "food", "toys"]}}),
        lambda: list(mdb[COLLECTION].find({"category": {"$in": ["books", "food", "toys"]}}))))

    results.append(bench("Find: sort amount desc, limit 10",
        lambda: oxi.find(COLLECTION, {}, sort={"amount": -1}, limit=10),
        lambda: list(mdb[COLLECTION].find({}).sort("amount", -1).limit(10))))

    results.append(bench("FindOne: order_id=500000",
        lambda: oxi.find_one(COLLECTION, {"order_id": 500000}),
        lambda: mdb[COLLECTION].find_one({"order_id": 500000})))

    # Count queries
    print()
    results.append(bench("Count: all documents",
        lambda: oxi.count(COLLECTION),
        lambda: mdb[COLLECTION].count_documents({})))

    results.append(bench("Count: status=completed",
        lambda: oxi.count(COLLECTION, {"status": "completed"}),
        lambda: mdb[COLLECTION].count_documents({"status": "completed"})))

    results.append(bench("Count: amount 100-500",
        lambda: oxi.count(COLLECTION, {"$and": [{"amount": {"$gte": 100}}, {"amount": {"$lte": 500}}]}),
        lambda: mdb[COLLECTION].count_documents({"$and": [{"amount": {"$gte": 100}}, {"amount": {"$lte": 500}}]})))

    # Aggregation queries
    print()
    results.append(bench("Agg: group by status, count",
        lambda: oxi.aggregate(COLLECTION, [
            {"$group": {"_id": "$status", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}]),
        lambda: list(mdb[COLLECTION].aggregate([
            {"$group": {"_id": "$status", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}]))))

    results.append(bench("Agg: group by category, sum amount",
        lambda: oxi.aggregate(COLLECTION, [
            {"$group": {"_id": "$category", "total": {"$sum": "$amount"}, "count": {"$sum": 1}}},
            {"$sort": {"total": -1}}]),
        lambda: list(mdb[COLLECTION].aggregate([
            {"$group": {"_id": "$category", "total": {"$sum": "$amount"}, "count": {"$sum": 1}}},
            {"$sort": {"total": -1}}]))))

    results.append(bench("Agg: group by country, avg amount",
        lambda: oxi.aggregate(COLLECTION, [
            {"$group": {"_id": "$country", "avg_amt": {"$avg": "$amount"}}},
            {"$sort": {"avg_amt": -1}},
            {"$limit": 5}]),
        lambda: list(mdb[COLLECTION].aggregate([
            {"$group": {"_id": "$country", "avg_amt": {"$avg": "$amount"}}},
            {"$sort": {"avg_amt": -1}},
            {"$limit": 5}]))))

    results.append(bench("Agg: match completed + group category",
        lambda: oxi.aggregate(COLLECTION, [
            {"$match": {"status": "completed"}},
            {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
            {"$sort": {"total": -1}},
            {"$limit": 5}]),
        lambda: list(mdb[COLLECTION].aggregate([
            {"$match": {"status": "completed"}},
            {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
            {"$sort": {"total": -1}},
            {"$limit": 5}]))))

    # Update queries
    print()
    results.append(bench("Update: $inc amount for order_id=1",
        lambda: oxi.update(COLLECTION, {"order_id": 1}, {"$inc": {"amount": 1}}),
        lambda: mdb[COLLECTION].update_one({"order_id": 1}, {"$inc": {"amount": 1}})))

    results.append(bench("Update: $set status for order_id=2",
        lambda: oxi.update(COLLECTION, {"order_id": 2}, {"$set": {"status": "shipped"}}),
        lambda: mdb[COLLECTION].update_one({"order_id": 2}, {"$set": {"status": "shipped"}})))

    # Delete
    results.append(bench("Delete: order_id=999999",
        lambda: oxi.delete(COLLECTION, {"order_id": 999999}),
        lambda: mdb[COLLECTION].delete_many({"order_id": 999999})))

    # ── Summary ───────────────────────────────────────────────────────
    print()
    print("  \033[1m╔══════════════════════════════════════════════════════════════════════════════╗\033[0m")
    print("  \033[1m║                                SUMMARY                                       ║\033[0m")
    print("  \033[1m╚══════════════════════════════════════════════════════════════════════════════╝\033[0m")
    print()

    oxi_wins = sum(1 for r in results if r["winner"] == "OxiDB")
    mongo_wins = sum(1 for r in results if r["winner"] == "MongoDB")
    total = len(results)
    oxi_total = sum(r["oxidb_ms"] for r in results)
    mongo_total = sum(r["mongo_ms"] for r in results)

    # Final memory snapshot & disk (before cleanup)
    oxi_mem_final = get_memory(oxi_pid)
    mongo_mem_final = get_memory(mongo_pid)
    oxi_disk = get_oxidb_data_size(OXIDB_DATA_DIR, COLLECTION)
    mongo_disk_stats = get_mongo_data_size(mdb, COLLECTION)

    print(f"  Insert 1M:       OxiDB {oxi_insert_time:.2f}s vs MongoDB {mongo_insert_time:.2f}s  ({insert_ratio:.2f}x)")
    print(f"  Query tests:     {total}")
    print(f"  OxiDB wins:      \033[92m{oxi_wins}\033[0m / {total}")
    print(f"  MongoDB wins:    \033[93m{mongo_wins}\033[0m / {total}")
    print(f"  OxiDB total:     {oxi_total:.2f} ms")
    print(f"  MongoDB total:   {mongo_total:.2f} ms")
    if oxi_total > 0:
        print(f"  Overall ratio:   {mongo_total / oxi_total:.2f}x")
    print()

    # ── Memory & Disk Usage ──────────────────────────────────────────
    print("  \033[1m── Memory & Disk Usage ──\033[0m")
    print()
    print(f"  {'':40s} {'OxiDB':>12s}  {'MongoDB':>12s}")
    print(f"  {'─'*40} {'─'*12}  {'─'*12}")

    if oxi_mem_before is not None and mongo_mem_before is not None:
        print(f"  {'RSS before insert':<40s} {_fmt_mb(oxi_mem_before):>12s}  {_fmt_mb(mongo_mem_before):>12s}")
    if oxi_mem_after_insert is not None and mongo_mem_after_insert is not None:
        print(f"  {'RSS after 1M insert':<40s} {_fmt_mb(oxi_mem_after_insert):>12s}  {_fmt_mb(mongo_mem_after_insert):>12s}")
        oxi_delta = oxi_mem_after_insert - (oxi_mem_before or 0)
        mongo_delta = mongo_mem_after_insert - (mongo_mem_before or 0)
        print(f"  {'RSS delta (insert)':<40s} {_fmt_mb(oxi_delta):>12s}  {_fmt_mb(mongo_delta):>12s}")
    if oxi_mem_final is not None and mongo_mem_final is not None:
        print(f"  {'RSS after all queries':<40s} {_fmt_mb(oxi_mem_final):>12s}  {_fmt_mb(mongo_mem_final):>12s}")
        oxi_total_delta = oxi_mem_final - (oxi_mem_before or 0)
        mongo_total_delta = mongo_mem_final - (mongo_mem_before or 0)
        print(f"  {'RSS delta (total)':<40s} {_fmt_mb(oxi_total_delta):>12s}  {_fmt_mb(mongo_total_delta):>12s}")

    if oxi_disk > 0 or mongo_disk_stats > 0:
        print(f"  {'Disk (data+indexes)':<40s} {_fmt_mb(oxi_disk):>12s}  {_fmt_mb(mongo_disk_stats):>12s}")
    print()

    # Cleanup
    try:
        oxi.drop_collection(COLLECTION)
    except Exception:
        pass
    mdb.drop_collection(COLLECTION)
    oxi.close()
    mongo.close()


if __name__ == "__main__":
    main()
