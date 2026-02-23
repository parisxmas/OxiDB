#!/usr/bin/env python3
"""
OxiDB vs PostgreSQL JSONB — 1M document side-by-side benchmark.

Both databases run on localhost:
  - OxiDB:      port 4444
  - PostgreSQL:  port 5432 (database: benchmark)
"""

import sys
import os
import time
import random
import json
import threading
from concurrent.futures import ThreadPoolExecutor

import psutil
import psycopg2
from psycopg2.extras import execute_values, Json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
from oxidb import OxiDbClient

HOST = "127.0.0.1"
OXIDB_PORT = 4444
PG_PORT = 5432
PG_DB = "benchmark"
TOTAL_DOCS = 1_000_000
BATCH_SIZE = 5_000
NUM_THREADS = 8
COLLECTION = "bench_1m"
SOCKET_TIMEOUT = 300.0

STATUSES = ["completed", "pending", "cancelled", "refunded"]
CATEGORIES = ["electronics", "clothing", "books", "home", "sports",
              "toys", "food", "beauty", "automotive", "garden"]
COUNTRIES = ["TR", "US", "DE", "GB", "FR", "JP", "BR", "IN", "CA", "AU"]


# -- Memory tracking ---------------------------------------------------------

def _fmt_mb(nbytes):
    return f"{nbytes / (1024 * 1024):.1f} MB"


def _find_pid(name_substring):
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            cmdline = " ".join(proc.info["cmdline"] or [])
            if name_substring in cmdline:
                return proc.info["pid"]
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None


def get_memory(pid):
    if pid is None:
        return None
    try:
        return psutil.Process(pid).memory_info().rss
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None


# -- Data generation ----------------------------------------------------------

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


# -- Progress -----------------------------------------------------------------

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
    bar = "\u2588" * filled + "\u2591" * (40 - filled)
    sys.stdout.write(
        f"\r  {label} [{bar}] {pct:5.1f}%  {done:>10,}/{total:,}  {rate:,.0f} docs/s  "
    )
    sys.stdout.flush()


# -- Insert workers -----------------------------------------------------------

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


def pg_worker(tid, batches):
    conn = psycopg2.connect(host=HOST, port=PG_PORT, dbname=PG_DB)
    conn.autocommit = True
    cur = conn.cursor()
    rng = random.Random(42 + tid)
    try:
        for start_id, count in batches:
            docs = generate_batch(start_id, count, rng)
            values = [(json.dumps(d),) for d in docs]
            execute_values(cur,
                f"INSERT INTO {COLLECTION} (data) VALUES %s",
                values, template="(%s::jsonb)")
            _bump(count)
    finally:
        cur.close()
        conn.close()


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


# -- Query helpers ------------------------------------------------------------

def pg_find(cur, query_sql, params=None):
    """Execute a SELECT and return all rows as dicts."""
    cur.execute(query_sql, params)
    return cur.fetchall()


def pg_count(cur, where_clause="TRUE", params=None):
    cur.execute(f"SELECT COUNT(*) FROM {COLLECTION} WHERE {where_clause}", params)
    return cur.fetchone()[0]


# -- Benchmark runner ---------------------------------------------------------

def bench(label, oxi_fn, pg_fn, runs=3):
    oxi_times = []
    pg_times = []
    for _ in range(runs):
        with Timer() as t:
            oxi_result = oxi_fn()
        oxi_times.append(t.ms)
        with Timer() as t:
            pg_result = pg_fn()
        pg_times.append(t.ms)

    oxi_best = min(oxi_times)
    pg_best = min(pg_times)
    ratio = pg_best / oxi_best if oxi_best > 0 else 0
    winner = "OxiDB" if oxi_best <= pg_best else "PgSQL"

    if isinstance(oxi_result, list):
        cnt = len(oxi_result)
    elif isinstance(oxi_result, int):
        cnt = oxi_result
    else:
        cnt = 0

    w_color = "\033[92m" if winner == "OxiDB" else "\033[93m"
    print(f"  {label:<48s} {oxi_best:>10.1f}ms {pg_best:>10.1f}ms  {w_color}{ratio:>6.2f}x {winner}\033[0m  ({cnt:,} results)")
    return {"test": label, "oxidb_ms": oxi_best, "pg_ms": pg_best, "ratio": ratio, "winner": winner}


# -- Main ---------------------------------------------------------------------

def main():
    print()
    print("  \033[1m\u2554" + "\u2550" * 78 + "\u2557\033[0m")
    print("  \033[1m\u2551              OxiDB vs PostgreSQL JSONB \u2014 1M Document Benchmark" + " " * 15 + "\u2551\033[0m")
    print("  \033[1m\u255a" + "\u2550" * 78 + "\u255d\033[0m")
    print()
    print(f"  Documents:  {TOTAL_DOCS:,}")
    print(f"  Batch size: {BATCH_SIZE:,}   Threads: {NUM_THREADS}")
    print(f"  OxiDB:      {HOST}:{OXIDB_PORT}")
    print(f"  PostgreSQL: {HOST}:{PG_PORT}/{PG_DB}")
    print(f"  Date:       {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Connect
    oxi = OxiDbClient(HOST, OXIDB_PORT, timeout=SOCKET_TIMEOUT)

    pg_conn = psycopg2.connect(host=HOST, port=PG_PORT, dbname=PG_DB)
    pg_conn.autocommit = True
    pg = pg_conn.cursor()

    # PIDs
    oxi_pid = _find_pid("oxidb-server")
    pg_pid = _find_pid("postgres")
    print(f"  PIDs:       OxiDB={oxi_pid}  PostgreSQL={pg_pid}")
    print()

    # Cleanup
    try:
        oxi.drop_collection(COLLECTION)
    except Exception:
        pass
    pg.execute(f"DROP TABLE IF EXISTS {COLLECTION}")

    # Create PG table: id serial + jsonb column
    pg.execute(f"""
        CREATE TABLE {COLLECTION} (
            id SERIAL PRIMARY KEY,
            data JSONB NOT NULL
        )
    """)

    # Baseline memory
    oxi_mem_before = get_memory(oxi_pid)
    pg_mem_before = get_memory(pg_pid)

    # == Phase 1: Insert 1M ==================================================
    print("  \033[1m\u2500\u2500 Phase 1: Insert 1,000,000 documents \u2500\u2500\033[0m")
    print()

    oxi_insert_time = run_inserts(oxi_worker, "OxiDB  ")
    oxi_rate = TOTAL_DOCS / oxi_insert_time

    pg_insert_time = run_inserts(pg_worker, "PgSQL  ")
    pg_rate = TOTAL_DOCS / pg_insert_time

    print()
    insert_ratio = pg_insert_time / oxi_insert_time if oxi_insert_time > 0 else 0
    insert_winner = "OxiDB" if oxi_insert_time <= pg_insert_time else "PgSQL"
    w_color = "\033[92m" if insert_winner == "OxiDB" else "\033[93m"
    print(f"  OxiDB:      {oxi_insert_time:.2f}s ({oxi_rate:,.0f} docs/s)")
    print(f"  PostgreSQL: {pg_insert_time:.2f}s ({pg_rate:,.0f} docs/s)")
    print(f"  Winner:     {w_color}{insert_winner} ({insert_ratio:.2f}x)\033[0m")
    print()

    # Memory after inserts
    oxi_mem_after_insert = get_memory(oxi_pid)
    pg_mem_after_insert = get_memory(pg_pid)

    # Verify counts
    oxi_count = oxi.count(COLLECTION)
    pg.execute(f"SELECT COUNT(*) FROM {COLLECTION}")
    pg_count_val = pg.fetchone()[0]
    print(f"  Verified: OxiDB={oxi_count:,}  PostgreSQL={pg_count_val:,}")
    print()

    # == Phase 2: Create indexes ==============================================
    print("  \033[1m\u2500\u2500 Phase 2: Create indexes \u2500\u2500\033[0m")
    print()
    print(f"  {'Index':<35s} {'OxiDB':>10s} {'PgSQL':>10s}  Winner")
    print(f"  {'\u2500'*35} {'\u2500'*10} {'\u2500'*10}  {'\u2500'*8}")

    index_fields = ["status", "category", "country", "amount", "order_id"]
    for field in index_fields:
        with Timer() as t1:
            oxi.create_index(COLLECTION, field)
        with Timer() as t2:
            pg.execute(f"CREATE INDEX ON {COLLECTION} ((data->>'{field}'))")
        ratio = t2.ms / t1.ms if t1.ms > 0 else 0
        winner = "OxiDB" if t1.ms <= t2.ms else "PgSQL"
        w_color = "\033[92m" if winner == "OxiDB" else "\033[93m"
        print(f"  {field:<35s} {t1.ms:>9.1f}ms {t2.ms:>9.1f}ms  {w_color}{ratio:.2f}x {winner}\033[0m")

    # ANALYZE so PG query planner has stats
    pg.execute(f"ANALYZE {COLLECTION}")
    print()

    # == Phase 3: Query benchmark =============================================
    print("  \033[1m\u2500\u2500 Phase 3: Queries (best of 3 runs, 1M docs) \u2500\u2500\033[0m")
    print()
    print(f"  {'Query':<48s} {'OxiDB':>10s}  {'PgSQL':>10s}  {'Ratio':>7s} Winner")
    print(f"  {'\u2500'*48} {'\u2500'*10}  {'\u2500'*10}  {'\u2500'*7} {'\u2500'*8}")

    results = []

    # Find queries
    results.append(bench("Find: status=completed",
        lambda: oxi.find(COLLECTION, {"status": "completed"}),
        lambda: pg_find(pg, f"SELECT data FROM {COLLECTION} WHERE data->>'status' = 'completed'")))

    results.append(bench("Find: category=electronics",
        lambda: oxi.find(COLLECTION, {"category": "electronics"}),
        lambda: pg_find(pg, f"SELECT data FROM {COLLECTION} WHERE data->>'category' = 'electronics'")))

    results.append(bench("Find: amount > 4000",
        lambda: oxi.find(COLLECTION, {"amount": {"$gt": 4000}}),
        lambda: pg_find(pg, f"SELECT data FROM {COLLECTION} WHERE (data->>'amount')::float > 4000")))

    results.append(bench("Find: country=TR + status=completed",
        lambda: oxi.find(COLLECTION, {"country": "TR", "status": "completed"}),
        lambda: pg_find(pg, f"SELECT data FROM {COLLECTION} WHERE data->>'country' = 'TR' AND data->>'status' = 'completed'")))

    results.append(bench("Find: priority >= 4",
        lambda: oxi.find(COLLECTION, {"priority": {"$gte": 4}}),
        lambda: pg_find(pg, f"SELECT data FROM {COLLECTION} WHERE (data->>'priority')::int >= 4")))

    results.append(bench("Find: $or country TR|US",
        lambda: oxi.find(COLLECTION, {"$or": [{"country": "TR"}, {"country": "US"}]}),
        lambda: pg_find(pg, f"SELECT data FROM {COLLECTION} WHERE data->>'country' IN ('TR', 'US')")))

    results.append(bench("Find: $in category [books,food,toys]",
        lambda: oxi.find(COLLECTION, {"category": {"$in": ["books", "food", "toys"]}}),
        lambda: pg_find(pg, f"SELECT data FROM {COLLECTION} WHERE data->>'category' IN ('books', 'food', 'toys')")))

    results.append(bench("Find: sort amount desc, limit 10",
        lambda: oxi.find(COLLECTION, {}, sort={"amount": -1}, limit=10),
        lambda: pg_find(pg, f"SELECT data FROM {COLLECTION} ORDER BY (data->>'amount')::float DESC LIMIT 10")))

    results.append(bench("FindOne: order_id=500000",
        lambda: oxi.find_one(COLLECTION, {"order_id": 500000}),
        lambda: pg_find(pg, f"SELECT data FROM {COLLECTION} WHERE (data->>'order_id')::int = 500000 LIMIT 1")))

    # Count queries
    print()
    results.append(bench("Count: all documents",
        lambda: oxi.count(COLLECTION),
        lambda: pg_count(pg)))

    results.append(bench("Count: status=completed",
        lambda: oxi.count(COLLECTION, {"status": "completed"}),
        lambda: pg_count(pg, "data->>'status' = 'completed'")))

    results.append(bench("Count: amount 100-500",
        lambda: oxi.count(COLLECTION, {"$and": [{"amount": {"$gte": 100}}, {"amount": {"$lte": 500}}]}),
        lambda: pg_count(pg, "(data->>'amount')::float >= 100 AND (data->>'amount')::float <= 500")))

    # Aggregation queries
    print()
    results.append(bench("Agg: group by status, count",
        lambda: oxi.aggregate(COLLECTION, [
            {"$group": {"_id": "$status", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}]),
        lambda: pg_find(pg, f"""
            SELECT data->>'status' AS status, COUNT(*) AS cnt
            FROM {COLLECTION} GROUP BY data->>'status' ORDER BY cnt DESC""")))

    results.append(bench("Agg: group by category, sum amount",
        lambda: oxi.aggregate(COLLECTION, [
            {"$group": {"_id": "$category", "total": {"$sum": "$amount"}, "count": {"$sum": 1}}},
            {"$sort": {"total": -1}}]),
        lambda: pg_find(pg, f"""
            SELECT data->>'category' AS cat, SUM((data->>'amount')::float) AS total, COUNT(*) AS cnt
            FROM {COLLECTION} GROUP BY data->>'category' ORDER BY total DESC""")))

    results.append(bench("Agg: group by country, avg amount",
        lambda: oxi.aggregate(COLLECTION, [
            {"$group": {"_id": "$country", "avg_amt": {"$avg": "$amount"}}},
            {"$sort": {"avg_amt": -1}},
            {"$limit": 5}]),
        lambda: pg_find(pg, f"""
            SELECT data->>'country' AS country, AVG((data->>'amount')::float) AS avg_amt
            FROM {COLLECTION} GROUP BY data->>'country' ORDER BY avg_amt DESC LIMIT 5""")))

    results.append(bench("Agg: match completed + group category",
        lambda: oxi.aggregate(COLLECTION, [
            {"$match": {"status": "completed"}},
            {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
            {"$sort": {"total": -1}},
            {"$limit": 5}]),
        lambda: pg_find(pg, f"""
            SELECT data->>'category' AS cat, SUM((data->>'amount')::float) AS total
            FROM {COLLECTION} WHERE data->>'status' = 'completed'
            GROUP BY data->>'category' ORDER BY total DESC LIMIT 5""")))

    # Update queries
    print()
    results.append(bench("Update: $inc amount for order_id=1",
        lambda: oxi.update(COLLECTION, {"order_id": 1}, {"$inc": {"amount": 1}}),
        lambda: pg.execute(f"""
            UPDATE {COLLECTION} SET data = jsonb_set(data, '{{amount}}',
                to_jsonb((data->>'amount')::float + 1))
            WHERE (data->>'order_id')::int = 1""")))

    results.append(bench("Update: $set status for order_id=2",
        lambda: oxi.update(COLLECTION, {"order_id": 2}, {"$set": {"status": "shipped"}}),
        lambda: pg.execute(f"""
            UPDATE {COLLECTION} SET data = jsonb_set(data, '{{status}}', '"shipped"')
            WHERE (data->>'order_id')::int = 2""")))

    # Delete
    results.append(bench("Delete: order_id=999999",
        lambda: oxi.delete(COLLECTION, {"order_id": 999999}),
        lambda: pg.execute(f"DELETE FROM {COLLECTION} WHERE (data->>'order_id')::int = 999999")))

    # == Summary ==============================================================
    print()
    print("  \033[1m\u2554" + "\u2550" * 78 + "\u2557\033[0m")
    print("  \033[1m\u2551" + "SUMMARY".center(78) + "\u2551\033[0m")
    print("  \033[1m\u255a" + "\u2550" * 78 + "\u255d\033[0m")
    print()

    oxi_wins = sum(1 for r in results if r["winner"] == "OxiDB")
    pg_wins = sum(1 for r in results if r["winner"] == "PgSQL")
    total = len(results)
    oxi_total = sum(r["oxidb_ms"] for r in results)
    pg_total = sum(r["pg_ms"] for r in results)

    # Final memory
    oxi_mem_final = get_memory(oxi_pid)
    pg_mem_final = get_memory(pg_pid)

    print(f"  Insert 1M:       OxiDB {oxi_insert_time:.2f}s vs PgSQL {pg_insert_time:.2f}s  ({insert_ratio:.2f}x)")
    print(f"  Query tests:     {total}")
    print(f"  OxiDB wins:      \033[92m{oxi_wins}\033[0m / {total}")
    print(f"  PgSQL wins:      \033[93m{pg_wins}\033[0m / {total}")
    print(f"  OxiDB total:     {oxi_total:.2f} ms")
    print(f"  PgSQL total:     {pg_total:.2f} ms")
    if oxi_total > 0:
        print(f"  Overall ratio:   {pg_total / oxi_total:.2f}x")
    print()

    # Memory & Disk
    print("  \033[1m\u2500\u2500 Memory & Disk Usage \u2500\u2500\033[0m")
    print()
    print(f"  {'':40s} {'OxiDB':>12s}  {'PgSQL':>12s}")
    print(f"  {'\u2500'*40} {'\u2500'*12}  {'\u2500'*12}")

    if oxi_mem_before is not None and pg_mem_before is not None:
        print(f"  {'RSS before insert':<40s} {_fmt_mb(oxi_mem_before):>12s}  {_fmt_mb(pg_mem_before):>12s}")
    if oxi_mem_after_insert is not None and pg_mem_after_insert is not None:
        print(f"  {'RSS after 1M insert':<40s} {_fmt_mb(oxi_mem_after_insert):>12s}  {_fmt_mb(pg_mem_after_insert):>12s}")
    if oxi_mem_final is not None and pg_mem_final is not None:
        print(f"  {'RSS after all queries':<40s} {_fmt_mb(oxi_mem_final):>12s}  {_fmt_mb(pg_mem_final):>12s}")

    # PG table size
    pg.execute(f"SELECT pg_total_relation_size('{COLLECTION}')")
    pg_disk = pg.fetchone()[0]
    print(f"  {'Disk (data+indexes)':<40s} {'':>12s}  {_fmt_mb(pg_disk):>12s}")
    print()

    # Cleanup
    try:
        oxi.drop_collection(COLLECTION)
    except Exception:
        pass
    pg.execute(f"DROP TABLE IF EXISTS {COLLECTION}")
    pg.close()
    pg_conn.close()
    oxi.close()


if __name__ == "__main__":
    main()
