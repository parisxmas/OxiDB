#!/usr/bin/env python3
"""
OxiDB 15M Document Benchmark — Date Range & Field Queries

Inserts 15 million realistic e-commerce order records, creates indexes,
then runs date-range, combined-field, numeric, aggregation, and count
queries, reporting all timings in milliseconds.

Usage:
    # Start the server first:
    env OXIDB_POOL_SIZE=16 OXIDB_IDLE_TIMEOUT=0 OXIDB_DATA=/tmp/oxidb_bench15m \
        ./target/release/oxidb-server

    # Run the benchmark:
    python3 examples/python/benchmark_15m.py
"""

import sys
import os
import time
import random
import statistics
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
from oxidb import OxiDbClient

# ── Configuration ─────────────────────────────────────────────────────

HOST = os.environ.get("OXIDB_HOST", "127.0.0.1")
PORT = int(os.environ.get("OXIDB_PORT", "4444"))
TOTAL_DOCS = 15_000_000
BATCH_SIZE = 5_000
NUM_INSERT_THREADS = 10
NUM_QUERY_RUNS = 3
COLLECTION = "orders"
SOCKET_TIMEOUT = 300.0  # 5 minutes for large operations

# ── Data generation constants ─────────────────────────────────────────

STATUSES = ["completed", "pending", "cancelled", "refunded"]
CATEGORIES = [
    "electronics", "clothing", "books", "home", "sports",
    "toys", "food", "beauty", "automotive", "garden",
]
COUNTRIES = [
    "TR", "US", "DE", "GB", "FR", "JP", "BR", "IN", "CA", "AU",
    "IT", "ES", "NL", "SE", "NO", "KR", "MX", "PL", "CH", "AT",
]

# Date range: 2022-01-01 to 2024-12-31 (3 years = 1096 days)
DATE_START = datetime(2022, 1, 1, tzinfo=timezone.utc)
DATE_RANGE_DAYS = 1096  # through 2024-12-31


def generate_batch(start_id: int, count: int, rng: random.Random) -> list:
    """Generate a batch of order documents."""
    docs = []
    for i in range(count):
        oid = start_id + i
        created_offset = rng.randint(0, DATE_RANGE_DAYS * 86400)
        created_dt = DATE_START + timedelta(seconds=created_offset)
        # updated_at is 0-30 days after created_at
        updated_dt = created_dt + timedelta(seconds=rng.randint(0, 30 * 86400))

        docs.append({
            "order_id": oid,
            "customer_id": rng.randint(1, 500_000),
            "amount": round(rng.uniform(5.0, 5000.0), 2),
            "status": rng.choice(STATUSES),
            "category": rng.choice(CATEGORIES),
            "country": rng.choice(COUNTRIES),
            "created_at": created_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "updated_at": updated_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "priority": rng.randint(1, 5),
        })
    return docs


# ── Helpers ───────────────────────────────────────────────────────────

class Timer:
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.elapsed = time.perf_counter() - self.t0
        self.ms = self.elapsed * 1000


# Progress tracking for inserts
_progress_lock = threading.Lock()
_docs_inserted = 0


def _update_progress(n: int):
    global _docs_inserted
    with _progress_lock:
        _docs_inserted += n


def _print_progress(total: int, t0: float):
    """Print progress bar — called from main thread."""
    global _docs_inserted
    with _progress_lock:
        done = _docs_inserted
    elapsed = time.perf_counter() - t0
    rate = done / elapsed if elapsed > 0 else 0
    pct = done / total * 100
    bar_len = 40
    filled = int(bar_len * done / total)
    bar = "█" * filled + "░" * (bar_len - filled)
    sys.stdout.write(
        f"\r  [{bar}] {pct:5.1f}%  {done:>12,} / {total:,}  "
        f"{rate:,.0f} docs/sec   "
    )
    sys.stdout.flush()


def insert_worker(thread_id: int, batches: list):
    """Worker that inserts assigned batches via its own connection."""
    client = OxiDbClient(HOST, PORT, timeout=SOCKET_TIMEOUT)
    rng = random.Random(42 + thread_id)
    try:
        for start_id, count in batches:
            docs = generate_batch(start_id, count, rng)
            client.insert_many(COLLECTION, docs)
            _update_progress(count)
    finally:
        client.close()


def run_query(label: str, fn, runs: int = NUM_QUERY_RUNS):
    """Run a query `runs` times, return (label, median_ms, result_count)."""
    times = []
    result_count = 0
    for _ in range(runs):
        with Timer() as t:
            result = fn()
        times.append(t.ms)
        # Determine result count
        if isinstance(result, list):
            result_count = len(result)
        elif isinstance(result, dict):
            result_count = result.get("count", result.get("modified", result.get("deleted", 0)))
        elif isinstance(result, int):
            result_count = result
    median_ms = statistics.median(times)
    return label, median_ms, result_count


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print()
    print("  \033[1m╔═══════════════════════════════════════════════════════════════════════╗\033[0m")
    print("  \033[1m║                   OxiDB 15M Document Benchmark                        ║\033[0m")
    print("  \033[1m╚═══════════════════════════════════════════════════════════════════════╝\033[0m")
    print()
    print(f"  Server:      {HOST}:{PORT}")
    print(f"  Documents:   {TOTAL_DOCS:,}")
    print(f"  Batch size:  {BATCH_SIZE:,}")
    print(f"  Threads:     {NUM_INSERT_THREADS}")
    print(f"  Date:        {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # ── Connect & cleanup ────────────────────────────────────────────
    client = OxiDbClient(HOST, PORT, timeout=SOCKET_TIMEOUT)
    print("  Dropping old collection (if exists)...", end=" ", flush=True)
    try:
        client.drop_collection(COLLECTION)
        print("done.")
    except Exception:
        print("not found.")

    # ── Phase 1: Insert 15M documents ────────────────────────────────
    print()
    print("  \033[1m── Phase 1: Inserting 15,000,000 documents ──\033[0m")
    print()

    # Prepare batch assignments for each thread
    total_batches = TOTAL_DOCS // BATCH_SIZE
    remainder = TOTAL_DOCS % BATCH_SIZE

    all_batches = []
    for b in range(total_batches):
        all_batches.append((b * BATCH_SIZE, BATCH_SIZE))
    if remainder > 0:
        all_batches.append((total_batches * BATCH_SIZE, remainder))

    # Distribute batches round-robin to threads
    thread_batches = [[] for _ in range(NUM_INSERT_THREADS)]
    for i, batch in enumerate(all_batches):
        thread_batches[i % NUM_INSERT_THREADS].append(batch)

    global _docs_inserted
    _docs_inserted = 0
    t0 = time.perf_counter()

    # Launch insert workers
    with ThreadPoolExecutor(max_workers=NUM_INSERT_THREADS) as pool:
        futures = []
        for tid in range(NUM_INSERT_THREADS):
            futures.append(pool.submit(insert_worker, tid, thread_batches[tid]))

        # Progress loop
        while not all(f.done() for f in futures):
            _print_progress(TOTAL_DOCS, t0)
            time.sleep(0.5)

        # Check for exceptions
        for f in futures:
            f.result()  # re-raises any exception

    insert_elapsed = time.perf_counter() - t0
    _print_progress(TOTAL_DOCS, t0)
    print()

    insert_rate = TOTAL_DOCS / insert_elapsed
    print()
    print(f"  Insert complete: {insert_elapsed:.1f}s ({insert_rate:,.0f} docs/sec)")

    # Verify count
    total_count = client.count(COLLECTION)
    print(f"  Verified count: {total_count:,}")
    print()

    # ── Phase 2: Create indexes ──────────────────────────────────────
    print("  \033[1m── Phase 2: Creating indexes ──\033[0m")
    print()

    indexes = [
        ("single", "created_at"),
        ("single", "updated_at"),
        ("single", "status"),
        ("single", "category"),
        ("single", "country"),
        ("single", "amount"),
        ("composite", ["status", "created_at"]),
        ("composite", ["category", "country"]),
    ]

    for idx_type, field in indexes:
        with Timer() as t:
            if idx_type == "single":
                client.create_index(COLLECTION, field)
            else:
                client.create_composite_index(COLLECTION, field)
        label = field if isinstance(field, str) else str(field)
        print(f"    {idx_type:>10s} index on {label:<30s} {t.ms:>10.1f} ms")

    print()

    # ── Phase 3: Queries ─────────────────────────────────────────────
    print("  \033[1m── Phase 3: Queries (median of 3 runs) ──\033[0m")
    print()
    print(f"  {'#':<4s} {'Query':<48s} {'Time(ms)':>10s} {'Results':>12s}")
    print(f"  {'─'*4} {'─'*48} {'─'*10} {'─'*12}")

    results = []
    qnum = 0

    def record(label, fn, runs=NUM_QUERY_RUNS):
        nonlocal qnum
        qnum += 1
        lbl, ms, cnt = run_query(label, fn, runs)
        results.append((qnum, lbl, ms, cnt))
        print(f"  {qnum:<4d} {lbl:<48s} {ms:>10.1f} {cnt:>12,}")

    # ── Date Range Queries ───────────────────────────────────────────

    print()
    print("  \033[1m  Date Range Queries\033[0m")
    print()

    # Q1: 1 month range
    record("Date range: 1 month (Jun 2023)", lambda: client.find(
        COLLECTION,
        {"$and": [
            {"created_at": {"$gte": "2023-06-01T00:00:00Z"}},
            {"created_at": {"$lt": "2023-07-01T00:00:00Z"}},
        ]},
    ))

    # Q2: 1 year range
    record("Date range: 1 year (2023)", lambda: client.find(
        COLLECTION,
        {"$and": [
            {"created_at": {"$gte": "2023-01-01T00:00:00Z"}},
            {"created_at": {"$lt": "2024-01-01T00:00:00Z"}},
        ]},
    ))

    # Q3: 1 week range
    record("Date range: 1 week (Jun 10-17 2023)", lambda: client.find(
        COLLECTION,
        {"$and": [
            {"created_at": {"$gte": "2023-06-10T00:00:00Z"}},
            {"created_at": {"$lt": "2023-06-17T00:00:00Z"}},
        ]},
    ))

    # Q4: Recent records (last 30 days of range)
    record("Date range: recent (Dec 2024+)", lambda: client.find(
        COLLECTION,
        {"created_at": {"$gte": "2024-12-01T00:00:00Z"}},
    ))

    # Q5: Count where updated > created (approximate via range)
    record("Count: updated in 2023 (proxy)", lambda: client.count(
        COLLECTION,
        {"$and": [
            {"updated_at": {"$gte": "2023-01-01T00:00:00Z"}},
            {"updated_at": {"$lt": "2024-01-01T00:00:00Z"}},
        ]},
    ))

    # ── Field + Date Combined Queries ────────────────────────────────

    print()
    print("  \033[1m  Field + Date Combined Queries\033[0m")
    print()

    # Q6: status + date range
    record("status=completed + Jun 2023", lambda: client.find(
        COLLECTION,
        {"$and": [
            {"status": "completed"},
            {"created_at": {"$gte": "2023-06-01T00:00:00Z"}},
            {"created_at": {"$lt": "2023-07-01T00:00:00Z"}},
        ]},
    ))

    # Q7: category + 1 year range
    record("category=electronics + year 2023", lambda: client.find(
        COLLECTION,
        {"$and": [
            {"category": "electronics"},
            {"created_at": {"$gte": "2023-01-01T00:00:00Z"}},
            {"created_at": {"$lt": "2024-01-01T00:00:00Z"}},
        ]},
    ))

    # Q8: country + recent
    record("country=TR + Jun 2024+", lambda: client.find(
        COLLECTION,
        {"$and": [
            {"country": "TR"},
            {"created_at": {"$gte": "2024-06-01T00:00:00Z"}},
        ]},
    ))

    # Q9: amount range + date range
    record("amount 100-500 + Jun-Dec 2023", lambda: client.find(
        COLLECTION,
        {"$and": [
            {"amount": {"$gte": 100}},
            {"amount": {"$lte": 500}},
            {"created_at": {"$gte": "2023-06-01T00:00:00Z"}},
            {"created_at": {"$lt": "2024-01-01T00:00:00Z"}},
        ]},
    ))

    # Q10: high priority + recent
    record("priority>=4 + year 2024", lambda: client.find(
        COLLECTION,
        {"$and": [
            {"priority": {"$gte": 4}},
            {"created_at": {"$gte": "2024-01-01T00:00:00Z"}},
        ]},
    ))

    # ── Numeric Comparison Queries ───────────────────────────────────

    print()
    print("  \033[1m  Numeric Comparison Queries\033[0m")
    print()

    # Q11: amount > 1000
    record("amount > 1000", lambda: client.count(
        COLLECTION,
        {"amount": {"$gt": 1000}},
    ))

    # Q12: amount 100-200
    record("amount 100-200", lambda: client.count(
        COLLECTION,
        {"$and": [
            {"amount": {"$gte": 100}},
            {"amount": {"$lte": 200}},
        ]},
    ))

    # Q13: priority 3-5
    record("priority 3-5", lambda: client.count(
        COLLECTION,
        {"$and": [
            {"priority": {"$gte": 3}},
            {"priority": {"$lte": 5}},
        ]},
    ))

    # ── Aggregation Queries ──────────────────────────────────────────

    print()
    print("  \033[1m  Aggregation Queries\033[0m")
    print()

    # Q14: group by category, sum amount (date-filtered)
    record("Agg: group by category, sum amount", lambda: client.aggregate(
        COLLECTION,
        [
            {"$match": {"$and": [
                {"created_at": {"$gte": "2023-01-01T00:00:00Z"}},
                {"created_at": {"$lt": "2024-01-01T00:00:00Z"}},
            ]}},
            {"$group": {"_id": "$category", "total_amount": {"$sum": "$amount"}, "count": {"$sum": 1}}},
            {"$sort": {"total_amount": -1}},
        ],
    ))

    # Q15: group by country, count (date-filtered)
    record("Agg: group by country, count", lambda: client.aggregate(
        COLLECTION,
        [
            {"$match": {"$and": [
                {"created_at": {"$gte": "2023-06-01T00:00:00Z"}},
                {"created_at": {"$lt": "2023-12-01T00:00:00Z"}},
            ]}},
            {"$group": {"_id": "$country", "order_count": {"$sum": 1}}},
            {"$sort": {"order_count": -1}},
        ],
    ))

    # Q16: group by status, count (full dataset)
    record("Agg: group by status, count (all)", lambda: client.aggregate(
        COLLECTION,
        [
            {"$group": {"_id": "$status", "order_count": {"$sum": 1}}},
            {"$sort": {"order_count": -1}},
        ],
    ))

    # ── Count Queries ────────────────────────────────────────────────

    print()
    print("  \033[1m  Count Queries\033[0m")
    print()

    # Q17: count all
    record("Count: all documents", lambda: client.count(COLLECTION))

    # Q18: count by status
    record("Count: status=completed", lambda: client.count(
        COLLECTION, {"status": "completed"},
    ))

    # Q19: count by date range
    record("Count: year 2023", lambda: client.count(
        COLLECTION,
        {"$and": [
            {"created_at": {"$gte": "2023-01-01T00:00:00Z"}},
            {"created_at": {"$lt": "2024-01-01T00:00:00Z"}},
        ]},
    ))

    # Q20: count by status + date range
    record("Count: completed + Jun 2023", lambda: client.count(
        COLLECTION,
        {"$and": [
            {"status": "completed"},
            {"created_at": {"$gte": "2023-06-01T00:00:00Z"}},
            {"created_at": {"$lt": "2023-07-01T00:00:00Z"}},
        ]},
    ))

    # ── Summary ──────────────────────────────────────────────────────
    print()
    print("  \033[1m╔═══════════════════════════════════════════════════════════════════════╗\033[0m")
    print("  \033[1m║                              Summary                                  ║\033[0m")
    print("  \033[1m╚═══════════════════════════════════════════════════════════════════════╝\033[0m")
    print()
    print(f"  Data:         {TOTAL_DOCS:>14,} order records (2022-2024)")
    print(f"  Insert time:  {insert_elapsed:>14.1f}s ({insert_rate:,.0f} docs/sec)")
    print()

    print(f"  \033[1m┌─────┬──────────────────────────────────────────────────┬──────────┬──────────────┐\033[0m")
    print(f"  \033[1m│  #  │ Query                                            │ Time(ms) │      Results │\033[0m")
    print(f"  \033[1m├─────┼──────────────────────────────────────────────────┼──────────┼──────────────┤\033[0m")

    for qn, lbl, ms, cnt in results:
        print(f"  │ {qn:>3d} │ {lbl:<48s} │ {ms:>8.1f} │ {cnt:>12,} │")

    print(f"  \033[1m└─────┴──────────────────────────────────────────────────┴──────────┴──────────────┘\033[0m")
    print()

    total_query_ms = sum(ms for _, _, ms, _ in results)
    print(f"  Total query time:  {total_query_ms:,.1f} ms ({total_query_ms / 1000:.2f}s)")
    print(f"  Avg per query:     {total_query_ms / len(results):,.1f} ms")
    print()

    # ── Cleanup ──────────────────────────────────────────────────────
    print("  Dropping collection...", end=" ", flush=True)
    client.drop_collection(COLLECTION)
    print("done.")
    client.close()
    print()


if __name__ == "__main__":
    main()
