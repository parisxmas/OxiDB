#!/usr/bin/env python3
"""
OxiDB vs MongoDB — 1 Million Document Benchmark
Focused on datetime indexing, range queries, aggregation, and mixed workloads.
"""

import sys
import time
import json
import random
import string
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/oxidb/python")

from oxidb import OxiDbClient
from pymongo import MongoClient, ASCENDING, DESCENDING

HOST = "127.0.0.1"
OXIDB_PORT = 4444
MONGO_PORT = 27017

# ── Helpers ────────────────────────────────────────────────────────────

class Timer:
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self
    def __exit__(self, *args):
        self.elapsed_ms = (time.perf_counter() - self.t0) * 1000

results = []

def bench(test_name, oxidb_fn, mongo_fn, runs=3):
    oxi_times, mongo_times = [], []
    for _ in range(runs):
        with Timer() as t: oxidb_fn()
        oxi_times.append(t.elapsed_ms)
        with Timer() as t: mongo_fn()
        mongo_times.append(t.elapsed_ms)
    oxi_best = min(oxi_times)
    mongo_best = min(mongo_times)
    ratio = mongo_best / oxi_best if oxi_best > 0 else 0
    winner = "OxiDB" if oxi_best <= mongo_best else "MongoDB"
    results.append({"test": test_name, "oxidb_ms": oxi_best, "mongo_ms": mongo_best, "ratio": ratio, "winner": winner})
    w_color = "\033[92m" if winner == "OxiDB" else "\033[93m"
    print(f"  {test_name:<55s} {oxi_best:10.2f}ms {mongo_best:10.2f}ms {w_color}{ratio:7.2f}x  {winner}\033[0m")

def bench_once(test_name, oxidb_fn, mongo_fn):
    with Timer() as t: oxidb_fn()
    oxi_ms = t.elapsed_ms
    with Timer() as t: mongo_fn()
    mongo_ms = t.elapsed_ms
    ratio = mongo_ms / oxi_ms if oxi_ms > 0 else 0
    winner = "OxiDB" if oxi_ms <= mongo_ms else "MongoDB"
    results.append({"test": test_name, "oxidb_ms": oxi_ms, "mongo_ms": mongo_ms, "ratio": ratio, "winner": winner})
    w_color = "\033[92m" if winner == "OxiDB" else "\033[93m"
    print(f"  {test_name:<55s} {oxi_ms:10.2f}ms {mongo_ms:10.2f}ms {w_color}{ratio:7.2f}x  {winner}\033[0m")

# ── Data Generator ────────────────────────────────────────────────────

DEPARTMENTS = ["engineering", "sales", "marketing", "support", "hr", "finance", "operations", "legal", "product", "design"]
STATUSES = ["active", "inactive", "pending", "suspended"]
COUNTRIES = ["US", "UK", "DE", "FR", "JP", "CA", "AU", "BR", "IN", "KR"]
CITIES = {
    "US": ["New York", "San Francisco", "Chicago", "Austin", "Seattle"],
    "UK": ["London", "Manchester", "Edinburgh", "Bristol", "Leeds"],
    "DE": ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"],
    "FR": ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"],
    "JP": ["Tokyo", "Osaka", "Kyoto", "Yokohama", "Nagoya"],
    "CA": ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"],
    "AU": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
    "BR": ["Sao Paulo", "Rio de Janeiro", "Brasilia", "Salvador", "Curitiba"],
    "IN": ["Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad"],
    "KR": ["Seoul", "Busan", "Incheon", "Daegu", "Daejeon"],
}
TAGS = ["python", "rust", "go", "java", "typescript", "react", "vue", "docker", "k8s", "aws", "gcp", "azure", "ml", "data", "devops", "security"]

BASE_DATE = datetime(2020, 1, 1)
DATE_RANGE_DAYS = 5 * 365  # 5 years

def generate_docs(n):
    docs = []
    for i in range(n):
        country = random.choice(COUNTRIES)
        city = random.choice(CITIES[country])
        created = BASE_DATE + timedelta(
            days=random.randint(0, DATE_RANGE_DAYS),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        updated = created + timedelta(days=random.randint(0, 365), hours=random.randint(0, 23))
        last_login = updated + timedelta(days=random.randint(0, 30))
        docs.append({
            "employee_id": f"EMP{i:07d}",
            "name": f"Employee_{i}",
            "department": random.choice(DEPARTMENTS),
            "status": random.choice(STATUSES),
            "country": country,
            "city": city,
            "salary": round(random.uniform(30000, 250000), 2),
            "score": round(random.uniform(0, 100), 1),
            "level": random.randint(1, 12),
            "created_at": created.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "updated_at": updated.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "last_login": last_login.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tags": random.sample(TAGS, k=random.randint(1, 5)),
            "active": random.choice([True, False]),
            "projects": random.randint(0, 50),
        })
    return docs


# ── Main ──────────────────────────────────────────────────────────────

def main():
    N = 500_000
    BATCH = 10_000

    print()
    print("  \033[1m======================================================================\033[0m")
    print("  \033[1m      OxiDB vs MongoDB — 500K Document Benchmark (DateTime Focus)      \033[0m")
    print("  \033[1m======================================================================\033[0m")
    print()
    print(f"  Documents:  {N:,}")
    print(f"  Batch size: {BATCH:,}")
    print(f"  Date:       {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    oxi = OxiDbClient(HOST, OXIDB_PORT, timeout=600.0)
    mongo = MongoClient(HOST, MONGO_PORT)
    mdb = mongo["bench_1m"]

    # Cleanup
    try: oxi.drop_collection("employees")
    except: pass
    mdb.drop_collection("employees")

    # ================================================================
    # PHASE 1: Bulk Insert 1M docs
    # ================================================================
    print("  \033[1m--- Phase 1: Bulk Insert ---\033[0m")
    hdr = f"  {'Test':<55s} {'OxiDB':>10s} {'MongoDB':>10s} {'Ratio':>7s}  Winner"
    sep = f"  {'-'*55} {'-'*10} {'-'*10} {'-'*7}  {'-'*8}"
    print(hdr); print(sep)

    print("  Generating 1M documents...", end=" ", flush=True)
    all_docs = generate_docs(N)
    print("done.")

    def oxi_bulk():
        for i in range(0, N, BATCH):
            oxi.insert_many("employees", [dict(d) for d in all_docs[i:i+BATCH]])

    def mongo_bulk():
        for i in range(0, N, BATCH):
            mdb.employees.insert_many([dict(d) for d in all_docs[i:i+BATCH]])

    bench_once(f"Bulk insert {N:,} docs (batches of {BATCH:,})", oxi_bulk, mongo_bulk)

    # ================================================================
    # PHASE 2: Index Creation
    # ================================================================
    print()
    print("  \033[1m--- Phase 2: Index Creation ---\033[0m")
    print(hdr); print(sep)

    for field in ["created_at", "updated_at", "last_login", "department", "status", "country", "salary", "level", "employee_id"]:
        bench_once(f"Create index ({field})",
                   lambda f=field: oxi.create_index("employees", f),
                   lambda f=field: mdb.employees.create_index(f))

    # ================================================================
    # PHASE 3: DateTime Queries
    # ================================================================
    print()
    print("  \033[1m--- Phase 3: DateTime Queries ---\033[0m")
    print(hdr); print(sep)

    # Exact date range: 1 month
    bench("Date range: created_at in Jan 2023",
          lambda: oxi.find("employees", {"created_at": {"$gte": "2023-01-01T00:00:00Z", "$lt": "2023-02-01T00:00:00Z"}}),
          lambda: list(mdb.employees.find({"created_at": {"$gte": "2023-01-01T00:00:00Z", "$lt": "2023-02-01T00:00:00Z"}})))

    # Narrow date range: 1 week
    bench("Date range: created_at in 1 week",
          lambda: oxi.find("employees", {"created_at": {"$gte": "2023-06-01T00:00:00Z", "$lt": "2023-06-08T00:00:00Z"}}),
          lambda: list(mdb.employees.find({"created_at": {"$gte": "2023-06-01T00:00:00Z", "$lt": "2023-06-08T00:00:00Z"}})))

    # Wide date range: 1 year
    bench("Date range: created_at in 2024 (wide)",
          lambda: oxi.find("employees", {"created_at": {"$gte": "2024-01-01T00:00:00Z", "$lt": "2025-01-01T00:00:00Z"}}),
          lambda: list(mdb.employees.find({"created_at": {"$gte": "2024-01-01T00:00:00Z", "$lt": "2025-01-01T00:00:00Z"}})))

    # Sort by datetime + limit
    bench("Sort by created_at desc, limit 10",
          lambda: oxi.find("employees", {}, sort={"created_at": -1}, limit=10),
          lambda: list(mdb.employees.find({}).sort("created_at", DESCENDING).limit(10)))

    bench("Sort by created_at asc, limit 100",
          lambda: oxi.find("employees", {}, sort={"created_at": 1}, limit=100),
          lambda: list(mdb.employees.find({}).sort("created_at", ASCENDING).limit(100)))

    # Date + other filter
    bench("Date + department filter",
          lambda: oxi.find("employees", {"created_at": {"$gte": "2023-01-01T00:00:00Z", "$lt": "2023-07-01T00:00:00Z"}, "department": "engineering"}),
          lambda: list(mdb.employees.find({"created_at": {"$gte": "2023-01-01T00:00:00Z", "$lt": "2023-07-01T00:00:00Z"}, "department": "engineering"})))

    # Sort by updated_at with skip+limit
    bench("Sort updated_at desc, skip 50, limit 20",
          lambda: oxi.find("employees", {}, sort={"updated_at": -1}, skip=50, limit=20),
          lambda: list(mdb.employees.find({}).sort("updated_at", DESCENDING).skip(50).limit(20)))

    # ================================================================
    # PHASE 4: Field Queries
    # ================================================================
    print()
    print("  \033[1m--- Phase 4: Field Queries ---\033[0m")
    print(hdr); print(sep)

    bench("Find by department (indexed)",
          lambda: oxi.find("employees", {"department": "engineering"}),
          lambda: list(mdb.employees.find({"department": "engineering"})))

    bench("Find by status (indexed)",
          lambda: oxi.find("employees", {"status": "active"}),
          lambda: list(mdb.employees.find({"status": "active"})))

    bench("Find by country (indexed)",
          lambda: oxi.find("employees", {"country": "US"}),
          lambda: list(mdb.employees.find({"country": "US"})))

    bench("Salary range ($50K-$100K)",
          lambda: oxi.find("employees", {"salary": {"$gte": 50000, "$lte": 100000}}),
          lambda: list(mdb.employees.find({"salary": {"$gte": 50000, "$lte": 100000}})))

    bench("Find by level (indexed)",
          lambda: oxi.find("employees", {"level": 10}),
          lambda: list(mdb.employees.find({"level": 10})))

    bench("FindOne by employee_id (unindexed)",
          lambda: oxi.find_one("employees", {"employee_id": "EMP0500000"}),
          lambda: mdb.employees.find_one({"employee_id": "EMP0500000"}))

    bench("Find sort+limit (salary desc, limit 10)",
          lambda: oxi.find("employees", {}, sort={"salary": -1}, limit=10),
          lambda: list(mdb.employees.find({}).sort("salary", DESCENDING).limit(10)))

    bench("Find limit only (no sort, limit 100)",
          lambda: oxi.find("employees", {}, limit=100),
          lambda: list(mdb.employees.find({}).limit(100)))

    bench("Find limit only (no sort, limit 10000)",
          lambda: oxi.find("employees", {}, limit=10000),
          lambda: list(mdb.employees.find({}).limit(10000)))

    # ================================================================
    # PHASE 5: Count Operations
    # ================================================================
    print()
    print("  \033[1m--- Phase 5: Count Operations ---\033[0m")
    print(hdr); print(sep)

    bench("Count all docs",
          lambda: oxi.count("employees"),
          lambda: mdb.employees.count_documents({}))

    bench("Count by department",
          lambda: oxi.count("employees", {"department": "engineering"}),
          lambda: mdb.employees.count_documents({"department": "engineering"}))

    bench("Count by date range (2023)",
          lambda: oxi.count("employees", {"created_at": {"$gte": "2023-01-01T00:00:00Z", "$lt": "2024-01-01T00:00:00Z"}}),
          lambda: mdb.employees.count_documents({"created_at": {"$gte": "2023-01-01T00:00:00Z", "$lt": "2024-01-01T00:00:00Z"}}))

    bench("Count by salary range",
          lambda: oxi.count("employees", {"salary": {"$gte": 100000}}),
          lambda: mdb.employees.count_documents({"salary": {"$gte": 100000}}))

    # ================================================================
    # PHASE 6: Aggregation Pipeline
    # ================================================================
    print()
    print("  \033[1m--- Phase 6: Aggregation Pipeline ---\033[0m")
    print(hdr); print(sep)

    bench("Agg: group by department",
          lambda: oxi.aggregate("employees", [
              {"$group": {"_id": "$department", "count": {"$sum": 1}, "avg_salary": {"$avg": "$salary"}}}
          ]),
          lambda: list(mdb.employees.aggregate([
              {"$group": {"_id": "$department", "count": {"$sum": 1}, "avg_salary": {"$avg": "$salary"}}}
          ])))

    bench("Agg: group by country",
          lambda: oxi.aggregate("employees", [
              {"$group": {"_id": "$country", "count": {"$sum": 1}, "total_salary": {"$sum": "$salary"}}}
          ]),
          lambda: list(mdb.employees.aggregate([
              {"$group": {"_id": "$country", "count": {"$sum": 1}, "total_salary": {"$sum": "$salary"}}}
          ])))

    bench("Agg: group by status + sort",
          lambda: oxi.aggregate("employees", [
              {"$group": {"_id": "$status", "count": {"$sum": 1}, "avg_score": {"$avg": "$score"}}},
              {"$sort": {"count": -1}}
          ]),
          lambda: list(mdb.employees.aggregate([
              {"$group": {"_id": "$status", "count": {"$sum": 1}, "avg_score": {"$avg": "$score"}}},
              {"$sort": {"count": -1}}
          ])))

    bench("Agg: match(dept=eng) + group by country",
          lambda: oxi.aggregate("employees", [
              {"$match": {"department": "engineering"}},
              {"$group": {"_id": "$country", "count": {"$sum": 1}, "avg_salary": {"$avg": "$salary"}}}
          ]),
          lambda: list(mdb.employees.aggregate([
              {"$match": {"department": "engineering"}},
              {"$group": {"_id": "$country", "count": {"$sum": 1}, "avg_salary": {"$avg": "$salary"}}}
          ])))

    bench("Agg: match(date range) + group by dept",
          lambda: oxi.aggregate("employees", [
              {"$match": {"created_at": {"$gte": "2023-01-01T00:00:00Z", "$lt": "2024-01-01T00:00:00Z"}}},
              {"$group": {"_id": "$department", "count": {"$sum": 1}, "avg_salary": {"$avg": "$salary"}}}
          ]),
          lambda: list(mdb.employees.aggregate([
              {"$match": {"created_at": {"$gte": "2023-01-01T00:00:00Z", "$lt": "2024-01-01T00:00:00Z"}}},
              {"$group": {"_id": "$department", "count": {"$sum": 1}, "avg_salary": {"$avg": "$salary"}}}
          ])))

    bench("Agg: group + sort + limit (top 5 cities by salary)",
          lambda: oxi.aggregate("employees", [
              {"$group": {"_id": "$city", "avg_salary": {"$avg": "$salary"}, "count": {"$sum": 1}}},
              {"$sort": {"avg_salary": -1}},
              {"$limit": 5}
          ]),
          lambda: list(mdb.employees.aggregate([
              {"$group": {"_id": "$city", "avg_salary": {"$avg": "$salary"}, "count": {"$sum": 1}}},
              {"$sort": {"avg_salary": -1}},
              {"$limit": 5}
          ])))

    bench("Agg: match(country=US) + group by level",
          lambda: oxi.aggregate("employees", [
              {"$match": {"country": "US"}},
              {"$group": {"_id": "$level", "count": {"$sum": 1}, "max_salary": {"$max": "$salary"}, "min_salary": {"$min": "$salary"}}}
          ]),
          lambda: list(mdb.employees.aggregate([
              {"$match": {"country": "US"}},
              {"$group": {"_id": "$level", "count": {"$sum": 1}, "max_salary": {"$max": "$salary"}, "min_salary": {"$min": "$salary"}}}
          ])))

    # ================================================================
    # PHASE 7: Update Operations
    # ================================================================
    print()
    print("  \033[1m--- Phase 7: Update Operations ---\033[0m")
    print(hdr); print(sep)

    bench("Update single doc (update_one, indexed employee_id)",
          lambda: oxi.update_one("employees", {"employee_id": "EMP0000042"}, {"$set": {"score": 99.9}}),
          lambda: mdb.employees.update_one({"employee_id": "EMP0000042"}, {"$set": {"score": 99.9}}))

    bench("Update by indexed field (status=pending)",
          lambda: oxi.update("employees", {"status": "pending", "level": 1}, {"$inc": {"projects": 1}}),
          lambda: mdb.employees.update_many({"status": "pending", "level": 1}, {"$inc": {"projects": 1}}))

    # ================================================================
    # CLEANUP
    # ================================================================
    try: oxi.drop_collection("employees")
    except: pass
    mdb.drop_collection("employees")
    oxi.close()
    mongo.close()

    # ── SUMMARY ────────────────────────────────────────────────────────
    print()
    print("  \033[1m======================================================================\033[0m")
    print("  \033[1m                          SUMMARY                                     \033[0m")
    print("  \033[1m======================================================================\033[0m")
    print()

    oxi_wins = sum(1 for r in results if r["winner"] == "OxiDB")
    mongo_wins = sum(1 for r in results if r["winner"] == "MongoDB")
    total = len(results)
    oxi_total = sum(r["oxidb_ms"] for r in results)
    mongo_total = sum(r["mongo_ms"] for r in results)

    print(f"  Total tests:    {total}")
    print(f"  OxiDB wins:     \033[92m{oxi_wins}\033[0m / {total}")
    print(f"  MongoDB wins:   \033[93m{mongo_wins}\033[0m / {total}")
    print(f"  OxiDB total:    {oxi_total:,.2f} ms")
    print(f"  MongoDB total:  {mongo_total:,.2f} ms")
    if oxi_total > 0:
        print(f"  Overall ratio:  {mongo_total / oxi_total:.2f}x")
    print()

    print(f"  {'Test':<55s} {'OxiDB':>10s} {'MongoDB':>10s} {'Ratio':>7s}  Winner")
    print(f"  {'-'*55} {'-'*10} {'-'*10} {'-'*7}  {'-'*8}")
    for r in results:
        w_color = "\033[92m" if r["winner"] == "OxiDB" else "\033[93m"
        print(f"  {r['test']:<55s} {r['oxidb_ms']:9.2f}ms {r['mongo_ms']:9.2f}ms {w_color}{r['ratio']:6.2f}x  {r['winner']}\033[0m")
    print()


if __name__ == "__main__":
    main()
