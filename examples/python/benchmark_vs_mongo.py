#!/usr/bin/env python3
"""
OxiDB vs MongoDB — side-by-side benchmark on the same remote server.

Both databases run on localhost:
  - OxiDB:   port 4444
  - MongoDB: port 27018
"""

import sys
import os
import time
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
from oxidb import OxiDbClient
from pymongo import MongoClient, ASCENDING, DESCENDING

HOST = "127.0.0.1"
OXIDB_PORT = 4444
MONGO_PORT = 27018

# ── Test data ──────────────────────────────────────────────────────────

USERS = [
    {"name": "Alice",   "email": "alice@bench.com",   "role": "editor",  "age": 30},
    {"name": "Bob",     "email": "bob@bench.com",     "role": "writer",  "age": 25},
    {"name": "Charlie", "email": "charlie@bench.com", "role": "writer",  "age": 35},
    {"name": "Diana",   "email": "diana@bench.com",   "role": "admin",   "age": 28},
    {"name": "Eve",     "email": "eve@bench.com",     "role": "editor",  "age": 42},
]

ARTICLES = [
    {"title": "Getting Started with Rust",       "author": "Alice",   "category": "programming", "rating": 5, "tags": ["rust", "beginner"],       "views": 1200},
    {"title": "Advanced Python Patterns",         "author": "Bob",     "category": "programming", "rating": 4, "tags": ["python", "advanced"],     "views": 850},
    {"title": "Database Internals Deep Dive",     "author": "Alice",   "category": "databases",   "rating": 5, "tags": ["database", "internals"],  "views": 2100},
    {"title": "Introduction to Machine Learning", "author": "Charlie", "category": "ai",          "rating": 4, "tags": ["ml", "beginner"],         "views": 3200},
    {"title": "Building REST APIs with Go",       "author": "Bob",     "category": "programming", "rating": 3, "tags": ["go", "api"],              "views": 670},
    {"title": "SQL vs NoSQL: When to Use What",   "author": "Alice",   "category": "databases",   "rating": 5, "tags": ["sql", "nosql"],           "views": 4500},
    {"title": "Kubernetes for Developers",        "author": "Diana",   "category": "devops",      "rating": 4, "tags": ["k8s", "docker"],          "views": 1800},
    {"title": "React Hooks Explained",            "author": "Charlie", "category": "frontend",    "rating": 3, "tags": ["react", "javascript"],    "views": 920},
    {"title": "Optimizing PostgreSQL Queries",    "author": "Alice",   "category": "databases",   "rating": 4, "tags": ["postgresql", "perf"],     "views": 1600},
    {"title": "Microservices Architecture",       "author": "Diana",   "category": "architecture","rating": 5, "tags": ["microservices", "dist"],  "views": 2800},
]

BULK_SIZES = [100, 500, 1000]


# ── Helpers ────────────────────────────────────────────────────────────

class Timer:
    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.elapsed_ms = (time.perf_counter() - self.t0) * 1000


results = []


def bench(test_name, oxidb_fn, mongo_fn, runs=3):
    """Run a benchmark, take best of `runs` attempts."""
    oxi_times = []
    mongo_times = []
    for _ in range(runs):
        with Timer() as t:
            oxidb_fn()
        oxi_times.append(t.elapsed_ms)
        with Timer() as t:
            mongo_fn()
        mongo_times.append(t.elapsed_ms)

    oxi_best = min(oxi_times)
    mongo_best = min(mongo_times)
    ratio = mongo_best / oxi_best if oxi_best > 0 else 0
    winner = "OxiDB" if oxi_best <= mongo_best else "MongoDB"
    diff = abs(oxi_best - mongo_best)

    results.append({
        "test": test_name,
        "oxidb_ms": oxi_best,
        "mongo_ms": mongo_best,
        "ratio": ratio,
        "winner": winner,
    })

    w_color = "\033[92m" if winner == "OxiDB" else "\033[93m"
    print(f"  {test_name:<45s}  {oxi_best:8.2f}ms  {mongo_best:8.2f}ms  {w_color}{ratio:5.2f}x  {winner}\033[0m")


def bench_once(test_name, oxidb_fn, mongo_fn):
    """Run a benchmark once (for destructive ops)."""
    with Timer() as t:
        oxidb_fn()
    oxi_ms = t.elapsed_ms
    with Timer() as t:
        mongo_fn()
    mongo_ms = t.elapsed_ms
    ratio = mongo_ms / oxi_ms if oxi_ms > 0 else 0
    winner = "OxiDB" if oxi_ms <= mongo_ms else "MongoDB"

    results.append({
        "test": test_name,
        "oxidb_ms": oxi_ms,
        "mongo_ms": mongo_ms,
        "ratio": ratio,
        "winner": winner,
    })

    w_color = "\033[92m" if winner == "OxiDB" else "\033[93m"
    print(f"  {test_name:<45s}  {oxi_ms:8.2f}ms  {mongo_ms:8.2f}ms  {w_color}{ratio:5.2f}x  {winner}\033[0m")


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print()
    print("  \033[1m╔══════════════════════════════════════════════════════════════════════╗\033[0m")
    print("  \033[1m║              OxiDB vs MongoDB — Benchmark Report                    ║\033[0m")
    print("  \033[1m╚══════════════════════════════════════════════════════════════════════╝\033[0m")
    print()
    print(f"  Server:   {HOST}")
    print(f"  OxiDB:    port {OXIDB_PORT} (Docker, Rust)")
    print(f"  MongoDB:  port {MONGO_PORT} (Docker, mongo:6.0)")
    print(f"  Date:     {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Client:   Python (remote over TCP)")
    print()

    # Connect
    oxi = OxiDbClient(HOST, OXIDB_PORT)
    mongo = MongoClient(HOST, MONGO_PORT)
    mdb = mongo["benchmark"]

    # Cleanup
    for col in ["users", "articles", "bulk"]:
        try:
            oxi.drop_collection(col)
        except Exception:
            pass
    mdb.drop_collection("users")
    mdb.drop_collection("articles")
    mdb.drop_collection("bulk")

    # ── 1. SINGLE INSERTS ──────────────────────────────────────────────
    print("  \033[1m┌─────────────────────────────────────────────────────────────────────┐\033[0m")
    print("  \033[1m│  TEST                                          OxiDB     MongoDB   │\033[0m")
    print("  \033[1m├─────────────────────────────────────────────────────────────────────┤\033[0m")

    # Insert users one by one
    u_idx = [0]

    def oxi_insert_users():
        for u in USERS:
            oxi.insert("users", dict(u))

    def mongo_insert_users():
        for u in USERS:
            mdb.users.insert_one(dict(u))

    bench_once("Insert 5 users (single)", oxi_insert_users, mongo_insert_users)

    # Insert articles batch
    bench_once("Insert 10 articles (batch)",
               lambda: oxi.insert_many("articles", [dict(a) for a in ARTICLES]),
               lambda: mdb.articles.insert_many([dict(a) for a in ARTICLES]))

    # ── 2. BULK INSERTS ────────────────────────────────────────────────
    for size in BULK_SIZES:
        docs = [{"i": i, "val": i * 3.14, "label": f"item-{i}", "active": i % 2 == 0} for i in range(size)]

        def oxi_bulk(d=docs):
            try:
                oxi.drop_collection("bulk")
            except Exception:
                pass
            oxi.insert_many("bulk", [dict(x) for x in d])

        def mongo_bulk(d=docs):
            mdb.drop_collection("bulk")
            mdb.bulk.insert_many([dict(x) for x in d])

        bench(f"Bulk insert {size} docs", oxi_bulk, mongo_bulk, runs=3)

    # ── 3. INDEXES ─────────────────────────────────────────────────────
    print("  \033[1m├─────────────────────────────────────────────────────────────────────┤\033[0m")

    bench_once("Create index (articles.category)",
               lambda: oxi.create_index("articles", "category"),
               lambda: mdb.articles.create_index("category"))

    bench_once("Create index (articles.author)",
               lambda: oxi.create_index("articles", "author"),
               lambda: mdb.articles.create_index("author"))

    bench_once("Create unique index (users.email)",
               lambda: oxi.create_unique_index("users", "email"),
               lambda: mdb.users.create_index("email", unique=True))

    # ── 4. FIND QUERIES ───────────────────────────────────────────────
    print("  \033[1m├─────────────────────────────────────────────────────────────────────┤\033[0m")

    bench("Find all users",
          lambda: oxi.find("users", {}),
          lambda: list(mdb.users.find({})))

    bench("Find by indexed field (category)",
          lambda: oxi.find("articles", {"category": "programming"}),
          lambda: list(mdb.articles.find({"category": "programming"})))

    bench("Find with range ($gte)",
          lambda: oxi.find("articles", {"rating": {"$gte": 5}}),
          lambda: list(mdb.articles.find({"rating": {"$gte": 5}})))

    bench("Find with $or",
          lambda: oxi.find("articles", {"$or": [{"category": "databases"}, {"category": "ai"}]}),
          lambda: list(mdb.articles.find({"$or": [{"category": "databases"}, {"category": "ai"}]})))

    bench("Find with $in",
          lambda: oxi.find("articles", {"author": {"$in": ["Alice", "Diana"]}}),
          lambda: list(mdb.articles.find({"author": {"$in": ["Alice", "Diana"]}})))

    bench("Find with sort + limit (top 5)",
          lambda: oxi.find("articles", {}, sort={"views": -1}, limit=5),
          lambda: list(mdb.articles.find({}).sort("views", DESCENDING).limit(5)))

    bench("FindOne by indexed field",
          lambda: oxi.find_one("users", {"email": "alice@bench.com"}),
          lambda: mdb.users.find_one({"email": "alice@bench.com"}))

    # ── 5. COUNT ───────────────────────────────────────────────────────
    print("  \033[1m├─────────────────────────────────────────────────────────────────────┤\033[0m")

    bench("Count all articles",
          lambda: oxi.count("articles"),
          lambda: mdb.articles.count_documents({}))

    bench("Count all users",
          lambda: oxi.count("users"),
          lambda: mdb.users.count_documents({}))

    # ── 6. UPDATES ─────────────────────────────────────────────────────
    print("  \033[1m├─────────────────────────────────────────────────────────────────────┤\033[0m")

    bench("Update $set (single doc)",
          lambda: oxi.update("users", {"name": "Alice"}, {"$set": {"role": "senior"}}),
          lambda: mdb.users.update_one({"name": "Alice"}, {"$set": {"role": "senior"}}))

    bench("Update $inc (single doc)",
          lambda: oxi.update("articles", {"title": "Getting Started with Rust"}, {"$inc": {"views": 1}}),
          lambda: mdb.articles.update_one({"title": "Getting Started with Rust"}, {"$inc": {"views": 1}}))

    bench("Update $push (array append)",
          lambda: oxi.update("articles", {"title": "Getting Started with Rust"}, {"$push": {"tags": "test"}}),
          lambda: mdb.articles.update_one({"title": "Getting Started with Rust"}, {"$push": {"tags": "test"}}))

    # ── 7. AGGREGATION ─────────────────────────────────────────────────
    print("  \033[1m├─────────────────────────────────────────────────────────────────────┤\033[0m")

    bench("Aggregate: group by category",
          lambda: oxi.aggregate("articles", [
              {"$group": {"_id": "$category", "count": {"$sum": 1}, "avg_rating": {"$avg": "$rating"}}}
          ]),
          lambda: list(mdb.articles.aggregate([
              {"$group": {"_id": "$category", "count": {"$sum": 1}, "avg_rating": {"$avg": "$rating"}}}
          ])))

    bench("Aggregate: group + sort + limit",
          lambda: oxi.aggregate("articles", [
              {"$group": {"_id": "$author", "total_views": {"$sum": "$views"}}},
              {"$sort": {"total_views": -1}},
              {"$limit": 3}
          ]),
          lambda: list(mdb.articles.aggregate([
              {"$group": {"_id": "$author", "total_views": {"$sum": "$views"}}},
              {"$sort": {"total_views": -1}},
              {"$limit": 3}
          ])))

    bench("Aggregate: unwind + group (tags)",
          lambda: oxi.aggregate("articles", [
              {"$unwind": "$tags"},
              {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
              {"$sort": {"count": -1}}
          ]),
          lambda: list(mdb.articles.aggregate([
              {"$unwind": "$tags"},
              {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
              {"$sort": {"count": -1}}
          ])))

    bench("Aggregate: match + count",
          lambda: oxi.aggregate("articles", [
              {"$match": {"rating": {"$gte": 4}}},
              {"$count": "high_rated"}
          ]),
          lambda: list(mdb.articles.aggregate([
              {"$match": {"rating": {"$gte": 4}}},
              {"$count": "high_rated"}
          ])))

    bench("Aggregate: addFields + project",
          lambda: oxi.aggregate("articles", [
              {"$addFields": {"score": {"$multiply": ["$rating", "$views"]}}},
              {"$project": {"title": 1, "score": 1, "_id": 0}},
              {"$sort": {"score": -1}},
              {"$limit": 3}
          ]),
          lambda: list(mdb.articles.aggregate([
              {"$addFields": {"score": {"$multiply": ["$rating", "$views"]}}},
              {"$project": {"title": 1, "score": 1, "_id": 0}},
              {"$sort": {"score": -1}},
              {"$limit": 3}
          ])))

    # ── 8. DELETE ──────────────────────────────────────────────────────
    print("  \033[1m├─────────────────────────────────────────────────────────────────────┤\033[0m")

    bench("Delete by query (category=frontend)",
          lambda: oxi.delete("articles", {"category": "frontend"}),
          lambda: mdb.articles.delete_many({"category": "frontend"}))

    # ── 9. BULK FIND ON LARGER DATA ───────────────────────────────────
    print("  \033[1m├─────────────────────────────────────────────────────────────────────┤\033[0m")

    # Prepare 1000-doc collection
    bulk_docs = [{"i": i, "val": i * 3.14, "label": f"item-{i}", "active": i % 2 == 0, "group": f"g{i % 10}"} for i in range(1000)]
    try:
        oxi.drop_collection("bulk")
    except Exception:
        pass
    mdb.drop_collection("bulk")
    oxi.insert_many("bulk", [dict(x) for x in bulk_docs])
    mdb.bulk.insert_many([dict(x) for x in bulk_docs])
    oxi.create_index("bulk", "group")
    mdb.bulk.create_index("group")

    bench("Find 100 docs (indexed, 1K collection)",
          lambda: oxi.find("bulk", {"group": "g5"}),
          lambda: list(mdb.bulk.find({"group": "g5"})))

    bench("Find all 1000 docs",
          lambda: oxi.find("bulk", {}),
          lambda: list(mdb.bulk.find({})))

    bench("Count 1000 docs",
          lambda: oxi.count("bulk"),
          lambda: mdb.bulk.count_documents({}))

    bench("Aggregate 1000 docs (group by 10)",
          lambda: oxi.aggregate("bulk", [
              {"$group": {"_id": "$group", "sum": {"$sum": "$val"}, "cnt": {"$sum": 1}}},
              {"$sort": {"sum": -1}}
          ]),
          lambda: list(mdb.bulk.aggregate([
              {"$group": {"_id": "$group", "sum": {"$sum": "$val"}, "cnt": {"$sum": 1}}},
              {"$sort": {"sum": -1}}
          ])))

    # ── CLEANUP ────────────────────────────────────────────────────────
    print("  \033[1m└─────────────────────────────────────────────────────────────────────┘\033[0m")

    for col in ["users", "articles", "bulk"]:
        try:
            oxi.drop_collection(col)
        except Exception:
            pass
    mdb.drop_collection("users")
    mdb.drop_collection("articles")
    mdb.drop_collection("bulk")

    oxi.close()
    mongo.close()

    # ── SUMMARY ────────────────────────────────────────────────────────
    print()
    print("  \033[1m╔══════════════════════════════════════════════════════════════════════╗\033[0m")
    print("  \033[1m║                          SUMMARY                                    ║\033[0m")
    print("  \033[1m╚══════════════════════════════════════════════════════════════════════╝\033[0m")
    print()

    oxi_wins = sum(1 for r in results if r["winner"] == "OxiDB")
    mongo_wins = sum(1 for r in results if r["winner"] == "MongoDB")
    total = len(results)
    oxi_total = sum(r["oxidb_ms"] for r in results)
    mongo_total = sum(r["mongo_ms"] for r in results)

    print(f"  Total tests:    {total}")
    print(f"  OxiDB wins:     \033[92m{oxi_wins}\033[0m / {total}")
    print(f"  MongoDB wins:   \033[93m{mongo_wins}\033[0m / {total}")
    print(f"  OxiDB total:    {oxi_total:.2f} ms")
    print(f"  MongoDB total:  {mongo_total:.2f} ms")
    print(f"  Overall ratio:  {mongo_total / oxi_total:.2f}x")
    print()

    # Detailed table
    print(f"  {'Test':<45s}  {'OxiDB':>8s}  {'MongoDB':>8s}  {'Ratio':>6s}  Winner")
    print(f"  {'─'*45}  {'─'*8}  {'─'*8}  {'─'*6}  {'─'*8}")
    for r in results:
        w_color = "\033[92m" if r["winner"] == "OxiDB" else "\033[93m"
        print(f"  {r['test']:<45s}  {r['oxidb_ms']:7.2f}ms {r['mongo_ms']:7.2f}ms  {r['ratio']:5.2f}x  {w_color}{r['winner']}\033[0m")

    print()


if __name__ == "__main__":
    main()
