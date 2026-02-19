#!/usr/bin/env python3
"""
OxiDB Python demo — connects to remote server, creates collections/indexes,
inserts data, searches, and logs every operation with timing and full results.
"""

import sys
import time
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
from oxidb import OxiDbClient

HOST = "188.68.37.53"
PORT = 4444

total_ops = 0
total_time = 0.0


def banner(title):
    print()
    print("=" * 80)
    print(f"  {title}")
    print("=" * 80)
    print()


def log(op, detail, elapsed_ms, result=None):
    global total_ops, total_time
    total_ops += 1
    total_time += elapsed_ms
    status = "\033[92mOK\033[0m"
    print(f"  [{total_ops:3d}] {status} [{elapsed_ms:7.2f}ms] {op:<22s} {detail}")
    if result is not None:
        if isinstance(result, list):
            for i, item in enumerate(result):
                print(f"        [{i}] {json.dumps(item, default=str)}")
        elif isinstance(result, dict):
            print(f"        => {json.dumps(result, default=str)}")
        else:
            print(f"        => {result}")


def timed(op, fn, detail="", show_result=False):
    t0 = time.perf_counter()
    result = fn()
    elapsed = (time.perf_counter() - t0) * 1000
    log(op, detail, elapsed, result if show_result else None)
    return result


def main():
    global total_ops, total_time

    print()
    print("  \033[1mOxiDB Remote Server Demo\033[0m")
    print(f"  Server: {HOST}:{PORT}")
    print(f"  Date:   {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    t_total_start = time.perf_counter()

    db = OxiDbClient(HOST, PORT)
    print(f"  Connected to {HOST}:{PORT}")

    # -- Cleanup from previous runs --
    for col in ["articles", "users", "logs"]:
        try:
            db.drop_collection(col)
        except Exception:
            pass
    for bucket in ["documents"]:
        try:
            for obj in db.list_objects(bucket):
                db.delete_object(bucket, obj["key"])
            db.delete_bucket(bucket)
        except Exception:
            pass

    # ================================================================
    # SCHEMA SETUP
    # ================================================================
    banner("1. SCHEMA SETUP — Collections & Indexes")

    timed("CREATE COLLECTION", lambda: db.create_collection("articles"), "articles", show_result=True)
    timed("CREATE COLLECTION", lambda: db.create_collection("users"), "users", show_result=True)
    timed("CREATE COLLECTION", lambda: db.create_collection("logs"), "logs", show_result=True)

    cols = timed("LIST COLLECTIONS", lambda: db.list_collections(), "all", show_result=True)

    timed("CREATE INDEX", lambda: db.create_index("articles", "category"), "articles.category (btree)", show_result=True)
    timed("CREATE INDEX", lambda: db.create_index("articles", "author"), "articles.author (btree)", show_result=True)
    timed("CREATE UNIQUE INDEX", lambda: db.create_unique_index("users", "email"), "users.email (unique)", show_result=True)
    timed("CREATE COMPOSITE IDX", lambda: db.create_composite_index("articles", ["category", "rating"]), "articles.[category, rating]", show_result=True)
    timed("CREATE INDEX", lambda: db.create_index("logs", "level"), "logs.level (btree)", show_result=True)

    print("\n  --- List & Drop indexes ---")
    timed("LIST INDEXES", lambda: db.list_indexes("articles"), "articles", show_result=True)
    timed("DROP INDEX", lambda: db.drop_index("articles", "author"), "articles.author", show_result=True)
    timed("LIST INDEXES", lambda: db.list_indexes("articles"), "articles (after drop)", show_result=True)

    # ================================================================
    # DATA INSERT
    # ================================================================
    banner("2. DATA INSERT — Users, Articles, Logs")

    users = [
        {"name": "Alice",   "email": "alice@demo.com",   "role": "editor",  "age": 30},
        {"name": "Bob",     "email": "bob@demo.com",     "role": "writer",  "age": 25},
        {"name": "Charlie", "email": "charlie@demo.com", "role": "writer",  "age": 35},
        {"name": "Diana",   "email": "diana@demo.com",   "role": "admin",   "age": 28},
        {"name": "Eve",     "email": "eve@demo.com",     "role": "editor",  "age": 42},
    ]
    for u in users:
        timed("INSERT", lambda u=u: db.insert("users", u), f"users <- {u['name']} ({u['role']})", show_result=True)

    articles = [
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
    timed("INSERT MANY", lambda: db.insert_many("articles", articles), f"articles <- {len(articles)} documents", show_result=True)

    logs = [{"level": "info" if i % 3 != 0 else "error", "msg": f"Event #{i}", "source": f"service-{i%4}"} for i in range(50)]
    timed("INSERT MANY", lambda: db.insert_many("logs", logs), f"logs <- {len(logs)} documents", show_result=False)

    # -- Unique index enforcement --
    print()
    print("  Testing unique index enforcement (duplicate email):")
    try:
        t0 = time.perf_counter()
        db.insert("users", {"name": "Fake", "email": "alice@demo.com"})
        elapsed = (time.perf_counter() - t0) * 1000
        print(f"  \033[91mERROR: should have thrown!\033[0m")
    except Exception as e:
        elapsed = (time.perf_counter() - t0) * 1000
        total_ops += 1
        total_time += elapsed
        print(f"  [{total_ops:3d}] \033[93mREJECTED\033[0m [{elapsed:7.2f}ms] INSERT (duplicate)    {e}")

    # ================================================================
    # QUERIES
    # ================================================================
    banner("3. QUERIES — Find, FindOne, Count")

    print("  --- Find all users ---")
    timed("FIND", lambda: db.find("users", {}), "users (all)", show_result=True)

    print("\n  --- Find articles by category (indexed) ---")
    timed("FIND", lambda: db.find("articles", {"category": "programming"}), 'articles WHERE category="programming"', show_result=True)

    print("\n  --- Find with range query ---")
    timed("FIND", lambda: db.find("articles", {"rating": {"$gte": 5}}), "articles WHERE rating >= 5", show_result=True)

    print("\n  --- Find with $or ---")
    timed("FIND ($or)", lambda: db.find("articles", {"$or": [{"category": "databases"}, {"category": "ai"}]}), "articles WHERE category IN (databases, ai)", show_result=True)

    print("\n  --- Find with $in ---")
    timed("FIND ($in)", lambda: db.find("articles", {"author": {"$in": ["Alice", "Diana"]}}), "articles WHERE author IN (Alice, Diana)", show_result=True)

    print("\n  --- Find with sort + limit ---")
    timed("FIND (sorted)", lambda: db.find("articles", {}, sort={"views": -1}, limit=5), "articles ORDER BY views DESC LIMIT 5", show_result=True)

    print("\n  --- Find with sort + skip + limit (pagination) ---")
    timed("FIND (page 1)", lambda: db.find("articles", {}, sort={"rating": -1}, skip=0, limit=3), "articles page 1 (skip=0, limit=3)", show_result=True)
    timed("FIND (page 2)", lambda: db.find("articles", {}, sort={"rating": -1}, skip=3, limit=3), "articles page 2 (skip=3, limit=3)", show_result=True)

    print("\n  --- FindOne ---")
    timed("FIND ONE", lambda: db.find_one("users", {"email": "diana@demo.com"}), 'users WHERE email="diana@demo.com"', show_result=True)

    print("\n  --- Count ---")
    timed("COUNT", lambda: db.count("articles"), "articles", show_result=True)
    timed("COUNT", lambda: db.count("users"), "users", show_result=True)
    timed("COUNT", lambda: db.count("logs"), "logs", show_result=True)

    # ================================================================
    # UPDATES
    # ================================================================
    banner("4. UPDATES — $set, $inc, $push, $pull, $unset, $rename, update_one")

    timed("UPDATE ($set)", lambda: db.update("users", {"name": "Alice"}, {"$set": {"role": "senior-editor", "verified": True}}), 'Alice: role="senior-editor"', show_result=True)
    timed("FIND ONE", lambda: db.find_one("users", {"name": "Alice"}), "Alice after $set", show_result=True)

    timed("UPDATE ($inc)", lambda: db.update("articles", {"title": "Getting Started with Rust"}, {"$inc": {"views": 100}}), "Rust article: views += 100", show_result=True)
    timed("FIND ONE", lambda: db.find_one("articles", {"title": "Getting Started with Rust"}), "Rust article after $inc", show_result=True)

    timed("UPDATE ($push)", lambda: db.update("articles", {"title": "Getting Started with Rust"}, {"$push": {"tags": "systems"}}), 'Rust article: tags.push("systems")', show_result=True)
    timed("FIND ONE", lambda: db.find_one("articles", {"title": "Getting Started with Rust"}), "Rust article after $push", show_result=True)

    timed("UPDATE ($pull)", lambda: db.update("articles", {"title": "Getting Started with Rust"}, {"$pull": {"tags": "beginner"}}), 'Rust article: tags.pull("beginner")', show_result=True)

    timed("UPDATE ($unset)", lambda: db.update("users", {"name": "Alice"}, {"$unset": {"verified": ""}}), "Alice: remove verified field", show_result=True)

    timed("UPDATE ($rename)", lambda: db.update("users", {"name": "Bob"}, {"$rename": {"role": "position"}}), 'Bob: rename role -> position', show_result=True)
    timed("FIND ONE", lambda: db.find_one("users", {"name": "Bob"}), "Bob after $rename", show_result=True)

    print("\n  --- update_one (single-document) ---")
    timed("UPDATE ONE", lambda: db.update_one("users", {"name": "Charlie"}, {"$set": {"verified": True}}), 'Charlie: verified=true', show_result=True)
    timed("FIND ONE", lambda: db.find_one("users", {"name": "Charlie"}), "Charlie after update_one", show_result=True)

    # ================================================================
    # AGGREGATION
    # ================================================================
    banner("5. AGGREGATION PIPELINE")

    print("  --- Articles per category with avg rating ---")
    timed("AGGREGATE", lambda: db.aggregate("articles", [
        {"$group": {"_id": "$category", "count": {"$sum": 1}, "avg_rating": {"$avg": "$rating"}, "total_views": {"$sum": "$views"}}},
        {"$sort": {"total_views": -1}}
    ]), "$group by category + $sort", show_result=True)

    print("\n  --- Author leaderboard ---")
    timed("AGGREGATE", lambda: db.aggregate("articles", [
        {"$group": {"_id": "$author", "articles": {"$sum": 1}, "best_rating": {"$max": "$rating"}, "total_views": {"$sum": "$views"}}},
        {"$sort": {"total_views": -1}}
    ]), "$group by author + $sort", show_result=True)

    print("\n  --- Tag frequency (unwind + group) ---")
    timed("AGGREGATE", lambda: db.aggregate("articles", [
        {"$unwind": "$tags"},
        {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]), "$unwind tags + $group + $sort", show_result=True)

    print("\n  --- Global stats ---")
    timed("AGGREGATE", lambda: db.aggregate("articles", [
        {"$group": {"_id": None, "total_articles": {"$sum": 1}, "avg_rating": {"$avg": "$rating"}, "total_views": {"$sum": "$views"}, "max_views": {"$max": "$views"}, "min_views": {"$min": "$views"}}}
    ]), "$group (global)", show_result=True)

    print("\n  --- Top 3 articles with computed field ---")
    timed("AGGREGATE", lambda: db.aggregate("articles", [
        {"$addFields": {"score": {"$multiply": ["$rating", "$views"]}}},
        {"$sort": {"score": -1}},
        {"$limit": 3},
        {"$project": {"title": 1, "author": 1, "rating": 1, "views": 1, "score": 1, "_id": 0}}
    ]), "$addFields + $sort + $limit + $project", show_result=True)

    print("\n  --- $count stage ---")
    timed("AGGREGATE", lambda: db.aggregate("articles", [
        {"$match": {"rating": {"$gte": 4}}},
        {"$count": "high_rated_count"}
    ]), "$match(rating>=4) + $count", show_result=True)

    print("\n  --- Error log count by source ---")
    timed("AGGREGATE", lambda: db.aggregate("logs", [
        {"$match": {"level": "error"}},
        {"$group": {"_id": "$source", "errors": {"$sum": 1}}},
        {"$sort": {"errors": -1}}
    ]), "logs: $match(error) + $group by source", show_result=True)

    # ================================================================
    # TRANSACTIONS
    # ================================================================
    banner("6. TRANSACTIONS")

    print("  --- Auto-commit transaction ---")
    t0 = time.perf_counter()
    with db.transaction():
        db.insert("articles", {"title": "TX Article 1", "author": "Eve", "category": "test", "rating": 3, "tags": [], "views": 0})
        db.insert("articles", {"title": "TX Article 2", "author": "Eve", "category": "test", "rating": 4, "tags": [], "views": 0})
    elapsed = (time.perf_counter() - t0) * 1000
    total_ops += 1
    total_time += elapsed
    print(f"  [{total_ops:3d}] \033[92mOK\033[0m [{elapsed:7.2f}ms] TRANSACTION (commit)  2 inserts committed")
    timed("COUNT", lambda: db.count("articles"), "articles after tx commit", show_result=True)

    print("\n  --- Rollback transaction ---")
    t0 = time.perf_counter()
    db.begin_tx()
    db.insert("articles", {"title": "SHOULD NOT EXIST", "author": "Ghost", "category": "void", "rating": 0, "tags": [], "views": 0})
    db.rollback_tx()
    elapsed = (time.perf_counter() - t0) * 1000
    total_ops += 1
    total_time += elapsed
    print(f"  [{total_ops:3d}] \033[92mOK\033[0m [{elapsed:7.2f}ms] TRANSACTION (rollback) insert rolled back")
    doc = timed("FIND ONE", lambda: db.find_one("articles", {"title": "SHOULD NOT EXIST"}), "verify rollback (should be None)", show_result=True)

    # ================================================================
    # DELETE
    # ================================================================
    banner("7. DELETE & DELETE ONE")

    timed("DELETE", lambda: db.delete("articles", {"category": "test"}), 'articles WHERE category="test"', show_result=True)
    timed("COUNT", lambda: db.count("articles"), "articles after delete", show_result=True)

    db.insert("articles", {"title": "Temp Article", "author": "Temp", "category": "temp", "rating": 1, "tags": [], "views": 0})
    timed("DELETE ONE", lambda: db.delete_one("articles", {"category": "temp"}), 'articles WHERE category="temp" (first only)', show_result=True)
    timed("COUNT", lambda: db.count("articles"), "articles after delete_one", show_result=True)

    # ================================================================
    # BLOB STORAGE
    # ================================================================
    banner("8. BLOB STORAGE")

    timed("CREATE BUCKET", lambda: db.create_bucket("documents"), "documents", show_result=True)
    timed("LIST BUCKETS", lambda: db.list_buckets(), "all", show_result=True)

    texts = {
        "rust-guide.txt":   "Rust is a systems programming language focused on safety and performance. It prevents memory bugs at compile time without garbage collection.",
        "python-tips.txt":  "Python is great for rapid prototyping and data science. Use list comprehensions, generators, and decorators for clean code.",
        "database-101.txt": "Databases store and retrieve data efficiently. Indexes speed up queries by orders of magnitude. Transactions ensure ACID consistency.",
        "devops-notes.txt": "Docker containers package applications with dependencies. Kubernetes orchestrates containers at scale across clusters.",
    }
    for key, content in texts.items():
        timed("PUT OBJECT", lambda k=key, c=content: db.put_object("documents", k, c.encode(), content_type="text/plain", metadata={"source": "demo"}),
              f"documents/{key} ({len(content)} bytes)", show_result=True)

    print()
    timed("LIST OBJECTS", lambda: db.list_objects("documents"), "documents/*", show_result=True)
    timed("LIST OBJECTS", lambda: db.list_objects("documents", prefix="python"), 'documents/python*', show_result=True)

    print("\n  --- Get object ---")
    data, meta = timed("GET OBJECT", lambda: db.get_object("documents", "rust-guide.txt"), "documents/rust-guide.txt", show_result=False)
    print(f"        content: \"{data.decode()[:80]}...\"")
    print(f"        metadata: {json.dumps(meta, default=str)}")

    print("\n  --- Head object (metadata only) ---")
    timed("HEAD OBJECT", lambda: db.head_object("documents", "python-tips.txt"), "documents/python-tips.txt", show_result=True)

    # ================================================================
    # FULL-TEXT SEARCH
    # ================================================================
    banner("9. FULL-TEXT SEARCH")

    time.sleep(1)  # wait for FTS indexing

    queries = [
        ("rust programming safety performance", None),
        ("database index queries transactions", None),
        ("python data science prototyping", "documents"),
        ("docker kubernetes containers", None),
        ("memory garbage collection", None),
    ]
    for query, bucket in queries:
        kwargs = {"bucket": bucket} if bucket else {}
        scope = f" (bucket={bucket})" if bucket else ""
        results = timed("SEARCH", lambda q=query, kw=kwargs: db.search(q, **kw), f'"{query}"{scope}', show_result=True)
        print()

    # ================================================================
    # DOCUMENT FULL-TEXT SEARCH
    # ================================================================
    banner("10. DOCUMENT FULL-TEXT SEARCH")

    timed("CREATE TEXT INDEX", lambda: db.create_text_index("articles", ["title", "author", "category"]),
          "articles.[title, author, category]", show_result=True)

    for q in ["Rust", "database internals", "Go API"]:
        timed("TEXT SEARCH", lambda q=q: db.text_search("articles", q, limit=5),
              f'articles: "{q}"', show_result=True)
        print()

    # ================================================================
    # COMPACTION
    # ================================================================
    banner("11. COMPACTION")

    # Create garbage
    for i in range(20):
        db.insert("logs", {"level": "debug", "msg": f"Garbage #{i}"})
    db.delete("logs", {"level": "debug"})
    timed("COMPACT", lambda: db.compact("logs"), "logs (reclaim deleted space)", show_result=True)

    # ================================================================
    # CLEANUP
    # ================================================================
    banner("CLEANUP")

    for col in ["articles", "users", "logs"]:
        timed("DROP COLLECTION", lambda c=col: db.drop_collection(c), col, show_result=True)
    for obj in db.list_objects("documents"):
        db.delete_object("documents", obj["key"])
    timed("DELETE BUCKET", lambda: db.delete_bucket("documents"), "documents", show_result=True)

    db.close()

    # ================================================================
    # SUMMARY
    # ================================================================
    t_total = (time.perf_counter() - t_total_start) * 1000

    banner("SUMMARY")
    print(f"  Total operations:  {total_ops}")
    print(f"  Total DB time:     {total_time:.2f} ms")
    print(f"  Wall clock time:   {t_total:.2f} ms")
    print(f"  Avg per operation: {total_time / total_ops:.2f} ms")
    print(f"  Server:            {HOST}:{PORT}")
    print()


if __name__ == "__main__":
    main()
