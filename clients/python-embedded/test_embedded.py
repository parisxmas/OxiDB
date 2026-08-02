"""Test script for oxidb-embedded — demonstrates all major features."""

import shutil
import os
import json

from oxidb_embedded import OxiDbEmbedded

# Clean previous test data
if os.path.exists("./test_data"):
    shutil.rmtree("./test_data")

with OxiDbEmbedded("./test_data") as db:
    # Insert documents
    r = db.insert("users", {"name": "Alice", "age": 30, "city": "Berlin"})
    print("Insert:", json.dumps(r, indent=2))

    db.insert("users", {"name": "Bob", "age": 25, "city": "Paris"})
    db.insert("users", {"name": "Charlie", "age": 35, "city": "Berlin"})
    db.insert("users", {"name": "Diana", "age": 28, "city": "London"})
    db.insert("users", {"name": "Eve", "age": 22, "city": "Paris"})

    # Find all
    print("\nAll users:")
    for u in db.find("users", {}):
        print(f'  {u["name"]:10s} age={u["age"]}  city={u["city"]}')

    # Query with filter
    print("\nUsers over 25:")
    users = db.find("users", {"age": {"$gt": 25}})
    for u in users:
        print(f'  {u["name"]:10s} age={u["age"]}')

    # Find one
    print("\nFind one (Alice):", json.dumps(db.find_one("users", {"name": "Alice"}), indent=2))

    # Count
    print("\nCount all:", db.count("users"))
    print("Count age>=28:", db.count("users", {"age": {"$gte": 28}}))

    # Update
    db.update_one("users", {"name": "Alice"}, {"$set": {"age": 31}})
    alice = db.find_one("users", {"name": "Alice"})
    print("\nAfter update Alice age:", alice["age"])

    # Aggregate - group by city
    print("\nUsers per city:")
    agg = db.aggregate("users", [
        {"$group": {"_id": "$city", "count": {"$sum": 1}, "avg_age": {"$avg": "$age"}}},
        {"$sort": {"count": -1}},
    ])
    for row in agg:
        print(f'  {row["_id"]:10s} count={row["count"]}  avg_age={row["avg_age"]:.1f}')

    # Index + sorted query
    db.create_index("users", "age")
    print("\nSorted by age (limit 3):")
    for u in db.find("users", {}, sort={"age": 1}, limit=3):
        print(f'  {u["name"]:10s} age={u["age"]}')

    # Transaction
    db.insert("accounts", {"owner": "Alice", "balance": 1000})
    db.insert("accounts", {"owner": "Bob", "balance": 500})
    with db.transaction():
        db.update_one("accounts", {"owner": "Alice"}, {"$inc": {"balance": -200}})
        db.update_one("accounts", {"owner": "Bob"}, {"$inc": {"balance": 200}})
    print("\nAfter transaction:")
    for a in db.find("accounts", {}):
        print(f'  {a["owner"]:10s} balance={a["balance"]}')

    # Delete
    db.delete_one("users", {"name": "Eve"})
    print("\nAfter deleting Eve, count:", db.count("users"))

    # Collections
    print("\nCollections:", db.list_collections())

    print("\nPing:", db.ping())
