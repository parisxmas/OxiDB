"""
OxiDB Embedded — E-Commerce Analytics Demo
============================================
A realistic example showing how to use OxiDB embedded in a Python application.
Simulates an e-commerce platform with users, products, orders, and reviews.
"""

import shutil
import os
import json
import random
import time
from datetime import datetime, timedelta

from oxidb_embedded import OxiDbEmbedded, OxiDbError, TransactionConflictError

DATA_DIR = "./ecommerce_data"

# Clean previous run
if os.path.exists(DATA_DIR):
    shutil.rmtree(DATA_DIR)


def banner(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


with OxiDbEmbedded(DATA_DIR) as db:

    # ------------------------------------------------------------------
    # 1. Schema Setup — Collections & Indexes
    # ------------------------------------------------------------------
    banner("1. Schema Setup")

    db.create_collection("users")
    db.create_collection("products")
    db.create_collection("orders")
    db.create_collection("reviews")

    db.create_unique_index("users", "email")
    db.create_index("users", "country")
    db.create_index("products", "category")
    db.create_index("products", "price")
    db.create_index("orders", "user_id")
    db.create_index("orders", "status")
    db.create_composite_index("orders", ["user_id", "status"])
    db.create_index("reviews", "product_id")
    db.create_text_index("products", ["name", "description"])

    print(f"Collections: {db.list_collections()}")
    print(f"User indexes: {db.list_indexes('users')}")

    # ------------------------------------------------------------------
    # 2. Seed Data
    # ------------------------------------------------------------------
    banner("2. Seeding Data")

    countries = ["Germany", "France", "UK", "Spain", "Italy", "Netherlands", "Sweden"]
    categories = ["Electronics", "Books", "Clothing", "Home", "Sports", "Food"]
    statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled"]

    # Users
    users = []
    for i in range(50):
        user = {
            "name": f"User_{i:03d}",
            "email": f"user{i}@example.com",
            "country": random.choice(countries),
            "age": random.randint(18, 65),
            "joined": (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 700))).isoformat(),
            "loyalty_points": 0,
        }
        result = db.insert("users", user)
        users.append(result["id"])

    # Products
    product_names = [
        ("Wireless Mouse", "Electronics", 29.99, "Ergonomic wireless mouse with USB-C receiver"),
        ("Mechanical Keyboard", "Electronics", 89.99, "Cherry MX Blue switches, RGB backlight"),
        ("USB-C Hub", "Electronics", 49.99, "7-in-1 hub with HDMI, SD card, USB 3.0"),
        ("Monitor Stand", "Electronics", 39.99, "Adjustable aluminum monitor riser"),
        ("Webcam HD", "Electronics", 59.99, "1080p webcam with auto-focus and noise cancellation"),
        ("Python Crash Course", "Books", 35.99, "Hands-on project-based introduction to Python"),
        ("Rust Programming", "Books", 42.99, "The Rust programming language official guide"),
        ("Database Internals", "Books", 55.99, "Deep dive into distributed data systems"),
        ("Clean Code", "Books", 38.99, "A handbook of agile software craftsmanship"),
        ("Running Shoes", "Sports", 120.00, "Lightweight trail running shoes with grip sole"),
        ("Yoga Mat", "Sports", 25.99, "Non-slip eco-friendly yoga mat 6mm thick"),
        ("Dumbbell Set", "Sports", 89.99, "Adjustable dumbbells 5-25kg with stand"),
        ("Cotton T-Shirt", "Clothing", 19.99, "100% organic cotton crew neck t-shirt"),
        ("Denim Jacket", "Clothing", 79.99, "Classic fit denim jacket with button closure"),
        ("Wool Sweater", "Clothing", 65.00, "Merino wool pullover sweater warm and soft"),
        ("Coffee Maker", "Home", 149.99, "Automatic drip coffee maker with grinder"),
        ("Cast Iron Pan", "Home", 45.99, "Pre-seasoned 12-inch cast iron skillet"),
        ("Desk Lamp", "Home", 34.99, "LED desk lamp with adjustable brightness"),
        ("Olive Oil", "Food", 12.99, "Extra virgin cold-pressed olive oil 500ml"),
        ("Dark Chocolate", "Food", 8.99, "85% cocoa single origin dark chocolate bar"),
    ]

    products = []
    for name, cat, price, desc in product_names:
        result = db.insert("products", {
            "name": name,
            "category": cat,
            "price": price,
            "description": desc,
            "stock": random.randint(10, 500),
            "rating": 0.0,
            "review_count": 0,
        })
        products.append(result["id"])

    # Orders (200 orders)
    orders = []
    for _ in range(200):
        num_items = random.randint(1, 4)
        items = []
        total = 0
        for _ in range(num_items):
            pid = random.choice(products)
            prod = db.find_one("products", {"_id": pid})
            qty = random.randint(1, 3)
            items.append({"product_id": pid, "name": prod["name"], "qty": qty, "price": prod["price"]})
            total += prod["price"] * qty

        order = {
            "user_id": random.choice(users),
            "items": items,
            "total": round(total, 2),
            "status": random.choice(statuses),
            "created_at": (datetime(2025, 1, 1) + timedelta(days=random.randint(0, 400))).isoformat(),
        }
        result = db.insert("orders", order)
        orders.append(result["id"])

    # Reviews
    for _ in range(300):
        db.insert("reviews", {
            "user_id": random.choice(users),
            "product_id": random.choice(products),
            "rating": random.randint(1, 5),
            "text": random.choice([
                "Great product, highly recommend!",
                "Good value for money.",
                "Average quality, nothing special.",
                "Not what I expected, returning.",
                "Excellent! Exceeded my expectations.",
                "Works perfectly, fast shipping.",
                "Decent but overpriced.",
                "Love it! Will buy again.",
            ]),
        })

    print(f"Users:    {db.count('users')}")
    print(f"Products: {db.count('products')}")
    print(f"Orders:   {db.count('orders')}")
    print(f"Reviews:  {db.count('reviews')}")

    # ------------------------------------------------------------------
    # 3. Queries
    # ------------------------------------------------------------------
    banner("3. Query Examples")

    # Top 5 most expensive products
    print("\nTop 5 most expensive products:")
    expensive = db.find("products", {}, sort={"price": -1}, limit=5)
    for p in expensive:
        print(f"  ${p['price']:>7.2f}  {p['name']} ({p['category']})")

    # Electronics under $50
    print("\nElectronics under $50:")
    cheap_tech = db.find("products", {"category": "Electronics", "price": {"$lt": 50}})
    for p in cheap_tech:
        print(f"  ${p['price']:>7.2f}  {p['name']}")

    # Users from Germany, sorted by age
    print("\nGerman users (youngest 5):")
    german = db.find("users", {"country": "Germany"}, sort={"age": 1}, limit=5)
    for u in german:
        print(f"  {u['name']:12s} age={u['age']}  joined={u['joined'][:10]}")

    # Orders over $200
    big_orders = db.find("orders", {"total": {"$gt": 200}}, sort={"total": -1}, limit=5)
    print(f"\nTop 5 orders over $200:")
    for o in big_orders:
        print(f"  Order #{o['_id']:>3d}  ${o['total']:>7.2f}  status={o['status']:10s}  items={len(o['items'])}")

    # Full-text search
    print("\nText search for 'wireless':")
    results = db.text_search("products", "wireless", limit=5)
    for r in results:
        print(f"  {r['name']} — {r['description']}")

    # ------------------------------------------------------------------
    # 4. Aggregation Analytics
    # ------------------------------------------------------------------
    banner("4. Aggregation Analytics")

    # Revenue by order status
    print("\nRevenue by order status:")
    revenue = db.aggregate("orders", [
        {"$group": {
            "_id": "$status",
            "count": {"$sum": 1},
            "total_revenue": {"$sum": "$total"},
            "avg_order": {"$avg": "$total"},
        }},
        {"$sort": {"total_revenue": -1}},
    ])
    for r in revenue:
        print(f"  {r['_id']:12s}  orders={r['count']:>3d}  revenue=${r['total_revenue']:>10.2f}  avg=${r['avg_order']:>7.2f}")

    # Users per country
    print("\nUsers per country:")
    by_country = db.aggregate("users", [
        {"$group": {"_id": "$country", "count": {"$sum": 1}, "avg_age": {"$avg": "$age"}}},
        {"$sort": {"count": -1}},
    ])
    for r in by_country:
        print(f"  {r['_id']:15s}  users={r['count']:>2d}  avg_age={r['avg_age']:.1f}")

    # Products per category with avg price
    print("\nProduct stats by category:")
    by_cat = db.aggregate("products", [
        {"$group": {
            "_id": "$category",
            "count": {"$sum": 1},
            "avg_price": {"$avg": "$price"},
            "min_price": {"$min": "$price"},
            "max_price": {"$max": "$price"},
        }},
        {"$sort": {"avg_price": -1}},
    ])
    for r in by_cat:
        print(f"  {r['_id']:12s}  count={r['count']}  avg=${r['avg_price']:>6.2f}  range=${r['min_price']:.2f}-${r['max_price']:.2f}")

    # Average review rating
    print("\nAverage review rating distribution:")
    ratings = db.aggregate("reviews", [
        {"$group": {"_id": "$rating", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ])
    total_reviews = db.count("reviews")
    for r in ratings:
        pct = r["count"] / total_reviews * 100
        bar = "#" * int(pct / 2)
        print(f"  {'*' * r['_id']:5s}  {r['count']:>3d} ({pct:4.1f}%)  {bar}")

    # ------------------------------------------------------------------
    # 5. Transactions — Loyalty Points
    # ------------------------------------------------------------------
    banner("5. Transactions — Award Loyalty Points")

    # Find delivered orders and award points to users
    delivered = db.find("orders", {"status": "delivered"}, limit=10)
    points_awarded = 0

    for order in delivered:
        points = int(order["total"])  # 1 point per dollar
        try:
            with db.transaction():
                db.update_one("users",
                    {"_id": order["user_id"]},
                    {"$inc": {"loyalty_points": points}},
                )
                db.update_one("orders",
                    {"_id": order["_id"]},
                    {"$set": {"points_awarded": True}},
                )
            points_awarded += points
        except TransactionConflictError:
            print(f"  Conflict on order #{order['_id']}, skipping")

    print(f"Awarded {points_awarded} loyalty points across {len(delivered)} delivered orders")

    # Top loyalty users
    print("\nTop 5 users by loyalty points:")
    top_loyal = db.find("users", {"loyalty_points": {"$gt": 0}}, sort={"loyalty_points": -1}, limit=5)
    for u in top_loyal:
        print(f"  {u['name']:12s}  {u['loyalty_points']:>4d} points  ({u['country']})")

    # ------------------------------------------------------------------
    # 6. Bulk Update — Price Increase
    # ------------------------------------------------------------------
    banner("6. Bulk Operations")

    # 10% price increase on Electronics
    electronics_before = db.find("products", {"category": "Electronics"})
    for p in electronics_before:
        new_price = round(p["price"] * 1.10, 2)
        db.update_one("products", {"_id": p["_id"]}, {"$set": {"price": new_price}})

    print("Electronics after 10% price increase:")
    for p in db.find("products", {"category": "Electronics"}, sort={"price": 1}):
        print(f"  ${p['price']:>7.2f}  {p['name']}")

    # Cancel all pending orders older than 30 days
    result = db.update("orders",
        {"status": "pending"},
        {"$set": {"status": "cancelled", "cancel_reason": "auto-expired"}},
    )
    print(f"\nAuto-cancelled pending orders: {result.get('modified', result)}")

    # ------------------------------------------------------------------
    # 7. Blob Storage — Store a Report
    # ------------------------------------------------------------------
    banner("7. Blob Storage")

    db.create_bucket("reports")

    # Generate a simple CSV report
    csv_lines = ["product,category,price,stock"]
    for p in db.find("products", {}, sort={"category": 1}):
        csv_lines.append(f"{p['name']},{p['category']},{p['price']},{p['stock']}")
    csv_data = "\n".join(csv_lines).encode("utf-8")

    db.put_object("reports", "inventory.csv", csv_data,
        content_type="text/csv",
        metadata={"generated_by": "example_app.py", "date": datetime.now().isoformat()})

    # Retrieve and verify
    data, meta = db.get_object("reports", "inventory.csv")
    print(f"Stored report: {len(data)} bytes")
    print(f"Metadata: {json.dumps(meta, indent=2)}")
    print(f"First 3 lines:")
    for line in data.decode("utf-8").split("\n")[:3]:
        print(f"  {line}")

    objects = db.list_objects("reports")
    print(f"\nBucket 'reports' objects: {len(objects)}")

    # ------------------------------------------------------------------
    # 8. Performance — Batch Insert Benchmark
    # ------------------------------------------------------------------
    banner("8. Performance — Batch Insert")

    db.create_collection("logs")
    db.create_index("logs", "level")

    batch_size = 5000
    start = time.perf_counter()
    for i in range(batch_size):
        db.insert("logs", {
            "level": random.choice(["DEBUG", "INFO", "WARN", "ERROR"]),
            "message": f"Log entry {i}",
            "timestamp": datetime.now().isoformat(),
            "source": random.choice(["api", "worker", "scheduler", "auth"]),
        })
    elapsed = time.perf_counter() - start
    print(f"Inserted {batch_size} log entries in {elapsed:.2f}s ({batch_size/elapsed:.0f} ops/sec)")

    # Query performance
    start = time.perf_counter()
    errors = db.find("logs", {"level": "ERROR"})
    elapsed = time.perf_counter() - start
    print(f"Found {len(errors)} ERROR logs in {elapsed*1000:.1f}ms")

    start = time.perf_counter()
    log_stats = db.aggregate("logs", [
        {"$group": {"_id": "$level", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ])
    elapsed = time.perf_counter() - start
    print(f"Aggregated log levels in {elapsed*1000:.1f}ms:")
    for r in log_stats:
        print(f"  {r['_id']:6s}  {r['count']:>4d}")

    # ------------------------------------------------------------------
    # 9. Compaction
    # ------------------------------------------------------------------
    banner("9. Compaction")

    # Delete half the logs then compact
    db.delete("logs", {"level": "DEBUG"})
    remaining = db.count("logs")
    print(f"After deleting DEBUG logs: {remaining} remaining")

    stats = db.compact("logs")
    print(f"Compaction result: {json.dumps(stats, indent=2)}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    banner("Summary")
    for coll in sorted(db.list_collections()):
        print(f"  {coll:12s}  {db.count(coll):>5d} docs")
    print(f"\n  Buckets: {db.list_buckets()}")
    print(f"  Ping: {db.ping()}")
    print("\nDone!")
