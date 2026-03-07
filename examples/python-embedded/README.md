# OxiDB Embedded Python Example

A complete e-commerce analytics demo using [OxiDB](https://oxidb.baltavista.com) embedded in Python. No server required — the database runs directly in your Python process via FFI.

## What it demonstrates

- **Schema setup** — collections, indexes (unique, composite, text)
- **Seed data** — 50 users, 20 products, 200 orders, 300 reviews
- **Queries** — filters, sort, limit, full-text search
- **Aggregation** — revenue by status, users by country, rating distribution
- **Transactions** — loyalty points with OCC conflict handling
- **Bulk operations** — price updates, mass cancellation
- **Blob storage** — CSV report generation and retrieval
- **Performance** — 5K insert benchmark, indexed query timing
- **Compaction** — reclaim disk space after deletes

## Requirements

1. Python 3.8+
2. The `oxidb-embedded` package:
   ```bash
   pip install oxidb-embedded
   ```
3. The native OxiDB FFI library (`liboxidb_embedded_ffi.dylib` on macOS, `.so` on Linux, `.dll` on Windows). Download from [oxidb.baltavista.com/downloads](https://oxidb.baltavista.com/downloads.html) or build from source:
   ```bash
   cargo build --release -p oxidb-embedded-ffi
   ```

## Run

```bash
# Set the library path and run
OXIDB_LIB_PATH=/path/to/liboxidb_embedded_ffi.dylib python3 example_app.py

# Or place the library in the same directory
cp /path/to/liboxidb_embedded_ffi.dylib .
python3 example_app.py
```

## Sample Output

```
============================================================
  1. Schema Setup
============================================================
Collections: ['orders', 'products', 'reviews', 'users']

============================================================
  2. Seeding Data
============================================================
Users:    50
Products: 20
Orders:   200
Reviews:  300

============================================================
  3. Query Examples
============================================================

Top 5 most expensive products:
  $ 149.99  Coffee Maker (Home)
  $ 120.00  Running Shoes (Sports)
  $  89.99  Dumbbell Set (Sports)
  $  89.99  Mechanical Keyboard (Electronics)
  $  79.99  Denim Jacket (Clothing)

============================================================
  4. Aggregation Analytics
============================================================

Revenue by order status:
  delivered     orders= 45  revenue=$  12973.08  avg=$ 288.29
  confirmed     orders= 38  revenue=$  11997.04  avg=$ 315.71
  ...

============================================================
  8. Performance — Batch Insert
============================================================
Inserted 5000 log entries in 20.70s (242 ops/sec)
Found 1300 ERROR logs in 2.6ms
```

## Links

- [OxiDB Website](https://oxidb.baltavista.com)
- [Python Examples](https://oxidb.baltavista.com/python-examples.html)
- [Downloads](https://oxidb.baltavista.com/downloads.html)
- [PyPI: oxidb-embedded](https://pypi.org/project/oxidb-embedded/)
