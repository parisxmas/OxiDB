#!/usr/bin/env python3
"""Emit the benchmark dataset as multi-row INSERT statements on stdout.

Deterministic (fixed seed), so both engines load byte-identical data and a
re-run compares against the same numbers. Row counts sum to 1,200,000:

    customers      200,000   surrogate PK, UNIQUE email, 2 indexes
    products        50,000   surrogate PK, UNIQUE sku, 2 indexes
    orders         400,000   surrogate PK, FK -> customers, 2 indexes
    order_items    300,000   COMPOSITE PK, FK -> orders, 1 index
    inventory      250,000   COMPOSITE PK (product x warehouse), FK -> products

Parents are emitted before children so foreign keys resolve on an engine that
enforces them row by row (both do).
"""

import argparse
import random

CUSTOMERS = 200_000
PRODUCTS = 50_000
ORDERS = 400_000
ORDER_ITEMS = 300_000

COUNTRIES = ["TR", "US", "DE", "FR", "GB", "NL", "ES", "IT", "PL", "SE"]
CATEGORIES = ["tools", "garden", "kitchen", "office", "outdoor", "audio", "pets"]
STATUSES = ["pending", "paid", "shipped", "delivered", "refunded"]
WAREHOUSES = ["ist", "ber", "ams", "nyc", "sfo"]
# 2024-01-01T00:00:00Z in epoch milliseconds; both engines take a timestamp
# literal, and this keeps the generated text identical for both.
EPOCH = 1_704_067_200


def q(s):
    return "'" + s.replace("'", "''") + "'"


def ts(sec):
    """A typed `TIMESTAMP '...'` literal — the form both engines accept.

    A bare string literal works on PostgreSQL (implicit cast) but OxiDB refuses
    it, so the portable spelling is the standard typed one.
    """
    import datetime

    stamp = datetime.datetime.fromtimestamp(sec, datetime.UTC).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    return "TIMESTAMP " + q(stamp)


def emit(out, table, columns, rows, batch):
    """Write `rows` as multi-row INSERTs of at most `batch` tuples each."""
    prefix = f"INSERT INTO {table} ({columns}) VALUES "
    buf = []
    for row in rows:
        buf.append(row)
        if len(buf) == batch:
            out.write(prefix + ",".join(buf) + ";\n")
            buf.clear()
    if buf:
        out.write(prefix + ",".join(buf) + ";\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, default=500, help="rows per INSERT")
    ap.add_argument("--out", default="-")
    # Everything above is sized for ~1.2M rows, which is where the memory work
    # was measured. Some costs in the engine are bounded (the open-time peak is
    # bounded by the fold interval) and some are per-row (the row-offset index),
    # and the two are only told apart by changing the row count.
    ap.add_argument(
        "--scale",
        type=float,
        default=1.0,
        help="multiply every table's row count (1.0 = the ~1.2M-row default)",
    )
    args = ap.parse_args()
    global CUSTOMERS, PRODUCTS, ORDERS, ORDER_ITEMS
    CUSTOMERS = int(CUSTOMERS * args.scale)
    PRODUCTS = int(PRODUCTS * args.scale)
    ORDERS = int(ORDERS * args.scale)
    # Kept a multiple of 3: three lines per order, which is what makes the
    # composite key a real one.
    ORDER_ITEMS = int(ORDER_ITEMS * args.scale) // 3 * 3
    out = open(args.out, "w") if args.out != "-" else __import__("sys").stdout

    rng = random.Random(20260728)

    emit(
        out,
        "customers",
        "id, email, name, country, created",
        (
            f"({i},{q(f'user{i}@example.com')},{q(f'Customer {i}')},"
            f"{q(rng.choice(COUNTRIES))},{ts(EPOCH + i * 7)})"
            for i in range(1, CUSTOMERS + 1)
        ),
        args.batch,
    )

    emit(
        out,
        "products",
        "id, sku, category, price, active",
        (
            f"({i},{q(f'SKU-{i:07d}')},{q(rng.choice(CATEGORIES))},"
            f"{round(rng.uniform(1.5, 990.0), 2)},{'TRUE' if i % 7 else 'FALSE'})"
            for i in range(1, PRODUCTS + 1)
        ),
        args.batch,
    )

    emit(
        out,
        "orders",
        "id, customer_id, status, total, created",
        (
            f"({i},{rng.randint(1, CUSTOMERS)},{q(rng.choice(STATUSES))},"
            f"{round(rng.uniform(5.0, 4000.0), 2)},{ts(EPOCH + i * 3)})"
            for i in range(1, ORDERS + 1)
        ),
        args.batch,
    )

    # Composite PK (order_id, line_no): 300k items spread over the first 100k
    # orders, 3 lines each — so the key really is composite, not a disguised
    # surrogate.
    emit(
        out,
        "order_items",
        "order_id, line_no, product, qty, amount",
        (
            f"({order},{line},{rng.randint(1, PRODUCTS)},{rng.randint(1, 9)},"
            f"{round(rng.uniform(1.0, 800.0), 2)})"
            for order in range(1, ORDER_ITEMS // 3 + 1)
            for line in range(1, 4)
        ),
        args.batch,
    )

    # Composite PK (product_id, warehouse): every product in every warehouse.
    emit(
        out,
        "inventory",
        "product_id, warehouse, on_hand, reorder_at",
        (
            f"({p},{q(w)},{rng.randint(0, 5000)},{rng.randint(5, 200)})"
            for p in range(1, PRODUCTS + 1)
            for w in WAREHOUSES
        ),
        args.batch,
    )

    if out is not __import__("sys").stdout:
        out.close()


if __name__ == "__main__":
    main()
