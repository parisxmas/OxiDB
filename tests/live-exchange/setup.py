#!/usr/bin/env python3
"""Seed the exchange: canonical price rows, user accounts, indexes."""
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from oxidb_client import OxiDB
from feeder import SYMBOLS

N_USERS = 10
START_CASH = 1_000_000  # each user's USD balance in cents-ish units

db = OxiDB()

# Prices: one row per canonical symbol, seeded so feeder $set updates match.
for sym in SYMBOLS:
    db.insert("prices", {"sym": sym, "price": 0.0, "venue": "seed", "ts": 0})
db.create_index("prices", "sym")

# Accounts: USD cash + a position row per (user, symbol) created lazily.
for u in range(N_USERS):
    db.insert("accounts", {"owner": f"user-{u}", "asset": "USD", "bal": START_CASH})
db.create_index("accounts", "owner")

# Ledger / trade / order collections + idempotency.
db.create_unique_index("receipts", "uid")
db.create_index("trades", "uid")
db.create_index("orders", "owner")
db.create_index("journal", "uid")

# Market data is pure noise once it's a few minutes old — expire ticks so
# the collection (and its resident index/cache footprint) stays bounded.
# The ledger (trades/journal/receipts) is the permanent record and is NOT
# expired; it needs archival, not TTL.
TICK_TTL_SECS = int(__import__("os").environ.get("TICK_TTL_SECS", "60"))
db.call({"cmd": "create_ttl_index", "collection": "ticks",
         "field": "created_at", "expireAfterSeconds": TICK_TTL_SECS})

print(f"seeded {len(SYMBOLS)} symbols, {N_USERS} users @ {START_CASH} USD, "
      f"indexes ready; ticks TTL = {TICK_TTL_SECS}s")
