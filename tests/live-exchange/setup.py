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

print(f"seeded {len(SYMBOLS)} symbols, {N_USERS} users @ {START_CASH} USD, indexes ready")
