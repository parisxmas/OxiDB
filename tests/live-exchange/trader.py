#!/usr/bin/env python3
"""A user process. Reads live market prices from OxiDB and places
buy/sell orders against them. Each fill is ONE atomic transaction that:

  - inserts an idempotency receipt (unique uid),
  - moves USD cash and the asset position,
  - records the trade + a filled order,
  - writes double-entry journal lines,

with retry on OCC conflict and idempotent replay on error — the exact
ledger pattern from the exchange-readiness test suite, now driven by
real market data. Money is conserved in USD terms: a BUY debits
cash = qty*price and credits the asset position (valued at that price),
so total portfolio value is preserved by construction; we assert the
invariant on cash + marked-to-trade positions instead.

Run inside the venv:
  tests/live-exchange/.venv/bin/python trader.py <user_index>
"""
import random
import sys
import time

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from oxidb_client import OxiDB
from feeder import SYMBOLS

USER = f"user-{int(sys.argv[1])}" if len(sys.argv) > 1 else "user-0"
db = OxiDB()
_seq = 0


def latest_price(sym):
    row = db.find_one("prices", {"sym": sym})
    return float(row["price"]) if row and row.get("price", 0) else 0.0


def position(owner, asset):
    row = db.find_one("accounts", {"owner": owner, "asset": asset})
    return float(row["bal"]) if row else 0.0


def ensure_position_row(owner, asset):
    if db.find_one("accounts", {"owner": owner, "asset": asset}) is None:
        db.insert("accounts", {"owner": owner, "asset": asset, "bal": 0.0})


def place(side, sym, qty, price):
    """One atomic fill. Returns 'ok' | 'dup' | 'conflict' | 'insufficient'."""
    global _seq
    _seq += 1
    uid = f"{USER}-{_seq}-{int(time.time()*1000)}"
    cost = round(qty * price, 6)
    # BUY: -cash, +asset. SELL: +cash, -asset.
    cash_delta = -cost if side == "buy" else cost
    asset_delta = qty if side == "buy" else -qty

    ensure_position_row(USER, sym)

    for _ in range(50):
        tx = db.begin()
        try:
            # Sufficiency check inside the tx (reads join the read-set).
            cash = None
            for a in db.call({"cmd": "find", "collection": "accounts",
                              "query": {"owner": USER}})["data"]:
                if a["asset"] == "USD":
                    cash = float(a["bal"])
            if side == "buy" and (cash is None or cash < cost):
                db.rollback()
                return "insufficient"
            if side == "sell" and position(USER, sym) < qty:
                db.rollback()
                return "insufficient"

            db.tx_insert("receipts", {"uid": uid})
            db.tx_update("accounts", {"owner": USER, "asset": "USD"},
                         {"$inc": {"bal": cash_delta}})
            db.tx_update("accounts", {"owner": USER, "asset": sym},
                         {"$inc": {"bal": asset_delta}})
            db.tx_insert("trades", {"uid": uid, "owner": USER, "side": side,
                                    "sym": sym, "qty": qty, "price": price})
            db.tx_insert("orders", {"uid": uid, "owner": USER, "status": "filled"})
            # Double-entry: cash leg + asset leg (USD-valued) sum to zero.
            db.tx_insert("journal", {"uid": uid, "owner": USER, "acct": "USD",
                                     "delta": cash_delta})
            db.tx_insert("journal", {"uid": uid, "owner": USER, "acct": sym,
                                     "delta_asset": asset_delta, "usd_value": -cash_delta})
            r = db.commit()
            if r.get("ok"):
                return "ok"
            # commit returned an error object
            if "unique" in str(r.get("error", "")).lower():
                return "dup"
            if "conflict" in str(r.get("error", "")).lower():
                continue
            return "conflict"
        except Exception:
            try:
                db.rollback()
            except Exception:
                db._connect()
            continue
    return "conflict"


def main():
    stats = {"ok": 0, "dup": 0, "conflict": 0, "insufficient": 0}
    print(f"[{USER}] trading…", flush=True)
    end = time.time() + int(sys.argv[2]) if len(sys.argv) > 2 else time.time() + 60
    while time.time() < end:
        sym = random.choice(SYMBOLS)
        price = latest_price(sym)
        if price <= 0:
            time.sleep(0.05)
            continue
        side = random.choice(["buy", "sell"])
        # Trade a small notional so cash lasts the session.
        notional = random.uniform(50, 500)
        qty = round(notional / price, 8)
        if qty <= 0:
            continue
        res = place(side, sym, qty, price)
        stats[res] = stats.get(res, 0) + 1
        if sum(stats.values()) % 50 == 0:
            print(f"[{USER}] {stats}", flush=True)
        time.sleep(random.uniform(0.01, 0.06))
    print(f"[{USER}] done: {stats}", flush=True)


if __name__ == "__main__":
    main()
