#!/usr/bin/env python3
"""A user process. Reads live market prices from OxiDB and places
buy/sell orders against them. Each fill is ONE atomic transaction that:

  - inserts an idempotency receipt (unique uid),
  - moves USD cash and the asset position,
  - records the trade + a filled order,
  - writes double-entry journal lines,

with retry on OCC conflict and idempotent replay on error — the exact
ledger pattern from the exchange-readiness test suite, now driven by
real market data.

Load knobs (env): THREADS_PER_USER (concurrent order flows per user,
default 1) and TRADE_DELAY_MAX (max inter-trade sleep seconds, default
0.06 — set 0 for a tight loop).

Run inside the venv:
  tests/live-exchange/.venv/bin/python trader.py <user_index> [seconds]
"""
import os
import random
import sys
import threading
import time

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from oxidb_client import OxiDB
from feeder import SYMBOLS

USER = f"user-{int(sys.argv[1])}" if len(sys.argv) > 1 else "user-0"
SECS = int(sys.argv[2]) if len(sys.argv) > 2 else 60
THREADS = int(os.environ.get("THREADS_PER_USER", "1"))
DELAY_MAX = float(os.environ.get("TRADE_DELAY_MAX", "0.06"))
# "for_update" (default): lock the touched accounts pessimistically so
# the concurrent flows of ONE user queue on that user's cash/position
# instead of racing to an OCC commit conflict. "occ": the optimistic
# path (many conflicts + retries when a user runs several flows).
MODE = os.environ.get("TX_MODE", "for_update")

_uid_lock = threading.Lock()
_uid_n = 0


def next_uid():
    global _uid_n
    with _uid_lock:
        _uid_n += 1
        return f"{USER}-{_uid_n}-{int(time.time() * 1000)}"


def latest_price(db, sym):
    row = db.find_one("prices", {"sym": sym})
    return float(row["price"]) if row and row.get("price", 0) else 0.0


def position(db, asset):
    row = db.find_one("accounts", {"owner": USER, "asset": asset})
    return float(row["bal"]) if row else 0.0


def ensure_position_row(db, asset):
    if db.find_one("accounts", {"owner": USER, "asset": asset}) is None:
        db.insert("accounts", {"owner": USER, "asset": asset, "bal": 0.0})


def place(db, side, sym, qty, price):
    """One atomic fill. Returns 'ok' | 'dup' | 'conflict' | 'insufficient'."""
    uid = next_uid()
    cost = round(qty * price, 6)
    cash_delta = -cost if side == "buy" else cost
    asset_delta = qty if side == "buy" else -qty

    ensure_position_row(db, sym)

    for _ in range(50):
        tx = db.begin()
        try:
            # Read (and, in for_update mode, LOCK) the two accounts this
            # fill touches — always USD first, then the symbol, a global
            # order so the user's concurrent flows can't deadlock. In
            # for_update mode this serializes them on the shared USD doc
            # instead of colliding at commit.
            if MODE == "for_update":
                usd_rows = db.tx_find_for_update("accounts", {"owner": USER, "asset": "USD"})
                db.tx_find_for_update("accounts", {"owner": USER, "asset": sym})
                cash = float(usd_rows[0]["bal"]) if usd_rows else None
            else:
                cash = None
                for a in db.call({"cmd": "find", "collection": "accounts",
                                  "query": {"owner": USER}})["data"]:
                    if a["asset"] == "USD":
                        cash = float(a["bal"])
            if side == "buy" and (cash is None or cash < cost):
                db.rollback()
                return "insufficient"
            if side == "sell" and position(db, sym) < qty:
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
            db.tx_insert("journal", {"uid": uid, "owner": USER, "acct": "USD",
                                     "delta": cash_delta})
            db.tx_insert("journal", {"uid": uid, "owner": USER, "acct": sym,
                                     "delta_asset": asset_delta, "usd_value": -cash_delta})
            r = db.commit()
            if r.get("ok"):
                return "ok"
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


def trade_loop(worker, stats, deadline):
    db = OxiDB()
    local = {"ok": 0, "dup": 0, "conflict": 0, "insufficient": 0}
    while time.time() < deadline:
        sym = random.choice(SYMBOLS)
        price = latest_price(db, sym)
        if price <= 0:
            time.sleep(0.05)
            continue
        side = random.choice(["buy", "sell"])
        qty = round(random.uniform(50, 500) / price, 8)
        if qty <= 0:
            continue
        local[place(db, side, sym, qty, price)] += 1
        if DELAY_MAX > 0:
            time.sleep(random.uniform(0, DELAY_MAX))
    with stats["lock"]:
        for k in ("ok", "dup", "conflict", "insufficient"):
            stats[k] += local[k]


def main():
    print(f"[{USER}] trading {THREADS} flow(s) for {SECS}s "
          f"(delay_max={DELAY_MAX})…", flush=True)
    deadline = time.time() + SECS
    stats = {"ok": 0, "dup": 0, "conflict": 0, "insufficient": 0,
             "lock": threading.Lock()}
    threads = [threading.Thread(target=trade_loop, args=(i, stats, deadline))
               for i in range(THREADS)]
    for t in threads:
        t.start()
    # progress heartbeat
    while any(t.is_alive() for t in threads):
        time.sleep(30)
        with stats["lock"]:
            done = stats["ok"] + stats["dup"] + stats["conflict"] + stats["insufficient"]
        print(f"[{USER}] {done} attempts (ok={stats['ok']} "
              f"conflict={stats['conflict']} insuff={stats['insufficient']})", flush=True)
    for t in threads:
        t.join()
    print(f"[{USER}] done: ok={stats['ok']} dup={stats['dup']} "
          f"conflict={stats['conflict']} insufficient={stats['insufficient']}", flush=True)


if __name__ == "__main__":
    main()
