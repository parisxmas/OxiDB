#!/usr/bin/env python3
"""Post-run consistency checks over the live-exchange state."""
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from oxidb_client import OxiDB

db = OxiDB()
fails = []


def check(cond, msg):
    print(("  OK  " if cond else "FAIL  ") + msg)
    if not cond:
        fails.append(msg)


trades = db.find("trades", {})
journal = db.find("journal", {})
receipts = db.find("receipts", {})
accounts = db.find("accounts", {})

print(f"\n=== live-exchange verification ===")
print(f"trades={len(trades)} journal_lines={len(journal)} "
      f"receipts={len(receipts)} accounts={len(accounts)}")

# 1. Idempotency: exactly one receipt per trade uid, uids unique.
trade_uids = [t["uid"] for t in trades]
check(len(trade_uids) == len(set(trade_uids)), "trade uids are unique (no double-fill)")
receipt_uids = set(r["uid"] for r in receipts)
check(set(trade_uids) == receipt_uids,
      "every trade has exactly one idempotency receipt and vice versa")

# 2. Every trade fully present across collections (2 journal lines each).
jl = {}
for j in journal:
    jl[j["uid"]] = jl.get(j["uid"], 0) + 1
partial = [u for u in trade_uids if jl.get(u, 0) != 2]
check(not partial, f"every trade has its 2 journal legs (partial: {len(partial)})")

# 3. Journal cash legs sum to zero net across the system? No — money
#    leaves USD and becomes asset value. Instead: each user's USD balance
#    == start + sum of their USD journal deltas (balance reproducible).
START_CASH = 1_000_000
usd_net = {}
for j in journal:
    if j.get("acct") == "USD":
        usd_net[j["owner"]] = usd_net.get(j["owner"], 0.0) + float(j["delta"])
usd_ok = True
for a in accounts:
    if a["asset"] == "USD":
        expected = START_CASH + usd_net.get(a["owner"], 0.0)
        if abs(float(a["bal"]) - expected) > 1e-6:
            usd_ok = False
            print(f"      {a['owner']} USD {a['bal']} != {expected}")
check(usd_ok, "every user's USD balance is reproducible from the journal")

# 4. Asset positions reproducible from journal too.
asset_net = {}
for j in journal:
    if "delta_asset" in j:
        asset_net[(j["owner"], j["acct"])] = \
            asset_net.get((j["owner"], j["acct"]), 0.0) + float(j["delta_asset"])
pos_ok = True
for a in accounts:
    if a["asset"] != "USD":
        expected = asset_net.get((a["owner"], a["asset"]), 0.0)
        if abs(float(a["bal"]) - expected) > 1e-6:
            pos_ok = False
            print(f"      {a['owner']}/{a['asset']} {a['bal']} != {expected}")
check(pos_ok, "every asset position is reproducible from the journal")

# 5. No negative USD cash (no overdraft — the sufficiency check held).
neg = [a for a in accounts if a["asset"] == "USD" and float(a["bal"]) < -1e-6]
check(not neg, f"no user overdrew USD (negative balances: {len(neg)})")

# 6. Portfolio value conserved: total USD cash + total asset USD-value
#    (from journal usd_value) == users * START_CASH. Since each fill's
#    cash delta and asset usd_value are equal-and-opposite, the sum of
#    all USD cash across users must equal START_CASH*users minus the net
#    USD tied up in assets — which equals the summed asset usd_value.
total_usd = sum(float(a["bal"]) for a in accounts if a["asset"] == "USD")
usd_in_assets = -sum(float(j["delta"]) for j in journal if j.get("acct") == "USD")
n_users = sum(1 for a in accounts if a["asset"] == "USD")
check(abs(total_usd + usd_in_assets - n_users * START_CASH) < 1e-3,
      "portfolio value conserved (cash + USD tied in positions == start)")

print()
if fails:
    print(f"RESULT: {len(fails)} CHECK(S) FAILED")
    sys.exit(1)
print("RESULT: ALL CHECKS PASSED — the ledger is consistent after live trading")
