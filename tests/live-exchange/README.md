# Live exchange integration test

An end-to-end test that runs OxiDB as a real exchange data layer under
**live market data**:

- **1 feeder process** connects to three real crypto exchanges over
  WebSocket — Binance, Coinbase, Kraken — and streams trade ticks for
  **20 distinct symbols** into OxiDB (updates a `prices` row per symbol,
  appends to `ticks`).
- **10 trader processes** (one per user) read the live prices and place
  atomic buy/sell fills. Each fill is ONE transaction touching five
  collections — `receipts` (idempotency), `accounts` (USD cash + asset
  position), `trades`, `orders`, `journal` (double-entry) — with retry
  on OCC conflict and idempotent replay, the ledger pattern from the
  exchange-readiness suite, now driven by real markets.
- **`verify.py`** then checks the ledger is consistent: trade uids
  unique, every trade has exactly one receipt and two journal legs, each
  balance is reproducible from the journal, no overdraft, portfolio
  value conserved.

## Run

```bash
# one-time: create the venv (needs internet for the websockets pkg)
python3 -m venv .venv && .venv/bin/pip install websockets

./run.sh 40      # 40 seconds of live trading, then verify
```

Requires internet (public exchange WebSocket feeds; no API keys). Uses a
dedicated port (4455) and its own data dir (`.data/`), both isolated
from any other running OxiDB. All artifacts (`.venv`, `.data`, logs) are
gitignored.

## What it demonstrates

Real-world load — bursty, unpredictable market data writes concurrent
with multi-user transactional trading — over the exact
durability/idempotency/atomicity machinery the unit and fault-injection
suites verify. A green `verify.py` is the ledger staying provably
consistent while live money moves against live prices.
