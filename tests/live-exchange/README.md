# Self-contained exchange over OxiDB (Go)

A real exchange, **not** a market-data replay: there is no external feed.
Prices are formed entirely by the traders' own order flow, matched by a
single matching engine — exactly how a venue works.

- **matcher** — the exchange core. One process, single-threaded per-symbol
  matching (as real matching engines are). It reads the resting order
  book, matches the best crossing bid/ask, and settles each fill in ONE
  atomic transaction: move USD + asset between the two users, insert the
  trade, write the double-entry journal, and **set the symbol's price to
  the last trade** — so the market price emerges from trading.
- **trader** (10 processes) — each a user. Reads the current price, places
  limit buy/sell orders around it (some cross the spread and cause
  trades), backed by its cash / holdings. Users start with USD **and** an
  opening position in every symbol, so it's a closed system: every buy is
  someone's sell.
- **TTL** — resting orders expire (`open_orders.created_at`, default 20s),
  so the order book can't grow without bound. The ledger
  (trades/journal/receipts) is permanent and is not expired.
- **verify** — after trading: trades == receipts (idempotent settlement),
  journal balances, total USD conserved (no money created), each symbol's
  holdings conserved, no overdraft, no naked shorts, every balance
  reproducible from the journal, and prices moved from seed (the market
  was formed by traders).

## Run

```bash
./run.sh 45        # 45s of trading, then verify
```

Needs Go and the release `oxidb-server`. Uses a dedicated port (4455) and
its own data dir (`.data/`); the Go binary and logs are gitignored.

## Throughput

The matching engine is **sharded by symbol** — one goroutine (own
connection) per symbol — so trades settle concurrently and group commit
batches their fsyncs. `run.sh` runs the server in **lazy-sync** mode
(`OXIDB_LAZY_SYNC`, batched fsyncs) because a live exchange is a
throughput demo, not a durability test (those are separate suites):
~150 trades/sec on a laptop. A user's USD account is shared across
symbols, so the concurrent matchers do hit OCC conflicts on it; they
retry (cheap under lazy-sync) and the ledger stays exactly consistent —
verified after every run.
