# crypto-mvp — Binance BTCUSDT realtime, WebGL, OxiDB backend

Minimal end-to-end demo:

```
Binance WS ──► feeder.js (Node) ──► OxiDB (REST) ──► WebGL chart (browser)
                                          ▲                    │
                                          └────── WS push ─────┘
```

Three processes — start in this order:

```bash
# 1. OxiDB server with REST + WebSocket enabled
OXIDB_HTTP_PORT=9080 OXIDB_WS_PORT=9082 \
OXIDB_DATA=/tmp/oxidb-crypto/data \
oxidb-server &

# 2. Binance → OxiDB feeder
cd crypto-mvp
node feeder.js

# 3. Static site for the chart UI
node serve.js
```

Then open <http://127.0.0.1:3000> — chart auto-loads the last 1 000
trades via REST, then polls every 250 ms for new trades.

*(WebSocket cross-connection event delivery is broken on the current
server build, so the chart polls instead. Switch to `onSnapshot` once
that's fixed — the chart code is one block.)*

## Files

| File | Purpose |
|---|---|
| `feeder.js`         | Binance `btcusdt@trade` WS → OxiDB `trades` collection |
| `serve.js`          | Tiny static HTTP server on :3000 |
| `public/index.html` | Page chrome + status bar + canvas |
| `public/chart.js`   | Raw WebGL line chart, REST snapshot + WS subscribe |

## Tuning

| Env var      | Default                        | Where     |
|--------------|--------------------------------|-----------|
| `SYMBOL`     | `btcusdt`                      | feeder    |
| `OXIDB_URL`  | `http://127.0.0.1:9080`        | feeder    |
| `KEEP_LAST`  | `5000`                         | feeder    |
| `PORT`       | `3000`                         | serve     |

The chart hard-codes REST/WS URLs at the top of `public/chart.js`.

## Dependencies

Zero npm installs. Uses Node 22+ built-in `WebSocket` and the local
`oxidb-js` SDK (`require("../../clients/js/index.js")`).
