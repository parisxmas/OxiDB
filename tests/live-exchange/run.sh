#!/usr/bin/env bash
# Self-contained exchange over OxiDB — NO external market data. 1 matching
# engine + 10 trader processes form the market; prices come from trades.
# Usage: ./run.sh [trade_seconds]   (default 45)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
BIN="$ROOT/target/release/oxidb-server"
DATA="$HERE/.data"
SECS="${1:-45}"
PORT=4455
export OXIDB_PORT="$PORT"
export OXIMEM_PORT="${OXIMEM_PORT:-6489}"
NUSERS="${NUSERS:-10}"; export NUSERS
# Throughput knobs. Every write shares one WAL, so an un-paced order flood
# starves settlement; pacing the order rate and lifting the taker share hands
# WAL bandwidth back to trades → ~200+ trades/s (vs ~130 un-paced).
export ORDER_RATE_EACH="${ORDER_RATE_EACH:-70}"   # orders/s per trader (×NUSERS)
export TAKER_PCT="${TAKER_PCT:-45}"               # % of orders that cross → trade
: "${ORDER_TTL_SECS:=35}"; export ORDER_TTL_SECS  # resting-order lifetime
PIDS=()

cleanup() { echo; echo "[run] stopping…"; for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

if lsof -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then echo "ERROR: port $PORT busy"; exit 1; fi
[[ -x "$BIN" ]] || { echo "building server…"; (cd "$ROOT" && cargo build --release -p oxidb-server); }

echo "[run] building exchange (Go)…"
(cd "$HERE" && go build -o .exchange .)
rm -rf "$DATA"; mkdir -p "$DATA"

# Lazy sync (batch fsyncs on a timer) — a live exchange is a throughput
# demo, not a durability test (those are separate). Trades commit far
# faster; the trade-off is up to SYNC_INTERVAL_MS of loss on a hard crash.
echo "[run] starting oxidb-server on :$PORT (metrics :14580, lazy-sync)…"
# Pool size: 12 matcher goroutines + 10 traders + web all hit the server
# concurrently. The default (4 worker threads) queues them; give it enough
# threads to actually run the fan-out in parallel.
OXIDB_DATA="$DATA" OXIDB_ADDR="127.0.0.1:$PORT" OXIDB_HTTP_PORT=14580 \
  OXIDB_OXIMEM_PORT="${OXIMEM_PORT:-6489}" \
  OXIDB_POOL_SIZE="${OXIDB_POOL_SIZE:-16}" \
  OXIDB_LAZY_SYNC=true OXIDB_SYNC_INTERVAL_MS="${SYNC_MS:-200}" \
  "$BIN" > "$HERE/.server.log" 2>&1 &
SERVER_PID=$!; PIDS+=($SERVER_PID); sleep 3
lsof -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1 || { echo "server failed to bind"; cat "$HERE/.server.log"; exit 1; }

echo "[run] seeding…"; "$HERE/.exchange" setup

echo "[run] starting matching engine…"
"$HERE/.exchange" matcher > "$HERE/.matcher.log" 2>&1 &
MATCHER_PID=$!; PIDS+=($MATCHER_PID)

WEB_PORT="${WEB_PORT:-8090}"
echo "[run] starting web dashboard…"
WEB_PORT="$WEB_PORT" SERVER_PID="$SERVER_PID" "$HERE/.exchange" web > "$HERE/.web.log" 2>&1 &
WEB_PID=$!; PIDS+=($WEB_PID)
echo "[run]   ┌───────────────────────────────────────────────┐"
echo "[run]   │  OPEN THE DASHBOARD:  http://localhost:$WEB_PORT      │"
echo "[run]   └───────────────────────────────────────────────┘"

echo "[run] launching $NUSERS traders for ${SECS}s…"
TRADER_PIDS=()
for u in $(seq 0 $((NUSERS-1))); do
  "$HERE/.exchange" trader "$u" "$SECS" > "$HERE/.trader-$u.log" 2>&1 &
  TRADER_PIDS+=($!); PIDS+=($!)
done
for p in "${TRADER_PIDS[@]}"; do wait "$p" 2>/dev/null || true; done

echo "[run] traders done — draining the book…"
sleep 2                                  # let the matcher settle the last crossing orders
kill "$MATCHER_PID" "$WEB_PID" 2>/dev/null || true   # quiesce before verify — no writes mid-snapshot
sleep 1
echo "[run] verifying…"
"$HERE/.exchange" verify
RESULT=$?

commits=$(curl -s http://127.0.0.1:14580/metrics | grep '^oxidb_tx_commits_total' | awk '{print $2}')
conflicts=$(curl -s http://127.0.0.1:14580/metrics | grep '^oxidb_tx_conflicts_total' | awk '{print $2}')
echo "[run] engine: commits=${commits:-?} conflicts=${conflicts:-?}"
echo "[run] matcher: $(grep -oE 'trades=[0-9]+ cancels=[0-9]+' "$HERE/.matcher.log" | tail -1)"
exit $RESULT
