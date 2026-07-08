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
PIDS=()

cleanup() { echo; echo "[run] stopping…"; for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

if lsof -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then echo "ERROR: port $PORT busy"; exit 1; fi
[[ -x "$BIN" ]] || { echo "building server…"; (cd "$ROOT" && cargo build --release -p oxidb-server); }

echo "[run] building exchange (Go)…"
(cd "$HERE" && go build -o .exchange .)
rm -rf "$DATA"; mkdir -p "$DATA"

echo "[run] starting oxidb-server on :$PORT (metrics :14580)…"
OXIDB_DATA="$DATA" OXIDB_ADDR="127.0.0.1:$PORT" OXIDB_HTTP_PORT=14580 "$BIN" > "$HERE/.server.log" 2>&1 &
PIDS+=($!); sleep 3
lsof -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1 || { echo "server failed to bind"; cat "$HERE/.server.log"; exit 1; }

echo "[run] seeding…"; "$HERE/.exchange" setup

echo "[run] starting matching engine…"
"$HERE/.exchange" matcher > "$HERE/.matcher.log" 2>&1 &
PIDS+=($!)

WEB_PORT="${WEB_PORT:-8090}"
echo "[run] starting web dashboard…"
WEB_PORT="$WEB_PORT" "$HERE/.exchange" web > "$HERE/.web.log" 2>&1 &
PIDS+=($!)
echo "[run]   ┌───────────────────────────────────────────────┐"
echo "[run]   │  OPEN THE DASHBOARD:  http://localhost:$WEB_PORT      │"
echo "[run]   └───────────────────────────────────────────────┘"

echo "[run] launching 10 traders for ${SECS}s…"
TRADER_PIDS=()
for u in $(seq 0 9); do
  "$HERE/.exchange" trader "$u" "$SECS" > "$HERE/.trader-$u.log" 2>&1 &
  TRADER_PIDS+=($!); PIDS+=($!)
done
for p in "${TRADER_PIDS[@]}"; do wait "$p" 2>/dev/null || true; done

echo "[run] traders done — draining the book, then verifying…"
sleep 3   # let the matcher settle the last crossing orders
"$HERE/.exchange" verify
RESULT=$?

commits=$(curl -s http://127.0.0.1:14580/metrics | grep '^oxidb_tx_commits_total' | awk '{print $2}')
conflicts=$(curl -s http://127.0.0.1:14580/metrics | grep '^oxidb_tx_conflicts_total' | awk '{print $2}')
echo "[run] engine: commits=${commits:-?} conflicts=${conflicts:-?}"
echo "[run] matcher: $(grep -oE 'trades=[0-9]+ cancels=[0-9]+' "$HERE/.matcher.log" | tail -1)"
exit $RESULT
