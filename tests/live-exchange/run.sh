#!/usr/bin/env bash
# Live exchange integration test:
#   1 feeder process  — real Binance/Coinbase/Kraken WebSocket -> OxiDB (20 symbols)
#   10 trader processes — read live prices, place atomic buy/sell fills
# then verify the ledger is consistent.
#
# Usage: ./run.sh [trade_seconds]   (default 45)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
PY="$HERE/.venv/bin/python"
BIN="$ROOT/target/release/oxidb-server"
DATA="$HERE/.data"
SECS="${1:-45}"
PORT=4455                       # dedicated port — avoid colliding with any other oxidb
export OXIDB_PORT="$PORT"       # picked up by oxidb_client.py in every child process
PIDS=()

if lsof -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "ERROR: port $PORT already in use — stop the other server first"; exit 1
fi

cleanup() {
  echo; echo "[run] stopping…"
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

[[ -x "$BIN" ]] || { echo "building server…"; (cd "$ROOT" && cargo build --release -p oxidb-server); }
rm -rf "$DATA"; mkdir -p "$DATA"

echo "[run] starting oxidb-server on :$PORT (metrics on :14580)…"
OXIDB_DATA="$DATA" OXIDB_ADDR="127.0.0.1:$PORT" OXIDB_HTTP_PORT=14580 \
  "$BIN" > "$HERE/.server.log" 2>&1 &
PIDS+=($!)
sleep 3
lsof -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1 || { echo "ERROR: server failed to bind :$PORT"; cat "$HERE/.server.log"; exit 1; }

echo "[run] seeding…"
"$PY" "$HERE/setup.py"

echo "[run] starting market-data feeder (live exchange WebSockets)…"
"$PY" "$HERE/feeder.py" > "$HERE/.feeder.log" 2>&1 &
PIDS+=($!)

echo "[run] waiting for live prices to arrive…"
for i in $(seq 1 30); do
  n=$("$PY" -c "import sys; sys.path.insert(0,'$HERE'); from oxidb_client import OxiDB; print(sum(1 for p in OxiDB().find('prices',{}) if p.get('price',0)>0))")
  echo "  $n/20 symbols have a live price"
  [[ "$n" -ge 10 ]] && break
  sleep 1
done

echo "[run] launching 10 trader processes for ${SECS}s…"
for u in $(seq 0 9); do
  "$PY" "$HERE/trader.py" "$u" "$SECS" > "$HERE/.trader-$u.log" 2>&1 &
  PIDS+=($!)
done

# Wait for traders (they self-terminate after SECS).
for u in $(seq 0 9); do wait "${PIDS[$((u+2))]}" 2>/dev/null || true; done

echo "[run] trading finished — verifying ledger…"
"$PY" "$HERE/verify.py"
RESULT=$?

echo "[run] feeder: $(grep -oE '[0-9]+ ticks written' "$HERE/.feeder.log" | tail -1)"
exit $RESULT
