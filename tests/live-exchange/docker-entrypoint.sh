#!/bin/sh
# Run the whole exchange from one container: oxidb-server + seed + matcher +
# web dashboard + traders (respawned forever). The ledger/orders/candles are
# TTL-bounded so memory stays flat over a long run.
set -eu

export OXIDB_DATA=/data
export OXIDB_ADDR=127.0.0.1:4455
export OXIDB_PORT=4455
export OXIDB_HTTP_PORT=14580
export OXIDB_OXIMEM_PORT=6489
export OXIMEM_PORT=6489
export OXIDB_POOL_SIZE="${OXIDB_POOL_SIZE:-16}"
export OXIDB_LAZY_SYNC=true
export OXIDB_SYNC_INTERVAL_MS=200
export OXIDB_IDLE_TIMEOUT=0   # never drop client connections (traders/matcher stay live)
export WEB_PORT=8090
export METRICS_URL=http://127.0.0.1:14580/metrics
export LEDGER_TTL_SECS="${LEDGER_TTL_SECS:-60}"
export ORDER_TTL_SECS="${ORDER_TTL_SECS:-35}"
export ORDER_RATE_EACH="${ORDER_RATE_EACH:-70}"
export TAKER_PCT="${TAKER_PCT:-45}"
export NUSERS="${NUSERS:-10}"
export BACKFILL_DAYS="${BACKFILL_DAYS:-5}"

# Fresh data each start so setup seeds cleanly. Clear CONTENTS only (when
# /data is a tmpfs mount its directory can't be removed).
mkdir -p /data
rm -rf /data/* /data/.[!.]* 2>/dev/null || true

echo "[entry] starting oxidb-server…"
oxidb-server >/tmp/server.log 2>&1 &
SRV=$!
export SERVER_PID="$SRV"
sleep 4

echo "[entry] seeding market…"
exchange setup

echo "[entry] starting matcher + web dashboard (:8090)…"
exchange matcher >/tmp/matcher.log 2>&1 &
exchange web >/tmp/web.log 2>&1 &

echo "[entry] launching $NUSERS traders (respawn forever)…"
u=0
while [ "$u" -lt "$NUSERS" ]; do
  ( while true; do exchange trader "$u" 86400 >/dev/null 2>&1; sleep 1; done ) &
  u=$((u + 1))
done

echo "[entry] up — dashboard on :8090"

# Watchdog: if trading freezes (commit counter flat for 90s), kill PID 1 so
# Docker restarts the container fresh. Belt-and-suspenders in case the market
# ever wedges.
(
  prev=-1; stall=0
  while sleep 30; do
    cur=$(curl -s http://127.0.0.1:14580/metrics 2>/dev/null | grep "^oxidb_tx_commits_total" | awk "{print \$2}")
    cur="${cur:-0}"
    if [ "$cur" = "$prev" ]; then stall=$((stall + 1)); else stall=0; fi
    prev="$cur"
    if [ "$stall" -ge 3 ]; then echo "[watchdog] trades frozen ~90s — restarting container"; kill 1; fi
  done
) &

# If the engine dies the container exits and Docker restarts it.
wait "$SRV"
