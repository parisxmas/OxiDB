#!/usr/bin/env bash
# Start a local OxiDB server with the Prometheus /metrics endpoint on,
# for the monitoring stack to scrape. Run this on the HOST, then
# `docker compose up -d` in this directory.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA_DIR="${OXIDB_MON_DATA:-$REPO_ROOT/monitoring/.oxidb-data}"
BIN="$REPO_ROOT/target/release/oxidb-server"

if [[ ! -x "$BIN" ]]; then
  echo "building oxidb-server (release)…"
  (cd "$REPO_ROOT" && cargo build --release -p oxidb-server)
fi

mkdir -p "$DATA_DIR"

echo "OxiDB:      TCP 127.0.0.1:4444   metrics http://127.0.0.1:14580/metrics"
echo "data dir:   $DATA_DIR"
echo "slow-query profiler: ON (>50ms -> _profile)"
echo
exec env \
  OXIDB_DATA="$DATA_DIR" \
  OXIDB_ADDR=127.0.0.1:4444 \
  OXIDB_HTTP_PORT=14580 \
  OXIDB_SLOW_QUERY_MS=50 \
  "$BIN"
