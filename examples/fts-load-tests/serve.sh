#!/bin/bash
# Long-running ftstests environment:
#   1. spin up oxidb-server with persistent data dir (so re-runs are cheap)
#   2. ensure the corpus is generated and uploaded
#   3. start the web client on http://127.0.0.1:8765
# Ctrl-C tears everything down cleanly.

set -e
cd "$(dirname "$0")"

OXIDB_PORT=14888
WEB_PORT=8765
OXIDB_DATA="$(pwd)/oxidb_data"   # persistent — survives between runs
mkdir -p "$OXIDB_DATA"

REPO_ROOT="$(cd .. && pwd)"
SERVER_BIN="$REPO_ROOT/target-local/release/oxidb-server"

cleanup() {
    echo
    echo "[cleanup] stopping web (pid=$WEB_PID) and oxidb (pid=$OXIDB_PID)"
    kill "$WEB_PID" "$OXIDB_PID" 2>/dev/null || true
    wait "$WEB_PID" "$OXIDB_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

if [[ ! -x "$SERVER_BIN" ]]; then
    echo "[build] release oxidb-server"
    (cd "$REPO_ROOT" && cargo build --release -p oxidb-server)
fi

echo "[start] oxidb-server on 127.0.0.1:$OXIDB_PORT (data=$OXIDB_DATA)"
OXIDB_ADDR=127.0.0.1:$OXIDB_PORT \
OXIDB_DATA="$OXIDB_DATA" \
OXIDB_IDLE_TIMEOUT=0 \
OXIDB_FTS_WORKERS=4 \
OXIDB_FTS_FLUSH_INTERVAL_MS=500 \
    "$SERVER_BIN" >server.log 2>&1 &
OXIDB_PID=$!

# Wait for the listener to come up.
for _ in $(seq 1 40); do
    if bash -c "echo > /dev/tcp/127.0.0.1/$OXIDB_PORT" 2>/dev/null; then
        break
    fi
    sleep 0.25
done

if ! ls data/*.docx >/dev/null 2>&1; then
    echo "[generate] downloading + chunking 100 .docx"
    python3 01_generate.py
fi

# put_object is idempotent — re-uploading an existing key just overwrites,
# so we always run this to make sure the index reflects whatever is in data/.
echo "[upload] syncing 100 blobs to OxiDB"
OXIDB_PORT=$OXIDB_PORT python3 02_upload.py

echo
echo "============================================================"
echo "  open http://127.0.0.1:$WEB_PORT/"
echo "  Ctrl-C to stop both servers"
echo "============================================================"
echo
WEB_PORT=$WEB_PORT OXIDB_PORT=$OXIDB_PORT python3 web.py &
WEB_PID=$!

# Block on the web server. cleanup() trap kills both children on exit.
wait "$WEB_PID"
