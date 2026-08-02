#!/bin/bash
# End-to-end FTS smoke test:
#   1. spin up oxidb-server on a private port
#   2. generate 100 .docx (skips if already in ./data)
#   3. upload them
#   4. run the search suite

set -e
cd "$(dirname "$0")"

OXIDB_PORT=14888
OXIDB_DATA=$(mktemp -d /tmp/oxidb_ftstests_XXXX)

cleanup() {
    echo "[cleanup] killing server $OXIDB_PID and removing $OXIDB_DATA"
    kill "$OXIDB_PID" 2>/dev/null || true
    wait "$OXIDB_PID" 2>/dev/null || true
    rm -rf "$OXIDB_DATA"
}
trap cleanup EXIT

REPO_ROOT="$(cd .. && pwd)"
SERVER_BIN="$REPO_ROOT/target-local/release/oxidb-server"

if [[ ! -x "$SERVER_BIN" ]]; then
    echo "[build] release oxidb-server (target-local/release)"
    (cd "$REPO_ROOT" && cargo build --release -p oxidb-server)
fi

echo "[start] oxidb-server on 127.0.0.1:$OXIDB_PORT"
OXIDB_ADDR=127.0.0.1:$OXIDB_PORT \
OXIDB_DATA=$OXIDB_DATA \
OXIDB_IDLE_TIMEOUT=0 \
OXIDB_FTS_WORKERS=4 \
OXIDB_FTS_FLUSH_INTERVAL_MS=200 \
    "$SERVER_BIN" >server.log 2>&1 &
OXIDB_PID=$!

# wait for the listener
for _ in $(seq 1 20); do
    if bash -c "echo > /dev/tcp/127.0.0.1/$OXIDB_PORT" 2>/dev/null; then
        echo "[start] listener up (pid=$OXIDB_PID)"
        break
    fi
    sleep 0.25
done

if ! ls data/*.docx >/dev/null 2>&1; then
    echo "[generate] downloading + chunking 100 .docx"
    python3 01_generate.py
fi

echo "[upload] sending blobs"
OXIDB_PORT=$OXIDB_PORT python3 02_upload.py

echo "[search] running query suite"
OXIDB_PORT=$OXIDB_PORT python3 03_search.py
