#!/bin/bash
set -e
cd "$(dirname "$0")"

CUST_COUNT=${1:-15000000}
OXIDB_PORT=14888

echo "Building OxiDB (release)..."
cargo build --release -p oxidb-server 2>&1 | tail -1

OXIDB_DATA=$(mktemp -d /tmp/oxidb_telco_XXXX)

cleanup() {
    echo ""
    echo "Cleaning up..."
    kill $OXIDB_PID 2>/dev/null || true
    wait $OXIDB_PID 2>/dev/null || true
    # Show data size
    du -sh "$OXIDB_DATA" 2>/dev/null || true
    rm -rf "$OXIDB_DATA"
}
trap cleanup EXIT

echo "Starting OxiDB..."
OXIDB_ADDR=127.0.0.1:$OXIDB_PORT OXIDB_DATA=$OXIDB_DATA OXIDB_IDLE_TIMEOUT=0 \
    ../../target-local/release/oxidb-server &
OXIDB_PID=$!
sleep 3

bash -c "echo > /dev/tcp/127.0.0.1/$OXIDB_PORT" 2>/dev/null || { echo "OxiDB not running!"; exit 1; }
echo "OxiDB: OK (PID: $OXIDB_PID)"

# Memory before
RSS_BEFORE=$(ps -p $OXIDB_PID -o rss= 2>/dev/null || echo 0)
echo "Memory before: $(echo "scale=0; $RSS_BEFORE / 1024" | bc)MB"
echo ""

echo "Building test..."
go build -o telco-test . 2>&1

echo ""
OXIDB_HOST=127.0.0.1 OXIDB_PORT=$OXIDB_PORT CUST_COUNT=$CUST_COUNT ./telco-test

# Memory after
echo ""
RSS_AFTER=$(ps -p $OXIDB_PID -o rss= 2>/dev/null || echo 0)
echo "Memory after: $(echo "scale=0; $RSS_AFTER / 1024" | bc)MB"
