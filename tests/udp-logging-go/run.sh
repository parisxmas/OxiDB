#!/bin/bash
set -e
cd "$(dirname "$0")"

OXIDB_PORT=14888
UDP_PORT=5140

echo "Building OxiDB (release)..."
cargo build --release -p oxidb-server 2>&1 | tail -1

OXIDB_DATA=$(mktemp -d /tmp/oxidb_udp_XXXX)

cleanup() {
    echo "Cleaning up..."
    kill $OXIDB_PID 2>/dev/null || true
    wait $OXIDB_PID 2>/dev/null || true
    rm -rf "$OXIDB_DATA"
}
trap cleanup EXIT

echo "Starting OxiDB with UDP ingestion..."
OXIDB_ADDR=127.0.0.1:$OXIDB_PORT OXIDB_DATA=$OXIDB_DATA OXIDB_POOL_SIZE=20 \
    OXIDB_IDLE_TIMEOUT=0 OXIDB_BTREE=true OXIDB_UDP_PORT=$UDP_PORT \
    ../../target-local/release/oxidb-server &
OXIDB_PID=$!
sleep 3

bash -c "echo > /dev/tcp/127.0.0.1/$OXIDB_PORT" 2>/dev/null || { echo "OxiDB not running!"; exit 1; }
echo "OxiDB: OK (PID: $OXIDB_PID)"
echo ""

echo "Building test..."
go build -o udp-bench . 2>&1

echo ""
OXIDB_HOST=127.0.0.1 OXIDB_PORT=$OXIDB_PORT UDP_PORT=$UDP_PORT ./udp-bench
