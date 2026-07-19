#!/bin/bash
# Local benchmark: OxiDB vs MongoDB — both native Apple Silicon
# Usage: ./local_bench.sh [doc_count] [btree]
# Examples:
#   ./local_bench.sh 100000        # 100K docs, append-only
#   ./local_bench.sh 100000 btree  # 100K docs, btree engine
#   ./local_bench.sh 1000000       # 1M docs

set -e
cd "$(dirname "$0")"

DOC_COUNT=${1:-100000}
USE_BTREE=${2:-""}
OXIDB_PORT=14888
MONGO_PORT=27099

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║   Local Benchmark: OxiDB vs MongoDB (Apple Silicon)        ║"
echo "║   Documents: $DOC_COUNT                                        ║"
echo "║   OxiDB engine: ${USE_BTREE:-append-only}                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# Build OxiDB release
echo "Building OxiDB (release)..."
cargo build --release -p oxidb-server 2>&1 | tail -1

# Temp dirs
OXIDB_DATA=$(mktemp -d /tmp/oxidb_bench_XXXX)
MONGO_DATA=$(mktemp -d /tmp/mongo_bench_XXXX)

cleanup() {
    echo "Cleaning up..."
    kill $OXIDB_PID $MONGO_PID 2>/dev/null || true
    wait $OXIDB_PID $MONGO_PID 2>/dev/null || true
    rm -rf "$OXIDB_DATA" "$MONGO_DATA"
}
trap cleanup EXIT

# Start MongoDB
echo "Starting MongoDB..."
mongod --dbpath "$MONGO_DATA" --port $MONGO_PORT --quiet --logpath "$MONGO_DATA/mongo.log" &
MONGO_PID=$!
sleep 3

# Start OxiDB
echo "Starting OxiDB..."
OXIDB_ENV="OXIDB_ADDR=127.0.0.1:$OXIDB_PORT OXIDB_DATA=$OXIDB_DATA OXIDB_POOL_SIZE=8 OXIDB_IDLE_TIMEOUT=0"
if [ "$USE_BTREE" = "btree" ]; then
    OXIDB_ENV="$OXIDB_ENV OXIDB_BTREE=true"
fi
eval "$OXIDB_ENV ../../target-local/release/oxidb-server &"
OXIDB_PID=$!
sleep 2

# Check both are running
echo "Checking connections..."
if command -v mongosh >/dev/null 2>&1; then
    mongosh --port $MONGO_PORT --eval "db.adminCommand('ping')" --quiet 2>/dev/null || { echo "MongoDB not running!"; exit 1; }
else
    bash -c "echo > /dev/tcp/127.0.0.1/$MONGO_PORT" 2>/dev/null || { echo "MongoDB not running!"; exit 1; }
fi
echo "  MongoDB: OK (PID: $MONGO_PID)"

# Quick TCP check for OxiDB
bash -c "echo > /dev/tcp/127.0.0.1/$OXIDB_PORT" 2>/dev/null || { echo "OxiDB not running!"; exit 1; }
echo "  OxiDB: OK (PID: $OXIDB_PID)"

# Memory before
echo ""
echo "═══ Memory (before) ═══"
ps -p $OXIDB_PID -o pid,rss,vsz,comm 2>/dev/null | head -2 || true
ps -p $MONGO_PID -o pid,rss,vsz,comm 2>/dev/null | head -2 || true

# Build and run Go benchmark
echo ""
echo "Building benchmark..."
cd ../../tests/benchmark-1m
GOOS=darwin GOARCH=arm64 go build -o bench-local . 2>&1

echo "Running benchmark..."
echo ""
OXIDB_HOST=127.0.0.1 OXIDB_PORT=$OXIDB_PORT MONGO_URI="mongodb://127.0.0.1:$MONGO_PORT" DOC_COUNT=$DOC_COUNT BATCH_SIZE=5000 ./bench-local

# Memory after
echo ""
echo "═══ Memory (after benchmark) ═══"
OXIDB_RSS=$(ps -p $OXIDB_PID -o rss= 2>/dev/null || echo 0)
MONGO_RSS=$(ps -p $MONGO_PID -o rss= 2>/dev/null || echo 0)
OXIDB_MB=$(echo "scale=1; $OXIDB_RSS / 1024" | bc 2>/dev/null || echo "?")
MONGO_MB=$(echo "scale=1; $MONGO_RSS / 1024" | bc 2>/dev/null || echo "?")
echo "  OxiDB:   ${OXIDB_MB} MB"
echo "  MongoDB: ${MONGO_MB} MB"
