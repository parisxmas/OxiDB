#!/bin/bash
# Run a specific complex benchmark test
# Usage: ./run.sh <test_name> [doc_count]
# Examples:
#   ./run.sh mixed_workload
#   ./run.sh pagination 200000
#   ./run.sh all              # run all tests sequentially

set -e
cd "$(dirname "$0")"

TEST=${1:-""}
DOC_COUNT=${2:-100000}
OXIDB_PORT=14888
MONGO_PORT=27099

if [ -z "$TEST" ]; then
    echo "Usage: ./run.sh <test_name> [doc_count]"
    echo ""
    echo "Available tests:"
    echo "  mixed_workload    — Concurrent 70% read / 20% update / 10% insert"
    echo "  pagination        — Skip+limit pagination through large result sets"
    echo "  nested_query      — Dot-notation queries on nested documents"
    echo "  pipeline          — Multi-stage aggregation pipelines"
    echo "  lookup            — Cross-collection \$lookup joins"
    echo "  large_result      — Fetching 1K/5K/10K document result sets"
    echo "  hot_key           — Concurrent updates on same document"
    echo "  write_heavy       — Sustained write throughput (insert+update)"
    echo "  bulk_ops          — Bulk update/delete 10K-50K documents"
    echo "  scale_test        — Same queries at 10K/50K/100K/500K scale"
    echo "  all               — Run all tests sequentially"
    exit 1
fi

# Build OxiDB
echo "Building OxiDB (release)..."
cargo build --release -p oxidb-server 2>&1 | tail -1

# Temp dirs
OXIDB_DATA=$(mktemp -d /tmp/oxidb_complex_XXXX)
MONGO_DATA=$(mktemp -d /tmp/mongo_complex_XXXX)

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

# Start OxiDB (btree engine)
echo "Starting OxiDB (btree)..."
OXIDB_ADDR=127.0.0.1:$OXIDB_PORT OXIDB_DATA=$OXIDB_DATA OXIDB_POOL_SIZE=20 OXIDB_IDLE_TIMEOUT=0 OXIDB_BTREE=true \
    ../../target-local/release/oxidb-server &
OXIDB_PID=$!
sleep 2

# Check
mongosh --port $MONGO_PORT --eval "db.adminCommand('ping')" --quiet 2>/dev/null || { echo "MongoDB not running!"; exit 1; }
echo "  MongoDB: OK (PID: $MONGO_PID)"
bash -c "echo > /dev/tcp/127.0.0.1/$OXIDB_PORT" 2>/dev/null || { echo "OxiDB not running!"; exit 1; }
echo "  OxiDB: OK (PID: $OXIDB_PID)"
echo ""

# Build test binary with the selected test tag
echo "Building test..."
go build -tags "$TEST" -o bench-complex . 2>&1

echo "Running: $TEST"
echo ""
OXIDB_HOST=127.0.0.1 OXIDB_PORT=$OXIDB_PORT MONGO_URI="mongodb://127.0.0.1:$MONGO_PORT" DOC_COUNT=$DOC_COUNT ./bench-complex "$TEST"
