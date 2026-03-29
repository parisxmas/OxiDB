#!/bin/bash
# Run replication tests against a 3-node Raft cluster
#
# Prerequisites: Docker Compose cluster must be running:
#   cd tests/cluster && docker compose -f docker-compose.3node.yml up -d --build
#
# Usage: ./run.sh

set -e
cd "$(dirname "$0")"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║       OxiDB Raft Replication Test Runner                    ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# Check if cluster is running
if ! bash -c "echo > /dev/tcp/127.0.0.1/5500" 2>/dev/null; then
    echo "Cluster not running. Starting with Docker Compose..."
    cd ../cluster
    docker compose -f docker-compose.3node.yml up -d --build
    cd ../replication-go
    echo "Waiting for cluster to start (30s)..."
    sleep 30
fi

echo "Building test..."
go build -o repl-test . 2>&1

echo "Running replication tests..."
echo ""
HOST=127.0.0.1 HAPROXY_PORT=5500 NODE1_PORT=5501 NODE2_PORT=5502 NODE3_PORT=5503 ./repl-test
