#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PROJECT="oxidb-cluster-test"
COMPOSE_FILE="docker-compose.cluster.yml"

cleanup() {
    echo "[run.sh] Tearing down cluster ..."
    docker compose -f "$COMPOSE_FILE" -p "$PROJECT" down -v --remove-orphans 2>/dev/null || true
}

dump_logs() {
    echo ""
    echo "[run.sh] === Container logs (last 30 lines each) ==="
    for node in oxidb-node1 oxidb-node2 oxidb-node3 oxidb-node4 haproxy; do
        container="${PROJECT}-${node}-1"
        echo ""
        echo "--- $container ---"
        docker logs --tail 30 "$container" 2>&1 || echo "(no logs)"
    done
}

trap cleanup EXIT

echo "[run.sh] Building images ..."
docker compose -f "$COMPOSE_FILE" -p "$PROJECT" build

echo "[run.sh] Starting cluster ..."
docker compose -f "$COMPOSE_FILE" -p "$PROJECT" up -d

echo "[run.sh] Waiting for containers to initialize ..."
sleep 2

echo "[run.sh] Running test ..."
if python3 test_cluster.py; then
    echo ""
    echo "[run.sh] SUCCESS"
else
    EXIT_CODE=$?
    dump_logs
    echo ""
    echo "[run.sh] FAILED (exit code $EXIT_CODE)"
    exit $EXIT_CODE
fi
