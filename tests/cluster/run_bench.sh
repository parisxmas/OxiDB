#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PROJECT="oxidb-3node"
COMPOSE_FILE="docker-compose.3node.yml"

cleanup() {
    echo "[bench] Tearing down cluster ..."
    docker compose -f "$COMPOSE_FILE" -p "$PROJECT" down -v --remove-orphans 2>/dev/null || true
}

dump_logs() {
    echo ""
    echo "[bench] === Container logs (last 50 lines each) ==="
    for node in oxidb-node1 oxidb-node2 oxidb-node3 haproxy; do
        container="${PROJECT}-${node}-1"
        echo ""
        echo "--- $container ---"
        docker logs --tail 50 "$container" 2>&1 || echo "(no logs)"
    done
}

trap cleanup EXIT

echo "[bench] Building images ..."
docker compose -f "$COMPOSE_FILE" -p "$PROJECT" build

echo "[bench] Starting 3-node cluster ..."
docker compose -f "$COMPOSE_FILE" -p "$PROJECT" up -d

echo "[bench] Waiting for containers to initialize ..."
sleep 3

echo "[bench] Running benchmark ..."
if python3 bench_3node.py; then
    echo ""
    echo "[bench] BENCHMARK COMPLETE"
else
    EXIT_CODE=$?
    dump_logs
    echo ""
    echo "[bench] BENCHMARK FAILED (exit code $EXIT_CODE)"
    exit $EXIT_CODE
fi
