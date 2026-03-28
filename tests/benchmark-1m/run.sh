#!/bin/bash
set -e

echo "Building and starting containers..."
docker compose up --build -d oxidb mongodb

echo "Waiting for services to be healthy..."
docker compose up --build -d bench

echo ""
echo "═══ Memory Usage (before benchmark) ═══"
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}" 2>/dev/null || true

echo ""
echo "Running benchmark..."
docker compose logs -f bench &
LOGS_PID=$!

# Wait for bench to finish
docker wait benchmark-1m-bench-1 2>/dev/null || true
kill $LOGS_PID 2>/dev/null || true

echo ""
echo "═══ Memory Usage (after benchmark) ═══"
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}" 2>/dev/null || true

echo ""
echo "Cleaning up..."
docker compose down -v
