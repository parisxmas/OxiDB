#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

BOLD='\033[1m'
GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
RESET='\033[0m'

echo -e "${BOLD}╔══════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║   OxiDB vs MongoDB — Docker Comparison Benchmark           ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════════════╝${RESET}"
echo ""

# ── Step 1: Build & start containers ──────────────────────────────────
echo -e "${CYAN}[1/4] Building and starting Docker containers...${RESET}"
docker compose down -v --remove-orphans 2>/dev/null || true
docker compose up -d --build --wait
echo -e "${GREEN}  OxiDB and MongoDB services are up and healthy.${RESET}"
echo ""

# ── Step 2: Resolve Go dependencies ──────────────────────────────────
echo -e "${CYAN}[2/4] Resolving Go dependencies...${RESET}"
go mod tidy
echo -e "${GREEN}  Dependencies ready.${RESET}"
echo ""

# ── Step 3: Run tests ────────────────────────────────────────────────
echo -e "${CYAN}[3/4] Running comparison benchmark (100K documents)...${RESET}"
echo ""
set +e
go test -v -count=1 -timeout 600s ./... 2>&1 | tee test_output.log
TEST_EXIT=$?
set -e
echo ""

# ── Step 4: Run Go benchmarks ───────────────────────────────────────
echo -e "${CYAN}[4/4] Running Go benchmarks...${RESET}"
echo ""
set +e
go test -bench=. -benchtime=3s -timeout 120s -run='^$' ./... 2>&1 | tee -a test_output.log
BENCH_EXIT=$?
set -e
echo ""

# ── Cleanup ──────────────────────────────────────────────────────────
echo -e "${CYAN}Stopping containers...${RESET}"
docker compose down -v --remove-orphans 2>/dev/null || true

# ── Results ──────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}══════════════════════════════════════════════════════════════${RESET}"
if [ -f report.html ]; then
    echo -e "${GREEN}  HTML Report: $(pwd)/report.html${RESET}"
fi
if [ -f report.json ]; then
    echo -e "${GREEN}  JSON Report: $(pwd)/report.json${RESET}"
fi
echo -e "${BOLD}══════════════════════════════════════════════════════════════${RESET}"

if [ $TEST_EXIT -ne 0 ]; then
    echo -e "${RED}  Some tests FAILED (exit code $TEST_EXIT)${RESET}"
    exit $TEST_EXIT
fi
echo -e "${GREEN}  All tests PASSED${RESET}"
