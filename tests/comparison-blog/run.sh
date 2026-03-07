#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

BOLD='\033[1m'
GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
RESET='\033[0m'

echo -e "${BOLD}╔══════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║   OxiDB vs PostgreSQL — Blog Benchmark (1000 Connections)   ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════════════╝${RESET}"
echo ""

# ── Step 1: Build & start containers ──────────────────────────────────
echo -e "${CYAN}[1/3] Building and starting Docker containers...${RESET}"
docker compose down -v --remove-orphans 2>/dev/null || true
docker compose up -d --build --wait
echo -e "${GREEN}  OxiDB and PostgreSQL services are up and healthy.${RESET}"
echo ""

# ── Step 2: Resolve Go dependencies ──────────────────────────────────
echo -e "${CYAN}[2/3] Resolving Go dependencies...${RESET}"
go mod tidy
echo -e "${GREEN}  Dependencies ready.${RESET}"
echo ""

# ── Step 3: Run tests ────────────────────────────────────────────────
echo -e "${CYAN}[3/3] Running blog benchmark tests...${RESET}"
echo ""
ulimit -n 4096 2>/dev/null || true
set +e
go test -v -count=1 -timeout 600s ./... 2>&1 | tee test_output.log
TEST_EXIT=$?
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
