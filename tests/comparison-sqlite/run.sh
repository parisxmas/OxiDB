#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

BOLD='\033[1m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
RESET='\033[0m'

echo -e "${BOLD}╔══════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║   OxiDB vs SQLite — Embedded Comparison Benchmark          ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════════════╝${RESET}"
echo ""

echo -e "${CYAN}[1/2] Building release binary...${RESET}"
cargo build --release --example bench_vs_sqlite
echo -e "${GREEN}  Build complete.${RESET}"
echo ""

echo -e "${CYAN}[2/2] Running benchmark (100K documents, embedded, best of 3)...${RESET}"
echo ""
./target/release/examples/bench_vs_sqlite 2>&1 | tee tests/comparison-sqlite/test_output.log
echo ""

echo -e "${BOLD}══════════════════════════════════════════════════════════════${RESET}"
if [ -f tests/comparison-sqlite/report.html ]; then
    echo -e "${GREEN}  HTML Report: $(pwd)/tests/comparison-sqlite/report.html${RESET}"
fi
echo -e "${GREEN}  Both databases ran in-process (no network overhead).${RESET}"
echo -e "${BOLD}══════════════════════════════════════════════════════════════${RESET}"
