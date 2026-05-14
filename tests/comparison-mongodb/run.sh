#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

BOLD='\033[1m'
GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
YELLOW='\033[0;33m'
RESET='\033[0m'

# ── Benchmark mode ───────────────────────────────────────────────────
#   host       — run the Go client on this machine, reaching the containers
#                through Docker's published-port forward (the "as-shipped" path)
#   innetwork  — run the Go client as a container ON the compose network,
#                container-to-container (apples-to-apples, no port-forward)
#   both       — run host first, then in-network (default)
BENCH_MODE="${BENCH_MODE:-both}"

REPO_ROOT="$(cd ../.. && pwd)"
NETWORK="comparison-mongodb_default"
RUNNER_IMAGE="${RUNNER_IMAGE:-golang:1.23}"

case "$BENCH_MODE" in
    host)      TOTAL=4 ;;
    innetwork) TOTAL=3 ;;
    both)      TOTAL=5 ;;
    *) echo -e "${RED}invalid BENCH_MODE '$BENCH_MODE' (expected: host | innetwork | both)${RESET}"; exit 1 ;;
esac

STEP=0
step() { STEP=$((STEP + 1)); echo -e "${CYAN}[$STEP/$TOTAL] $1${RESET}"; }
FAIL=0

echo -e "${BOLD}╔══════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║   OxiDB vs MongoDB — Docker Comparison Benchmark           ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════════════╝${RESET}"
echo -e "  mode: ${BOLD}${BENCH_MODE}${RESET}"
echo ""

# ── Step 1: Build & start containers ──────────────────────────────────
step "Building and starting Docker containers..."
docker compose down -v --remove-orphans 2>/dev/null || true
docker compose up -d --build --wait
echo -e "${GREEN}  OxiDB and MongoDB services are up and healthy.${RESET}"
echo ""

# ── Step 2: Resolve Go dependencies ──────────────────────────────────
step "Resolving Go dependencies..."
go mod tidy
echo -e "${GREEN}  Dependencies ready.${RESET}"
echo ""

# ── Host-side run: client on this machine, via published-port forward ─
run_host() {
    step "Running comparison benchmark — HOST (port-forward)..."
    echo ""
    set +e
    go test -v -count=1 -timeout 600s ./... 2>&1 | tee test_output.log
    [ "${PIPESTATUS[0]}" -ne 0 ] && FAIL=1
    set -e
    echo ""

    step "Running Go benchmarks — HOST (port-forward)..."
    echo ""
    set +e
    go test -bench=. -benchtime=3s -timeout 120s -run='^$' ./... 2>&1 | tee -a test_output.log
    [ "${PIPESTATUS[0]}" -ne 0 ] && FAIL=1
    set -e
    echo ""
}

# ── In-network run: client as a container ON the compose network ──────
# Container-to-container — bypasses Docker's host port-forward, so the
# latency numbers reflect the engines, not the desktop networking stack.
# TestResourceUsage is skipped: it shells out to the `docker` CLI (not
# available in the runner) and its disk/memory numbers are host-independent.
run_innetwork() {
    step "Running comparison benchmark — IN-NETWORK (apples-to-apples)..."
    echo -e "  ${YELLOW}runner: ${RUNNER_IMAGE} on network ${NETWORK} (first run pulls the image)${RESET}"
    echo ""
    set +e
    docker run --rm \
        --network "$NETWORK" \
        -v "$REPO_ROOT":/repo \
        -v comparison-mongodb-gomod:/go/pkg/mod \
        -v comparison-mongodb-gobuild:/root/.cache/go-build \
        -w /repo/tests/comparison-mongodb \
        -e OXIDB_HOST=oxidb -e OXIDB_PORT=4444 \
        -e MONGO_URI=mongodb://mongodb:27017 -e MONGO_ADDR=mongodb:27017 \
        "$RUNNER_IMAGE" \
        sh -c '
            rc=0
            go test -v -count=1 -timeout 600s -skip TestResourceUsage ./... || rc=1
            echo ""
            go test -bench=. -benchtime=3s -timeout 120s -run="^\$" ./... || rc=1
            exit $rc
        ' 2>&1 | tee test_output_innetwork.log
    [ "${PIPESTATUS[0]}" -ne 0 ] && FAIL=1
    set -e
    echo ""
}

case "$BENCH_MODE" in
    host)      run_host ;;
    innetwork) run_innetwork ;;
    both)      run_host; run_innetwork ;;
esac

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
[ "$BENCH_MODE" != "host" ] && [ -f test_output_innetwork.log ] && \
    echo -e "${GREEN}  In-network log: $(pwd)/test_output_innetwork.log${RESET}"
echo -e "${BOLD}══════════════════════════════════════════════════════════════${RESET}"

if [ "$FAIL" -ne 0 ]; then
    echo -e "${RED}  Some tests FAILED${RESET}"
    exit 1
fi
echo -e "${GREEN}  All tests PASSED${RESET}"
