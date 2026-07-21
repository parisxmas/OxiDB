#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────────────
# OxiDB vs Redis Benchmark
# Compares performance using redis-benchmark (built-in Redis tool)
# ─────────────────────────────────────────────────────────────────────

REDIS_PORT=6399
OXIDB_REDIS_PORT=6400
OXIDB_NATIVE_PORT=4499

REQUESTS=100000
CLIENTS=50
PIPELINE=1
DATA_SIZE=64

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

cleanup() {
    echo ""
    echo -e "${YELLOW}Cleaning up...${NC}"
    [ -n "${REDIS_PID:-}" ] && kill "$REDIS_PID" 2>/dev/null && wait "$REDIS_PID" 2>/dev/null
    [ -n "${OXIDB_PID:-}" ] && kill "$OXIDB_PID" 2>/dev/null && wait "$OXIDB_PID" 2>/dev/null
    rm -rf /tmp/oxidb_redis_bench /tmp/redis_bench_data
}
trap cleanup EXIT

echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}         OxiDB vs Redis — Performance Benchmark${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  Requests:  ${CYAN}${REQUESTS}${NC}"
echo -e "  Clients:   ${CYAN}${CLIENTS}${NC}"
echo -e "  Data size: ${CYAN}${DATA_SIZE} bytes${NC}"
echo -e "  Pipeline:  ${CYAN}${PIPELINE}${NC}"
echo ""

# ── Start Redis ──────────────────────────────────────────────────────
echo -e "${BLUE}Starting Redis server on port ${REDIS_PORT}...${NC}"
mkdir -p /tmp/redis_bench_data
redis-server --port "$REDIS_PORT" --save "" --appendonly no \
    --dir /tmp/redis_bench_data --loglevel warning --daemonize no &
REDIS_PID=$!
sleep 1

if ! kill -0 "$REDIS_PID" 2>/dev/null; then
    echo -e "${RED}Failed to start Redis${NC}"
    exit 1
fi
REDIS_VERSION=$(redis-cli -p "$REDIS_PORT" INFO server 2>/dev/null | grep redis_version | tr -d '\r' | cut -d: -f2)
echo -e "  Redis ${REDIS_VERSION} running (PID ${REDIS_PID})"

# ── Start OxiDB ─────────────────────────────────────────────────────
echo -e "${BLUE}Starting OxiDB server on port ${OXIDB_REDIS_PORT}...${NC}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Build release if needed
echo -e "  Building oxidb-server (release)..."
(cd "$PROJECT_ROOT" && cargo build --release -p oxidb-server 2>&1 | tail -1)

OXIDB_MODE=memory \
OXIDB_OXIMEM_PORT="$OXIDB_REDIS_PORT" \
OXIDB_ADDR="127.0.0.1:${OXIDB_NATIVE_PORT}" \
"$PROJECT_ROOT/target/release/oxidb-server" 2>/dev/null &
OXIDB_PID=$!
sleep 2

if ! kill -0 "$OXIDB_PID" 2>/dev/null; then
    echo -e "${RED}Failed to start OxiDB${NC}"
    exit 1
fi
echo -e "  OxiDB running (PID ${OXIDB_PID})"

# Verify both respond to PING
redis-cli -p "$REDIS_PORT" PING > /dev/null 2>&1 || { echo -e "${RED}Redis not responding${NC}"; exit 1; }
redis-cli -p "$OXIDB_REDIS_PORT" PING > /dev/null 2>&1 || { echo -e "${RED}OxiDB not responding${NC}"; exit 1; }

echo ""
echo -e "${BOLD}─── Benchmark Results ─────────────────────────────────────────${NC}"
echo ""

# ── Helper to extract ops/sec from redis-benchmark output ────────────
extract_ops() {
    # redis-benchmark CSV mode: "test","rps","avg_latency_ms","min_latency_ms","p50","p95","p99","max_latency_ms"
    echo "$1" | tail -1 | cut -d',' -f2 | tr -d '"'
}

extract_latency_p50() {
    echo "$1" | tail -1 | cut -d',' -f5 | tr -d '"'
}

extract_latency_p99() {
    echo "$1" | tail -1 | cut -d',' -f7 | tr -d '"'
}

run_bench() {
    local test_name="$1"
    local bench_args="$2"

    # Run against Redis
    local redis_out
    redis_out=$(redis-benchmark -p "$REDIS_PORT" -c "$CLIENTS" -n "$REQUESTS" \
        -d "$DATA_SIZE" --csv $bench_args 2>/dev/null)
    local redis_ops=$(extract_ops "$redis_out")
    local redis_p50=$(extract_latency_p50 "$redis_out")
    local redis_p99=$(extract_latency_p99 "$redis_out")

    # Flush between tests
    redis-cli -p "$REDIS_PORT" FLUSHDB > /dev/null 2>&1

    # Run against OxiDB
    local oxidb_out
    oxidb_out=$(redis-benchmark -p "$OXIDB_REDIS_PORT" -c "$CLIENTS" -n "$REQUESTS" \
        -d "$DATA_SIZE" --csv $bench_args 2>/dev/null)
    local oxidb_ops=$(extract_ops "$oxidb_out")
    local oxidb_p50=$(extract_latency_p50 "$oxidb_out")
    local oxidb_p99=$(extract_latency_p99 "$oxidb_out")

    # Flush between tests
    redis-cli -p "$OXIDB_REDIS_PORT" FLUSHDB > /dev/null 2>&1

    # Calculate ratio
    local ratio=""
    if [ -n "$redis_ops" ] && [ -n "$oxidb_ops" ]; then
        ratio=$(echo "scale=2; $oxidb_ops / $redis_ops * 100" | bc 2>/dev/null || echo "N/A")
    fi

    printf "  ${BOLD}%-14s${NC}  │  %12s ops/s  │  %12s ops/s  │  %5s%%\n" \
        "$test_name" "$redis_ops" "$oxidb_ops" "$ratio"
    printf "  %-14s  │  p50=%6s p99=%6s  │  p50=%6s p99=%6s  │\n" \
        "" "${redis_p50}ms" "${redis_p99}ms" "${oxidb_p50}ms" "${oxidb_p99}ms"
}

printf "  ${BOLD}%-14s${NC}  │  ${GREEN}%-21s${NC}│  ${CYAN}%-21s${NC}│  ${YELLOW}%s${NC}\n" \
    "Test" "    Redis 8.6" "     OxiDB" " Ratio"
echo "  ────────────────┼───────────────────────┼───────────────────────┼────────"

run_bench "PING" "-t ping"
run_bench "SET" "-t set"
run_bench "GET" "-t get"
run_bench "INCR" "-t incr"
run_bench "LPUSH" "-t lpush"
run_bench "RPUSH" "-t rpush"
run_bench "LPOP" "-t lpop"
run_bench "RPOP" "-t rpop"
run_bench "SADD" "-t sadd"
run_bench "HSET" "-t hset"
run_bench "MSET (10)" "-t mset"

echo "  ────────────────┼───────────────────────┼───────────────────────┼────────"

# ── Pipeline benchmark ───────────────────────────────────────────────
echo ""
echo -e "${BOLD}─── Pipeline Benchmark (16 commands batched) ──────────────────${NC}"
echo ""

PIPELINE=16

run_bench_pipeline() {
    local test_name="$1"
    local bench_args="$2"

    local redis_out
    redis_out=$(redis-benchmark -p "$REDIS_PORT" -c "$CLIENTS" -n "$REQUESTS" \
        -d "$DATA_SIZE" -P "$PIPELINE" --csv $bench_args 2>/dev/null)
    local redis_ops=$(extract_ops "$redis_out")
    local redis_p50=$(extract_latency_p50 "$redis_out")
    local redis_p99=$(extract_latency_p99 "$redis_out")

    redis-cli -p "$REDIS_PORT" FLUSHDB > /dev/null 2>&1

    local oxidb_out
    oxidb_out=$(redis-benchmark -p "$OXIDB_REDIS_PORT" -c "$CLIENTS" -n "$REQUESTS" \
        -d "$DATA_SIZE" -P "$PIPELINE" --csv $bench_args 2>/dev/null)
    local oxidb_ops=$(extract_ops "$oxidb_out")
    local oxidb_p50=$(extract_latency_p50 "$oxidb_out")
    local oxidb_p99=$(extract_latency_p99 "$oxidb_out")

    redis-cli -p "$OXIDB_REDIS_PORT" FLUSHDB > /dev/null 2>&1

    local ratio=""
    if [ -n "$redis_ops" ] && [ -n "$oxidb_ops" ]; then
        ratio=$(echo "scale=2; $oxidb_ops / $redis_ops * 100" | bc 2>/dev/null || echo "N/A")
    fi

    printf "  ${BOLD}%-14s${NC}  │  %12s ops/s  │  %12s ops/s  │  %5s%%\n" \
        "$test_name" "$redis_ops" "$oxidb_ops" "$ratio"
    printf "  %-14s  │  p50=%6s p99=%6s  │  p50=%6s p99=%6s  │\n" \
        "" "${redis_p50}ms" "${redis_p99}ms" "${oxidb_p50}ms" "${oxidb_p99}ms"
}

printf "  ${BOLD}%-14s${NC}  │  ${GREEN}%-21s${NC}│  ${CYAN}%-21s${NC}│  ${YELLOW}%s${NC}\n" \
    "Test (P=16)" "    Redis 8.6" "     OxiDB" " Ratio"
echo "  ────────────────┼───────────────────────┼───────────────────────┼────────"

run_bench_pipeline "SET" "-t set"
run_bench_pipeline "GET" "-t get"
run_bench_pipeline "INCR" "-t incr"
run_bench_pipeline "LPUSH" "-t lpush"
run_bench_pipeline "HSET" "-t hset"

echo "  ────────────────┼───────────────────────┼───────────────────────┼────────"

# ── Large payload benchmark ──────────────────────────────────────────
echo ""
echo -e "${BOLD}─── Large Payload Benchmark (1KB, 4KB values) ─────────────────${NC}"
echo ""

printf "  ${BOLD}%-14s${NC}  │  ${GREEN}%-21s${NC}│  ${CYAN}%-21s${NC}│  ${YELLOW}%s${NC}\n" \
    "Test" "    Redis 8.6" "     OxiDB" " Ratio"
echo "  ────────────────┼───────────────────────┼───────────────────────┼────────"

# 1KB payload
run_bench_large() {
    local test_name="$1"
    local dsize="$2"
    local bench_args="$3"

    local redis_out
    redis_out=$(redis-benchmark -p "$REDIS_PORT" -c "$CLIENTS" -n "$REQUESTS" \
        -d "$dsize" --csv $bench_args 2>/dev/null)
    local redis_ops=$(extract_ops "$redis_out")
    local redis_p50=$(extract_latency_p50 "$redis_out")
    local redis_p99=$(extract_latency_p99 "$redis_out")

    redis-cli -p "$REDIS_PORT" FLUSHDB > /dev/null 2>&1

    local oxidb_out
    oxidb_out=$(redis-benchmark -p "$OXIDB_REDIS_PORT" -c "$CLIENTS" -n "$REQUESTS" \
        -d "$dsize" --csv $bench_args 2>/dev/null)
    local oxidb_ops=$(extract_ops "$oxidb_out")
    local oxidb_p50=$(extract_latency_p50 "$oxidb_out")
    local oxidb_p99=$(extract_latency_p99 "$oxidb_out")

    redis-cli -p "$OXIDB_REDIS_PORT" FLUSHDB > /dev/null 2>&1

    local ratio=""
    if [ -n "$redis_ops" ] && [ -n "$oxidb_ops" ]; then
        ratio=$(echo "scale=2; $oxidb_ops / $redis_ops * 100" | bc 2>/dev/null || echo "N/A")
    fi

    printf "  ${BOLD}%-14s${NC}  │  %12s ops/s  │  %12s ops/s  │  %5s%%\n" \
        "$test_name" "$redis_ops" "$oxidb_ops" "$ratio"
    printf "  %-14s  │  p50=%6s p99=%6s  │  p50=%6s p99=%6s  │\n" \
        "" "${redis_p50}ms" "${redis_p99}ms" "${oxidb_p50}ms" "${oxidb_p99}ms"
}

run_bench_large "SET 1KB" 1024 "-t set"
run_bench_large "GET 1KB" 1024 "-t get"
run_bench_large "SET 4KB" 4096 "-t set"
run_bench_large "GET 4KB" 4096 "-t get"

echo "  ────────────────┼───────────────────────┼───────────────────────┼────────"

echo ""
echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  Notes:${NC}"
echo -e "  • Redis 8.6 is single-threaded (io-threads not used here)"
echo -e "  • OxiDB uses thread-per-connection with RwLock-based concurrency"
echo -e "  • OxiDB stores data as JSON documents (richer data model)"
echo -e "  • Ratio = OxiDB ops/s ÷ Redis ops/s × 100"
echo -e "  • Both running in-memory, no persistence"
echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"
