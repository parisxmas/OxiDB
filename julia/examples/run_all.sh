#!/usr/bin/env bash
# Run every OxiDb.jl example against a local oxidb-server.
#
# If a server is already listening on 127.0.0.1:4444 it is reused; otherwise
# a throwaway one is started (built from source if needed) on a temp data
# directory and torn down on exit. Exits non-zero if any example fails.
#
#   julia/examples/run_all.sh
set -uo pipefail

EXAMPLES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$EXAMPLES_DIR/../.." && pwd)"
HOST=127.0.0.1
PORT=4444

GREEN=$'\033[0;32m'; RED=$'\033[0;31m'; CYAN=$'\033[0;36m'; RESET=$'\033[0m'

server_up() { (exec 3<>"/dev/tcp/$HOST/$PORT") 2>/dev/null; }

SERVER_PID=""
DATA_DIR=""
cleanup() {
    [ -n "$SERVER_PID" ] && { kill "$SERVER_PID" 2>/dev/null; wait "$SERVER_PID" 2>/dev/null; }
    [ -n "$DATA_DIR" ] && rm -rf "$DATA_DIR"
}
trap cleanup EXIT

# ── Server ───────────────────────────────────────────────────────────
if server_up; then
    echo "${CYAN}Reusing the oxidb-server already on $HOST:$PORT${RESET}"
else
    SERVER_BIN="$REPO_ROOT/target/release/oxidb-server"
    if [ ! -x "$SERVER_BIN" ]; then
        echo "${CYAN}Building oxidb-server (release) — first run only...${RESET}"
        (cd "$REPO_ROOT" && cargo build --release -p oxidb-server) \
            || { echo "${RED}oxidb-server build failed${RESET}"; exit 1; }
    fi
    DATA_DIR="$(mktemp -d)"
    echo "${CYAN}Starting oxidb-server on $HOST:$PORT (temp data: $DATA_DIR)${RESET}"
    OXIDB_DATA="$DATA_DIR" OXIDB_ADDR="$HOST:$PORT" "$SERVER_BIN" >/dev/null 2>&1 &
    SERVER_PID=$!
    for _ in $(seq 1 40); do server_up && break; sleep 0.25; done
    server_up || { echo "${RED}oxidb-server did not come up${RESET}"; exit 1; }
fi

# ── Examples environment ─────────────────────────────────────────────
echo "${CYAN}Instantiating the examples environment...${RESET}"
julia --project="$EXAMPLES_DIR" -e 'using Pkg; Pkg.instantiate()' \
    || { echo "${RED}Pkg.instantiate failed${RESET}"; exit 1; }
echo ""

# ── Run every numbered example ───────────────────────────────────────
pass=0; fail=0
tmp_out="$(mktemp)"
for f in "$EXAMPLES_DIR"/[0-9]*.jl; do
    name="$(basename "$f")"
    if julia --project="$EXAMPLES_DIR" "$f" >"$tmp_out" 2>&1; then
        echo "${GREEN}PASS${RESET}  $name"
        pass=$((pass + 1))
    else
        echo "${RED}FAIL${RESET}  $name"
        sed 's/^/      /' "$tmp_out" | tail -8
        fail=$((fail + 1))
    fi
done
rm -f "$tmp_out"

echo ""
echo "${CYAN}$pass passed, $fail failed${RESET}"
[ "$fail" -eq 0 ]
