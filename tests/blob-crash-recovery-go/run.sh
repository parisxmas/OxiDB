#!/usr/bin/env bash
# Usage: ./run.sh                  # default: 500 objects
#        N=5000 ./run.sh           # heavier crash test
#        OXIDB_BIN=/path ./run.sh  # custom binary
set -euo pipefail

cd "$(dirname "$0")"

# Build the server if it's missing — saves a step when running fresh.
DEFAULT_BIN="../../target/release/oxidb-server"
if [ -z "${OXIDB_BIN:-}" ] && [ ! -x "$DEFAULT_BIN" ]; then
  echo "oxidb-server not found at $DEFAULT_BIN — building..."
  ( cd ../../oxidb-server && cargo build --release )
fi

exec go run .
