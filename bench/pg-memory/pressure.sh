#!/usr/bin/env bash
# Does the evictable/non-evictable split actually hold under memory pressure?
#
# `fair.sh` measures what each engine costs an unconstrained machine and shows
# that 83% of OxiDB's is clean file-backed pages against 64% of PostgreSQL's.
# That predicts a behavioural difference — OxiDB should keep working where
# PostgreSQL cannot — but a percentage is not a proof. This runs both engines
# in Linux cgroups with a hard memory limit and finds where each one stops.
#
# A cgroup limit is the right instrument: page cache is charged to the cgroup
# and reclaimed under pressure, while anonymous memory is not. That is exactly
# the distinction being claimed, enforced by the kernel rather than inferred
# from a metric.
#
# Both engines get the same data, the same client, and the same workload. The
# only variable is `--memory`.
#
# PostgreSQL is run twice on purpose: at its stock `shared_buffers=128MB`, which
# is what a user gets, and at 32MB, because "it can be tuned down" is a fair
# rebuttal and should be in the results rather than in a footnote.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
WORK="${1:-$(mktemp -d)}"
LIMITS=${LIMITS:-"1024m 512m 256m 192m 128m 96m"}
OXI_BIN="$HERE/../../target-local/aarch64-unknown-linux-musl/release/oxidb-server"
mkdir -p "$WORK"

[ -f "$OXI_BIN" ] || {
  echo "missing Linux build: cargo build --release --target aarch64-unknown-linux-musl -p oxidb-server"
  exit 1
}
[ -f "$WORK/data.sql" ] || python3 "$HERE/gen.py" --batch 500 --out "$WORK/data.sql"

cleanup() { docker rm -f oxi-pressure pg-pressure >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

# The workload: read every table, then use three indexes. Same statements for
# both engines, run from a client outside the limited container so the client's
# own memory never counts against the engine.
cat > "$WORK/work.sql" <<'SQL'
-- Aggregates over real columns, not COUNT(*): both engines answer a bare
-- COUNT(*) from metadata without reading a row, which makes it useless here —
-- it faults no pages and so applies no cache pressure at all. Summing a column
-- forces every row to be read.
SELECT sum(total) FROM orders;
SELECT sum(price) FROM products;
SELECT sum(on_hand) FROM inventory;
SELECT sum(amount) FROM order_items;
SELECT count(*) FROM customers WHERE country = 'TR';
SELECT sum(total) FROM orders WHERE status = 'paid';
SELECT max(created) FROM customers;
SELECT count(*) FROM orders WHERE customer_id = 42;
SQL

# --- image for OxiDB: the static binary and nothing else --------------------
if ! docker image inspect oxidb-pressure >/dev/null 2>&1; then
  mkdir -p "$WORK/img"
  cp "$OXI_BIN" "$WORK/img/oxidb-server"
  cat > "$WORK/img/Dockerfile" <<'EOF'
FROM alpine:3.20
COPY oxidb-server /usr/local/bin/oxidb-server
ENV OXIDB_DOC=0 OXIDB_SQL=1 OXIDB_SQL_DISK_FIRST=1
ENV OXIDB_DATA=/data OXIDB_ADDR=0.0.0.0:4444 OXIDB_PG_PORT=5432
CMD ["/usr/local/bin/oxidb-server"]
EOF
  docker build -q -t oxidb-pressure "$WORK/img" >/dev/null
fi

wait_ready() { # wait_ready <port> <db> <seconds>
  local port=$1 db=$2 n=$3
  for _ in $(seq "$n"); do
    if psql -q -h 127.0.0.1 -p "$port" -U bench -d "$db" -tAc "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# --- one-time load, unconstrained, into a docker volume ---------------------
load_oxidb() {
  docker volume rm oxi-data >/dev/null 2>&1 || true
  docker volume create oxi-data >/dev/null
  docker run -d --name oxi-pressure -v oxi-data:/data -p 15432:5432 oxidb-pressure >/dev/null
  wait_ready 15432 oxidb 60 || {
    echo "oxidb never came up; last log lines:"
    docker logs oxi-pressure 2>&1 | tail -6
    exit 1
  }
  psql -q -h 127.0.0.1 -p 15432 -U bench -d oxidb -v ON_ERROR_STOP=1 -f "$HERE/schema.sql" >/dev/null
  psql -q -h 127.0.0.1 -p 15432 -U bench -d oxidb -v ON_ERROR_STOP=1 -f "$WORK/data.sql" >/dev/null
  docker rm -f oxi-pressure >/dev/null
}

load_pg() { # load_pg <shared_buffers>
  docker volume rm pg-data >/dev/null 2>&1 || true
  docker volume create pg-data >/dev/null
  docker run -d --name pg-pressure -e POSTGRES_USER=bench -e POSTGRES_DB=bench \
    -e POSTGRES_HOST_AUTH_METHOD=trust -v pg-data:/var/lib/postgresql \
    -p 15433:5432 postgres:18-alpine -c "shared_buffers=$1" >/dev/null
  wait_ready 15433 bench 90 || {
    echo "postgres never came up; last log lines:"
    docker logs pg-pressure 2>&1 | tail -6
    exit 1
  }
  psql -q -h 127.0.0.1 -p 15433 -U bench -d bench -v ON_ERROR_STOP=1 -f "$HERE/schema.sql" >/dev/null
  psql -q -h 127.0.0.1 -p 15433 -U bench -d bench -v ON_ERROR_STOP=1 -f "$WORK/data.sql" >/dev/null
  docker rm -f pg-pressure >/dev/null
}

# --- run the workload at a memory limit and report what happened -----------
try_oxidb() { # try_oxidb <limit>
  docker rm -f oxi-pressure >/dev/null 2>&1 || true
  docker run -d --name oxi-pressure --memory="$1" --memory-swap="$1" \
    -v oxi-data:/data -p 15432:5432 oxidb-pressure >/dev/null 2>&1
  if ! wait_ready 15432 oxidb 45; then
    printf '  %-18s %-9s %s\n' "oxidb" "$1" "DID NOT START"
    docker rm -f oxi-pressure >/dev/null 2>&1 || true
    return
  fi
  local t0 t1 ok
  t0=$(python3 -c 'import time;print(time.time())')
  if psql -q -h 127.0.0.1 -p 15432 -U bench -d oxidb -v ON_ERROR_STOP=1 \
      -f "$WORK/work.sql" >/dev/null 2>&1; then ok=ok; else ok=FAILED; fi
  t1=$(python3 -c 'import time;print(time.time())')
  local dead
  dead=$(docker inspect -f '{{.State.OOMKilled}}' oxi-pressure 2>/dev/null || echo "?")
  printf '  %-18s %-9s %-8s %6.2fs   oom=%s\n' "oxidb" "$1" "$ok" \
    "$(python3 -c "print($t1-$t0)")" "$dead"
  docker rm -f oxi-pressure >/dev/null 2>&1 || true
}

try_pg() { # try_pg <limit> <shared_buffers>
  docker rm -f pg-pressure >/dev/null 2>&1 || true
  docker run -d --name pg-pressure --memory="$1" --memory-swap="$1" \
    -e POSTGRES_USER=bench -e POSTGRES_DB=bench -e POSTGRES_HOST_AUTH_METHOD=trust \
    -v pg-data:/var/lib/postgresql -p 15433:5432 \
    postgres:18-alpine -c "shared_buffers=$2" >/dev/null 2>&1
  if ! wait_ready 15433 bench 45; then
    printf '  %-18s %-9s %s\n' "postgres/sb=$2" "$1" "DID NOT START"
    docker rm -f pg-pressure >/dev/null 2>&1 || true
    return
  fi
  local t0 t1 ok
  t0=$(python3 -c 'import time;print(time.time())')
  if psql -q -h 127.0.0.1 -p 15433 -U bench -d bench -v ON_ERROR_STOP=1 \
      -f "$WORK/work.sql" >/dev/null 2>&1; then ok=ok; else ok=FAILED; fi
  t1=$(python3 -c 'import time;print(time.time())')
  local dead
  dead=$(docker inspect -f '{{.State.OOMKilled}}' pg-pressure 2>/dev/null || echo "?")
  printf '  %-18s %-9s %-8s %6.2fs   oom=%s\n' "postgres/sb=$2" "$1" "$ok" \
    "$(python3 -c "print($t1-$t0)")" "$dead"
  docker rm -f pg-pressure >/dev/null 2>&1 || true
}

echo "loading (unconstrained)..."
load_oxidb
load_pg 128MB

echo
echo "workload at each memory limit (engine, limit, result, wall time):"
for L in $LIMITS; do
  try_oxidb "$L"
  try_pg "$L" 128MB
  try_pg "$L" 32MB
  echo
done
echo "workdir: $WORK"
