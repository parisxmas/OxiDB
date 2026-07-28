#!/usr/bin/env bash
# OxiDB vs PostgreSQL — resident memory over the same 1,000,000-row database.
#
# Both engines get the same schema (bench/pg-memory/schema.sql) and the same
# rows (gen.py, fixed seed), loaded through the same client (psql) over the
# same protocol (the PostgreSQL v3 wire) — OxiDB speaks it on OXIDB_PG_PORT,
# so nothing about the load path differs between them.
#
# Three points are measured, each after a settle:
#   boot    a freshly started server with an empty database
#   loaded  the same process after ingesting 1M rows
#   reboot  a restart against the 1M rows already on disk, before any query
#   warm    the same process after every table has been read
#   +index  after every index has been used at least once        <- both matter
#
# The reboot row is measured on purpose *and* is the one not to quote alone:
# OxiDB opens its SQL engine lazily, so before the first statement it has not
# read the database at all. See docs/pg-memory-benchmark.md.
#
# Usage: ./run.sh [workdir]     (default: a temp dir; kept for inspection)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
WORK="${1:-$(mktemp -d)}"
PG_PORT=${PG_PORT:-5480}
OXI_PG_PORT=${OXI_PG_PORT:-5481}
OXI_ADDR=${OXI_ADDR:-127.0.0.1:4480}
OXIDB_BIN=${OXIDB_BIN:-$HERE/../../target-local/release/oxidb-server}

mkdir -p "$WORK"
echo "workdir: $WORK"

# ---------------------------------------------------------------------------
# Memory accounting
#
# `ps rss` counts a shared page once per process that maps it, so summing it
# over PostgreSQL's 9-process family counts shared_buffers nine times. macOS's
# phys_footprint (what Activity Monitor calls Memory) is the honest number: it
# charges each physical page to one owner. Both are reported — the RSS sum is
# what a naive comparison would print, and the gap between them is the reason
# not to use it.
# ---------------------------------------------------------------------------
measure() { # measure <label> <root-pid>
  local label=$1 root=$2 pids rss=0 foot=0 n=0
  pids=$(echo "$root $(pgrep -P "$root" 2>/dev/null || true)")
  for p in $pids; do
    ps -p "$p" >/dev/null 2>&1 || continue
    n=$((n + 1))
    rss=$((rss + $(ps -o rss= -p "$p" | tr -d ' ')))
    local f
    f=$(footprint -p "$p" 2>/dev/null | awk '/phys_footprint:/ {print $2, $3}' | tail -1)
    case "$f" in
      *GB) foot=$((foot + $(echo "$f" | awk '{printf "%d", $1 * 1024}'))) ;;
      *MB) foot=$((foot + $(echo "$f" | awk '{printf "%d", $1}'))) ;;
      *KB) foot=$((foot + $(echo "$f" | awk '{printf "%d", $1 / 1024}'))) ;;
    esac
  done
  printf '%-22s procs=%-3s rss_sum=%6s MB   phys_footprint=%5s MB\n' \
    "$label" "$n" "$((rss / 1024))" "$foot"
}

sql() { psql -q -h 127.0.0.1 -p "$1" -U bench -d "$2" -v ON_ERROR_STOP=1 "${@:3}"; }

# Touch every table, so "warm" means the same thing on both sides. Without this
# the comparison is unfair in whichever direction the engine happens to be lazy:
# PostgreSQL fills shared_buffers only with pages it reads, and OxiDB does not
# open the database at all until the first statement arrives.
warm() { sql "$1" "$2" -tAc "SELECT count(*) FROM customers;
                              SELECT count(*) FROM products;
                              SELECT count(*) FROM orders;
                              SELECT count(*) FROM order_items;
                              SELECT count(*) FROM inventory" >/dev/null; }

# Every index exercised at least once. Measured separately from `warm` because
# the two answer different questions, and quoting only the first would flatter
# OxiDB: it builds an index when a query wants it, so a workload that scans and
# never seeks pays for none of them. PostgreSQL reads index pages on demand too,
# but its cache is capped, so this step barely moves it and moves OxiDB a lot.
seek() { sql "$1" "$2" -tAc "SELECT count(*) FROM customers WHERE country = 'TR';
    SELECT count(*) FROM products WHERE category = 'tools';
    SELECT count(*) FROM orders WHERE customer_id = 42;
    SELECT count(*) FROM order_items WHERE product = 7;
    SELECT count(*) FROM inventory WHERE warehouse = 'ist';
    SELECT count(*) FROM customers WHERE country='TR' AND created = TIMESTAMP '2024-01-01 00:00:07';
    SELECT count(*) FROM products WHERE category='tools' AND price = 1.5;
    SELECT count(*) FROM orders WHERE status='paid' AND created = TIMESTAMP '2024-01-01 00:00:03'" >/dev/null; }

# ---------------------------------------------------------------------------
# Dataset (generated once, used by both)
# ---------------------------------------------------------------------------
if [ ! -f "$WORK/data.sql" ]; then
  echo "generating 1,000,000 rows..."
  python3 "$HERE/gen.py" --batch 500 --out "$WORK/data.sql"
fi

# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------
echo
echo "=== PostgreSQL $(psql --version | awk '{print $3}') (stock config: shared_buffers=128MB) ==="
rm -rf "$WORK/pgdata"
initdb -D "$WORK/pgdata" -U bench --auth=trust -E UTF8 >/dev/null
pg_ctl -D "$WORK/pgdata" -o "-p $PG_PORT" -l "$WORK/pg.log" start >/dev/null
sleep 3
sql "$PG_PORT" postgres -c "CREATE DATABASE bench" >/dev/null
PGPID=$(head -1 "$WORK/pgdata/postmaster.pid")
measure "postgres boot" "$PGPID"

sql "$PG_PORT" bench -f "$HERE/schema.sql" >/dev/null
echo "loading..."
time sql "$PG_PORT" bench -f "$WORK/data.sql" >/dev/null
sleep 3
measure "postgres loaded" "$PGPID"

pg_ctl -D "$WORK/pgdata" -m fast stop >/dev/null
pg_ctl -D "$WORK/pgdata" -o "-p $PG_PORT" -l "$WORK/pg.log" start >/dev/null
sleep 3
PGPID=$(head -1 "$WORK/pgdata/postmaster.pid")
measure "postgres reboot" "$PGPID"
warm "$PG_PORT" bench
sleep 2
measure "postgres warm" "$PGPID"
seek "$PG_PORT" bench
sleep 2
measure "postgres +indexes" "$PGPID"
pg_ctl -D "$WORK/pgdata" -m fast stop >/dev/null

# ---------------------------------------------------------------------------
# OxiDB — SQL engine only (OXIDB_DOC=0), in both storage modes
# ---------------------------------------------------------------------------
run_oxidb() { # run_oxidb <label> <disk_first 0|1>
  local label=$1 disk=$2 dir="$WORK/oxidb-$2"
  echo
  echo "=== OxiDB $label ==="
  rm -rf "$dir"
  OXIDB_DOC=0 OXIDB_SQL=1 OXIDB_SQL_DISK_FIRST=$disk \
    OXIDB_DATA="$dir" OXIDB_ADDR="$OXI_ADDR" OXIDB_PG_PORT=$OXI_PG_PORT \
    "$OXIDB_BIN" >"$WORK/oxidb-$2.log" 2>&1 &
  local pid=$!
  sleep 4
  measure "oxidb boot" "$pid"

  sql "$OXI_PG_PORT" oxidb -f "$HERE/schema.sql" >/dev/null
  echo "loading..."
  time sql "$OXI_PG_PORT" oxidb -f "$WORK/data.sql" >/dev/null
  sleep 3
  measure "oxidb loaded" "$pid"

  kill "$pid"; wait "$pid" 2>/dev/null || true; sleep 2
  OXIDB_DOC=0 OXIDB_SQL=1 OXIDB_SQL_DISK_FIRST=$disk \
    OXIDB_DATA="$dir" OXIDB_ADDR="$OXI_ADDR" OXIDB_PG_PORT=$OXI_PG_PORT \
    "$OXIDB_BIN" >>"$WORK/oxidb-$2.log" 2>&1 &
  pid=$!
  sleep 6
  measure "oxidb reboot" "$pid"
  warm "$OXI_PG_PORT" oxidb
  sleep 2
  measure "oxidb warm" "$pid"
  seek "$OXI_PG_PORT" oxidb
  sleep 2
  measure "oxidb +indexes" "$pid"
  echo "on-disk: $(du -sh "$dir" | awk '{print $1}')"
  kill "$pid"; wait "$pid" 2>/dev/null || true; sleep 1
}

run_oxidb "resident (default)" 0
run_oxidb "disk-first (OXIDB_SQL_DISK_FIRST=1)" 1

echo
echo "on-disk postgres: $(du -sh "$WORK/pgdata" | awk '{print $1}')"
echo "workdir kept at: $WORK"
