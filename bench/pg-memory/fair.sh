#!/usr/bin/env bash
# Total physical memory each engine costs the *machine*, not just the process.
#
# `phys_footprint` — what the main benchmark reports — deliberately excludes
# clean file-backed pages, because the OS may evict them. That is a defensible
# metric, but it is not a fair comparison between an engine that mmaps its data
# and one that copies it into anonymous shared memory: OxiDB's `.rdat`/`.sidx`
# pages are file-backed and go uncounted, while PostgreSQL's `shared_buffers`
# are anonymous and count in full. Both engines *also* leave pages in the OS
# page cache that no process is charged for.
#
# So this measures the machine: system-wide anonymous and file-backed page
# counts from `vm_stat`, before and after warming each engine from cold. The
# deltas include page cache, whoever caused it.
#
# Caveats, stated because they bound what the numbers mean:
#   - The page cache is not purged between runs (that needs root), so each
#     engine is measured in its own process from a cold start and the deltas
#     are compared, not the absolutes.
#   - Other activity on the machine is noise. Run it quiet; the deltas here are
#     tens to hundreds of MB, well above the noise floor, but do not read
#     single-digit MB differences as real.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
WORK="${1:-$(mktemp -d)}"
PG_PORT=${PG_PORT:-5560}
OXI_PG_PORT=${OXI_PG_PORT:-5561}
OXIDB_BIN=${OXIDB_BIN:-$HERE/../../target-local/release/oxidb-server}
mkdir -p "$WORK"

# vm_stat pages -> MB (16 KiB pages on Apple silicon, 4 KiB on Intel).
PAGE=$(sysctl -n hw.pagesize)
stat_mb() { # stat_mb <field>
  vm_stat | awk -v f="$1" -v p="$PAGE" '$0 ~ f {gsub(/\./,"",$NF); printf "%d", $NF * p / 1048576}'
}
snapshot() { echo "$(stat_mb 'Anonymous pages') $(stat_mb 'File-backed pages')"; }

report() { # report <label> <before> <after> <proc-footprint-MB>
  local label=$1 a0 f0 a1 f1
  read -r a0 f0 <<<"$2"
  read -r a1 f1 <<<"$3"
  printf '%-26s anon %+6d MB   file %+6d MB   TOTAL %+6d MB   (process footprint %s MB)\n' \
    "$label" "$((a1 - a0))" "$((f1 - f0))" "$((a1 - a0 + f1 - f0))" "$4"
}

# phys_footprint in MB. The tool prints KB/MB/GB depending on size, so the unit
# has to be read — summing the bare number reported PostgreSQL at 28 GB once.
foot() {
  footprint -p "$1" 2>/dev/null | awk '/phys_footprint:/ {v=$2; u=$3}
    END { if (u=="GB") printf "%d", v*1024; else if (u=="KB") printf "%d", v/1024; else printf "%d", v }'
}
sql() { psql -q -h 127.0.0.1 -p "$1" -U bench -d "$2" -v ON_ERROR_STOP=1 "${@:3}"; }
warm() { sql "$1" "$2" -tAc "SELECT count(*) FROM customers;
                              SELECT count(*) FROM products;
                              SELECT count(*) FROM orders;
                              SELECT count(*) FROM order_items;
                              SELECT count(*) FROM inventory;
                              SELECT count(*) FROM orders WHERE customer_id = 42;
                              SELECT count(*) FROM customers WHERE country = 'TR';
                              SELECT count(*) FROM inventory WHERE warehouse = 'ist'" >/dev/null; }

[ -f "$WORK/data.sql" ] || python3 "$HERE/gen.py" --batch 500 --out "$WORK/data.sql"

# --- PostgreSQL: load, stop, then measure a cold start + warm ---------------
if [ ! -d "$WORK/pgdata" ]; then
  initdb -D "$WORK/pgdata" -U bench --auth=trust -E UTF8 >/dev/null 2>&1
  pg_ctl -D "$WORK/pgdata" -o "-p $PG_PORT" -l "$WORK/pg.log" start >/dev/null
  sleep 3
  sql "$PG_PORT" postgres -c "CREATE DATABASE bench" >/dev/null
  sql "$PG_PORT" bench -f "$HERE/schema.sql" >/dev/null
  sql "$PG_PORT" bench -f "$WORK/data.sql" >/dev/null
  pg_ctl -D "$WORK/pgdata" -m fast stop >/dev/null
fi
sleep 5
BEFORE=$(snapshot)
pg_ctl -D "$WORK/pgdata" -o "-p $PG_PORT" -l "$WORK/pg.log" start >/dev/null
sleep 3
warm "$PG_PORT" bench
sleep 3
PGPID=$(head -1 "$WORK/pgdata/postmaster.pid")
PGFOOT=0
for p in $PGPID $(pgrep -P "$PGPID"); do PGFOOT=$((PGFOOT + $(foot "$p"))); done
report "postgres (cold -> warm)" "$BEFORE" "$(snapshot)" "$PGFOOT"
pg_ctl -D "$WORK/pgdata" -m fast stop >/dev/null

# --- OxiDB disk-first: same shape ------------------------------------------
if [ ! -d "$WORK/oxi" ]; then
  OXIDB_DOC=0 OXIDB_SQL=1 OXIDB_SQL_DISK_FIRST=1 OXIDB_DATA="$WORK/oxi" \
    OXIDB_ADDR=127.0.0.1:4560 OXIDB_PG_PORT=$OXI_PG_PORT "$OXIDB_BIN" >/dev/null 2>&1 &
  pid=$!
  sleep 4
  sql "$OXI_PG_PORT" oxidb -f "$HERE/schema.sql" >/dev/null
  sql "$OXI_PG_PORT" oxidb -f "$WORK/data.sql" >/dev/null
  kill "$pid"; wait "$pid" 2>/dev/null || true
fi
sleep 5
BEFORE=$(snapshot)
OXIDB_DOC=0 OXIDB_SQL=1 OXIDB_SQL_DISK_FIRST=1 OXIDB_DATA="$WORK/oxi" \
  OXIDB_ADDR=127.0.0.1:4560 OXIDB_PG_PORT=$OXI_PG_PORT "$OXIDB_BIN" >/dev/null 2>&1 &
pid=$!
sleep 12
warm "$OXI_PG_PORT" oxidb
sleep 3
report "oxidb disk-first (cold -> warm)" "$BEFORE" "$(snapshot)" "$(foot "$pid")"
echo
echo "oxidb mapped-file pages actually resident:"
vmmap -summary "$pid" 2>/dev/null | awk '/^mapped file/ {print "  " $0}'
kill "$pid" 2>/dev/null || true
echo
echo "workdir: $WORK"
