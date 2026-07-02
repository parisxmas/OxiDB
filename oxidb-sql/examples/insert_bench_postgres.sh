#!/bin/bash
# PostgreSQL side of the oxidb-sql insert benchmark (see insert_bench.rs).
# Generates the exact same INSERT statements (same batching, same values) and
# times them with psql \timing against a local PostgreSQL.
#
# Repro:
#   createdb oxidb_sql_bench
#   ./insert_bench_postgres.sh              # scale 1 (100k bulk + 2k single)
#   SCALE=5 ./insert_bench_postgres.sh
set -euo pipefail

DB="${DB:-oxidb_sql_bench}"
SCALE="${SCALE:-1}"
BATCH_ROWS=1000
BATCHES=$((100 * SCALE))
SINGLES=2000
BULK_ROWS=$((BATCHES * BATCH_ROWS))
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

gen() { # gen <table> <first> <count> <rows_per_stmt>
python3 - "$@" <<'PY'
import sys
table, first, count, per = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
out = sys.stdout
for s in range(0, count, per):
    out.write(f"INSERT INTO {table} VALUES ")
    out.write(",".join(
        f"({i+1}, {(i%1000)+1}, 'k{i%8}', {1700000000000 + i*1000}, {(i*7)%10000})"
        for i in range(first + s, first + min(s + per, count))
    ))
    out.write(";\n")
PY
}

psql -q -d "$DB" <<'SQL'
DROP TABLE IF EXISTS events, events_bare;
CREATE TABLE events (id BIGINT PRIMARY KEY, user_id INT NOT NULL,
  kind TEXT NOT NULL, ts BIGINT NOT NULL, amount INT NOT NULL);
CREATE INDEX ev_user ON events (user_id);
CREATE INDEX ev_kind ON events (kind);
CREATE INDEX ev_amount ON events (amount);
CREATE INDEX ev_user_kind ON events (user_id, kind);
CREATE TABLE events_bare (id BIGINT, user_id INT, kind TEXT, ts BIGINT, amount INT);
SQL

run_timed() { # run_timed <sqlfile> -> total ms across statements
  { echo '\timing on'; cat "$1"; } | psql -q -d "$DB" 2>/dev/null \
    | awk '/^Time:/ {t += $2} END {printf "%.2f", t}'
}

echo "postgres insert benchmark — PK + 4 secondary indexes, ${BATCHES} batches x ${BATCH_ROWS} rows + ${SINGLES} single inserts"
echo

gen events 0 "$BULK_ROWS" "$BATCH_ROWS" > "$WORK/bulk.sql"
MS=$(run_timed "$WORK/bulk.sql")
awk -v r="$BULK_ROWS" -v ms="$MS" 'BEGIN{printf "bulk indexed    %8d rows  %8.2f ms  %9.0f rows/s\n", r, ms, r/(ms/1000)}'

gen events "$BULK_ROWS" "$SINGLES" 1 > "$WORK/single.sql"
MS=$(run_timed "$WORK/single.sql")
awk -v r="$SINGLES" -v ms="$MS" 'BEGIN{printf "single indexed  %8d rows  %8.2f ms  %9.0f rows/s  (%.3f ms/insert)\n", r, ms, r/(ms/1000), ms/r}'

gen events_bare 0 "$BULK_ROWS" "$BATCH_ROWS" > "$WORK/bare.sql"
MS=$(run_timed "$WORK/bare.sql")
awk -v r="$BULK_ROWS" -v ms="$MS" 'BEGIN{printf "bulk bare       %8d rows  %8.2f ms  %9.0f rows/s\n", r, ms, r/(ms/1000)}'

echo
for q in \
  "SELECT COUNT(*) AS n, SUM(amount) AS total FROM events" \
  "SELECT COUNT(*) AS n FROM events WHERE user_id = 42" \
  "SELECT COUNT(*) AS n FROM events WHERE user_id = 42 AND kind = 'k1'" \
  "SELECT COUNT(*) AS n FROM events WHERE amount = 7777"; do
  echo "parity  $q  -> $(psql -tA -d "$DB" -c "$q" | tr '|' '|')"
done
