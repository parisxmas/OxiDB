#!/usr/bin/env bash
# How does each engine's memory grow with the number of rows?
#
# The other scripts here compare the two engines at one size, which cannot answer
# the question that decides whether a database fits on a machine at all: what
# does row number N cost? A fixed difference is a tuning matter; a difference in
# *slope* is a design one.
#
# The metric is the cgroup's own breakdown, not a total, because the total mixes
# two things that behave oppositely under pressure. `file` is page cache: the
# kernel drops it and the engine reads from disk again, slower. `anon` (plus
# `shmem`, which is where a shared buffer pool lands) cannot be dropped — it is
# what an OOM kill is decided on. So the question is specifically whether the
# non-evictable part grows with the data.
#
# Both engines get the same schema at each scale, loaded from the same file
# through the same client over the PostgreSQL wire, then the same read workload
# so nothing is measured cold.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
WORK="${1:-$(mktemp -d)}"
SCALES=${SCALES:-"1 8"}
PG_SHARED_BUFFERS=${PG_SHARED_BUFFERS:-128MB}
OXI_BIN="$HERE/../../target-local/aarch64-unknown-linux-musl/release/oxidb-server"
mkdir -p "$WORK"

[ -f "$OXI_BIN" ] || {
  echo "missing Linux build: cargo build --release --target aarch64-unknown-linux-musl -p oxidb-server"
  exit 1
}

cleanup() { docker rm -f oxi-scale pg-scale >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

cat > "$WORK/work.sql" <<'SQL'
-- Reads that touch every table and three indexes. Aggregates over real columns,
-- never COUNT(*): both engines answer that from metadata without reading a row,
-- so it would fault nothing and warm nothing.
SELECT sum(total) FROM orders;
SELECT sum(price) FROM products;
SELECT sum(on_hand) FROM inventory;
SELECT sum(amount) FROM order_items;
SELECT count(*) FROM customers WHERE country = 'TR';
SELECT sum(total) FROM orders WHERE status = 'paid';
SELECT count(*) FROM orders WHERE customer_id = 42;
SQL

mkdir -p "$WORK/img"; cp "$OXI_BIN" "$WORK/img/oxidb-server"
cat > "$WORK/img/Dockerfile" <<'EOF'
FROM alpine:3.20
COPY oxidb-server /usr/local/bin/oxidb-server
ENV OXIDB_DOC=0 OXIDB_SQL=1 OXIDB_SQL_DISK_FIRST=1
ENV OXIDB_DATA=/data OXIDB_ADDR=0.0.0.0:4444 OXIDB_PG_PORT=5432
CMD ["/usr/local/bin/oxidb-server"]
EOF
docker build -q -t oxidb-scale "$WORK/img" >/dev/null

wait_ready() { # <port> <db>
  for _ in $(seq 90); do
    psql -q -h 127.0.0.1 -p "$1" -U bench -d "$2" -tAc "SELECT 1" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

# anon + shmem = what cannot be reclaimed under pressure; file = what can.
breakdown() { # <container>
  docker exec "$1" sh -c 'cat /sys/fs/cgroup/memory.stat; echo ---; cat /sys/fs/cgroup/memory.current' |
    awk '
      /^anon /  {a=$2}
      /^file /  {f=$2}
      /^shmem / {s=$2}
      /^[0-9]+$/ {t=$1}
      END {printf "%d %d %d %d", (a+s)/1048576, f/1048576, a/1048576, t/1048576}'
}

printf '\n%-10s %-9s %12s %12s %12s %12s\n' \
  "engine" "rows" "NON-EVICT" "page cache" "(anon only)" "total"

for SC in $SCALES; do
  [ -f "$WORK/data.$SC.sql" ] || python3 "$HERE/gen.py" --scale "$SC" --batch 500 --out "$WORK/data.$SC.sql"
  ROWS=$(python3 -c "print(f'{int(1_200_000*$SC/1000)}k')")

  # --- OxiDB: load, restart (so the measurement is a settled engine), warm ----
  docker volume rm oxi-scale >/dev/null 2>&1 || true
  docker volume create oxi-scale >/dev/null
  docker run -d --name oxi-scale -v oxi-scale:/data -p 15470:5432 oxidb-scale >/dev/null
  wait_ready 15470 oxidb || { echo "oxidb never came up"; exit 1; }
  psql -q -h 127.0.0.1 -p 15470 -U bench -d oxidb -v ON_ERROR_STOP=1 -f "$HERE/schema.sql" >/dev/null
  psql -q -h 127.0.0.1 -p 15470 -U bench -d oxidb -v ON_ERROR_STOP=1 -f "$WORK/data.$SC.sql" >/dev/null
  docker rm -f oxi-scale >/dev/null
  docker run -d --name oxi-scale -v oxi-scale:/data -p 15470:5432 oxidb-scale >/dev/null
  wait_ready 15470 oxidb || { echo "oxidb did not restart"; exit 1; }
  psql -q -h 127.0.0.1 -p 15470 -U bench -d oxidb -v ON_ERROR_STOP=1 -f "$WORK/work.sql" >/dev/null
  printf '%-10s %-9s %12s %12s %12s %12s\n' "oxidb" "$ROWS" $(breakdown oxi-scale)
  docker rm -f oxi-scale >/dev/null

  # --- PostgreSQL: same, and restarted too, for the same reason --------------
  docker volume rm pg-scale >/dev/null 2>&1 || true
  docker volume create pg-scale >/dev/null
  docker run -d --name pg-scale -e POSTGRES_USER=bench -e POSTGRES_DB=bench \
    -e POSTGRES_HOST_AUTH_METHOD=trust -v pg-scale:/var/lib/postgresql \
    -p 15471:5432 postgres:18-alpine -c "shared_buffers=$PG_SHARED_BUFFERS" >/dev/null
  wait_ready 15471 bench || { echo "postgres never came up"; exit 1; }
  psql -q -h 127.0.0.1 -p 15471 -U bench -d bench -v ON_ERROR_STOP=1 -f "$HERE/schema.sql" >/dev/null
  psql -q -h 127.0.0.1 -p 15471 -U bench -d bench -v ON_ERROR_STOP=1 -f "$WORK/data.$SC.sql" >/dev/null
  psql -q -h 127.0.0.1 -p 15471 -U bench -d bench -tAc "VACUUM ANALYZE" >/dev/null
  docker restart pg-scale >/dev/null
  wait_ready 15471 bench || { echo "postgres did not restart"; exit 1; }
  psql -q -h 127.0.0.1 -p 15471 -U bench -d bench -v ON_ERROR_STOP=1 -f "$WORK/work.sql" >/dev/null
  printf '%-10s %-9s %12s %12s %12s %12s\n' "postgres" "$ROWS" $(breakdown pg-scale)
  docker rm -f pg-scale >/dev/null
  echo
done

echo "NON-EVICT is anon+shmem in MB: what pressure cannot reclaim, so what an OOM"
echo "kill is decided on. Compare its growth between scales, not its absolute value."
echo "workdir: $WORK"
