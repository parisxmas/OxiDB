#!/usr/bin/env bash
# Many small tenants: one OxiDB process against one PostgreSQL per tenant.
#
# The other benchmarks here compare one large database against one PostgreSQL,
# which is the wrong shape for how OxiDB is actually deployed. OxiBase is a
# multi-tenant control plane: a project is a database, and every project on a
# host shares one engine process. Supabase's model is the opposite — a project
# is a Postgres *instance*, so its fixed costs (shared_buffers, a postmaster
# and eight background processes, per-backend memory) are paid once per tenant
# rather than once per host.
#
# That difference does not show up in a single-database comparison at all. This
# measures the slope: what does tenant number N cost each side?
#
# Both sides get the same per-tenant schema and row counts, loaded through the
# same client over the PostgreSQL wire. Memory is the cgroup's own accounting
# (`memory.current`), which charges page cache to the container that faulted it,
# summed over every container an engine needs.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
WORK="${1:-$(mktemp -d)}"
TENANTS=${TENANTS:-10}
ROWS=${ROWS:-20000}          # orders per tenant; other tables scale from it
PG_SHARED_BUFFERS=${PG_SHARED_BUFFERS:-32MB}   # tuned down, as the fair case
OXI_BIN="$HERE/../../target-local/aarch64-unknown-linux-musl/release/oxidb-server"
mkdir -p "$WORK"

[ -f "$OXI_BIN" ] || {
  echo "missing Linux build: cargo build --release --target aarch64-unknown-linux-musl -p oxidb-server"
  exit 1
}

cleanup() {
  docker rm -f oxi-tenants >/dev/null 2>&1 || true
  for i in $(seq 0 $((TENANTS - 1))); do
    docker rm -f "pg-tenant-$i" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT
cleanup

# --- per-tenant dataset -----------------------------------------------------
# A small SaaS tenant: customers, orders with a foreign key, order lines under a
# composite key, and the indexes such an app actually queries by.
cat > "$WORK/schema.sql" <<'SQL'
CREATE TABLE customers (
  id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT NOT NULL, country TEXT NOT NULL
);
CREATE INDEX idx_customers_country ON customers (country);
CREATE TABLE orders (
  id INT PRIMARY KEY, customer_id INT NOT NULL REFERENCES customers (id),
  status TEXT NOT NULL, total DOUBLE PRECISION NOT NULL
);
CREATE INDEX idx_orders_customer ON orders (customer_id);
CREATE INDEX idx_orders_status ON orders (status);
CREATE TABLE order_items (
  order_id INT NOT NULL REFERENCES orders (id), line_no INT NOT NULL,
  product INT NOT NULL, amount DOUBLE PRECISION NOT NULL,
  CONSTRAINT pk_items PRIMARY KEY (order_id, line_no)
);
SQL

python3 - "$ROWS" > "$WORK/data.sql" <<'PY'
import sys, random
orders = int(sys.argv[1]); customers = orders // 4; items = orders * 2
rng = random.Random(20260729)
COUNTRIES = ["TR","US","DE","FR","GB"]; STATUS = ["pending","paid","shipped"]
def emit(table, cols, rows, batch=500):
    buf = []
    for r in rows:
        buf.append(r)
        if len(buf) == batch:
            print(f"INSERT INTO {table} ({cols}) VALUES " + ",".join(buf) + ";"); buf = []
    if buf: print(f"INSERT INTO {table} ({cols}) VALUES " + ",".join(buf) + ";")
emit("customers", "id, email, name, country",
     (f"({i},'u{i}@t.example','Customer {i}','{rng.choice(COUNTRIES)}')" for i in range(1, customers+1)))
emit("orders", "id, customer_id, status, total",
     (f"({i},{rng.randint(1,customers)},'{rng.choice(STATUS)}',{round(rng.uniform(5,900),2)})" for i in range(1, orders+1)))
emit("order_items", "order_id, line_no, product, amount",
     (f"({o},{l},{rng.randint(1,500)},{round(rng.uniform(1,300),2)})"
      for o in range(1, items//2+1) for l in (1,2)))
PY

# The workload every tenant is asked for, so nothing is cold when measured.
cat > "$WORK/work.sql" <<'SQL'
SELECT sum(total) FROM orders;
SELECT sum(amount) FROM order_items;
SELECT count(*) FROM customers WHERE country = 'TR';
SELECT count(*) FROM orders WHERE customer_id = 7;
SELECT count(*) FROM orders WHERE status = 'paid';
SQL

wait_ready() { # wait_ready <port> <db> <tries>
  for _ in $(seq "$3"); do
    psql -q -h 127.0.0.1 -p "$1" -U bench -d "$2" -tAc "SELECT 1" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}
mem_mb() { docker exec "$1" sh -c 'cat /sys/fs/cgroup/memory.current' 2>/dev/null | awk '{printf "%d", $1/1048576}'; }

# --- image ------------------------------------------------------------------
mkdir -p "$WORK/img"; cp "$OXI_BIN" "$WORK/img/oxidb-server"
cat > "$WORK/img/Dockerfile" <<'EOF'
FROM alpine:3.20
COPY oxidb-server /usr/local/bin/oxidb-server
# The document engine stays ON here, unlike the single-database benchmarks.
# That is how OxiBase runs — and it is currently required: provisioning a
# tenant database goes through the document manager, which with OXIDB_DOC=0 is
# in-memory and so never creates the on-disk directory the SQL registry looks
# for. OXIDB_DOC=0 therefore implies a single database today. Keeping documents
# on also makes this the honest configuration to compare: it is what a tenant
# host actually runs.
ENV OXIDB_SQL=1 OXIDB_SQL_DISK_FIRST=1
ENV OXIDB_DATA=/data OXIDB_ADDR=0.0.0.0:4444 OXIDB_PG_PORT=5432
CMD ["/usr/local/bin/oxidb-server"]
EOF
docker build -q -t oxidb-tenants "$WORK/img" >/dev/null

# --- OxiDB: one process, one database per tenant ----------------------------
echo "=== OxiDB: $TENANTS tenants in ONE process ==="
docker volume rm oxi-tenants >/dev/null 2>&1 || true
docker volume create oxi-tenants >/dev/null
docker run -d --name oxi-tenants -v oxi-tenants:/data -p 15440:5432 -p 15441:4444 oxidb-tenants >/dev/null
wait_ready 15440 oxidb 60 || { echo "oxidb never came up"; exit 1; }
for i in $(seq 0 $((TENANTS - 1))); do
  # Provisioning a tenant database is a control-plane action, so it goes over
  # OxiWire — the PostgreSQL listener serves an existing database and does not
  # implement CREATE DATABASE. This is what OxiBase itself does.
  python3 "$HERE/mkdb.py" 15441 "tenant$i"
  psql -q -h 127.0.0.1 -p 15440 -U bench -d "tenant$i" -v ON_ERROR_STOP=1 -f "$WORK/schema.sql" >/dev/null
  psql -q -h 127.0.0.1 -p 15440 -U bench -d "tenant$i" -v ON_ERROR_STOP=1 -f "$WORK/data.sql" >/dev/null
  printf '  tenant %-3s loaded   process total: %s MB\n' "$i" "$(mem_mb oxi-tenants)"
done
# Restart, then warm every tenant: the steady state an operator actually runs.
docker rm -f oxi-tenants >/dev/null
docker run -d --name oxi-tenants -v oxi-tenants:/data -p 15440:5432 -p 15441:4444 oxidb-tenants >/dev/null
wait_ready 15440 oxidb 60 || { echo "oxidb did not restart"; exit 1; }
for i in $(seq 0 $((TENANTS - 1))); do
  psql -q -h 127.0.0.1 -p 15440 -U bench -d "tenant$i" -v ON_ERROR_STOP=1 -f "$WORK/work.sql" >/dev/null
done
OXI_TOTAL=$(mem_mb oxi-tenants)
echo "  after restart, all $TENANTS warmed:  ${OXI_TOTAL} MB total"
docker rm -f oxi-tenants >/dev/null

# --- PostgreSQL: one instance per tenant, as a project-per-instance host ----
echo
echo "=== PostgreSQL: $TENANTS tenants, ONE INSTANCE EACH (shared_buffers=$PG_SHARED_BUFFERS) ==="
PG_TOTAL=0
for i in $(seq 0 $((TENANTS - 1))); do
  port=$((15500 + i))
  docker volume rm "pg-tenant-$i" >/dev/null 2>&1 || true
  docker volume create "pg-tenant-$i" >/dev/null
  docker run -d --name "pg-tenant-$i" -e POSTGRES_USER=bench -e POSTGRES_DB=bench \
    -e POSTGRES_HOST_AUTH_METHOD=trust -v "pg-tenant-$i":/var/lib/postgresql \
    -p "$port":5432 postgres:18-alpine -c "shared_buffers=$PG_SHARED_BUFFERS" >/dev/null
  wait_ready "$port" bench 90 || { echo "pg-tenant-$i never came up"; exit 1; }
  psql -q -h 127.0.0.1 -p "$port" -U bench -d bench -v ON_ERROR_STOP=1 -f "$WORK/schema.sql" >/dev/null
  psql -q -h 127.0.0.1 -p "$port" -U bench -d bench -v ON_ERROR_STOP=1 -f "$WORK/data.sql" >/dev/null
  psql -q -h 127.0.0.1 -p "$port" -U bench -d bench -v ON_ERROR_STOP=1 -f "$WORK/work.sql" >/dev/null
  one=$(mem_mb "pg-tenant-$i"); PG_TOTAL=$((PG_TOTAL + one))
  printf '  tenant %-3s loaded   this instance: %-4s MB   fleet total: %s MB\n' "$i" "$one" "$PG_TOTAL"
done

echo
printf 'OxiDB   %s tenants, 1 process:   %s MB   (%s MB per tenant)\n' \
  "$TENANTS" "$OXI_TOTAL" "$((OXI_TOTAL / TENANTS))"
printf 'Postgres %s tenants, %s instances: %s MB   (%s MB per tenant)\n' \
  "$TENANTS" "$TENANTS" "$PG_TOTAL" "$((PG_TOTAL / TENANTS))"
echo "workdir: $WORK"
