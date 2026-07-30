#!/usr/bin/env bash
# What does *opening* a database cost, against what running it costs?
#
# The other scripts here measure a warm, settled engine. That is the wrong
# number for sizing a container: a database opened with an unflushed WAL tail —
# the state a bulk load or a crash leaves — replays that tail, and every
# replayed record is materialized in RAM until a fold moves it to disk. So the
# binding figure is the *peak* during open, not the steady state after it, and a
# server sized for the latter cannot restart.
#
# Measured with the cgroup's own `memory.peak`, which is the kernel's high-water
# mark for the container and needs no sampling. `memory.current` right after is
# the steady state, so the ratio between them is the margin an operator must
# leave. The engine opens a database lazily, on its first statement, so the open
# is triggered explicitly and the peak read after it.
#
# Both cases use the *default* configuration — in particular the default
# `OXIDB_SQL_CHECKPOINT_BYTES`, which is what bounds how large a tail can be in
# normal operation. Forcing a bigger tail makes a worse-looking number that no
# default-configured server would ever reach.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
WORK="${1:-$(mktemp -d)}"
OXI_BIN="$HERE/../../target-local/aarch64-unknown-linux-musl/release/oxidb-server"
mkdir -p "$WORK"

[ -f "$OXI_BIN" ] || {
  echo "missing Linux build: cargo build --release --target aarch64-unknown-linux-musl -p oxidb-server"
  exit 1
}
[ -f "$WORK/data.sql" ] || python3 "$HERE/gen.py" --batch 500 --out "$WORK/data.sql"

cleanup() { docker rm -f oxi-openpeak >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

mkdir -p "$WORK/img"; cp "$OXI_BIN" "$WORK/img/oxidb-server"
cat > "$WORK/img/Dockerfile" <<'EOF'
FROM alpine:3.20
COPY oxidb-server /usr/local/bin/oxidb-server
ENV OXIDB_DOC=0 OXIDB_SQL=1 OXIDB_SQL_DISK_FIRST=1
ENV OXIDB_DATA=/data OXIDB_ADDR=0.0.0.0:4444 OXIDB_PG_PORT=5432
CMD ["/usr/local/bin/oxidb-server"]
EOF
docker build -q -t oxidb-openpeak "$WORK/img" >/dev/null

wait_ready() {
  for _ in $(seq 60); do
    psql -q -h 127.0.0.1 -p 15450 -U bench -d oxidb -tAc "SELECT 1" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}
# The kernel's own high-water mark. `memory.peak` needs Linux 5.19+; without it
# there is no reliable peak (sampling misses it), so say so rather than report a
# number that means something else.
peak_mb() {
  docker exec oxi-openpeak sh -c 'cat /sys/fs/cgroup/memory.peak 2>/dev/null' |
    awk '{printf "%d", $1/1048576}'
}
cur_mb() {
  docker exec oxi-openpeak sh -c 'cat /sys/fs/cgroup/memory.current' |
    awk '{printf "%d", $1/1048576}'
}
start() { docker run -d --name oxi-openpeak -v oxi-openpeak:/data -p 15450:5432 oxidb-openpeak >/dev/null; }

echo "loading 1.2M rows (default settings, so the tail is whatever the engine leaves)..."
docker volume rm oxi-openpeak >/dev/null 2>&1 || true
docker volume create oxi-openpeak >/dev/null
start
wait_ready || { echo "never came up"; docker logs oxi-openpeak 2>&1 | tail -5; exit 1; }
psql -q -h 127.0.0.1 -p 15450 -U bench -d oxidb -v ON_ERROR_STOP=1 -f "$HERE/schema.sql" >/dev/null
psql -q -h 127.0.0.1 -p 15450 -U bench -d oxidb -v ON_ERROR_STOP=1 -f "$WORK/data.sql" >/dev/null
TAIL_MB=$(docker exec oxi-openpeak sh -c 'wc -c < /data/sql/wal/live.wal' | awk '{printf "%d", $1/1048576}')
# SIGKILL, not a graceful stop: the point is to leave the tail unflushed, which
# is exactly the state a crash or a killed bulk load leaves behind.
docker kill -s KILL oxi-openpeak >/dev/null
docker rm -f oxi-openpeak >/dev/null

printf '\nWAL tail left behind: %s MB\n\n' "$TAIL_MB"

echo "=== opening that (replays the tail) ==="
start
if [ -z "$(peak_mb)" ]; then
  echo "  memory.peak unavailable (needs cgroup v2 on Linux 5.19+); no peak to report"
  exit 1
fi
wait_ready || { echo "did not come up"; exit 1; }
printf '  peak %s MB   settled %s MB\n' "$(peak_mb)" "$(cur_mb)"

echo
echo "=== reopening it clean (tail folded by the open above) ==="
docker rm -f oxi-openpeak >/dev/null
start
wait_ready || { echo "did not come up"; exit 1; }
printf '  peak %s MB   settled %s MB\n' "$(peak_mb)" "$(cur_mb)"
echo
echo "The first peak divided by the second settled figure is the margin a server"
echo "must be sized for to survive a restart after a bulk load."
echo "workdir: $WORK"
