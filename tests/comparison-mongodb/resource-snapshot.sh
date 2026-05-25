#!/usr/bin/env bash
# Post-bench resource snapshot.
#
# Captures container memory (via `docker stats`) and on-disk data-dir
# size (via `docker exec ... du`) for both OxiDB and MongoDB *before*
# `docker compose down` tears them down. Without this hook the
# TestResourceUsage Go test only fires in host-mode runs (it shells out
# to `docker` which is missing from the in-network runner container);
# in-network runs would otherwise have no memory/disk numbers.

set -u

OXI_CTR="comparison-mongodb-oxidb-1"
MONGO_CTR="comparison-mongodb-mongodb-1"

# Don't fail the run.sh if the snapshot tools aren't where we expect.
if ! command -v docker >/dev/null 2>&1; then
    echo "(skipping resource snapshot — docker CLI not on PATH)"
    exit 0
fi

echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "  Resource snapshot (post-bench, before teardown)"
echo "════════════════════════════════════════════════════════════════════"
echo "Memory (docker stats):"
docker stats --no-stream --format "  {{.Name}}  {{.MemUsage}}" "$OXI_CTR" "$MONGO_CTR" 2>/dev/null \
    || echo "  (containers not available)"
echo ""
echo "Disk (data dir):"
docker exec "$OXI_CTR" du -sh /data 2>/dev/null \
    | awk '{printf "  oxidb     %s\n", $1}' \
    || echo "  oxidb     (n/a)"
docker exec "$MONGO_CTR" du -sh /data/db 2>/dev/null \
    | awk '{printf "  mongodb   %s\n", $1}' \
    || echo "  mongodb   (n/a)"
echo "════════════════════════════════════════════════════════════════════"
echo ""
