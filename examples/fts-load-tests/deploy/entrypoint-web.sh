#!/bin/bash
# Web container entrypoint:
#   1. wait for OxiDB TCP port to come up
#   2. on first run (empty data volume), upload the bundled 100 .docx
#      corpus so the demo has searchable content out of the box
#   3. exec the web server
set -e

cd /app/ftstests

echo "[entrypoint] waiting for $OXIDB_HOST:$OXIDB_PORT"
for _ in $(seq 1 60); do
    if python3 -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('$OXIDB_HOST', $OXIDB_PORT))" 2>/dev/null; then
        echo "[entrypoint] OxiDB is up"
        break
    fi
    sleep 1
done

# Marker file — created after the first successful seed. Re-runs skip
# the upload, so restarts are cheap and data survives across restarts.
# The image no longer ships a corpus; opt in by setting
# OXIDB_FTS_DEMO_AUTOSEED=1 to fetch + chunk the Project Gutenberg
# books at first boot.
SEED_MARKER=/data/.seeded
if [ ! -f "$SEED_MARKER" ] && [ "${OXIDB_FTS_DEMO_AUTOSEED:-0}" = "1" ]; then
    echo "[entrypoint] OXIDB_FTS_DEMO_AUTOSEED=1 — fetching demo corpus"
    if python3 /app/ftstests/01_generate.py && python3 /app/ftstests/02_upload.py; then
        mkdir -p /data && touch "$SEED_MARKER"
        echo "[entrypoint] seed complete"
    else
        echo "[entrypoint] seed failed — continuing without corpus" >&2
    fi
else
    echo "[entrypoint] seed skipped (set OXIDB_FTS_DEMO_AUTOSEED=1 to enable)"
fi

echo "[entrypoint] starting web server on :$WEB_PORT"
exec python3 /app/ftstests/web.py
