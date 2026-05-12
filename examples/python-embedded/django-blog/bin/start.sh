#!/usr/bin/env bash
#
# Production-style runner for the embedded blog — single gunicorn
# worker, many threads.
#
# Why exactly one process: embedded OxiDB holds the database as
# in-process state (the B-tree, the WAL, the doc cache, the FTS
# worker pool). Two processes opening the same data directory would
# race on writes and corrupt the B-tree image. The Rust engine is
# already internally thread-safe — per-collection RwLocks, lock-free
# concurrent buckets — so threads inside one process are the right
# concurrency knob.
#
# If you actually need multiple OS processes serving the blog, that
# is the job of a separate `oxidb-server` (TCP) deployment with the
# pure-Python `oxidb` client — a different example, not this one.
#
# Usage:
#   THREADS=8 bin/start.sh
#
# Knobs (env vars, all optional):
#   THREADS=8            gunicorn thread pool size (worker stays at 1)
#   HOST=127.0.0.1       listen address
#   PORT=8000            listen port
#   BLOG_DATA_DIR=…      where OxiDB persists (defaults to ./_data)
#   DJANGO_SECRET_KEY=…  production secret (defaults to insecure dev key)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

THREADS="${THREADS:-8}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8000}"

VENV_PY="$ROOT/.venv/bin/python"
VENV_GUNICORN="$ROOT/.venv/bin/gunicorn"
if [[ ! -x "$VENV_PY" || ! -x "$VENV_GUNICORN" ]]; then
  echo "ERROR: venv not set up. From the example dir, run:" >&2
  echo "  python3 -m venv .venv && source .venv/bin/activate" >&2
  echo "  pip install -r requirements.txt" >&2
  exit 1
fi

export DJANGO_SECRET_KEY="${DJANGO_SECRET_KEY:-insecure-dev-key-replace-me-via-env}"
export DJANGO_DEBUG="${DJANGO_DEBUG:-0}"
export DJANGO_ALLOWED_HOSTS="${DJANGO_ALLOWED_HOSTS:-localhost,127.0.0.1,$HOST}"
export DJANGO_SETTINGS_MODULE="blog.settings"

echo "▸ gunicorn → http://$HOST:$PORT  (1 worker × $THREADS threads, embedded OxiDB)"
exec "$VENV_GUNICORN" blog.wsgi:application \
  --bind "$HOST:$PORT" \
  --workers 1 \
  --threads "$THREADS" \
  --worker-class gthread \
  --timeout 30 \
  --keep-alive 5 \
  --access-logfile - \
  --error-logfile - \
  --capture-output
