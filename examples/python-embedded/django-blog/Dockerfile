# syntax=docker/dockerfile:1.6
#
# Build context is the OxiDB repo root — the embedded FFI crate
# depends on the workspace `oxidb` crate via a path = ".." reference,
# so we need Cargo.toml + src/ from the parent directory available
# during the cargo build.
#
# Build from the repo root:
#   docker build -f examples/python-embedded/django-blog/Dockerfile -t oxidb-embedded-django .
# Or via the bundled compose file in this same directory.

# ─────────────────────────────────────────────────────────────────────
# Stage 1 — cross-build the embedded FFI shared library for linux/amd64
# ─────────────────────────────────────────────────────────────────────

FROM rust:1.88-bookworm AS rust-builder
WORKDIR /build

# Just the bits we need to compile oxidb-embedded-ffi.
COPY Cargo.toml Cargo.lock ./
COPY src/                src/
COPY oxidb-embedded-ffi/ oxidb-embedded-ffi/

# Stub out the workspace members we don't need so cargo workspace
# resolution still succeeds. Same trick as the top-level Dockerfile.
RUN for crate in oxidb-server oxidb-client-ffi oxidb-cli oxidb-tail oxipool oxidb-wasm; do \
      mkdir -p "$crate/src" && \
      printf '[package]\nname = "%s"\nversion = "0.0.0"\nedition = "2024"\n' "$crate" > "$crate/Cargo.toml" && \
      printf '' > "$crate/src/lib.rs" ; \
    done

# Default features only (no OCR, no GPU, no cluster) — the embedded
# blog doesn't need any of that. `--locked` is dropped because the
# stub workspace members above invalidate Cargo.lock entries for the
# crates we've replaced; cargo regenerates the lock for those slots.
RUN cargo build --release -p oxidb-embedded-ffi && \
    test -f target/release/liboxidb_embedded_ffi.so

# ─────────────────────────────────────────────────────────────────────
# Stage 2 — Django app on top of the .so
# ─────────────────────────────────────────────────────────────────────

FROM python:3.12-slim-bookworm AS app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DJANGO_SETTINGS_MODULE=blog.settings \
    DJANGO_DEBUG=0 \
    BLOG_DATA_DIR=/app/_data

WORKDIR /app

# Install pip deps. `tini` is included to reap zombie children
# cleanly when gunicorn forks workers (it runs as PID 1 in the
# container).
RUN apt-get update && \
    apt-get install -y --no-install-recommends tini && \
    rm -rf /var/lib/apt/lists/*

# `oxidb-embedded` itself is installed manually below (we drop the
# wrapper + a freshly-built Linux .so into site-packages), so the
# pip-install step skips that line. PyPI is also still on 0.18.0
# at the time of writing — the in-tree wrapper is newer.
COPY examples/python-embedded/django-blog/requirements.txt /tmp/req.txt
RUN grep -v '^oxidb-embedded' /tmp/req.txt > /tmp/req-noembed.txt && \
    pip install --no-cache-dir -r /tmp/req-noembed.txt

# Drop in the `oxidb_embedded` Python wrapper, then overlay the
# Linux .so we just built. The .dylib that lives next to the
# wrapper in the repo is a macOS dev convenience we deliberately
# don't copy here.
RUN mkdir -p /usr/local/lib/python3.12/site-packages/oxidb_embedded
COPY python-embedded/oxidb_embedded/__init__.py \
     /usr/local/lib/python3.12/site-packages/oxidb_embedded/__init__.py
COPY --from=rust-builder /build/target/release/liboxidb_embedded_ffi.so \
     /usr/local/lib/python3.12/site-packages/oxidb_embedded/liboxidb_embedded_ffi.so

# The Django app itself
COPY examples/python-embedded/django-blog/blog    /app/blog
COPY examples/python-embedded/django-blog/posts   /app/posts
COPY examples/python-embedded/django-blog/manage.py /app/manage.py

# WhiteNoise serves /static/ from STATIC_ROOT, so we need the assets
# collected at build time.
RUN python manage.py collectstatic --noinput --verbosity 0

# The data dir is the only persistent state; everything else is
# rebuildable from the image.
RUN mkdir -p /app/_data
VOLUME ["/app/_data"]

EXPOSE 8000

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["gunicorn", "blog.wsgi:application", \
     "--bind", "0.0.0.0:8000", \
     "--workers", "1", \
     "--threads", "8", \
     "--worker-class", "gthread", \
     "--timeout", "30", \
     "--keep-alive", "5", \
     "--access-logfile", "-", \
     "--error-logfile", "-", \
     "--capture-output"]
