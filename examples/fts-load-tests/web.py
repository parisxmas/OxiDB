#!/usr/bin/env python3
"""
Tiny web client for the ftstests FTS corpus. Serves a single HTML page
and proxies search queries to a running OxiDB server over the TCP wire
protocol. Run this after `run.sh` (or `serve.sh`) has populated the
server with the 100 .docx files.

Env:
    OXIDB_HOST  default 127.0.0.1
    OXIDB_PORT  default 14888
    WEB_PORT    default 8765
"""
import hashlib
import json
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import quote, unquote

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "python"))
import oxidb  # type: ignore

HERE = Path(__file__).resolve().parent
HOST = os.environ.get("OXIDB_HOST", "127.0.0.1")
PORT = int(os.environ.get("OXIDB_PORT", "14888"))
WEB_PORT = int(os.environ.get("WEB_PORT", "8765"))
# Default to localhost so a developer running web.py directly is not
# surprised by an open port; in Docker we set WEB_BIND=0.0.0.0 so the
# host-side docker-proxy can reach the listener.
WEB_BIND = os.environ.get("WEB_BIND", "127.0.0.1")
BUCKET = "ftstests"
# Collection used as a content-hash → key dedup index. Each document
# is `{hash, key, size, content_type, uploaded_at}`. The leading
# underscore mirrors the convention used by other internal collections
# (_blobs, _fts, etc.) and keeps it out of the way of FTS searches.
HASH_INDEX_COLL = "_uploads_hash"

# Cap PUT body size at 5 MB so a runaway client cannot exhaust RAM —
# http.server reads the entire body into memory before we can hand it
# to OxiDB. nginx in front of this also enforces 5 MB, but the
# in-process check is the authoritative ceiling for direct callers.
MAX_UPLOAD_BYTES = 5 * 1024 * 1024

# File extensions the demo's FTS pipeline can actually extract from.
# Anything else is rejected at the upload boundary so the bucket
# doesn't fill with blobs the search index can't see. Match the
# frontend's <input accept=...> list one-for-one.
ALLOWED_EXTENSIONS = frozenset({
    "pdf", "docx", "xlsx",
    "txt", "html", "htm", "xml", "json", "md", "csv",
    "png", "jpg", "jpeg", "tif", "tiff", "bmp",
})


def _ext_of(filename: str) -> str:
    """Lowercase extension after the last '.' (no leading dot).
    Empty string for names without an extension."""
    if "." not in filename:
        return ""
    return filename.rsplit(".", 1)[1].lower()


def _mask_ip(ip: str) -> str:
    """Reduce an IP to its first octet/segment so the public feed
    can show coarse provenance ("123.x.x.x") without leaking full
    addresses to other visitors. Full IPs remain stored in OxiDB for
    the operator's own audit needs."""
    if not ip:
        return "?"
    if "." in ip:
        parts = ip.split(".")
        if len(parts) == 4:
            return f"{parts[0]}.x.x.x"
        return ip
    if ":" in ip:
        first = ip.split(":", 1)[0] or "::"
        return f"{first}:x:x:x"
    return ip


def _ensure_hash_indexes_once():
    """Idempotently make sure the dedup collection has indexes on
    the `hash` (lookup by content) and `key` (cleanup on overwrite)
    fields. Safe to call multiple times — duplicate index creation
    errors are swallowed."""
    try:
        with oxidb.OxiDbClient(host=HOST, port=PORT, timeout=5) as c:
            try:
                c.create_index(HASH_INDEX_COLL, "hash")
            except Exception:
                pass
            try:
                c.create_index(HASH_INDEX_COLL, "key")
            except Exception:
                pass
    except Exception as e:
        # Server not yet up — first PUT will retry implicitly via
        # auto-collection-create on insert. Print to stderr so it's
        # visible in container logs but don't fail startup.
        print(f"[web] could not pre-create hash indexes: {e}", file=sys.stderr)


def _backfill_hash_index():
    """Scan the blob bucket and add any object that isn't yet in the
    hash index. Closes the gap created when blobs were uploaded before
    the dedup feature existed (or by a future tool that bypasses
    web.py). Idempotent — safe on every startup. Cost is roughly
    O(missing_blobs * blob_size) for hashing; negligible at this
    deployment's scale."""
    try:
        with oxidb.OxiDbClient(host=HOST, port=PORT, timeout=30) as c:
            try:
                blobs = c.list_objects(BUCKET)
            except Exception:
                # Bucket not created yet — nothing to backfill.
                return
            if not blobs:
                return

            try:
                indexed = c.find(HASH_INDEX_COLL, {})
                indexed_keys = {d.get("key") for d in indexed}
            except Exception:
                indexed_keys = set()

            missing = [b for b in blobs if b.get("key") not in indexed_keys]
            if not missing:
                return

            print(f"[web] backfilling hash index for {len(missing)} pre-existing blob(s)",
                  file=sys.stderr)
            for blob in missing:
                key = blob.get("key")
                if not key:
                    continue
                try:
                    data, meta = c.get_object(BUCKET, key)
                    sha256 = hashlib.sha256(data).hexdigest()
                    c.insert(HASH_INDEX_COLL, {
                        "hash": sha256,
                        "key": key,
                        "size": len(data),
                        "content_type": meta.get("content_type", "application/octet-stream"),
                        "uploaded_at": int(time.time()),
                        "backfilled": True,
                    })
                    print(f"[web]   backfilled: {key} ({sha256[:12]}…)", file=sys.stderr)
                except Exception as e:
                    print(f"[web]   backfill failed for {key}: {e}", file=sys.stderr)
    except Exception as e:
        print(f"[web] hash index backfill failed: {e}", file=sys.stderr)


def search_oxidb(query: str, limit: int, highlight: bool):
    with oxidb.OxiDbClient(host=HOST, port=PORT, timeout=15) as client:
        if highlight:
            payload = {
                "cmd": "search",
                "query": query,
                "bucket": BUCKET,
                "limit": limit,
                "highlight": {"snippet_chars": 140, "max_snippets": 3},
            }
            return client._checked(payload)
        return client.search(query, bucket=BUCKET, limit=limit)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        # Quieter access log; real errors still go to stderr.
        sys.stderr.write(f"{self.address_string()} {fmt % args}\n")

    def _send(self, status: int, body: bytes, ctype: str):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            try:
                html = (HERE / "index.html").read_bytes()
            except FileNotFoundError:
                self._send(500, b"index.html missing", "text/plain")
                return
            self._send(200, html, "text/html; charset=utf-8")
        elif self.path == "/healthz":
            try:
                with oxidb.OxiDbClient(host=HOST, port=PORT, timeout=2) as c:
                    c.ping()
                self._send(200, b'{"ok":true}', "application/json")
            except Exception as e:
                self._send(503, json.dumps({"ok": False, "error": str(e)}).encode(), "application/json")
        elif self.path.startswith("/api/blob/"):
            self._serve_blob(self.path[len("/api/blob/"):])
        elif self.path.startswith("/api/text/"):
            self._serve_extracted_text(self.path[len("/api/text/"):])
        elif self.path == "/api/stats":
            self._serve_stats()
        elif self.path == "/api/fts-status":
            self._serve_fts_status()
        elif self.path == "/api/proc-status":
            self._serve_proc_status()
        elif self.path == "/api/uploads-feed":
            self._serve_uploads_feed()
        else:
            self._send(404, b"not found", "text/plain")

    def _serve_stats(self):
        """Return how many objects live in the FTS bucket and their
        cumulative byte size — the header counter polls this."""
        try:
            with oxidb.OxiDbClient(host=HOST, port=PORT, timeout=10) as client:
                objs = client.list_objects(BUCKET)
            count = len(objs)
            total_bytes = sum(o.get("size", 0) for o in objs)
        except Exception as e:
            # Bucket is created lazily on first upload; treat its absence
            # as "0 uploads" rather than a hard error.
            if "bucket not found" in str(e).lower():
                count = 0
                total_bytes = 0
            else:
                self._send(500, json.dumps({"error": str(e)}).encode(), "application/json")
                return
        body = json.dumps({
            "bucket": BUCKET,
            "count": count,
            "total_bytes": total_bytes,
        }).encode()
        self._send(200, body, "application/json")

    def _serve_fts_status(self):
        """Snapshot of the FTS indexing pipeline (queue depth + per-worker)."""
        try:
            with oxidb.OxiDbClient(host=HOST, port=PORT, timeout=5) as client:
                status = client.fts_status()
        except Exception as e:
            self._send(500, json.dumps({"error": str(e)}).encode(), "application/json")
            return
        self._send(200, json.dumps(status).encode(), "application/json")

    def _serve_proc_status(self):
        """Process self-metrics for oxidb-server (cpu/mem/threads/uptime)."""
        try:
            with oxidb.OxiDbClient(host=HOST, port=PORT, timeout=5) as client:
                status = client.proc_status()
        except Exception as e:
            self._send(500, json.dumps({"error": str(e)}).encode(), "application/json")
            return
        self._send(200, json.dumps(status).encode(), "application/json")

    def _serve_uploads_feed(self):
        """Public activity feed: last 30 uploads, newest first.
        Client IPs are masked here (server-side) so the raw address
        never leaves the box — full IPs stay in the OxiDB doc for the
        operator's audit trail."""
        try:
            with oxidb.OxiDbClient(host=HOST, port=PORT, timeout=10) as client:
                try:
                    docs = client.find(
                        HASH_INDEX_COLL, {},
                        sort={"uploaded_at": -1}, limit=30,
                    )
                except Exception:
                    # Collection not yet created — empty feed.
                    docs = []
        except Exception as e:
            self._send(500, json.dumps({"error": str(e)}).encode(), "application/json")
            return
        items = [
            {
                "key": d.get("key", ""),
                "size": d.get("size", 0),
                "stored_size": d.get("stored_size"),
                "client_ip": _mask_ip(d.get("client_ip", "")),
                "uploaded_at": d.get("uploaded_at", 0),
            }
            for d in docs
        ]
        body = json.dumps({"items": items}).encode()
        self._send(200, body, "application/json")

    def _client_ip(self):
        """Real client IP. nginx sets X-Real-IP to CF-Connecting-IP via
        `real_ip_header CF-Connecting-IP`, so this is the end user's
        public address even when Cloudflare and the local stream relay
        are in the path. Falls back to X-Forwarded-For's first hop and
        finally to the docker-proxy address."""
        real = self.headers.get("X-Real-IP")
        if real:
            return real.strip()
        xff = self.headers.get("X-Forwarded-For", "")
        if xff:
            return xff.split(",")[0].strip()
        return self.client_address[0] if self.client_address else "unknown"

    def _safe_key(self, path_tail: str):
        key = unquote(path_tail)
        if not key or "/" in key or ".." in key or key.startswith("."):
            return None
        return key

    def _serve_blob(self, path_tail: str):
        key = self._safe_key(path_tail)
        if key is None:
            self._send(400, b'{"error":"invalid key"}', "application/json")
            return
        try:
            with oxidb.OxiDbClient(host=HOST, port=PORT, timeout=15) as client:
                data, meta = client.get_object(BUCKET, key)
        except oxidb.OxiDbError as e:
            self._send(404, json.dumps({"error": str(e)}).encode(), "application/json")
            return
        except Exception as e:
            self._send(500, json.dumps({"error": str(e)}).encode(), "application/json")
            return
        ctype = meta.get("content_type") or "application/octet-stream"
        # Inline disposition so browsers (especially for PDFs) render
        # rather than prompting download.
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "private, max-age=60")
        # RFC 5987 — filename* with UTF-8 percent-encoding handles
        # non-ASCII names (Turkish "İ", CJK, emoji). Without this,
        # Python's stdlib header path tries to latin-1 encode the
        # filename and crashes on anything outside U+0000..U+00FF.
        ascii_fallback = key.encode("ascii", "replace").decode("ascii").replace('"', "_")
        encoded = quote(key, safe="")
        self.send_header(
            "Content-Disposition",
            f"inline; filename=\"{ascii_fallback}\"; filename*=UTF-8''{encoded}",
        )
        self.end_headers()
        self.wfile.write(data)

    def _serve_extracted_text(self, path_tail: str):
        key = self._safe_key(path_tail)
        if key is None:
            self._send(400, b'{"error":"invalid key"}', "application/json")
            return
        try:
            with oxidb.OxiDbClient(host=HOST, port=PORT, timeout=30) as client:
                text = client.extract_text(BUCKET, key)
        except oxidb.OxiDbError as e:
            self._send(415, json.dumps({"error": str(e)}).encode(), "application/json")
            return
        except Exception as e:
            self._send(500, json.dumps({"error": str(e)}).encode(), "application/json")
            return
        body = json.dumps({"key": key, "text": text}).encode()
        self._send(200, body, "application/json")

    def do_POST(self):
        if self.path != "/api/search":
            self._send(404, b"not found", "text/plain")
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            query = (body.get("query") or "").strip()
            limit = int(body.get("limit", 10))
            highlight = bool(body.get("highlight", True))
            if not query:
                self._send(400, b'{"error":"query required"}', "application/json")
                return
            results = search_oxidb(query, limit, highlight)
        except Exception as e:
            payload = {"error": str(e), "type": type(e).__name__}
            self._send(500, json.dumps(payload).encode(), "application/json")
            return
        self._send(200, json.dumps({"results": results}).encode(), "application/json")

    def do_PUT(self):
        if not self.path.startswith("/api/upload/"):
            self._send(404, b"not found", "text/plain")
            return
        # /api/upload/<filename>  — filename may contain url-encoded
        # spaces and unicode, but reject path separators outright.
        filename = unquote(self.path[len("/api/upload/"):])
        if not filename or "/" in filename or ".." in filename or filename.startswith("."):
            self._send(400, b'{"error":"invalid filename"}', "application/json")
            return
        ext = _ext_of(filename)
        if ext not in ALLOWED_EXTENSIONS:
            self._send(
                415,
                json.dumps({
                    "error": f"unsupported file type: .{ext or '(none)'}",
                    "unsupported": True,
                    "extension": ext,
                    "allowed": sorted(ALLOWED_EXTENSIONS),
                }).encode(),
                "application/json",
            )
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0:
            self._send(411, b'{"error":"missing Content-Length"}', "application/json")
            return
        if length > MAX_UPLOAD_BYTES:
            self._send(
                413,
                json.dumps(
                    {"error": f"file too large ({length} > {MAX_UPLOAD_BYTES} bytes)"}
                ).encode(),
                "application/json",
            )
            return
        content_type = self.headers.get("Content-Type", "application/octet-stream")
        client_ip = self._client_ip()
        user_agent = self.headers.get("User-Agent", "")
        try:
            data = self.rfile.read(length)
            if len(data) != length:
                raise RuntimeError(f"short read: got {len(data)} of {length}")
            sha256 = hashlib.sha256(data).hexdigest()

            with oxidb.OxiDbClient(host=HOST, port=PORT, timeout=60) as client:
                # Content-based dedup: if any prior upload had the same
                # SHA-256, refuse this one and point the user at the
                # existing key. Treat lookup errors (e.g. collection
                # not yet created on first ever upload) as "not found"
                # so we still accept the very first file.
                existing = None
                try:
                    existing = client.find_one(HASH_INDEX_COLL, {"hash": sha256})
                except Exception:
                    existing = None
                if existing:
                    payload = {
                        "error": f"duplicate of {existing.get('key')}",
                        "duplicate": True,
                        "existing_key": existing.get("key"),
                        "hash": sha256,
                    }
                    self._send(409, json.dumps(payload).encode(), "application/json")
                    return

                # Carry uploader attribution into the blob's own
                # metadata so it stays accessible via head_object and
                # survives even if the dedup index is wiped.
                put_meta = client.put_object(
                    BUCKET, filename, data,
                    content_type=content_type,
                    metadata={
                        "client_ip": client_ip,
                        "user_agent": user_agent[:512],
                        "sha256": sha256,
                    },
                )
                # The server reports the post-encode byte count via
                # `stored_size`. Older binaries omit the field, so
                # fall back to the original logical length so the
                # feed still has a number to render.
                stored_size = (put_meta or {}).get("stored_size") or length

                # Best-effort: if this filename was previously uploaded
                # with a different content (overwrite), drop its stale
                # hash entry so the index doesn't keep pointing at a
                # checksum the blob no longer has.
                try:
                    client.delete(HASH_INDEX_COLL, {"key": filename})
                except Exception:
                    pass

                # Record the new content → key mapping with full
                # attribution. Truncate user_agent to keep the doc
                # bounded (Mozilla UAs can be 200+ chars).
                try:
                    client.insert(HASH_INDEX_COLL, {
                        "hash": sha256,
                        "key": filename,
                        "size": length,
                        "stored_size": stored_size,
                        "content_type": content_type,
                        "uploaded_at": int(time.time()),
                        "client_ip": client_ip,
                        "user_agent": user_agent[:512],
                    })
                except Exception as ie:
                    # Indexing is best-effort — the blob is already
                    # stored, so don't fail the upload. Log so it's
                    # visible in container logs.
                    print(f"[web] hash index insert failed: {ie}", file=sys.stderr)
        except Exception as e:
            payload = {"error": str(e), "type": type(e).__name__}
            self._send(500, json.dumps(payload).encode(), "application/json")
            return
        body = json.dumps(
            {
                "key": filename,
                "bucket": BUCKET,
                "size": length,
                "content_type": content_type,
                "hash": sha256,
            }
        ).encode()
        self._send(200, body, "application/json")


def main() -> int:
    print(f"[web] {WEB_BIND}:{WEB_PORT}/  (proxying to OxiDB at {HOST}:{PORT})", flush=True)
    _ensure_hash_indexes_once()
    _backfill_hash_index()
    print("[web] Ctrl-C to stop", flush=True)
    try:
        ThreadingHTTPServer((WEB_BIND, WEB_PORT), Handler).serve_forever()
    except KeyboardInterrupt:
        print("\n[web] stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
