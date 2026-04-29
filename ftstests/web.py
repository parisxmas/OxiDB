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
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote

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

# Cap PUT body size at 5 MB so a runaway client cannot exhaust RAM —
# http.server reads the entire body into memory before we can hand it
# to OxiDB. nginx in front of this also enforces 5 MB, but the
# in-process check is the authoritative ceiling for direct callers.
MAX_UPLOAD_BYTES = 5 * 1024 * 1024


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
        else:
            self._send(404, b"not found", "text/plain")

    def _serve_stats(self):
        """Return how many objects live in the FTS bucket."""
        try:
            with oxidb.OxiDbClient(host=HOST, port=PORT, timeout=10) as client:
                objs = client.list_objects(BUCKET)
        except Exception as e:
            self._send(500, json.dumps({"error": str(e)}).encode(), "application/json")
            return
        body = json.dumps({"bucket": BUCKET, "count": len(objs)}).encode()
        self._send(200, body, "application/json")

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
        self.send_header("Content-Disposition", f'inline; filename="{key}"')
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
        try:
            data = self.rfile.read(length)
            if len(data) != length:
                raise RuntimeError(f"short read: got {len(data)} of {length}")
            with oxidb.OxiDbClient(host=HOST, port=PORT, timeout=60) as client:
                client.put_object(BUCKET, filename, data, content_type=content_type)
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
            }
        ).encode()
        self._send(200, body, "application/json")


def main() -> int:
    print(f"[web] {WEB_BIND}:{WEB_PORT}/  (proxying to OxiDB at {HOST}:{PORT})", flush=True)
    print("[web] Ctrl-C to stop", flush=True)
    try:
        ThreadingHTTPServer((WEB_BIND, WEB_PORT), Handler).serve_forever()
    except KeyboardInterrupt:
        print("\n[web] stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
