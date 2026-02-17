"""
OxiDB Python client library.

Zero external dependencies — uses only the Python standard library.
Communicates with oxidb-server over TCP using the length-prefixed JSON protocol.

Usage:
    from oxidb import OxiDbClient

    client = OxiDbClient("127.0.0.1", 4444)
    client.insert("users", {"name": "Alice", "age": 30})
    docs = client.find("users", {"name": "Alice"})
    client.close()

    # or as a context manager:
    with OxiDbClient("127.0.0.1", 4444) as db:
        db.insert("users", {"name": "Bob"})
"""

import json
import socket
import struct
import base64
from contextlib import contextmanager


class OxiDbError(Exception):
    """Raised when the server returns an error response."""

    pass


class TransactionConflictError(OxiDbError):
    """Raised on OCC version conflict during commit."""

    pass


class OxiDbClient:
    """TCP client for oxidb-server.

    Protocol: each message is [4-byte little-endian length][JSON payload].
    Server responds with {"ok": true, "data": ...} or {"ok": false, "error": "..."}.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 4444, timeout: float = 5.0):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock.settimeout(timeout)
        self._sock.connect((host, port))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        """Close the TCP connection."""
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()

    # ------------------------------------------------------------------
    # Low-level protocol
    # ------------------------------------------------------------------

    def _send_raw(self, data: bytes):
        self._sock.sendall(struct.pack("<I", len(data)) + data)

    def _recv_raw(self) -> bytes:
        length_bytes = self._recv_exact(4)
        length = struct.unpack("<I", length_bytes)[0]
        return self._recv_exact(length)

    def _recv_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("connection closed by server")
            buf.extend(chunk)
        return bytes(buf)

    def _request(self, payload: dict) -> dict:
        """Send a JSON request and return the parsed JSON response."""
        self._send_raw(json.dumps(payload).encode("utf-8"))
        resp_bytes = self._recv_raw()
        return json.loads(resp_bytes)

    def _checked(self, payload: dict):
        """Send a request and return data on success, raise on error."""
        resp = self._request(payload)
        if not resp.get("ok"):
            error_msg = resp.get("error", "unknown error")
            if "conflict" in error_msg.lower():
                raise TransactionConflictError(error_msg)
            raise OxiDbError(error_msg)
        return resp.get("data")

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def ping(self) -> str:
        """Ping the server. Returns 'pong'."""
        return self._checked({"cmd": "ping"})

    # ------------------------------------------------------------------
    # Collection management
    # ------------------------------------------------------------------

    def create_collection(self, name: str):
        """Explicitly create a collection. Collections are also auto-created on insert."""
        return self._checked({"cmd": "create_collection", "collection": name})

    def list_collections(self) -> list:
        """Return a list of collection names."""
        return self._checked({"cmd": "list_collections"})

    def drop_collection(self, name: str):
        """Drop a collection and its data."""
        return self._checked({"cmd": "drop_collection", "collection": name})

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def insert(self, collection: str, doc: dict):
        """Insert a single document. Returns {"id": ...} outside tx, "buffered" inside tx."""
        return self._checked({"cmd": "insert", "collection": collection, "doc": doc})

    def insert_many(self, collection: str, docs: list):
        """Insert multiple documents."""
        return self._checked({"cmd": "insert_many", "collection": collection, "docs": docs})

    def find(self, collection: str, query: dict = None, *, sort: dict = None,
             skip: int = None, limit: int = None) -> list:
        """Find documents matching a query. Returns a list of documents."""
        payload = {"cmd": "find", "collection": collection, "query": query or {}}
        if sort is not None:
            payload["sort"] = sort
        if skip is not None:
            payload["skip"] = skip
        if limit is not None:
            payload["limit"] = limit
        return self._checked(payload)

    def find_one(self, collection: str, query: dict = None):
        """Find a single document matching a query. Returns the document or None."""
        return self._checked({"cmd": "find_one", "collection": collection, "query": query or {}})

    def update(self, collection: str, query: dict, update: dict):
        """Update all documents matching a query. Returns {"modified": n} outside tx."""
        return self._checked({
            "cmd": "update", "collection": collection,
            "query": query, "update": update,
        })

    def update_one(self, collection: str, query: dict, update: dict):
        """Update the first document matching a query. Returns {"modified": n}."""
        return self._checked({
            "cmd": "update_one", "collection": collection,
            "query": query, "update": update,
        })

    def delete(self, collection: str, query: dict):
        """Delete all documents matching a query. Returns {"deleted": n} outside tx."""
        return self._checked({"cmd": "delete", "collection": collection, "query": query})

    def delete_one(self, collection: str, query: dict):
        """Delete the first document matching a query. Returns {"deleted": n}."""
        return self._checked({"cmd": "delete_one", "collection": collection, "query": query})

    def count(self, collection: str, query: dict = None) -> int:
        """Count documents matching a query."""
        result = self._checked({"cmd": "count", "collection": collection, "query": query or {}})
        return result["count"]

    # ------------------------------------------------------------------
    # Indexes
    # ------------------------------------------------------------------

    def create_index(self, collection: str, field: str):
        """Create a non-unique index on a field."""
        return self._checked({"cmd": "create_index", "collection": collection, "field": field})

    def create_unique_index(self, collection: str, field: str):
        """Create a unique index on a field."""
        return self._checked({"cmd": "create_unique_index", "collection": collection, "field": field})

    def create_composite_index(self, collection: str, fields: list):
        """Create a composite index on multiple fields."""
        return self._checked({"cmd": "create_composite_index", "collection": collection, "fields": fields})

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def aggregate(self, collection: str, pipeline: list) -> list:
        """Run an aggregation pipeline. Returns list of result documents."""
        return self._checked({"cmd": "aggregate", "collection": collection, "pipeline": pipeline})

    # ------------------------------------------------------------------
    # Compaction
    # ------------------------------------------------------------------

    def compact(self, collection: str) -> dict:
        """Compact a collection. Returns {old_size, new_size, docs_kept}."""
        return self._checked({"cmd": "compact", "collection": collection})

    # ------------------------------------------------------------------
    # Transactions
    # ------------------------------------------------------------------

    def begin_tx(self) -> dict:
        """Begin a transaction on this connection. Returns {"tx_id": ...}."""
        return self._checked({"cmd": "begin_tx"})

    def commit_tx(self):
        """Commit the active transaction. Raises TransactionConflictError on OCC conflict."""
        return self._checked({"cmd": "commit_tx"})

    def rollback_tx(self):
        """Rollback the active transaction."""
        return self._checked({"cmd": "rollback_tx"})

    @contextmanager
    def transaction(self):
        """Context manager for transactions. Auto-rolls back on exception.

        Usage:
            with client.transaction():
                client.insert("col", {"x": 1})
                client.update("col", {"x": 1}, {"$set": {"x": 2}})
            # auto-committed here

            # or on error:
            with client.transaction():
                client.insert("col", {"x": 1})
                raise ValueError("oops")
            # auto-rolled back
        """
        self.begin_tx()
        try:
            yield
            self.commit_tx()
        except Exception:
            try:
                self.rollback_tx()
            except OxiDbError:
                pass  # rollback may fail if commit already failed
            raise

    # ------------------------------------------------------------------
    # Blob storage
    # ------------------------------------------------------------------

    def create_bucket(self, bucket: str):
        """Create a blob storage bucket."""
        return self._checked({"cmd": "create_bucket", "bucket": bucket})

    def list_buckets(self) -> list:
        """List all blob storage buckets."""
        return self._checked({"cmd": "list_buckets"})

    def delete_bucket(self, bucket: str):
        """Delete a blob storage bucket."""
        return self._checked({"cmd": "delete_bucket", "bucket": bucket})

    def put_object(self, bucket: str, key: str, data: bytes,
                   content_type: str = "application/octet-stream",
                   metadata: dict = None):
        """Upload a blob object. `data` is raw bytes (base64-encoded automatically)."""
        payload = {
            "cmd": "put_object",
            "bucket": bucket,
            "key": key,
            "data": base64.b64encode(data).decode("ascii"),
            "content_type": content_type,
        }
        if metadata:
            payload["metadata"] = metadata
        return self._checked(payload)

    def get_object(self, bucket: str, key: str) -> tuple:
        """Download a blob object. Returns (bytes, metadata_dict)."""
        result = self._checked({"cmd": "get_object", "bucket": bucket, "key": key})
        data = base64.b64decode(result["content"])
        return data, result["metadata"]

    def head_object(self, bucket: str, key: str) -> dict:
        """Get blob object metadata without downloading the content."""
        return self._checked({"cmd": "head_object", "bucket": bucket, "key": key})

    def delete_object(self, bucket: str, key: str):
        """Delete a blob object."""
        return self._checked({"cmd": "delete_object", "bucket": bucket, "key": key})

    def list_objects(self, bucket: str, prefix: str = None, limit: int = None) -> list:
        """List objects in a bucket."""
        payload = {"cmd": "list_objects", "bucket": bucket}
        if prefix is not None:
            payload["prefix"] = prefix
        if limit is not None:
            payload["limit"] = limit
        return self._checked(payload)

    # ------------------------------------------------------------------
    # Full-text search
    # ------------------------------------------------------------------

    def search(self, query: str, bucket: str = None, limit: int = 10) -> list:
        """Full-text search across blobs. Returns [{bucket, key, score}, ...]."""
        payload = {"cmd": "search", "query": query, "limit": limit}
        if bucket is not None:
            payload["bucket"] = bucket
        return self._checked(payload)
