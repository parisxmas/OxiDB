"""Minimal OxiDB TCP wire client (length-prefixed JSON), zero deps.
Shared by the market-data feeders and the user/trader processes."""
import json
import os
import socket
import struct


class OxiDB:
    def __init__(self, host="127.0.0.1", port=None):
        self.host = host
        self.port = port or int(os.environ.get("OXIDB_PORT", 4444))
        self._connect()

    def _connect(self):
        self.sock = socket.create_connection((self.host, self.port))
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def call(self, obj):
        b = json.dumps(obj).encode()
        try:
            self.sock.sendall(struct.pack("<I", len(b)) + b)
            n = struct.unpack("<I", self._recv(4))[0]
            return json.loads(self._recv(n))
        except (OSError, struct.error):
            self._connect()
            raise

    def _recv(self, n):
        data = b""
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise OSError("server closed")
            data += chunk
        return data

    # convenience
    def insert(self, coll, doc):
        return self.call({"cmd": "insert", "collection": coll, "doc": doc})

    def find(self, coll, query=None, limit=None):
        req = {"cmd": "find", "collection": coll, "query": query or {}}
        if limit is not None:
            req["limit"] = limit
        return self.call(req).get("data", [])

    def find_one(self, coll, query):
        docs = self.find(coll, query, limit=1)
        return docs[0] if docs else None

    def count(self, coll, query=None):
        return self.call({"cmd": "count", "collection": coll, "query": query or {}})["data"]["count"]

    def create_index(self, coll, field):
        return self.call({"cmd": "create_index", "collection": coll, "field": field})

    def create_unique_index(self, coll, field):
        return self.call({"cmd": "create_unique_index", "collection": coll, "field": field})

    def begin(self):
        return self.call({"cmd": "begin_tx"})["data"]["tx_id"]

    def tx_update(self, coll, query, update):
        return self.call({"cmd": "update", "collection": coll, "query": query, "update": update})

    def tx_find_for_update(self, coll, query, lock_ms=5000):
        """SELECT ... FOR UPDATE — locks matched docs until commit/rollback.
        Requires an active transaction."""
        return self.call({"cmd": "find_for_update", "collection": coll,
                          "query": query, "lock_timeout_ms": lock_ms}).get("data", [])

    def tx_insert(self, coll, doc):
        return self.call({"cmd": "insert", "collection": coll, "doc": doc})

    def commit(self):
        return self.call({"cmd": "commit_tx"})

    def rollback(self):
        return self.call({"cmd": "rollback_tx"})
