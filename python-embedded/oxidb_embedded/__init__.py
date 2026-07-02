"""
OxiDB Embedded — run OxiDB directly in your Python process via FFI.

No server required. Zero network overhead. The database engine runs in-process
using the native shared library (Rust → C FFI → Python ctypes).

Usage:
    from oxidb_embedded import OxiDbEmbedded

    db = OxiDbEmbedded("./mydata")
    db.insert("users", {"name": "Alice", "age": 30})
    docs = db.find("users", {"name": "Alice"})
    db.close()

    # or as a context manager:
    with OxiDbEmbedded("./mydata") as db:
        db.insert("users", {"name": "Bob"})
"""

import ctypes
import ctypes.util
import json
import os
import platform
import sys
from contextlib import contextmanager


class OxiDbError(Exception):
    """Raised when the database returns an error response."""
    pass


class TransactionConflictError(OxiDbError):
    """Raised on OCC version conflict during commit."""
    pass


def _find_library():
    """Find the native OxiDB embedded FFI shared library."""
    system = platform.system()
    machine = platform.machine().lower()

    if system == "Darwin":
        lib_name = "liboxidb_embedded_ffi.dylib"
    elif system == "Linux":
        lib_name = "liboxidb_embedded_ffi.so"
    elif system == "Windows":
        lib_name = "oxidb_embedded_ffi.dll"
    else:
        raise OSError(f"Unsupported platform: {system}")

    # Search order:
    # 1. Next to this Python file (bundled in wheel)
    pkg_dir = os.path.dirname(os.path.abspath(__file__))
    candidate = os.path.join(pkg_dir, lib_name)
    if os.path.isfile(candidate):
        return candidate

    # 2. In a 'lib' subdirectory
    candidate = os.path.join(pkg_dir, "lib", lib_name)
    if os.path.isfile(candidate):
        return candidate

    # 3. Current working directory
    candidate = os.path.join(os.getcwd(), lib_name)
    if os.path.isfile(candidate):
        return candidate

    # 4. OXIDB_LIB_PATH environment variable
    env_path = os.environ.get("OXIDB_LIB_PATH")
    if env_path and os.path.isfile(env_path):
        return env_path

    # 5. System library path
    found = ctypes.util.find_library("oxidb_embedded_ffi")
    if found:
        return found

    raise OSError(
        f"Cannot find {lib_name}. Either:\n"
        f"  - Place it next to this Python file ({pkg_dir}/)\n"
        f"  - Set OXIDB_LIB_PATH environment variable\n"
        f"  - Install it to a system library path"
    )


def _load_library():
    """Load the native library and set up function signatures."""
    path = _find_library()
    lib = ctypes.CDLL(path)

    # oxidb_open(path: *const c_char) -> *mut c_void
    lib.oxidb_open.argtypes = [ctypes.c_char_p]
    lib.oxidb_open.restype = ctypes.c_void_p

    # oxidb_open_encrypted(path: *const c_char, key_path: *const c_char) -> *mut c_void
    lib.oxidb_open_encrypted.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
    lib.oxidb_open_encrypted.restype = ctypes.c_void_p

    # oxidb_close(handle: *mut c_void)
    lib.oxidb_close.argtypes = [ctypes.c_void_p]
    lib.oxidb_close.restype = None

    # oxidb_execute(handle: *mut c_void, cmd_json: *const c_char) -> *mut c_char
    lib.oxidb_execute.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    lib.oxidb_execute.restype = ctypes.c_void_p

    # oxidb_free_string(ptr: *mut c_char)
    lib.oxidb_free_string.argtypes = [ctypes.c_void_p]
    lib.oxidb_free_string.restype = None

    return lib


class OxiDbEmbedded:
    """Embedded OxiDB database — runs in-process via FFI.

    No server required. The database files are stored at the given path.
    """

    def __init__(self, data_dir: str, encryption_key_path: str = None):
        self._lib = _load_library()
        path_bytes = data_dir.encode("utf-8")

        if encryption_key_path:
            key_bytes = encryption_key_path.encode("utf-8")
            self._handle = self._lib.oxidb_open_encrypted(path_bytes, key_bytes)
        else:
            self._handle = self._lib.oxidb_open(path_bytes)

        if not self._handle:
            raise OxiDbError(f"Failed to open database at {data_dir}")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        """Close the database and free resources."""
        if self._handle:
            self._lib.oxidb_close(self._handle)
            self._handle = None

    def _execute(self, cmd: dict):
        """Execute a JSON command and return the parsed response data."""
        if not self._handle:
            raise OxiDbError("Database is closed")

        cmd_json = json.dumps(cmd).encode("utf-8")
        result_ptr = self._lib.oxidb_execute(self._handle, cmd_json)

        if not result_ptr:
            raise OxiDbError("FFI returned null")

        result_str = ctypes.cast(result_ptr, ctypes.c_char_p).value
        self._lib.oxidb_free_string(result_ptr)

        resp = json.loads(result_str)
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
        return self._execute({"cmd": "ping"})

    # ------------------------------------------------------------------
    # Collection management
    # ------------------------------------------------------------------

    def create_collection(self, name: str):
        return self._execute({"cmd": "create_collection", "collection": name})

    def list_collections(self) -> list:
        return self._execute({"cmd": "list_collections"})

    def drop_collection(self, name: str):
        return self._execute({"cmd": "drop_collection", "collection": name})

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def insert(self, collection: str, doc: dict):
        return self._execute({"cmd": "insert", "collection": collection, "doc": doc})

    def insert_many(self, collection: str, docs: list):
        return self._execute({"cmd": "insert_many", "collection": collection, "docs": docs})

    def find(self, collection: str, query: dict = None, *, sort: dict = None,
             skip: int = None, limit: int = None) -> list:
        payload = {"cmd": "find", "collection": collection, "query": query or {}}
        if sort is not None:
            payload["sort"] = sort
        if skip is not None:
            payload["skip"] = skip
        if limit is not None:
            payload["limit"] = limit
        return self._execute(payload)

    def find_one(self, collection: str, query: dict = None):
        return self._execute({"cmd": "find_one", "collection": collection, "query": query or {}})

    def update(self, collection: str, query: dict, update: dict):
        return self._execute({"cmd": "update", "collection": collection, "query": query, "update": update})

    def update_one(self, collection: str, query: dict, update: dict):
        return self._execute({"cmd": "update_one", "collection": collection, "query": query, "update": update})

    def delete(self, collection: str, query: dict):
        return self._execute({"cmd": "delete", "collection": collection, "query": query})

    def delete_one(self, collection: str, query: dict):
        return self._execute({"cmd": "delete_one", "collection": collection, "query": query})

    def count(self, collection: str, query: dict = None) -> int:
        result = self._execute({"cmd": "count", "collection": collection, "query": query or {}})
        return result["count"]

    # ------------------------------------------------------------------
    # Indexes
    # ------------------------------------------------------------------

    def create_index(self, collection: str, field: str):
        return self._execute({"cmd": "create_index", "collection": collection, "field": field})

    def create_unique_index(self, collection: str, field: str):
        return self._execute({"cmd": "create_unique_index", "collection": collection, "field": field})

    def create_composite_index(self, collection: str, fields: list):
        return self._execute({"cmd": "create_composite_index", "collection": collection, "fields": fields})

    def create_text_index(self, collection: str, fields: list):
        return self._execute({"cmd": "create_text_index", "collection": collection, "fields": fields})

    def list_indexes(self, collection: str) -> list:
        return self._execute({"cmd": "list_indexes", "collection": collection})

    def drop_index(self, collection: str, index_name: str):
        return self._execute({"cmd": "drop_index", "collection": collection, "index": index_name})

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def text_search(self, collection: str, query: str, limit: int = 10) -> list:
        return self._execute({"cmd": "text_search", "collection": collection, "query": query, "limit": limit})

    def search(self, query: str, bucket: str = None, limit: int = 10) -> list:
        payload = {"cmd": "search", "query": query, "limit": limit}
        if bucket is not None:
            payload["bucket"] = bucket
        return self._execute(payload)

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def aggregate(self, collection: str, pipeline: list) -> list:
        return self._execute({"cmd": "aggregate", "collection": collection, "pipeline": pipeline})

    # ------------------------------------------------------------------
    # SQL engine (second engine, ADR-0010)
    # ------------------------------------------------------------------

    def sql(self, sql: str, params: list = None) -> list:
        """Execute SQL against the embedded standalone SQL engine.

        The engine's data lives under <data dir>/sql, entirely separate from
        document collections, and opens lazily on the first call. `params`
        optionally binds `?` / `$N` placeholders left-to-right.

        Returns a list with one result per statement:
        - SELECT: {"columns": [...], "rows": [[...], ...]}
        - INSERT/UPDATE/DELETE: {"affected": N}
        - DDL: {"ddl": True}
        - BEGIN/COMMIT/ROLLBACK: {"transaction": True}
        """
        payload = {"engine": "sql", "cmd": "sql", "sql": sql}
        if params is not None:
            payload["params"] = params
        return self._execute(payload)

    # ------------------------------------------------------------------
    # Compaction
    # ------------------------------------------------------------------

    def compact(self, collection: str) -> dict:
        return self._execute({"cmd": "compact", "collection": collection})

    # ------------------------------------------------------------------
    # Transactions
    # ------------------------------------------------------------------

    def begin_tx(self) -> dict:
        return self._execute({"cmd": "begin_tx"})

    def commit_tx(self):
        return self._execute({"cmd": "commit_tx"})

    def rollback_tx(self):
        return self._execute({"cmd": "rollback_tx"})

    @contextmanager
    def transaction(self):
        """Context manager for transactions. Auto-rolls back on exception."""
        self.begin_tx()
        try:
            yield
            self.commit_tx()
        except Exception:
            try:
                self.rollback_tx()
            except OxiDbError:
                pass
            raise

    # ------------------------------------------------------------------
    # Blob storage
    # ------------------------------------------------------------------

    def create_bucket(self, bucket: str):
        return self._execute({"cmd": "create_bucket", "bucket": bucket})

    def list_buckets(self) -> list:
        return self._execute({"cmd": "list_buckets"})

    def delete_bucket(self, bucket: str):
        return self._execute({"cmd": "delete_bucket", "bucket": bucket})

    def put_object(self, bucket: str, key: str, data: bytes,
                   content_type: str = "application/octet-stream",
                   metadata: dict = None):
        import base64
        payload = {
            "cmd": "put_object", "bucket": bucket, "key": key,
            "data": base64.b64encode(data).decode("ascii"),
            "content_type": content_type,
        }
        if metadata:
            payload["metadata"] = metadata
        return self._execute(payload)

    def get_object(self, bucket: str, key: str) -> tuple:
        import base64
        result = self._execute({"cmd": "get_object", "bucket": bucket, "key": key})
        data = base64.b64decode(result["content"])
        return data, result["metadata"]

    def head_object(self, bucket: str, key: str) -> dict:
        return self._execute({"cmd": "head_object", "bucket": bucket, "key": key})

    def delete_object(self, bucket: str, key: str):
        return self._execute({"cmd": "delete_object", "bucket": bucket, "key": key})

    def list_objects(self, bucket: str, prefix: str = None, limit: int = None) -> list:
        payload = {"cmd": "list_objects", "bucket": bucket}
        if prefix is not None:
            payload["prefix"] = prefix
        if limit is not None:
            payload["limit"] = limit
        return self._execute(payload)
