#!/usr/bin/env python3
"""
ACID compliance integration tests for OxiDB — Python edition.

Automatically builds and starts oxidb-server, runs all tests, then stops it.

Usage:
    python3 python/test_acid.py
"""

import atexit
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import traceback

# Allow running from repo root or from python/
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, SCRIPT_DIR)
from oxidb import OxiDbClient, OxiDbError, TransactionConflictError

HOST = "127.0.0.1"
PORT = 0  # assigned during server startup
_server_proc = None
_tmp_dir = None


def client() -> OxiDbClient:
    return OxiDbClient(HOST, PORT)


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------

def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_port(host, port, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def start_server():
    global PORT, _server_proc, _tmp_dir

    PORT = find_free_port()
    _tmp_dir = tempfile.mkdtemp(prefix="oxidb_test_")

    # Build first
    print("Building oxidb-server...")
    build = subprocess.run(
        ["cargo", "build", "--package", "oxidb-server"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if build.returncode != 0:
        print(f"Build failed:\n{build.stderr}")
        sys.exit(1)

    # Start server
    env = os.environ.copy()
    env["OXIDB_ADDR"] = f"{HOST}:{PORT}"
    env["OXIDB_DATA"] = _tmp_dir

    _server_proc = subprocess.Popen(
        ["cargo", "run", "--package", "oxidb-server"],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    atexit.register(stop_server)

    if not wait_for_port(HOST, PORT):
        print(f"ERROR: Server failed to start on {HOST}:{PORT}")
        stop_server()
        sys.exit(1)

    print(f"Server running on {HOST}:{PORT} (pid={_server_proc.pid}, data={_tmp_dir})\n")


def stop_server():
    global _server_proc, _tmp_dir
    if _server_proc and _server_proc.poll() is None:
        _server_proc.send_signal(signal.SIGTERM)
        try:
            _server_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _server_proc.kill()
            _server_proc.wait()
    _server_proc = None

    if _tmp_dir:
        import shutil
        shutil.rmtree(_tmp_dir, ignore_errors=True)
        _tmp_dir = None


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

passed = 0
failed = 0
errors = []


def run_test(name, fn):
    global passed, failed
    try:
        fn()
        passed += 1
        print(f"  PASS  {name}")
    except Exception as e:
        failed += 1
        errors.append((name, e))
        print(f"  FAIL  {name}: {e}")
        traceback.print_exc()
        print()


def cleanup(*collections):
    with client() as c:
        for col in collections:
            try:
                c.drop_collection(col)
            except OxiDbError:
                pass


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

def test_ping():
    with client() as c:
        assert c.ping() == "pong"


def test_atomicity_commit():
    cleanup("t_atom_users", "t_atom_orders")
    with client() as c:
        c.begin_tx()
        c.insert("t_atom_users", {"name": "Alice"})
        c.insert("t_atom_orders", {"item": "Widget", "user": "Alice"})
        c.commit_tx()
    with client() as c:
        users = c.find("t_atom_users", {})
        orders = c.find("t_atom_orders", {})
        assert len(users) == 1 and users[0]["name"] == "Alice"
        assert len(orders) == 1 and orders[0]["item"] == "Widget"
    cleanup("t_atom_users", "t_atom_orders")


def test_atomicity_rollback():
    cleanup("t_rb_users", "t_rb_orders")
    with client() as c:
        c.begin_tx()
        c.insert("t_rb_users", {"name": "Bob"})
        c.insert("t_rb_orders", {"item": "Gadget", "user": "Bob"})
        c.rollback_tx()
    with client() as c:
        assert len(c.find("t_rb_users", {})) == 0
        assert len(c.find("t_rb_orders", {})) == 0
    cleanup("t_rb_users", "t_rb_orders")


def test_atomicity_disconnect_auto_rollback():
    cleanup("t_disc_users")
    c1 = client()
    c1.begin_tx()
    c1.insert("t_disc_users", {"name": "Ghost"})
    c1.close()
    time.sleep(0.1)
    with client() as c2:
        assert len(c2.find("t_disc_users", {})) == 0
    cleanup("t_disc_users")


def test_consistency_version_conflict():
    cleanup("t_occ_accounts")
    with client() as c:
        c.insert("t_occ_accounts", {"owner": "Alice", "balance": 100})
    c1 = client()
    c1.begin_tx()
    c1.find("t_occ_accounts", {"owner": "Alice"})
    with client() as c2:
        c2.update("t_occ_accounts", {"owner": "Alice"}, {"$set": {"balance": 200}})
    c1.update("t_occ_accounts", {"owner": "Alice"}, {"$set": {"balance": 150}})
    try:
        c1.commit_tx()
        assert False, "expected TransactionConflictError"
    except TransactionConflictError:
        pass
    finally:
        c1.close()
    cleanup("t_occ_accounts")


def test_isolation_uncommitted_not_visible():
    cleanup("t_iso_items")
    c1 = client()
    c1.begin_tx()
    c1.insert("t_iso_items", {"name": "Secret"})
    with client() as c2:
        assert len(c2.find("t_iso_items", {})) == 0
    c1.commit_tx()
    c1.close()
    with client() as c3:
        docs = c3.find("t_iso_items", {})
        assert len(docs) == 1 and docs[0]["name"] == "Secret"
    cleanup("t_iso_items")


def test_isolation_concurrent_tx_no_conflict():
    cleanup("t_conc_a", "t_conc_b")
    c1, c2 = client(), client()
    c1.begin_tx()
    c2.begin_tx()
    c1.insert("t_conc_a", {"x": 1})
    c2.insert("t_conc_b", {"y": 2})
    c1.commit_tx()
    c2.commit_tx()
    c1.close()
    c2.close()
    with client() as r:
        assert len(r.find("t_conc_a", {})) == 1
        assert len(r.find("t_conc_b", {})) == 1
    cleanup("t_conc_a", "t_conc_b")


def test_tx_insert_update_delete_commit():
    cleanup("t_multi")
    with client() as c:
        c.insert("t_multi", {"name": "Alpha", "v": 1})
        c.insert("t_multi", {"name": "Beta", "v": 1})
    with client() as c:
        c.begin_tx()
        c.insert("t_multi", {"name": "Gamma", "v": 1})
        c.update("t_multi", {"name": "Alpha"}, {"$set": {"v": 2}})
        c.delete("t_multi", {"name": "Beta"})
        c.commit_tx()
    with client() as c:
        docs = c.find("t_multi", {})
        names = {d["name"] for d in docs}
        assert names == {"Alpha", "Gamma"}
        alpha = [d for d in docs if d["name"] == "Alpha"][0]
        assert alpha["v"] == 2
    cleanup("t_multi")


def test_transaction_context_manager():
    cleanup("t_ctx")
    with client() as c:
        with c.transaction():
            c.insert("t_ctx", {"a": 1})
            c.insert("t_ctx", {"b": 2})
    with client() as c:
        assert len(c.find("t_ctx", {})) == 2
    cleanup("t_ctx")


def test_transaction_context_manager_rollback():
    cleanup("t_ctx_rb")
    with client() as c:
        try:
            with c.transaction():
                c.insert("t_ctx_rb", {"a": 1})
                raise ValueError("intentional error")
        except ValueError:
            pass
    with client() as c:
        assert len(c.find("t_ctx_rb", {})) == 0
    cleanup("t_ctx_rb")


def test_double_begin_rejected():
    with client() as c:
        c.begin_tx()
        try:
            c.begin_tx()
            assert False, "expected OxiDbError"
        except OxiDbError as e:
            assert "already active" in str(e).lower()


def test_commit_without_begin_rejected():
    with client() as c:
        try:
            c.commit_tx()
            assert False, "expected OxiDbError"
        except OxiDbError as e:
            assert "no active transaction" in str(e).lower()


def test_crud_no_tx():
    cleanup("t_crud")
    with client() as c:
        result = c.insert("t_crud", {"name": "Alice", "age": 30})
        assert "id" in result
        c.insert_many("t_crud", [{"name": "Bob", "age": 25}, {"name": "Charlie", "age": 35}])
        assert len(c.find("t_crud", {})) == 3
        assert c.find("t_crud", {"name": "Alice"})[0]["age"] == 30
        doc = c.find_one("t_crud", {"name": "Bob"})
        assert doc is not None and doc["age"] == 25
        assert c.count("t_crud") == 3
        assert c.update("t_crud", {"name": "Alice"}, {"$set": {"age": 31}})["modified"] == 1
        assert c.find_one("t_crud", {"name": "Alice"})["age"] == 31
        assert c.delete("t_crud", {"name": "Charlie"})["deleted"] == 1
        assert c.count("t_crud") == 2
    cleanup("t_crud")


def test_find_with_sort_skip_limit():
    cleanup("t_opts")
    with client() as c:
        for i in range(5):
            c.insert("t_opts", {"idx": i, "val": f"item_{i}"})
        docs = c.find("t_opts", {}, sort={"idx": -1}, limit=3)
        assert len(docs) == 3 and docs[0]["idx"] == 4 and docs[2]["idx"] == 2
        docs = c.find("t_opts", {}, sort={"idx": 1}, skip=2, limit=2)
        assert len(docs) == 2 and docs[0]["idx"] == 2 and docs[1]["idx"] == 3
    cleanup("t_opts")


def test_indexes():
    cleanup("t_idx")
    with client() as c:
        c.insert("t_idx", {"name": "Alice", "age": 30, "city": "NYC"})
        c.create_index("t_idx", "name")
        c.create_unique_index("t_idx", "age")
        c.create_composite_index("t_idx", ["name", "city"])
        assert len(c.find("t_idx", {"name": "Alice"})) == 1
    cleanup("t_idx")


def test_collection_management():
    cleanup("t_col_mgmt")
    with client() as c:
        c.create_collection("t_col_mgmt")
        assert "t_col_mgmt" in c.list_collections()
        c.drop_collection("t_col_mgmt")
        assert "t_col_mgmt" not in c.list_collections()


def test_compact():
    cleanup("t_compact")
    with client() as c:
        for i in range(10):
            c.insert("t_compact", {"i": i})
        c.delete("t_compact", {"i": {"$gte": 5}})
        result = c.compact("t_compact")
        assert result["docs_kept"] == 5
    cleanup("t_compact")


def test_aggregate():
    cleanup("t_agg")
    with client() as c:
        c.insert_many("t_agg", [
            {"dept": "eng", "salary": 100},
            {"dept": "eng", "salary": 120},
            {"dept": "sales", "salary": 80},
        ])
        result = c.aggregate("t_agg", [{"$match": {"dept": "eng"}}])
        assert len(result) == 2
    cleanup("t_agg")


def test_blob_storage():
    with client() as c:
        c.create_bucket("test-bucket")
        assert "test-bucket" in c.list_buckets()
        data = b"Hello, OxiDB blobs!"
        c.put_object("test-bucket", "greeting.txt", data,
                      content_type="text/plain", metadata={"author": "test"})
        retrieved, meta = c.get_object("test-bucket", "greeting.txt")
        assert retrieved == data
        assert c.head_object("test-bucket", "greeting.txt") is not None
        keys = [o["key"] for o in c.list_objects("test-bucket")]
        assert "greeting.txt" in keys
        assert len(c.list_objects("test-bucket", prefix="greet")) >= 1
        c.delete_object("test-bucket", "greeting.txt")
        c.delete_bucket("test-bucket")
        assert "test-bucket" not in c.list_buckets()


def test_full_text_search():
    with client() as c:
        c.create_bucket("fts-bucket")
        c.put_object("fts-bucket", "doc1.txt",
                      b"The quick brown fox jumps over the lazy dog",
                      content_type="text/plain")
        time.sleep(0.2)
        results = c.search("quick brown fox", bucket="fts-bucket")
        assert len(results) >= 1 and results[0]["key"] == "doc1.txt"
        c.delete_object("fts-bucket", "doc1.txt")
        c.delete_bucket("fts-bucket")


# ---------------------------------------------------------------------------
# Stress / benchmark tests
# ---------------------------------------------------------------------------

STRESS_COUNT = 100_000
BATCH_SIZE = 1000


def test_stress_100k_insert():
    """Insert 100K records one-by-one and measure throughput."""
    cleanup("t_stress")
    with client() as c:
        t0 = time.monotonic()
        for i in range(STRESS_COUNT):
            c.insert("t_stress", {"i": i, "name": f"user_{i}", "score": i * 0.1})
        elapsed = time.monotonic() - t0
        rate = STRESS_COUNT / elapsed
        print(f"    -> {STRESS_COUNT:,} single inserts in {elapsed:.2f}s ({rate:,.0f} ops/s)")

        # verify count
        n = c.count("t_stress")
        assert n == STRESS_COUNT, f"expected {STRESS_COUNT}, got {n}"
    cleanup("t_stress")


def test_stress_100k_insert_many():
    """Insert 100K records via insert_many in batches and measure throughput."""
    cleanup("t_stress_batch")
    with client() as c:
        t0 = time.monotonic()
        for batch_start in range(0, STRESS_COUNT, BATCH_SIZE):
            batch = [
                {"i": i, "name": f"user_{i}", "score": i * 0.1}
                for i in range(batch_start, min(batch_start + BATCH_SIZE, STRESS_COUNT))
            ]
            c.insert_many("t_stress_batch", batch)
        elapsed = time.monotonic() - t0
        rate = STRESS_COUNT / elapsed
        print(f"    -> {STRESS_COUNT:,} batch inserts ({BATCH_SIZE}/batch) in {elapsed:.2f}s ({rate:,.0f} docs/s)")

        n = c.count("t_stress_batch")
        assert n == STRESS_COUNT, f"expected {STRESS_COUNT}, got {n}"
    cleanup("t_stress_batch")


def test_stress_100k_find():
    """Insert 100K records then measure find/count performance."""
    cleanup("t_stress_read")
    with client() as c:
        # bulk load
        for batch_start in range(0, STRESS_COUNT, BATCH_SIZE):
            batch = [
                {"i": i, "name": f"user_{i}", "category": "even" if i % 2 == 0 else "odd"}
                for i in range(batch_start, min(batch_start + BATCH_SIZE, STRESS_COUNT))
            ]
            c.insert_many("t_stress_read", batch)

        # count all
        t0 = time.monotonic()
        n = c.count("t_stress_read")
        elapsed_count = time.monotonic() - t0
        assert n == STRESS_COUNT
        print(f"    -> count({STRESS_COUNT:,}) in {elapsed_count:.3f}s")

        # filtered find with limit
        t0 = time.monotonic()
        docs = c.find("t_stress_read", {"category": "even"}, limit=100)
        elapsed_find = time.monotonic() - t0
        assert len(docs) == 100
        assert all(d["category"] == "even" for d in docs)
        print(f"    -> find(category=even, limit=100) in {elapsed_find:.3f}s")

        # find_one
        t0 = time.monotonic()
        doc = c.find_one("t_stress_read", {"i": 50000})
        elapsed_one = time.monotonic() - t0
        assert doc is not None and doc["i"] == 50000
        print(f"    -> find_one(i=50000) in {elapsed_one:.3f}s")

    cleanup("t_stress_read")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

TESTS = [
    ("ping", test_ping),
    ("crud_no_tx", test_crud_no_tx),
    ("find_with_sort_skip_limit", test_find_with_sort_skip_limit),
    ("indexes", test_indexes),
    ("collection_management", test_collection_management),
    ("compact", test_compact),
    ("aggregate", test_aggregate),
    ("atomicity_commit", test_atomicity_commit),
    ("atomicity_rollback", test_atomicity_rollback),
    ("atomicity_disconnect_auto_rollback", test_atomicity_disconnect_auto_rollback),
    ("consistency_version_conflict", test_consistency_version_conflict),
    ("isolation_uncommitted_not_visible", test_isolation_uncommitted_not_visible),
    ("isolation_concurrent_tx_no_conflict", test_isolation_concurrent_tx_no_conflict),
    ("tx_insert_update_delete_commit", test_tx_insert_update_delete_commit),
    ("transaction_context_manager", test_transaction_context_manager),
    ("transaction_context_manager_rollback", test_transaction_context_manager_rollback),
    ("double_begin_rejected", test_double_begin_rejected),
    ("commit_without_begin_rejected", test_commit_without_begin_rejected),
    ("blob_storage", test_blob_storage),
    ("full_text_search", test_full_text_search),
    ("stress_100k_insert", test_stress_100k_insert),
    ("stress_100k_insert_many", test_stress_100k_insert_many),
    ("stress_100k_find", test_stress_100k_find),
]


def main():
    start_server()

    print(f"Running {len(TESTS)} tests...\n")
    for name, fn in TESTS:
        run_test(name, fn)

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed out of {passed + failed}")

    if errors:
        print(f"\nFailed tests:")
        for name, e in errors:
            print(f"  - {name}: {e}")

    stop_server()
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
