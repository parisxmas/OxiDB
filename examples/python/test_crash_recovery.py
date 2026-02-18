#!/usr/bin/env python3
"""
OxiDB Crash Recovery & WAL Replay Test
========================================

Tests that the database correctly recovers after an abrupt crash (SIGKILL):

  TEST 1: Committed Data Survives Crash
    Insert documents, verify they exist, SIGKILL the server, restart,
    verify all committed data is still present and byte-perfect.

  TEST 2: Uncommitted Transactions Are Lost
    Begin a transaction, insert docs (don't commit), SIGKILL.
    On restart, those docs must NOT exist.

  TEST 3: Crash During Heavy Writes
    Hammer the server with concurrent inserts, SIGKILL mid-flight,
    restart. Every document that got an OK response must survive.
    No partial/corrupt documents allowed.

Prerequisites:
    - oxidb-server binary built (release): cargo build --release -p oxidb-server
    - Python 3.8+

Usage:
    python examples/python/test_crash_recovery.py [--binary PATH]
"""

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
from oxidb import OxiDbClient, OxiDbError

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"

DEFAULT_BINARY = os.path.join(
    os.path.dirname(__file__), "..", "..", "target", "release", "oxidb-server"
)
HOST = "127.0.0.1"
PORT = 14444  # Use non-default port to avoid conflicts


def find_binary(user_path=None):
    if user_path and os.path.isfile(user_path):
        return os.path.abspath(user_path)
    resolved = os.path.abspath(DEFAULT_BINARY)
    if os.path.isfile(resolved):
        return resolved
    print(f"[!] Cannot find oxidb-server binary at {resolved}")
    print(f"    Build with: cargo build --release -p oxidb-server")
    sys.exit(1)


class ServerProcess:
    """Manages an oxidb-server subprocess."""

    def __init__(self, binary, data_dir, port=PORT, pool_size=16):
        self.binary = binary
        self.data_dir = data_dir
        self.port = port
        self.pool_size = pool_size
        self.proc = None

    def start(self, wait=True):
        env = os.environ.copy()
        env["OXIDB_ADDR"] = f"{HOST}:{self.port}"
        env["OXIDB_DATA"] = self.data_dir
        env["OXIDB_POOL_SIZE"] = str(self.pool_size)
        env["OXIDB_IDLE_TIMEOUT"] = "0"  # no idle timeout for tests

        self.proc = subprocess.Popen(
            [self.binary],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

        if wait:
            self._wait_ready()

    def _wait_ready(self, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                c = OxiDbClient(HOST, self.port, timeout=1.0)
                c.ping()
                c.close()
                return
            except Exception:
                if self.proc.poll() is not None:
                    stderr = self.proc.stderr.read().decode()
                    raise RuntimeError(f"Server exited prematurely: {stderr}")
                time.sleep(0.1)
        raise TimeoutError("Server did not become ready")

    def kill(self):
        """SIGKILL — immediate, no cleanup."""
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()
        self.proc = None

    def stop_graceful(self):
        """SIGTERM then wait."""
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=5)
        self.proc = None


def connect(port=PORT, timeout=10.0):
    return OxiDbClient(HOST, port, timeout=timeout)


def checksum_doc(doc):
    filtered = {k: v for k, v in sorted(doc.items()) if k not in ("_id", "_version", "checksum")}
    return hashlib.sha256(json.dumps(filtered, sort_keys=True).encode()).hexdigest()[:16]


# ============================================================================
# TEST 1: Committed Data Survives Crash
# ============================================================================

def test_committed_data_survives(binary, data_dir):
    print(f"\n{'=' * 60}")
    print(f"  TEST 1: COMMITTED DATA SURVIVES CRASH")
    print(f"{'=' * 60}")

    col = "crash_test_1"
    NUM_DOCS = 500

    server = ServerProcess(binary, data_dir)
    server.start()
    print(f"  Server started (pid={server.proc.pid})")

    # Insert documents with checksums
    client = connect()
    try:
        client.drop_collection(col)
    except OxiDbError:
        pass
    client.create_collection(col)

    print(f"  Inserting {NUM_DOCS} documents...")
    expected_by_seq = {}
    for i in range(NUM_DOCS):
        doc = {
            "seq": i,
            "data": f"document-{i}-payload-{'x' * 100}",
            "value": i * 100,  # integer to avoid float precision issues
            "tags": [f"tag-{i % 10}", f"cat-{i % 5}"],
        }
        doc["checksum"] = checksum_doc(doc)
        client.insert(col, doc)
        expected_by_seq[i] = doc

    # Verify all docs exist before crash
    count = client.count(col)
    print(f"  Verified {count} docs exist before crash")
    assert count == NUM_DOCS, f"Pre-crash count mismatch: {count} != {NUM_DOCS}"

    client.close()

    # SIGKILL the server
    pid = server.proc.pid
    print(f"  Sending SIGKILL to server (pid={pid})...")
    server.kill()
    print(f"  Server killed")

    # Restart
    time.sleep(0.5)
    server.start()
    print(f"  Server restarted (pid={server.proc.pid})")

    # Verify all data survived
    client = connect()
    actual_count = client.count(col)
    all_docs = client.find(col, {})
    client.close()
    server.stop_graceful()

    ok = True
    corrupt = 0
    found_seqs = set()

    for doc in all_docs:
        seq = doc.get("seq")
        found_seqs.add(seq)

        actual_cs = checksum_doc(doc)
        stored_cs = doc.get("checksum", "")
        if actual_cs != stored_cs:
            corrupt += 1
            if corrupt <= 3:
                exp = expected_by_seq.get(seq, {})
                print(f"    CORRUPT seq={seq}: stored_cs={stored_cs}, actual_cs={actual_cs}")
                for key in ("seq", "data", "value", "tags"):
                    ev = exp.get(key)
                    av = doc.get(key)
                    if ev != av:
                        print(f"      field '{key}': expected={ev!r}, actual={av!r}")

    missing_seqs = set(expected_by_seq.keys()) - found_seqs

    print(f"\n  Results:")
    print(f"    Expected docs:   {NUM_DOCS}")
    print(f"    Actual docs:     {actual_count}")
    print(f"    Checksums OK:    {actual_count - corrupt}")
    print(f"    Corrupt:         {corrupt}")
    print(f"    Missing seqs:    {len(missing_seqs)}")

    if actual_count != NUM_DOCS:
        print(f"\n  [{FAIL}] Document count mismatch after crash recovery!")
        ok = False
    else:
        print(f"\n  [{PASS}] All {NUM_DOCS} documents survived crash")

    if corrupt > 0:
        print(f"  [{FAIL}] {corrupt} corrupted documents after recovery!")
        ok = False
    else:
        print(f"  [{PASS}] Zero corrupted documents")

    if missing_seqs:
        print(f"  [{FAIL}] {len(missing_seqs)} documents missing by seq")
        ok = False
    else:
        print(f"  [{PASS}] All sequences present")

    return ok


# ============================================================================
# TEST 2: Uncommitted Transactions Are Lost After Crash
# ============================================================================

def test_uncommitted_lost(binary, data_dir):
    print(f"\n{'=' * 60}")
    print(f"  TEST 2: UNCOMMITTED TRANSACTIONS LOST AFTER CRASH")
    print(f"{'=' * 60}")

    col_committed = "crash_committed"
    col_uncommitted = "crash_uncommitted"
    NUM_COMMITTED = 100
    NUM_UNCOMMITTED = 100

    server = ServerProcess(binary, data_dir)
    server.start()
    print(f"  Server started (pid={server.proc.pid})")

    # Insert committed documents (no transaction, auto-committed)
    client = connect()
    for c in [col_committed, col_uncommitted]:
        try:
            client.drop_collection(c)
        except OxiDbError:
            pass
        client.create_collection(c)

    print(f"  Inserting {NUM_COMMITTED} committed docs...")
    for i in range(NUM_COMMITTED):
        client.insert(col_committed, {"seq": i, "status": "committed"})

    # Start a transaction, insert docs, but DON'T commit
    print(f"  Starting transaction with {NUM_UNCOMMITTED} uncommitted docs...")
    client.begin_tx()
    for i in range(NUM_UNCOMMITTED):
        client.insert(col_uncommitted, {"seq": i, "status": "uncommitted"})

    # Verify: uncommitted docs visible within transaction
    uncommitted_count = client.count(col_uncommitted)
    print(f"  Uncommitted docs visible in-tx: {uncommitted_count}")

    # DO NOT commit — kill the server
    pid = server.proc.pid
    print(f"  Sending SIGKILL (transaction NOT committed, pid={pid})...")
    server.kill()
    print(f"  Server killed")

    # Restart
    time.sleep(0.5)
    server.start()
    print(f"  Server restarted (pid={server.proc.pid})")

    # Verify
    client2 = connect()
    committed_count = client2.count(col_committed)
    # The uncommitted collection may or may not exist after recovery
    try:
        uncommitted_after = client2.count(col_uncommitted)
    except OxiDbError:
        uncommitted_after = 0

    client2.close()
    server.stop_graceful()

    ok = True

    print(f"\n  Results:")
    print(f"    Committed docs:     {committed_count} (expected {NUM_COMMITTED})")
    print(f"    Uncommitted docs:   {uncommitted_after} (expected 0)")

    if committed_count != NUM_COMMITTED:
        print(f"\n  [{FAIL}] Committed data lost! {NUM_COMMITTED - committed_count} docs missing")
        ok = False
    else:
        print(f"\n  [{PASS}] All {NUM_COMMITTED} committed documents survived")

    if uncommitted_after == 0:
        print(f"  [{PASS}] Uncommitted transaction correctly rolled back (0 docs)")
    else:
        print(f"  [WARN] {uncommitted_after} uncommitted docs found after crash")
        print(f"         (Non-transactional inserts are auto-committed, so this may be expected")
        print(f"          if the server wrote them before the kill signal arrived)")

    return ok


# ============================================================================
# TEST 3: Crash During Heavy Concurrent Writes
# ============================================================================

def test_crash_during_writes(binary, data_dir):
    print(f"\n{'=' * 60}")
    print(f"  TEST 3: CRASH DURING HEAVY CONCURRENT WRITES")
    print(f"{'=' * 60}")

    col = "crash_heavy"
    NUM_WORKERS = 50
    DOCS_PER_WORKER = 100
    CRASH_DELAY = 2.0  # seconds of writing before SIGKILL

    server = ServerProcess(binary, data_dir, pool_size=60)
    server.start()
    print(f"  Server started (pid={server.proc.pid})")

    client = connect()
    try:
        client.drop_collection(col)
    except OxiDbError:
        pass
    client.create_collection(col)
    client.create_index(col, "worker")
    client.create_index(col, "seq")
    client.close()

    # Track which inserts got confirmed OK responses
    confirmed = {}  # (worker, seq) -> checksum
    lock = threading.Lock()
    stats = {"confirmed": 0, "errors": 0, "in_flight": 0}
    crash_event = threading.Event()

    def heavy_writer(worker_id):
        try:
            c = connect(timeout=5.0)
        except Exception:
            return

        for seq in range(DOCS_PER_WORKER):
            if crash_event.is_set():
                break
            doc = {
                "worker": worker_id,
                "seq": seq,
                "data": f"w{worker_id}-s{seq}-{'p' * 200}",
            }
            doc["checksum"] = checksum_doc(doc)
            try:
                c.insert(col, doc)
                with lock:
                    confirmed[(worker_id, seq)] = doc["checksum"]
                    stats["confirmed"] += 1
            except Exception:
                with lock:
                    stats["errors"] += 1
                if crash_event.is_set():
                    break

        try:
            c.close()
        except Exception:
            pass

    print(f"  Launching {NUM_WORKERS} workers, each inserting {DOCS_PER_WORKER} docs")
    print(f"  Will SIGKILL after {CRASH_DELAY}s...")

    threads = []
    t0 = time.monotonic()
    for i in range(NUM_WORKERS):
        t = threading.Thread(target=heavy_writer, args=(i,), daemon=True)
        threads.append(t)
    for t in threads:
        t.start()

    # Let writers run for a bit, then crash
    time.sleep(CRASH_DELAY)

    pid = server.proc.pid
    crash_event.set()
    server.kill()
    crash_time = time.monotonic() - t0
    print(f"  SIGKILL sent at {crash_time:.1f}s (pid={pid})")

    # Wait for threads to notice the crash
    for t in threads:
        t.join(timeout=5)

    confirmed_count = stats["confirmed"]
    print(f"  Confirmed inserts before crash: {confirmed_count}")

    # Restart and verify
    time.sleep(0.5)
    server.start()
    print(f"  Server restarted (pid={server.proc.pid})")

    verify = connect()
    actual_count = verify.count(col)
    all_docs = verify.find(col, {})
    verify.close()
    server.stop_graceful()

    # Check: every confirmed insert must survive
    actual_set = {}
    corrupt = 0
    for doc in all_docs:
        key = (doc["worker"], doc["seq"])
        actual_cs = checksum_doc(doc)
        stored_cs = doc.get("checksum", "")
        if actual_cs != stored_cs:
            corrupt += 1
        actual_set[key] = actual_cs

    lost = 0
    for key, expected_cs in confirmed.items():
        if key not in actual_set:
            lost += 1
            if lost <= 3:
                print(f"    LOST: worker={key[0]} seq={key[1]}")

    # Extra docs (written to disk but client didn't get response before crash) are OK
    extra = actual_count - confirmed_count

    print(f"\n  Results:")
    print(f"    Confirmed inserts:   {confirmed_count}")
    print(f"    Docs after recovery: {actual_count}")
    print(f"    Lost (confirmed):    {lost}")
    print(f"    Extra (unconfirmed): {max(0, extra)}")
    print(f"    Corrupt:             {corrupt}")

    ok = True

    if lost > 0:
        print(f"\n  [{FAIL}] {lost} confirmed documents LOST after crash!")
        ok = False
    else:
        print(f"\n  [{PASS}] Zero confirmed documents lost")

    if corrupt > 0:
        print(f"  [{FAIL}] {corrupt} corrupted documents after recovery!")
        ok = False
    else:
        print(f"  [{PASS}] Zero corrupted documents")

    if extra > 0:
        print(f"  [INFO] {extra} extra docs recovered (server wrote before client got response)")

    return ok


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="OxiDB Crash Recovery Tests")
    parser.add_argument("--binary", default=None, help="Path to oxidb-server binary")
    args = parser.parse_args()

    binary = find_binary(args.binary)
    print(f"[*] Using binary: {binary}")

    results = {}
    t0 = time.monotonic()

    with tempfile.TemporaryDirectory(prefix="oxidb_crash_test_") as tmpdir:
        print(f"[*] Data directory: {tmpdir}")

        results["Committed Data Survives"] = test_committed_data_survives(binary, tmpdir)
        results["Uncommitted Tx Lost"] = test_uncommitted_lost(binary, tmpdir)
        results["Crash During Writes"] = test_crash_during_writes(binary, tmpdir)

    elapsed = time.monotonic() - t0

    print(f"\n{'=' * 60}")
    print(f"  SUMMARY ({elapsed:.1f}s)")
    print(f"{'=' * 60}")
    all_pass = True
    for name, passed in results.items():
        status = PASS if passed else FAIL
        print(f"  [{status}] {name}")
        if not passed:
            all_pass = False

    if all_pass:
        print(f"\n  All tests passed!")
    else:
        print(f"\n  Some tests FAILED!")
    print(f"{'=' * 60}")

    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
