#!/usr/bin/env python3
"""
OxiDB Encryption at Rest Test
================================

Verifies that encryption at rest actually works:

  TEST 1: Plaintext Never Appears on Disk
    Write known data with encryption enabled, scan raw .dat and .wal
    files — the plaintext must never appear in any file on disk.

  TEST 2: Encrypted Data Readable After Restart
    Write data, stop server, restart with the same key — all data
    must be intact and byte-perfect (WAL replay with decryption).

  TEST 3: Wrong Key Fails to Read
    Write data with key A, stop server, restart with key B — the
    server should fail or return garbage (AES-GCM auth tag mismatch).

  TEST 4: Encryption Survives Crash (SIGKILL)
    Write data with encryption, SIGKILL mid-operation, restart with
    same key — committed data must survive and be readable.

  TEST 5: Unencrypted vs Encrypted File Sizes
    Same data written with and without encryption. Encrypted files
    must be larger (nonce + auth tag overhead per record).

Prerequisites:
    - oxidb-server binary built (release): cargo build --release -p oxidb-server
    - Python 3.8+

Usage:
    python examples/python/test_encryption.py [--binary PATH]
"""

import argparse
import os
import signal
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
from oxidb import OxiDbClient, OxiDbError

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"

DEFAULT_BINARY = os.path.join(
    os.path.dirname(__file__), "..", "..", "target", "release", "oxidb-server"
)
HOST = "127.0.0.1"
BASE_PORT = 14555  # Use unique port range to avoid conflicts


def find_binary(user_path=None):
    if user_path and os.path.isfile(user_path):
        return os.path.abspath(user_path)
    resolved = os.path.abspath(DEFAULT_BINARY)
    if os.path.isfile(resolved):
        return resolved
    print(f"[!] Cannot find oxidb-server binary at {resolved}")
    print(f"    Build with: cargo build --release -p oxidb-server")
    sys.exit(1)


def generate_key(path):
    """Generate a random 32-byte AES-256 key file."""
    key_bytes = os.urandom(32)
    with open(path, "wb") as f:
        f.write(key_bytes)
    return path


class ServerProcess:
    def __init__(self, binary, data_dir, port, encryption_key_path=None, pool_size=8):
        self.binary = binary
        self.data_dir = data_dir
        self.port = port
        self.encryption_key_path = encryption_key_path
        self.pool_size = pool_size
        self.proc = None

    def start(self, wait=True):
        env = os.environ.copy()
        env["OXIDB_ADDR"] = f"{HOST}:{self.port}"
        env["OXIDB_DATA"] = self.data_dir
        env["OXIDB_POOL_SIZE"] = str(self.pool_size)
        env["OXIDB_IDLE_TIMEOUT"] = "0"
        if self.encryption_key_path:
            env["OXIDB_ENCRYPTION_KEY"] = self.encryption_key_path

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
                    raise RuntimeError(f"Server exited: {stderr}")
                time.sleep(0.1)
        raise TimeoutError("Server did not become ready")

    def kill(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()
        self.proc = None

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.proc = None

    def is_running(self):
        return self.proc and self.proc.poll() is None


def connect(port, timeout=10.0):
    return OxiDbClient(HOST, port, timeout=timeout)


def scan_files_for_plaintext(directory, needles):
    """
    Scan all files under directory for any of the needle strings.
    Returns dict: {needle: [list of files containing it]}.
    """
    found = {n: [] for n in needles}
    for root, dirs, files in os.walk(directory):
        for fname in files:
            fpath = os.path.join(root, fname)
            try:
                with open(fpath, "rb") as f:
                    content = f.read()
            except (PermissionError, IsADirectoryError):
                continue
            for needle in needles:
                if isinstance(needle, str):
                    needle_bytes = needle.encode("utf-8")
                else:
                    needle_bytes = needle
                if needle_bytes in content:
                    found[needle].append(fpath)
    return found


def total_data_size(directory):
    """Sum of all .dat and .wal file sizes under directory."""
    total = 0
    for root, dirs, files in os.walk(directory):
        for fname in files:
            if fname.endswith(".dat") or fname.endswith(".wal"):
                total += os.path.getsize(os.path.join(root, fname))
    return total


# ============================================================================
# TEST 1: Plaintext Never Appears on Disk
# ============================================================================

def test_no_plaintext_on_disk(binary, tmpdir):
    print(f"\n{'=' * 60}")
    print(f"  TEST 1: PLAINTEXT NEVER APPEARS ON DISK")
    print(f"{'=' * 60}")

    port = BASE_PORT
    data_dir = os.path.join(tmpdir, "enc_test1")
    os.makedirs(data_dir, exist_ok=True)
    key_path = os.path.join(tmpdir, "test1.key")
    generate_key(key_path)

    server = ServerProcess(binary, data_dir, port, encryption_key_path=key_path)
    server.start()
    print(f"  Server started with encryption (pid={server.proc.pid})")

    # Insert documents with distinctive, searchable plaintext
    client = connect(port)
    col = "secret_data"
    client.create_collection(col)

    # These are distinctive strings that should NEVER appear in raw files
    secrets = [
        "SUPER_SECRET_PASSWORD_12345",
        "credit_card_4111_1111_1111_1111",
        "social_security_123_45_6789",
        "api_key_sk_live_ABCDEFGHIJKLMNOP",
        "classified_document_top_secret_alpha",
    ]

    print(f"  Inserting {len(secrets)} documents with distinctive plaintext...")
    for i, secret in enumerate(secrets):
        client.insert(col, {
            "seq": i,
            "secret": secret,
            "description": f"This is secret record number {i}",
            "nested": {"sensitive": secret, "level": "top_secret"},
        })

    # Also do some updates to ensure WAL has the data too
    for i, secret in enumerate(secrets):
        client.update_one(col, {"seq": i}, {
            "$set": {"updated_secret": f"UPDATED_{secret}"}
        })

    client.close()
    server.stop()
    time.sleep(0.3)

    # Scan all files on disk for the plaintext
    print(f"  Scanning data directory for plaintext...")
    all_needles = secrets + [f"UPDATED_{s}" for s in secrets]
    # Also search for partial matches
    all_needles += ["SUPER_SECRET", "credit_card_4111", "social_security_123",
                    "sk_live_ABCDEF", "top_secret_alpha"]

    results = scan_files_for_plaintext(data_dir, all_needles)

    ok = True
    found_any = False
    for needle, files in results.items():
        if files:
            found_any = True
            rel_files = [os.path.relpath(f, data_dir) for f in files]
            print(f"    [{FAIL}] '{needle}' found in: {', '.join(rel_files)}")

    print(f"\n  Results:")
    print(f"    Needles searched:    {len(all_needles)}")
    print(f"    Plaintext found:     {'YES' if found_any else 'NONE'}")

    if found_any:
        print(f"\n  [{FAIL}] Plaintext data found in encrypted database files!")
        ok = False
    else:
        print(f"\n  [{PASS}] No plaintext found in any file on disk")

    # Also verify data dir has actual files (not empty)
    dat_files = []
    for root, dirs, files in os.walk(data_dir):
        for f in files:
            if f.endswith(".dat") or f.endswith(".wal"):
                dat_files.append(f)
    if len(dat_files) == 0:
        print(f"  [{FAIL}] No .dat/.wal files found — test may be invalid")
        ok = False
    else:
        print(f"  [{PASS}] Found {len(dat_files)} data/wal files (test is valid)")

    return ok


# ============================================================================
# TEST 2: Encrypted Data Readable After Restart
# ============================================================================

def test_readable_after_restart(binary, tmpdir):
    print(f"\n{'=' * 60}")
    print(f"  TEST 2: ENCRYPTED DATA READABLE AFTER RESTART")
    print(f"{'=' * 60}")

    port = BASE_PORT + 1
    data_dir = os.path.join(tmpdir, "enc_test2")
    os.makedirs(data_dir, exist_ok=True)
    key_path = os.path.join(tmpdir, "test2.key")
    generate_key(key_path)

    server = ServerProcess(binary, data_dir, port, encryption_key_path=key_path)
    server.start()
    print(f"  Server started with encryption (pid={server.proc.pid})")

    # Insert diverse data
    client = connect(port)
    col = "persist_test"
    client.create_collection(col)

    NUM_DOCS = 200
    print(f"  Inserting {NUM_DOCS} documents...")
    expected = {}
    for i in range(NUM_DOCS):
        doc = {
            "seq": i,
            "name": f"record-{i}",
            "value": i * 100,
            "tags": [f"t{i % 7}", f"g{i % 3}"],
            "nested": {"a": i, "b": f"nested-{i}"},
        }
        client.insert(col, doc)
        expected[i] = doc

    count_before = client.count(col)
    print(f"  Count before restart: {count_before}")
    client.close()

    # Stop and restart with same key
    server.stop()
    time.sleep(0.5)
    server.start()
    print(f"  Server restarted with same key (pid={server.proc.pid})")

    # Verify all data
    client = connect(port)
    count_after = client.count(col)
    all_docs = client.find(col, {})
    client.close()
    server.stop()

    ok = True
    mismatches = 0

    actual_by_seq = {d["seq"]: d for d in all_docs}
    for seq, exp in expected.items():
        actual = actual_by_seq.get(seq)
        if actual is None:
            mismatches += 1
            if mismatches <= 3:
                print(f"    MISSING: seq={seq}")
            continue
        for field in ("name", "value", "tags", "nested"):
            if exp[field] != actual.get(field):
                mismatches += 1
                if mismatches <= 3:
                    print(f"    MISMATCH: seq={seq} field={field}")
                    print(f"      expected: {exp[field]!r}")
                    print(f"      actual:   {actual.get(field)!r}")

    print(f"\n  Results:")
    print(f"    Docs before restart: {count_before}")
    print(f"    Docs after restart:  {count_after}")
    print(f"    Field mismatches:    {mismatches}")

    if count_after != NUM_DOCS:
        print(f"\n  [{FAIL}] Document count changed: {count_before} -> {count_after}")
        ok = False
    else:
        print(f"\n  [{PASS}] All {NUM_DOCS} documents survived restart")

    if mismatches > 0:
        print(f"  [{FAIL}] {mismatches} field mismatches after restart!")
        ok = False
    else:
        print(f"  [{PASS}] All data byte-perfect after encrypted restart")

    return ok


# ============================================================================
# TEST 3: Wrong Key Fails
# ============================================================================

def test_wrong_key_fails(binary, tmpdir):
    print(f"\n{'=' * 60}")
    print(f"  TEST 3: WRONG KEY FAILS TO READ DATA")
    print(f"{'=' * 60}")

    port = BASE_PORT + 2
    data_dir = os.path.join(tmpdir, "enc_test3")
    os.makedirs(data_dir, exist_ok=True)
    key_a_path = os.path.join(tmpdir, "test3_key_a.key")
    key_b_path = os.path.join(tmpdir, "test3_key_b.key")
    generate_key(key_a_path)
    generate_key(key_b_path)

    # Write data with key A
    server = ServerProcess(binary, data_dir, port, encryption_key_path=key_a_path)
    server.start()
    print(f"  Server started with Key A (pid={server.proc.pid})")

    client = connect(port)
    col = "wrong_key_test"
    client.create_collection(col)
    for i in range(50):
        client.insert(col, {"seq": i, "data": f"encrypted-with-key-a-{i}"})
    count_a = client.count(col)
    print(f"  Inserted {count_a} docs with Key A")
    client.close()
    server.stop()
    time.sleep(0.5)

    # Try to start with key B
    print(f"  Restarting with Key B (different key)...")
    server_b = ServerProcess(binary, data_dir, port, encryption_key_path=key_b_path)

    ok = True
    wrong_key_detected = False

    try:
        server_b.start(wait=True)
        # If server starts, try to read data
        try:
            client_b = connect(port, timeout=5.0)
            docs = client_b.find(col, {})
            # If we get here with correct data, the test fails
            if len(docs) == count_a:
                # Check if data is actually readable (not garbage)
                readable = all("seq" in d and "data" in d for d in docs)
                if readable:
                    print(f"    Server started and returned valid data with wrong key!")
                    ok = False
                else:
                    wrong_key_detected = True
            else:
                wrong_key_detected = True
            client_b.close()
        except (OxiDbError, ConnectionError, OSError) as e:
            wrong_key_detected = True
            print(f"    Read failed with wrong key: {type(e).__name__}")
        finally:
            server_b.stop()
    except (RuntimeError, TimeoutError) as e:
        # Server failed to start — expected with wrong key
        wrong_key_detected = True
        print(f"    Server failed to start with wrong key: {e}")
        server_b.stop()

    print(f"\n  Results:")
    print(f"    Wrong key detected: {'YES' if wrong_key_detected else 'NO'}")

    if wrong_key_detected:
        print(f"\n  [{PASS}] Wrong key correctly prevented data access")
    else:
        print(f"\n  [{FAIL}] Data accessible with wrong key — encryption broken!")
        ok = False

    # Verify correct key still works
    print(f"  Verifying Key A still works...")
    server_a = ServerProcess(binary, data_dir, port, encryption_key_path=key_a_path)
    try:
        server_a.start()
        client_a = connect(port)
        count_verify = client_a.count(col)
        client_a.close()
        server_a.stop()

        if count_verify == count_a:
            print(f"  [{PASS}] Original key still reads {count_verify} docs correctly")
        else:
            print(f"  [{FAIL}] Original key reads {count_verify} docs (expected {count_a})")
            ok = False
    except Exception as e:
        print(f"  [{FAIL}] Original key failed after wrong-key attempt: {e}")
        server_a.stop()
        ok = False

    return ok


# ============================================================================
# TEST 4: Encryption Survives Crash (SIGKILL)
# ============================================================================

def test_encryption_survives_crash(binary, tmpdir):
    print(f"\n{'=' * 60}")
    print(f"  TEST 4: ENCRYPTION SURVIVES CRASH (SIGKILL)")
    print(f"{'=' * 60}")

    port = BASE_PORT + 3
    data_dir = os.path.join(tmpdir, "enc_test4")
    os.makedirs(data_dir, exist_ok=True)
    key_path = os.path.join(tmpdir, "test4.key")
    generate_key(key_path)

    server = ServerProcess(binary, data_dir, port, encryption_key_path=key_path)
    server.start()
    print(f"  Server started with encryption (pid={server.proc.pid})")

    client = connect(port)
    col = "crash_enc_test"
    client.create_collection(col)

    NUM_DOCS = 300
    print(f"  Inserting {NUM_DOCS} documents...")
    for i in range(NUM_DOCS):
        client.insert(col, {
            "seq": i,
            "data": f"crash-test-doc-{i}-{'z' * 100}",
            "value": i * 7,
        })
    count_before = client.count(col)
    print(f"  Count before crash: {count_before}")
    client.close()

    # SIGKILL
    pid = server.proc.pid
    print(f"  Sending SIGKILL (pid={pid})...")
    server.kill()

    # Verify plaintext not on disk after crash
    needles = ["crash-test-doc-0-", "crash-test-doc-150-", "crash-test-doc-299-"]
    found = scan_files_for_plaintext(data_dir, needles)
    plaintext_found = any(files for files in found.values())

    # Restart with same key
    time.sleep(0.5)
    server.start()
    print(f"  Server restarted with same key (pid={server.proc.pid})")

    client = connect(port)
    count_after = client.count(col)
    # Verify a sample of documents
    sample = client.find(col, {"seq": 0})
    sample2 = client.find(col, {"seq": 150})
    sample3 = client.find(col, {"seq": 299})
    client.close()
    server.stop()

    ok = True

    print(f"\n  Results:")
    print(f"    Docs before crash:   {count_before}")
    print(f"    Docs after crash:    {count_after}")
    print(f"    Plaintext on disk:   {'YES' if plaintext_found else 'NO'}")

    if count_after != NUM_DOCS:
        print(f"\n  [{FAIL}] Documents lost after crash: {NUM_DOCS - count_after}")
        ok = False
    else:
        print(f"\n  [{PASS}] All {NUM_DOCS} encrypted documents survived crash")

    if plaintext_found:
        print(f"  [{FAIL}] Plaintext found on disk after crash!")
        ok = False
    else:
        print(f"  [{PASS}] No plaintext on disk after crash")

    # Verify sample data correctness
    samples_ok = True
    for seq, docs in [(0, sample), (150, sample2), (299, sample3)]:
        if not docs:
            print(f"  [{FAIL}] Sample seq={seq} missing after recovery")
            samples_ok = False
        elif docs[0].get("value") != seq * 7:
            print(f"  [{FAIL}] Sample seq={seq} has wrong value: {docs[0].get('value')} != {seq * 7}")
            samples_ok = False

    if samples_ok:
        print(f"  [{PASS}] Sample documents verified (correct values after decrypt)")
    else:
        ok = False

    return ok


# ============================================================================
# TEST 5: Encrypted Files Are Larger (Overhead Verification)
# ============================================================================

def test_encryption_overhead(binary, tmpdir):
    print(f"\n{'=' * 60}")
    print(f"  TEST 5: ENCRYPTION OVERHEAD VERIFICATION")
    print(f"{'=' * 60}")

    NUM_DOCS = 100
    col = "overhead_test"

    # Write data WITHOUT encryption
    port_plain = BASE_PORT + 4
    data_dir_plain = os.path.join(tmpdir, "enc_test5_plain")
    os.makedirs(data_dir_plain, exist_ok=True)

    server_plain = ServerProcess(binary, data_dir_plain, port_plain)
    server_plain.start()
    print(f"  Writing {NUM_DOCS} docs WITHOUT encryption...")

    client = connect(port_plain)
    client.create_collection(col)
    for i in range(NUM_DOCS):
        client.insert(col, {"seq": i, "data": f"test-data-{i}", "value": i})
    client.close()
    server_plain.stop()
    time.sleep(0.3)

    plain_size = total_data_size(data_dir_plain)

    # Write same data WITH encryption
    port_enc = BASE_PORT + 5
    data_dir_enc = os.path.join(tmpdir, "enc_test5_enc")
    os.makedirs(data_dir_enc, exist_ok=True)
    key_path = os.path.join(tmpdir, "test5.key")
    generate_key(key_path)

    server_enc = ServerProcess(binary, data_dir_enc, port_enc, encryption_key_path=key_path)
    server_enc.start()
    print(f"  Writing {NUM_DOCS} docs WITH encryption...")

    client = connect(port_enc)
    client.create_collection(col)
    for i in range(NUM_DOCS):
        client.insert(col, {"seq": i, "data": f"test-data-{i}", "value": i})
    client.close()
    server_enc.stop()
    time.sleep(0.3)

    enc_size = total_data_size(data_dir_enc)

    overhead = enc_size - plain_size
    overhead_pct = (overhead / plain_size * 100) if plain_size > 0 else 0

    print(f"\n  Results:")
    print(f"    Plain data size:     {plain_size:,} bytes")
    print(f"    Encrypted size:      {enc_size:,} bytes")
    print(f"    Overhead:            {overhead:,} bytes ({overhead_pct:.1f}%)")
    # AES-GCM adds 12-byte nonce + 16-byte auth tag = 28 bytes per record
    print(f"    Expected per-record: ~28 bytes (12B nonce + 16B auth tag)")

    ok = True

    if enc_size <= plain_size:
        print(f"\n  [{FAIL}] Encrypted files not larger — encryption may not be working!")
        ok = False
    else:
        print(f"\n  [{PASS}] Encrypted files are larger ({overhead_pct:.1f}% overhead)")

    # Verify plain files DO contain plaintext (control check)
    found_plain = scan_files_for_plaintext(data_dir_plain, ["test-data-0", "test-data-50"])
    plain_has_text = any(files for files in found_plain.values())

    # Verify encrypted files do NOT contain plaintext
    found_enc = scan_files_for_plaintext(data_dir_enc, ["test-data-0", "test-data-50"])
    enc_has_text = any(files for files in found_enc.values())

    if plain_has_text and not enc_has_text:
        print(f"  [{PASS}] Control: plaintext in unencrypted files, absent in encrypted")
    elif not plain_has_text:
        print(f"  [WARN] Control check: plaintext not found in unencrypted files either")
    else:
        print(f"  [{FAIL}] Plaintext found in encrypted files!")
        ok = False

    return ok


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="OxiDB Encryption at Rest Tests")
    parser.add_argument("--binary", default=None, help="Path to oxidb-server binary")
    args = parser.parse_args()

    binary = find_binary(args.binary)
    print(f"[*] Using binary: {binary}")

    results = {}
    t0 = time.monotonic()

    with tempfile.TemporaryDirectory(prefix="oxidb_enc_test_") as tmpdir:
        print(f"[*] Temp directory: {tmpdir}")

        results["No Plaintext on Disk"] = test_no_plaintext_on_disk(binary, tmpdir)
        results["Readable After Restart"] = test_readable_after_restart(binary, tmpdir)
        results["Wrong Key Fails"] = test_wrong_key_fails(binary, tmpdir)
        results["Survives Crash"] = test_encryption_survives_crash(binary, tmpdir)
        results["Encryption Overhead"] = test_encryption_overhead(binary, tmpdir)

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
