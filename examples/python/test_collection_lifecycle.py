#!/usr/bin/env python3
"""
OxiDB Concurrent Collection Lifecycle Test
=============================================

Tests collection create/drop operations under heavy concurrent access:

  TEST 1: Create-While-Writing
    Writers continuously insert into a collection while another thread
    drops and recreates it. No server crash allowed. Writers must get
    clean errors or clean successes, never corruption.

  TEST 2: Drop-While-Reading
    Readers continuously query a collection while it's being dropped
    and recreated. Reads must return valid data or clean errors.

  TEST 3: Rapid Collection Churn
    50 threads each create a unique collection, write docs, read them
    back, verify, and drop it — all concurrently. Tests the collection
    map under heavy concurrent structural mutations.

  TEST 4: Cross-Collection Isolation
    While one collection is being dropped/recreated, verify that other
    collections are completely unaffected (no data loss, no corruption).

Prerequisites:
    - oxidb-server running on 127.0.0.1:4444 (pool_size >= 110)
    - Python 3.8+

Usage:
    python examples/python/test_collection_lifecycle.py [--host HOST] [--port PORT]
"""

import argparse
import hashlib
import json
import os
import random
import string
import sys
import threading
import time
import traceback

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
from oxidb import OxiDbClient, OxiDbError

NUM_CONNECTIONS = 100
PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"


def connect(host, port, timeout=30.0):
    return OxiDbClient(host, port, timeout=timeout)


def random_string(length=16):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def checksum_doc(doc):
    filtered = {k: v for k, v in sorted(doc.items()) if k not in ("_id", "_version", "checksum")}
    return hashlib.sha256(json.dumps(filtered, sort_keys=True).encode()).hexdigest()[:16]


# ============================================================================
# TEST 1: Create-While-Writing
# ============================================================================

def test_create_while_writing(host, port):
    print(f"\n{'=' * 60}")
    print(f"  TEST 1: DROP/RECREATE WHILE WRITING")
    print(f"{'=' * 60}")

    col = "lifecycle_write"
    DURATION = 5.0  # seconds
    NUM_WRITERS = 50

    setup = connect(host, port)
    try:
        setup.drop_collection(col)
    except OxiDbError:
        pass
    setup.create_collection(col)
    setup.close()

    lock = threading.Lock()
    stats = {"inserts_ok": 0, "inserts_err": 0, "drops": 0, "creates": 0, "panics": 0}
    stop = threading.Event()

    def writer(worker_id):
        client = connect(host, port, timeout=10.0)
        seq = 0
        while not stop.is_set():
            try:
                client.insert(col, {"worker": worker_id, "seq": seq, "data": random_string(50)})
                with lock:
                    stats["inserts_ok"] += 1
                seq += 1
            except OxiDbError:
                with lock:
                    stats["inserts_err"] += 1
            except Exception as e:
                with lock:
                    stats["panics"] += 1
                    if stats["panics"] <= 3:
                        print(f"    UNEXPECTED: writer-{worker_id}: {type(e).__name__}: {e}")
                break
        client.close()

    def dropper():
        client = connect(host, port, timeout=10.0)
        while not stop.is_set():
            time.sleep(0.3)
            try:
                client.drop_collection(col)
                with lock:
                    stats["drops"] += 1
            except OxiDbError:
                pass
            try:
                client.create_collection(col)
                with lock:
                    stats["creates"] += 1
            except OxiDbError:
                pass
        client.close()

    print(f"  {NUM_WRITERS} writers + 1 dropper for {DURATION}s")
    print(f"  Running...")

    threads = []
    t0 = time.monotonic()

    # Start writers
    for i in range(NUM_WRITERS):
        t = threading.Thread(target=writer, args=(i,), daemon=True)
        threads.append(t)
    # Start dropper
    t = threading.Thread(target=dropper, daemon=True)
    threads.append(t)

    for t in threads:
        t.start()

    time.sleep(DURATION)
    stop.set()

    for t in threads:
        t.join(timeout=10)

    elapsed = time.monotonic() - t0

    # Cleanup
    cleanup = connect(host, port)
    try:
        cleanup.drop_collection(col)
    except OxiDbError:
        pass
    cleanup.close()

    print(f"\n  Results ({elapsed:.1f}s):")
    print(f"    Successful inserts:  {stats['inserts_ok']:,}")
    print(f"    Insert errors:       {stats['inserts_err']:,} (expected — collection dropped)")
    print(f"    Drops:               {stats['drops']}")
    print(f"    Creates:             {stats['creates']}")
    print(f"    Unexpected panics:   {stats['panics']}")

    ok = True
    if stats["panics"] > 0:
        print(f"\n  [{FAIL}] {stats['panics']} unexpected exceptions (not OxiDbError)")
        ok = False
    else:
        print(f"\n  [{PASS}] All errors were clean OxiDbError (no crashes/panics)")

    if stats["drops"] == 0:
        print(f"  [{FAIL}] Dropper never succeeded — test didn't actually test drops")
        ok = False
    else:
        print(f"  [{PASS}] Collection dropped {stats['drops']} times during writes")

    return ok


# ============================================================================
# TEST 2: Drop-While-Reading
# ============================================================================

def test_drop_while_reading(host, port):
    print(f"\n{'=' * 60}")
    print(f"  TEST 2: DROP/RECREATE WHILE READING")
    print(f"{'=' * 60}")

    col = "lifecycle_read"
    DURATION = 5.0
    NUM_READERS = 50
    SEED_DOCS = 200

    # Seed data
    setup = connect(host, port)
    try:
        setup.drop_collection(col)
    except OxiDbError:
        pass
    setup.create_collection(col)
    for i in range(SEED_DOCS):
        setup.insert(col, {"seq": i, "data": f"seed-{i}"})
    setup.close()

    lock = threading.Lock()
    stats = {"reads_ok": 0, "reads_empty": 0, "reads_err": 0, "corrupt": 0,
             "drops": 0, "seeds": 0, "panics": 0}
    stop = threading.Event()

    def reader(worker_id):
        client = connect(host, port, timeout=10.0)
        while not stop.is_set():
            try:
                docs = client.find(col, {})
                with lock:
                    if len(docs) > 0:
                        stats["reads_ok"] += 1
                        # Verify docs are valid JSON with expected fields
                        for d in docs:
                            if "seq" not in d or "data" not in d:
                                stats["corrupt"] += 1
                    else:
                        stats["reads_empty"] += 1
            except OxiDbError:
                with lock:
                    stats["reads_err"] += 1
            except Exception as e:
                with lock:
                    stats["panics"] += 1
                    if stats["panics"] <= 3:
                        print(f"    UNEXPECTED: reader-{worker_id}: {type(e).__name__}: {e}")
                break
        client.close()

    def dropper_reseeder():
        client = connect(host, port, timeout=10.0)
        while not stop.is_set():
            time.sleep(0.4)
            try:
                client.drop_collection(col)
                with lock:
                    stats["drops"] += 1
            except OxiDbError:
                pass
            try:
                client.create_collection(col)
                # Re-seed with fewer docs
                for i in range(20):
                    client.insert(col, {"seq": i, "data": f"reseed-{i}"})
                with lock:
                    stats["seeds"] += 1
            except OxiDbError:
                pass
        client.close()

    print(f"  {NUM_READERS} readers + 1 dropper/reseeder for {DURATION}s")
    print(f"  Running...")

    threads = []
    t0 = time.monotonic()

    for i in range(NUM_READERS):
        t = threading.Thread(target=reader, args=(i,), daemon=True)
        threads.append(t)
    t = threading.Thread(target=dropper_reseeder, daemon=True)
    threads.append(t)

    for t in threads:
        t.start()

    time.sleep(DURATION)
    stop.set()

    for t in threads:
        t.join(timeout=10)

    elapsed = time.monotonic() - t0

    cleanup = connect(host, port)
    try:
        cleanup.drop_collection(col)
    except OxiDbError:
        pass
    cleanup.close()

    print(f"\n  Results ({elapsed:.1f}s):")
    print(f"    Reads with data:     {stats['reads_ok']:,}")
    print(f"    Reads empty:         {stats['reads_empty']:,}")
    print(f"    Read errors:         {stats['reads_err']:,}")
    print(f"    Corrupt results:     {stats['corrupt']}")
    print(f"    Drops:               {stats['drops']}")
    print(f"    Re-seeds:            {stats['seeds']}")
    print(f"    Unexpected panics:   {stats['panics']}")

    ok = True
    if stats["panics"] > 0:
        print(f"\n  [{FAIL}] {stats['panics']} unexpected exceptions")
        ok = False
    else:
        print(f"\n  [{PASS}] All errors were clean OxiDbError")

    if stats["corrupt"] > 0:
        print(f"  [{FAIL}] {stats['corrupt']} corrupt documents returned!")
        ok = False
    else:
        print(f"  [{PASS}] Zero corrupt documents in reads")

    return ok


# ============================================================================
# TEST 3: Rapid Collection Churn
# ============================================================================

def test_collection_churn(host, port):
    print(f"\n{'=' * 60}")
    print(f"  TEST 3: RAPID COLLECTION CHURN (50 THREADS)")
    print(f"{'=' * 60}")

    NUM_THREADS = 50
    DOCS_PER_THREAD = 30

    lock = threading.Lock()
    stats = {"ok": 0, "data_mismatch": 0, "errors": 0, "details": []}

    def churn_worker(worker_id, host, port):
        col = f"churn_{worker_id}"
        client = connect(host, port, timeout=15.0)

        try:
            # Create
            try:
                client.drop_collection(col)
            except OxiDbError:
                pass
            client.create_collection(col)

            # Write docs with checksums
            expected = {}
            for seq in range(DOCS_PER_THREAD):
                doc = {"worker": worker_id, "seq": seq, "data": random_string(80)}
                doc["checksum"] = checksum_doc(doc)
                result = client.insert(col, doc)
                expected[seq] = doc

            # Read back and verify
            all_docs = client.find(col, {})
            if len(all_docs) != DOCS_PER_THREAD:
                with lock:
                    stats["data_mismatch"] += 1
                    stats["details"].append(
                        f"w{worker_id}: count mismatch {len(all_docs)} != {DOCS_PER_THREAD}"
                    )
            else:
                for doc in all_docs:
                    seq = doc["seq"]
                    actual_cs = checksum_doc(doc)
                    stored_cs = doc.get("checksum", "")
                    if actual_cs != stored_cs:
                        with lock:
                            stats["data_mismatch"] += 1
                            stats["details"].append(
                                f"w{worker_id}/s{seq}: checksum mismatch"
                            )

            # Drop
            client.drop_collection(col)

            with lock:
                stats["ok"] += 1

        except Exception as e:
            with lock:
                stats["errors"] += 1
                stats["details"].append(f"w{worker_id}: {type(e).__name__}: {e}")
        finally:
            client.close()

    print(f"  {NUM_THREADS} threads, each: create -> write {DOCS_PER_THREAD} docs -> verify -> drop")
    print(f"  Running...")

    threads = []
    t0 = time.monotonic()
    for i in range(NUM_THREADS):
        t = threading.Thread(target=churn_worker, args=(i, host, port), daemon=True)
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    elapsed = time.monotonic() - t0

    print(f"\n  Results ({elapsed:.1f}s):")
    print(f"    Successful cycles:   {stats['ok']}/{NUM_THREADS}")
    print(f"    Data mismatches:     {stats['data_mismatch']}")
    print(f"    Errors:              {stats['errors']}")

    if stats["details"]:
        for d in stats["details"][:5]:
            print(f"      {d}")

    ok = True
    if stats["ok"] != NUM_THREADS:
        print(f"\n  [{FAIL}] Only {stats['ok']}/{NUM_THREADS} threads completed successfully")
        ok = False
    else:
        print(f"\n  [{PASS}] All {NUM_THREADS} create-write-verify-drop cycles succeeded")

    if stats["data_mismatch"] > 0:
        print(f"  [{FAIL}] {stats['data_mismatch']} data mismatches!")
        ok = False
    else:
        print(f"  [{PASS}] Zero data mismatches")

    return ok


# ============================================================================
# TEST 4: Cross-Collection Isolation
# ============================================================================

def test_cross_collection_isolation(host, port):
    print(f"\n{'=' * 60}")
    print(f"  TEST 4: CROSS-COLLECTION ISOLATION")
    print(f"{'=' * 60}")

    stable_col = "isolation_stable"
    volatile_col = "isolation_volatile"
    DURATION = 5.0
    STABLE_DOCS = 200
    NUM_WRITERS = 20
    NUM_READERS = 20

    # Setup: seed the stable collection
    setup = connect(host, port)
    for c in [stable_col, volatile_col]:
        try:
            setup.drop_collection(c)
        except OxiDbError:
            pass
        setup.create_collection(c)

    print(f"  Seeding {STABLE_DOCS} docs into '{stable_col}'...")
    stable_checksums = {}
    for i in range(STABLE_DOCS):
        doc = {"seq": i, "data": f"stable-doc-{i}-{'x' * 50}"}
        doc["checksum"] = checksum_doc(doc)
        result = setup.insert(stable_col, doc)
        stable_checksums[i] = doc["checksum"]
    setup.close()

    lock = threading.Lock()
    stats = {
        "volatile_ops": 0, "volatile_errors": 0,
        "stable_reads": 0, "stable_errors": 0,
        "stable_wrong_count": 0, "stable_corrupt": 0,
        "panics": 0,
    }
    stop = threading.Event()

    def volatile_worker(worker_id):
        """Thrash the volatile collection: write, drop, recreate."""
        client = connect(host, port, timeout=10.0)
        while not stop.is_set():
            try:
                # Insert a bunch of docs
                for j in range(10):
                    client.insert(volatile_col, {"v": worker_id, "j": j, "trash": random_string(100)})
                with lock:
                    stats["volatile_ops"] += 1

                # Occasionally drop and recreate
                if random.random() < 0.3:
                    client.drop_collection(volatile_col)
                    client.create_collection(volatile_col)
                    with lock:
                        stats["volatile_ops"] += 1
            except OxiDbError:
                with lock:
                    stats["volatile_errors"] += 1
            except Exception:
                with lock:
                    stats["panics"] += 1
                break
        client.close()

    def stable_reader(worker_id):
        """Continuously verify the stable collection is untouched."""
        client = connect(host, port, timeout=10.0)
        while not stop.is_set():
            try:
                docs = client.find(stable_col, {})
                with lock:
                    stats["stable_reads"] += 1
                    if len(docs) != STABLE_DOCS:
                        stats["stable_wrong_count"] += 1
                    else:
                        for d in docs:
                            actual_cs = checksum_doc(d)
                            expected_cs = stable_checksums.get(d["seq"], "")
                            if actual_cs != expected_cs:
                                stats["stable_corrupt"] += 1
            except OxiDbError:
                with lock:
                    stats["stable_errors"] += 1
            except Exception:
                with lock:
                    stats["panics"] += 1
                break
        client.close()

    print(f"  {NUM_WRITERS} volatile workers + {NUM_READERS} stable readers for {DURATION}s")
    print(f"  Running...")

    threads = []
    t0 = time.monotonic()

    for i in range(NUM_WRITERS):
        t = threading.Thread(target=volatile_worker, args=(i,), daemon=True)
        threads.append(t)
    for i in range(NUM_READERS):
        t = threading.Thread(target=stable_reader, args=(i,), daemon=True)
        threads.append(t)

    for t in threads:
        t.start()

    time.sleep(DURATION)
    stop.set()

    for t in threads:
        t.join(timeout=10)

    elapsed = time.monotonic() - t0

    # Final verification of stable collection
    final = connect(host, port)
    final_docs = final.find(stable_col, {})
    final_corrupt = 0
    for d in final_docs:
        actual_cs = checksum_doc(d)
        expected_cs = stable_checksums.get(d["seq"], "")
        if actual_cs != expected_cs:
            final_corrupt += 1

    # Cleanup
    for c in [stable_col, volatile_col]:
        try:
            final.drop_collection(c)
        except OxiDbError:
            pass
    final.close()

    print(f"\n  Results ({elapsed:.1f}s):")
    print(f"    Volatile ops:          {stats['volatile_ops']:,}")
    print(f"    Volatile errors:       {stats['volatile_errors']:,}")
    print(f"    Stable reads:          {stats['stable_reads']:,}")
    print(f"    Stable wrong count:    {stats['stable_wrong_count']}")
    print(f"    Stable corrupt:        {stats['stable_corrupt']}")
    print(f"    Final doc count:       {len(final_docs)} (expected {STABLE_DOCS})")
    print(f"    Final corrupt:         {final_corrupt}")
    print(f"    Unexpected panics:     {stats['panics']}")

    ok = True

    if stats["panics"] > 0:
        print(f"\n  [{FAIL}] {stats['panics']} unexpected exceptions")
        ok = False
    else:
        print(f"\n  [{PASS}] No unexpected exceptions")

    if stats["stable_wrong_count"] > 0:
        print(f"  [{FAIL}] Stable collection had wrong count {stats['stable_wrong_count']} times!")
        ok = False
    else:
        print(f"  [{PASS}] Stable collection count always correct during volatile ops")

    if stats["stable_corrupt"] > 0:
        print(f"  [{FAIL}] {stats['stable_corrupt']} corrupt reads from stable collection!")
        ok = False
    else:
        print(f"  [{PASS}] Zero corrupt reads from stable collection")

    if len(final_docs) != STABLE_DOCS:
        print(f"  [{FAIL}] Final count {len(final_docs)} != {STABLE_DOCS} — data lost!")
        ok = False
    else:
        print(f"  [{PASS}] Final stable data intact: {STABLE_DOCS} docs")

    if final_corrupt > 0:
        print(f"  [{FAIL}] {final_corrupt} documents corrupted in final check!")
        ok = False
    else:
        print(f"  [{PASS}] Final checksum verification passed")

    return ok


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="OxiDB Collection Lifecycle Tests")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4444)
    args = parser.parse_args()

    host, port = args.host, args.port

    try:
        probe = connect(host, port, timeout=5.0)
        probe.ping()
        probe.close()
    except Exception as e:
        print(f"[!] Cannot connect to oxidb-server at {host}:{port}: {e}")
        sys.exit(1)

    print(f"[*] Connected to oxidb-server at {host}:{port}")
    print(f"[*] Running collection lifecycle test suite\n")

    results = {}
    t0 = time.monotonic()

    results["Drop While Writing"] = test_create_while_writing(host, port)
    results["Drop While Reading"] = test_drop_while_reading(host, port)
    results["Collection Churn"] = test_collection_churn(host, port)
    results["Cross-Collection Isolation"] = test_cross_collection_isolation(host, port)

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
