#!/usr/bin/env python3
"""
OxiDB OCC Conflict Storm + Data Integrity Test
=================================================
Two test suites:

1. OCC Conflict Storm — 100 connections all updating the same documents
   simultaneously via transactions. Verifies transaction isolation:
   every committed transaction must leave data consistent, and conflicts
   must be detected (never silent corruption).

2. Data Integrity — insert known data under heavy concurrent writes,
   read it all back, verify every document is byte-perfect. Checks that
   concurrent inserts, updates, and deletes never corrupt each other's data.

Prerequisites:
    - oxidb-server running on 127.0.0.1:4444 (pool_size >= 110)
    - Python 3.8+

Usage:
    python examples/python/test_occ_and_integrity.py [--host HOST] [--port PORT]
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
from oxidb import OxiDbClient, OxiDbError, TransactionConflictError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NUM_CONNECTIONS = 100
PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"


def connect(host, port, timeout=30.0):
    return OxiDbClient(host, port, timeout=timeout)


def random_string(length=16):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def checksum(doc):
    """Deterministic checksum of a document (excluding metadata and the checksum itself)."""
    filtered = {k: v for k, v in sorted(doc.items()) if k not in ("_id", "_version", "checksum")}
    return hashlib.sha256(json.dumps(filtered, sort_keys=True).encode()).hexdigest()[:16]


# ============================================================================
# TEST 1: OCC Conflict Storm
# ============================================================================

def test_occ_conflict_storm(host, port):
    """
    Setup: one collection with a single "bank account" document {balance: 1000}.
    Storm: 100 connections each try to do a read-modify-write transaction
           (read balance, increment by 1, write back) in a loop.

    Expected: many TransactionConflictErrors (which is correct OCC behavior).
    The final balance must equal 1000 + (number of successful commits).
    No silent corruption allowed.
    """
    print(f"\n{'=' * 60}")
    print(f"  TEST 1: OCC CONFLICT STORM")
    print(f"{'=' * 60}")

    col = "occ_storm"
    setup = connect(host, port)
    try:
        setup.drop_collection(col)
    except OxiDbError:
        pass
    setup.create_collection(col)
    setup.create_index(col, "account")
    # Insert the shared account
    setup.insert(col, {"account": "shared", "balance": 1000})
    setup.close()

    ROUNDS = 50
    commits = threading.atomic = {"count": 0}  # not real atomic, use lock
    lock = threading.Lock()
    conflicts = {"count": 0}
    errors = {"count": 0, "details": []}

    def occ_worker(worker_id, host, port):
        client = connect(host, port)
        for r in range(ROUNDS):
            try:
                with client.transaction():
                    # Read current balance
                    docs = client.find(col, {"account": "shared"})
                    if not docs:
                        with lock:
                            errors["count"] += 1
                            errors["details"].append(f"w{worker_id}/r{r}: no doc found in tx")
                        continue
                    current = docs[0]["balance"]
                    # Increment
                    client.update(
                        col,
                        {"account": "shared"},
                        {"$set": {"balance": current + 1}},
                    )
                # If we get here, commit succeeded
                with lock:
                    commits["count"] += 1
            except TransactionConflictError:
                with lock:
                    conflicts["count"] += 1
            except Exception as e:
                with lock:
                    errors["count"] += 1
                    errors["details"].append(f"w{worker_id}/r{r}: {e}")
        client.close()

    # Open connections and run
    print(f"  Setup: 1 account with balance=1000")
    print(f"  Storm: {NUM_CONNECTIONS} connections x {ROUNDS} rounds of read-modify-write tx")
    print(f"  Running...")

    threads = []
    t0 = time.monotonic()
    for i in range(NUM_CONNECTIONS):
        t = threading.Thread(target=occ_worker, args=(i, host, port), daemon=True)
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    elapsed = time.monotonic() - t0

    # Verify
    check = connect(host, port)
    docs = check.find(col, {"account": "shared"})
    check.drop_collection(col)
    check.close()

    if not docs:
        print(f"  [{FAIL}] Account document missing after test!")
        return False

    final_balance = docs[0]["balance"]
    expected_balance = 1000 + commits["count"]
    total_attempts = NUM_CONNECTIONS * ROUNDS

    print(f"\n  Results ({elapsed:.1f}s):")
    print(f"    Total attempts:      {total_attempts:,}")
    print(f"    Successful commits:  {commits['count']:,}")
    print(f"    OCC conflicts:       {conflicts['count']:,}")
    print(f"    Errors:              {errors['count']}")
    print(f"    Conflict rate:       {conflicts['count'] / total_attempts * 100:.1f}%")
    print(f"    Expected balance:    {expected_balance}")
    print(f"    Actual balance:      {final_balance}")

    if errors["details"]:
        for d in errors["details"][:5]:
            print(f"      ERROR: {d}")

    ok = True

    if final_balance != expected_balance:
        print(f"\n  [{FAIL}] BALANCE MISMATCH! Expected {expected_balance}, got {final_balance}")
        print(f"         Difference: {final_balance - expected_balance}")
        print(f"         This means a transaction committed without proper isolation!")
        ok = False
    else:
        print(f"\n  [{PASS}] Balance is correct: 1000 + {commits['count']} commits = {final_balance}")

    if conflicts["count"] == 0:
        print(f"  [WARN] Zero conflicts with {NUM_CONNECTIONS} concurrent writers — suspicious")

    if errors["count"] > 0:
        print(f"  [{FAIL}] {errors['count']} unexpected errors")
        ok = False
    else:
        print(f"  [{PASS}] Zero unexpected errors")

    return ok


# ============================================================================
# TEST 2: Multi-account Transfer Integrity
# ============================================================================

def test_transfer_integrity(host, port):
    """
    Setup: 10 accounts each with balance=10000 (total = 100,000).
    Storm: 100 connections each do transfer transactions:
           debit one random account, credit another (preserving total).

    Expected: final sum of all balances must still be 100,000.
    Conflicts are OK. Corruption is not.
    """
    print(f"\n{'=' * 60}")
    print(f"  TEST 2: MULTI-ACCOUNT TRANSFER INTEGRITY")
    print(f"{'=' * 60}")

    col = "transfer_test"
    NUM_ACCOUNTS = 10
    INITIAL_BALANCE = 10000
    TOTAL = NUM_ACCOUNTS * INITIAL_BALANCE
    ROUNDS = 30

    setup = connect(host, port)
    try:
        setup.drop_collection(col)
    except OxiDbError:
        pass
    setup.create_collection(col)
    setup.create_index(col, "acct_id")

    for i in range(NUM_ACCOUNTS):
        setup.insert(col, {"acct_id": i, "balance": INITIAL_BALANCE})
    setup.close()

    lock = threading.Lock()
    stats = {"commits": 0, "conflicts": 0, "errors": 0, "details": []}

    def transfer_worker(worker_id, host, port):
        client = connect(host, port)
        for r in range(ROUNDS):
            src = random.randint(0, NUM_ACCOUNTS - 1)
            dst = random.randint(0, NUM_ACCOUNTS - 2)
            if dst >= src:
                dst += 1  # ensure src != dst
            amount = random.randint(1, 100)

            try:
                with client.transaction():
                    # Read both accounts
                    src_docs = client.find(col, {"acct_id": src})
                    dst_docs = client.find(col, {"acct_id": dst})
                    if not src_docs or not dst_docs:
                        with lock:
                            stats["errors"] += 1
                        continue

                    src_bal = src_docs[0]["balance"]
                    dst_bal = dst_docs[0]["balance"]

                    # Transfer (allow negative balances — we're testing integrity, not business rules)
                    client.update(col, {"acct_id": src}, {"$set": {"balance": src_bal - amount}})
                    client.update(col, {"acct_id": dst}, {"$set": {"balance": dst_bal + amount}})

                with lock:
                    stats["commits"] += 1
            except TransactionConflictError:
                with lock:
                    stats["conflicts"] += 1
            except Exception as e:
                with lock:
                    stats["errors"] += 1
                    stats["details"].append(f"w{worker_id}/r{r}: {e}")
        client.close()

    print(f"  Setup: {NUM_ACCOUNTS} accounts x {INITIAL_BALANCE} = {TOTAL:,} total")
    print(f"  Storm: {NUM_CONNECTIONS} connections x {ROUNDS} rounds of random transfers")
    print(f"  Running...")

    threads = []
    t0 = time.monotonic()
    for i in range(NUM_CONNECTIONS):
        t = threading.Thread(target=transfer_worker, args=(i, host, port), daemon=True)
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    elapsed = time.monotonic() - t0

    # Verify
    check = connect(host, port)
    accounts = check.find(col, {})
    check.drop_collection(col)
    check.close()

    final_sum = sum(a["balance"] for a in accounts)
    total_attempts = NUM_CONNECTIONS * ROUNDS

    print(f"\n  Results ({elapsed:.1f}s):")
    print(f"    Total attempts:      {total_attempts:,}")
    print(f"    Successful commits:  {stats['commits']:,}")
    print(f"    OCC conflicts:       {stats['conflicts']:,}")
    print(f"    Errors:              {stats['errors']}")
    print(f"    Conflict rate:       {stats['conflicts'] / total_attempts * 100:.1f}%")
    print(f"    Expected total:      {TOTAL:,}")
    print(f"    Actual total:        {final_sum:,}")

    if stats["details"]:
        for d in stats["details"][:5]:
            print(f"      ERROR: {d}")

    ok = True

    if final_sum != TOTAL:
        print(f"\n  [{FAIL}] TOTAL MISMATCH! Expected {TOTAL:,}, got {final_sum:,}")
        print(f"         Money {'appeared' if final_sum > TOTAL else 'disappeared'}: {abs(final_sum - TOTAL):,}")
        print(f"         This means transactions violated isolation!")
        ok = False
    else:
        print(f"\n  [{PASS}] Total preserved: {final_sum:,} (conservation of money)")

    # Check individual accounts
    for a in sorted(accounts, key=lambda x: x["acct_id"]):
        delta = a["balance"] - INITIAL_BALANCE
        sign = "+" if delta >= 0 else ""
        print(f"    Account {a['acct_id']}: {a['balance']:>8,} ({sign}{delta})")

    if stats["errors"] > 0:
        print(f"  [{FAIL}] {stats['errors']} unexpected errors")
        ok = False
    else:
        print(f"  [{PASS}] Zero unexpected errors")

    return ok


# ============================================================================
# TEST 3: Data Integrity Under Concurrent Writes
# ============================================================================

def test_data_integrity(host, port):
    """
    Each of 100 workers inserts a unique set of documents with a known
    checksum embedded in each doc. Concurrently, workers also update
    their own documents and delete some. After all workers finish,
    every surviving document must have a valid checksum and correct data.
    """
    print(f"\n{'=' * 60}")
    print(f"  TEST 3: DATA INTEGRITY UNDER CONCURRENT WRITES")
    print(f"{'=' * 60}")

    col = "integrity_test"
    DOCS_PER_WORKER = 50
    UPDATES_PER_WORKER = 20
    DELETES_PER_WORKER = 10

    setup = connect(host, port)
    try:
        setup.drop_collection(col)
    except OxiDbError:
        pass
    setup.create_collection(col)
    setup.create_index(col, "worker_id")
    setup.create_index(col, "seq")
    setup.create_composite_index(col, ["worker_id", "seq"])
    setup.close()

    lock = threading.Lock()
    # Track what each worker inserted/updated/deleted
    worker_state = {}  # worker_id -> {seq -> expected_doc}
    stats = {"inserts": 0, "updates": 0, "deletes": 0, "errors": 0, "details": []}

    def integrity_worker(worker_id, host, port):
        client = connect(host, port)
        local_docs = {}  # seq -> expected_doc

        # Phase 1: Insert unique documents with checksums
        for seq in range(DOCS_PER_WORKER):
            payload = random_string(100)
            tags = [random_string(8) for _ in range(3)]
            doc = {
                "worker_id": worker_id,
                "seq": seq,
                "payload": payload,
                "tags": tags,
                "score": round(random.uniform(0, 1000), 2),
                "nested": {"x": worker_id, "y": seq},
            }
            doc["checksum"] = checksum(doc)
            try:
                result = client.insert(col, doc)
                doc["_id"] = result["id"]
                local_docs[seq] = doc
                with lock:
                    stats["inserts"] += 1
            except Exception as e:
                with lock:
                    stats["errors"] += 1
                    stats["details"].append(f"w{worker_id} insert seq={seq}: {e}")

        # Phase 2: Update some documents (change payload, recompute checksum)
        update_seqs = random.sample(range(DOCS_PER_WORKER), min(UPDATES_PER_WORKER, DOCS_PER_WORKER))
        for seq in update_seqs:
            if seq not in local_docs:
                continue
            new_payload = random_string(100)
            new_score = round(random.uniform(0, 1000), 2)
            try:
                # Update local expected state first to compute new checksum
                local_docs[seq]["payload"] = new_payload
                local_docs[seq]["score"] = new_score
                new_cs = checksum(local_docs[seq])
                local_docs[seq]["checksum"] = new_cs
                client.update_one(
                    col,
                    {"worker_id": worker_id, "seq": seq},
                    {"$set": {"payload": new_payload, "score": new_score, "checksum": new_cs}},
                )
                with lock:
                    stats["updates"] += 1
            except Exception as e:
                with lock:
                    stats["errors"] += 1
                    stats["details"].append(f"w{worker_id} update seq={seq}: {e}")

        # Phase 3: Delete some documents
        delete_seqs = random.sample(range(DOCS_PER_WORKER), min(DELETES_PER_WORKER, DOCS_PER_WORKER))
        for seq in delete_seqs:
            if seq not in local_docs:
                continue
            try:
                client.delete_one(col, {"worker_id": worker_id, "seq": seq})
                del local_docs[seq]
                with lock:
                    stats["deletes"] += 1
            except Exception as e:
                with lock:
                    stats["errors"] += 1
                    stats["details"].append(f"w{worker_id} delete seq={seq}: {e}")

        with lock:
            worker_state[worker_id] = local_docs

        client.close()

    print(f"  Setup: {NUM_CONNECTIONS} workers x {DOCS_PER_WORKER} docs each")
    print(f"  Each worker: insert {DOCS_PER_WORKER}, update {UPDATES_PER_WORKER}, delete {DELETES_PER_WORKER}")
    print(f"  Running...")

    threads = []
    t0 = time.monotonic()
    for i in range(NUM_CONNECTIONS):
        t = threading.Thread(target=integrity_worker, args=(i, host, port), daemon=True)
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    elapsed = time.monotonic() - t0

    # ---- Verification Phase ----
    print(f"\n  Operations ({elapsed:.1f}s):")
    print(f"    Inserts:   {stats['inserts']:,}")
    print(f"    Updates:   {stats['updates']:,}")
    print(f"    Deletes:   {stats['deletes']:,}")
    print(f"    Errors:    {stats['errors']}")

    if stats["details"]:
        for d in stats["details"][:5]:
            print(f"      ERROR: {d}")

    print(f"\n  Verifying data integrity...")

    verify = connect(host, port)
    all_ok = True
    missing_count = 0
    extra_count = 0
    corrupt_count = 0
    checksum_ok_count = 0

    for worker_id, expected_docs in worker_state.items():
        # Fetch all docs for this worker
        actual_docs = verify.find(col, {"worker_id": worker_id})
        actual_by_seq = {}
        for d in actual_docs:
            actual_by_seq[d["seq"]] = d

        # Check every expected doc exists and is correct
        for seq, expected in expected_docs.items():
            if seq not in actual_by_seq:
                missing_count += 1
                if missing_count <= 3:
                    print(f"    [{FAIL}] MISSING: worker={worker_id} seq={seq}")
                all_ok = False
                continue

            actual = actual_by_seq[seq]

            # Verify checksum
            actual_cs = checksum(actual)
            expected_cs = expected["checksum"]
            if actual_cs != expected_cs:
                corrupt_count += 1
                if corrupt_count <= 3:
                    print(f"    [{FAIL}] CORRUPT: worker={worker_id} seq={seq}")
                    print(f"            expected checksum: {expected_cs}")
                    print(f"            actual checksum:   {actual_cs}")
                    # Show which fields differ
                    for key in set(list(expected.keys()) + list(actual.keys())):
                        if key in ("_id", "_version", "checksum"):
                            continue
                        ev = expected.get(key)
                        av = actual.get(key)
                        if ev != av:
                            print(f"            field '{key}': expected={ev!r}, actual={av!r}")
                all_ok = False
            else:
                checksum_ok_count += 1

            # Also verify specific fields match
            for field in ("worker_id", "seq", "payload", "tags", "score", "nested"):
                if expected.get(field) != actual.get(field):
                    if corrupt_count <= 3:
                        print(f"    [{FAIL}] FIELD MISMATCH: worker={worker_id} seq={seq} "
                              f"field={field}: expected={expected.get(field)!r}, "
                              f"actual={actual.get(field)!r}")
                    all_ok = False

        # Check for extra docs (docs that should have been deleted)
        for seq in actual_by_seq:
            if seq not in expected_docs:
                extra_count += 1
                if extra_count <= 3:
                    print(f"    [{FAIL}] EXTRA DOC: worker={worker_id} seq={seq} (should be deleted)")
                all_ok = False

    # Count verification
    expected_total = sum(len(docs) for docs in worker_state.values())
    actual_total = verify.count(col)

    verify.drop_collection(col)
    verify.close()

    print(f"\n  Verification:")
    print(f"    Expected documents:  {expected_total:,}")
    print(f"    Actual documents:    {actual_total:,}")
    print(f"    Checksums OK:        {checksum_ok_count:,}")
    print(f"    Missing:             {missing_count}")
    print(f"    Extra (not deleted): {extra_count}")
    print(f"    Corrupt:             {corrupt_count}")

    if actual_total != expected_total:
        print(f"  [{FAIL}] Document count mismatch: expected {expected_total}, got {actual_total}")
        all_ok = False
    else:
        print(f"  [{PASS}] Document count matches: {actual_total:,}")

    if corrupt_count == 0:
        print(f"  [{PASS}] Zero corrupted documents")
    else:
        print(f"  [{FAIL}] {corrupt_count} corrupted documents!")

    if missing_count == 0 and extra_count == 0:
        print(f"  [{PASS}] No missing or extra documents")
    else:
        print(f"  [{FAIL}] {missing_count} missing, {extra_count} extra")

    if stats["errors"] > 0:
        print(f"  [{FAIL}] {stats['errors']} operation errors")
        all_ok = False
    else:
        print(f"  [{PASS}] Zero operation errors")

    return all_ok


# ============================================================================
# TEST 4: Concurrent Counter Integrity (non-transactional)
# ============================================================================

def test_counter_integrity(host, port):
    """
    Tests $inc atomicity: 100 connections each $inc the same counter 100 times.
    Final value must equal 100 * 100 = 10,000.
    Uses update_one (non-transactional) to test the engine's write-lock correctness.
    """
    print(f"\n{'=' * 60}")
    print(f"  TEST 4: CONCURRENT $INC COUNTER (NON-TX)")
    print(f"{'=' * 60}")

    col = "counter_test"
    ROUNDS = 100

    setup = connect(host, port)
    try:
        setup.drop_collection(col)
    except OxiDbError:
        pass
    setup.create_collection(col)
    setup.insert(col, {"name": "counter", "value": 0})
    setup.close()

    lock = threading.Lock()
    stats = {"errors": 0, "details": []}

    def inc_worker(worker_id, host, port):
        client = connect(host, port)
        for _ in range(ROUNDS):
            try:
                client.update_one(col, {"name": "counter"}, {"$inc": {"value": 1}})
            except Exception as e:
                with lock:
                    stats["errors"] += 1
                    stats["details"].append(f"w{worker_id}: {e}")
        client.close()

    print(f"  Setup: counter starting at 0")
    print(f"  Storm: {NUM_CONNECTIONS} connections x {ROUNDS} increments each")
    print(f"  Expected final value: {NUM_CONNECTIONS * ROUNDS:,}")
    print(f"  Running...")

    threads = []
    t0 = time.monotonic()
    for i in range(NUM_CONNECTIONS):
        t = threading.Thread(target=inc_worker, args=(i, host, port), daemon=True)
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    elapsed = time.monotonic() - t0

    check = connect(host, port)
    docs = check.find(col, {"name": "counter"})
    check.drop_collection(col)
    check.close()

    final_value = docs[0]["value"] if docs else None
    expected = NUM_CONNECTIONS * ROUNDS

    print(f"\n  Results ({elapsed:.1f}s):")
    print(f"    Expected:  {expected:,}")
    print(f"    Actual:    {final_value:,}")
    print(f"    Errors:    {stats['errors']}")

    ok = True
    if final_value != expected:
        print(f"\n  [{FAIL}] COUNTER MISMATCH! Lost {expected - final_value:,} increments")
        print(f"         This means concurrent $inc operations are not atomic!")
        ok = False
    else:
        print(f"\n  [{PASS}] Counter is exact: {final_value:,}")

    if stats["errors"] > 0:
        print(f"  [{FAIL}] {stats['errors']} errors during increments")
        ok = False
    else:
        print(f"  [{PASS}] Zero errors")

    return ok


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="OxiDB OCC + Integrity Tests")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4444)
    args = parser.parse_args()

    host, port = args.host, args.port

    # Verify connectivity
    try:
        probe = connect(host, port, timeout=5.0)
        probe.ping()
        probe.close()
    except Exception as e:
        print(f"[!] Cannot connect to oxidb-server at {host}:{port}: {e}")
        sys.exit(1)

    print(f"[*] Connected to oxidb-server at {host}:{port}")
    print(f"[*] Running OCC conflict + data integrity test suite\n")

    results = {}
    t0 = time.monotonic()

    results["OCC Conflict Storm"] = test_occ_conflict_storm(host, port)
    results["Transfer Integrity"] = test_transfer_integrity(host, port)
    results["Data Integrity"] = test_data_integrity(host, port)
    results["Counter Integrity"] = test_counter_integrity(host, port)

    elapsed = time.monotonic() - t0

    # Summary
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
