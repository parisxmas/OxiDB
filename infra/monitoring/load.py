#!/usr/bin/env python3
"""Synthetic workload for the monitoring demo — drives a live OxiDB over
its TCP wire protocol so the Grafana dashboard has something to show:
a steady mix of inserts, finds, counts, aggregations, and transactions
(including deliberate hot-account contention to move the conflict-ratio
panel), plus the occasional slow query for the profiler.

Usage:  python3 load.py [host] [port]   (default 127.0.0.1 4444)
Stop with Ctrl-C.
"""
import json
import os
import random
import socket
import struct
import sys
import threading
import time

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 4444


def connect():
    s = socket.create_connection((HOST, PORT))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return s


def rpc(sock, obj):
    b = json.dumps(obj).encode()
    sock.sendall(struct.pack("<I", len(b)) + b)
    n = struct.unpack("<I", sock.recv(4))[0]
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("server closed")
        data += chunk
    return json.loads(data)


def seed():
    s = connect()
    # Idempotent: skip if the account pool already exists (the data dir
    # persists across restarts). Otherwise re-seeding would double the
    # accounts and skew the money-conservation baseline.
    existing = rpc(s, {"cmd": "count", "collection": "accounts", "query": {}})["data"]["count"]
    if existing >= 201:
        print(f"accounts already seeded ({existing}) — skipping")
        s.close()
        return
    for i in range(200):
        rpc(s, {"cmd": "insert", "collection": "accounts",
                "doc": {"id": f"acct-{i}", "balance": 1_000_000}})
    rpc(s, {"cmd": "insert", "collection": "accounts", "doc": {"id": "fee", "balance": 0}})
    rpc(s, {"cmd": "create_index", "collection": "accounts", "field": "id"})
    rpc(s, {"cmd": "create_index", "collection": "trades", "field": "sym"})
    s.close()
    print("seeded 201 accounts + indexes")


def crud_worker(wid):
    s = connect()
    syms = ["BTC", "ETH", "SOL", "DOGE"]
    n = 0
    while True:
        r = random.random()
        try:
            if r < 0.45:
                rpc(s, {"cmd": "insert", "collection": "trades",
                        "doc": {"sym": random.choice(syms),
                                "price": round(random.uniform(1, 70000), 2),
                                "qty": round(random.uniform(0.01, 5), 3),
                                "ts": time.time()}})
            elif r < 0.7:
                rpc(s, {"cmd": "find", "collection": "trades",
                        "query": {"sym": random.choice(syms)}, "limit": 20})
            elif r < 0.85:
                rpc(s, {"cmd": "count", "collection": "trades",
                        "query": {"sym": random.choice(syms)}})
            else:
                rpc(s, {"cmd": "aggregate", "collection": "trades", "pipeline": [
                    {"$match": {"sym": random.choice(syms)}},
                    {"$group": {"_id": "$sym", "vol": {"$sum": "$qty"},
                                "avg": {"$avg": "$price"}}},
                ]})
            n += 1
        except Exception as e:
            print(f"crud[{wid}] reconnect: {e}")
            time.sleep(0.5)
            s = connect()
        time.sleep(random.uniform(0.01, 0.05))


# Concurrency strategy for tx workers: "occ" (optimistic — conflicts on
# the hot account) or "for_update" (pessimistic locks — no conflicts,
# transactions queue on the hot account). Set via env MODE.
TX_MODE = os.environ.get("MODE", "occ")


def tx_worker(wid):
    """Transfers that all touch the hot 'fee' account.

    occ:        blind $inc updates + commit -> commit conflicts under
                contention (the retry-storm signal).
    for_update: lock all touched accounts (sorted, 'fee' last) with
                find_for_update before writing -> contenders queue on the
                lock, zero commit conflicts.
    """
    s = connect()
    while True:
        try:
            frm = f"acct-{random.randrange(200)}"
            to = f"acct-{random.randrange(200)}"
            rpc(s, {"cmd": "begin_tx"})
            if TX_MODE == "for_update":
                # Lock in a global order to avoid deadlock: sorted account
                # ids, then "fee" (sorts after every "acct-*").
                for acct in sorted({frm, to}) + ["fee"]:
                    rpc(s, {"cmd": "find_for_update", "collection": "accounts",
                            "query": {"id": acct}, "lock_timeout_ms": 5000})
            # Proper double-entry: the fee is DEBITED from the sender, so
            # every transfer nets zero and total balance is conserved.
            rpc(s, {"cmd": "update", "collection": "accounts",
                    "query": {"id": frm}, "update": {"$inc": {"balance": -2}}})
            rpc(s, {"cmd": "update", "collection": "accounts",
                    "query": {"id": to}, "update": {"$inc": {"balance": 1}}})
            rpc(s, {"cmd": "update", "collection": "accounts",
                    "query": {"id": "fee"}, "update": {"$inc": {"balance": 1}}})
            rpc(s, {"cmd": "commit_tx"})  # occ: may conflict; for_update: won't
        except Exception as e:
            print(f"tx[{wid}] reconnect: {e}")
            time.sleep(0.5)
            s = connect()
        time.sleep(random.uniform(0.02, 0.08))


def slow_worker():
    """Occasional unindexed regex scan -> trips the slow-query profiler."""
    s = connect()
    while True:
        try:
            rpc(s, {"cmd": "find", "collection": "trades",
                    "query": {"sym": {"$regex": "B.*T"}}, "limit": 5})
        except Exception:
            time.sleep(1)
            s = connect()
        time.sleep(3)


if __name__ == "__main__":
    seed()
    threads = [threading.Thread(target=crud_worker, args=(i,), daemon=True) for i in range(6)]
    threads += [threading.Thread(target=tx_worker, args=(i,), daemon=True) for i in range(4)]
    threads.append(threading.Thread(target=slow_worker, daemon=True))
    for t in threads:
        t.start()
    print(f"load running: 6 CRUD + 4 tx (hot-account, MODE={TX_MODE}) + 1 slow worker -> {HOST}:{PORT}")
    print("Ctrl-C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nstopped.")
