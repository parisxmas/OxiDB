#!/usr/bin/env python3
"""Integration tests for OxiScript stored procedures.

Tests compilation, execution, procedure-calling-procedure,
error handling, and measures execution times.
"""

import json
import os
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time


PORT = 14777


def send_recv(sock, payload):
    data = json.dumps(payload).encode()
    sock.sendall(struct.pack("<I", len(data)) + data)
    lb = b""
    while len(lb) < 4:
        lb += sock.recv(4 - len(lb))
    length = struct.unpack("<I", lb)[0]
    rb = b""
    while len(rb) < length:
        rb += sock.recv(length - len(rb))
    return json.loads(rb)


def connect(port, retries=30, delay=0.3):
    for _ in range(retries):
        try:
            sock = socket.create_connection(("127.0.0.1", port), timeout=5)
            r = send_recv(sock, {"cmd": "ping"})
            if r.get("ok"):
                return sock
            sock.close()
        except (ConnectionRefusedError, ConnectionError, OSError):
            pass
        time.sleep(delay)
    raise RuntimeError(f"cannot connect to port {port}")


def get_data(resp):
    return resp.get("data")


def timed(sock, payload):
    """Send command and return (response, elapsed_ms)."""
    t0 = time.perf_counter()
    resp = send_recv(sock, payload)
    elapsed = (time.perf_counter() - t0) * 1000
    return resp, elapsed


def main():
    server_bin = os.path.join(os.path.dirname(__file__), "..", "target", "release", "oxidb-server")
    if not os.path.exists(server_bin):
        print("FATAL: oxidb-server not found. Run `cargo build --release`.")
        sys.exit(1)

    tmpdir = tempfile.mkdtemp(prefix="oxiscript_test_")
    proc = None

    try:
        proc = subprocess.Popen(
            [server_bin],
            env={**os.environ, "OXIDB_ADDR": f"127.0.0.1:{PORT}", "OXIDB_DATA": tmpdir},
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        s = connect(PORT)
        print("Server ready.\n")

        passed = 0
        failed = 0
        timings = []

        def check(name, condition, detail=""):
            nonlocal passed, failed
            if condition:
                print(f"  PASS: {name}")
                passed += 1
            else:
                print(f"  FAIL: {name} {detail}")
                failed += 1

        # ════════════════════════════════════════════════════════════
        # Setup: seed data
        # ════════════════════════════════════════════════════════════

        send_recv(s, {"cmd": "drop_collection", "collection": "accounts"})
        send_recv(s, {"cmd": "drop_collection", "collection": "transactions"})
        send_recv(s, {"cmd": "drop_collection", "collection": "alerts"})
        send_recv(s, {"cmd": "drop_collection", "collection": "reports"})
        send_recv(s, {"cmd": "drop_collection", "collection": "products"})
        send_recv(s, {"cmd": "drop_collection", "collection": "orders"})
        send_recv(s, {"cmd": "drop_collection", "collection": "order_items"})
        send_recv(s, {"cmd": "drop_collection", "collection": "logs"})

        # Create accounts
        for acc in [
            {"account_id": "alice", "name": "Alice", "balance": 10000, "total_sent": 0, "tx_count": 0},
            {"account_id": "bob", "name": "Bob", "balance": 5000, "total_sent": 0, "tx_count": 0},
            {"account_id": "charlie", "name": "Charlie", "balance": 200, "total_sent": 0, "tx_count": 0},
        ]:
            send_recv(s, {"cmd": "insert", "collection": "accounts", "doc": acc})

        # Create products
        for prod in [
            {"sku": "LAPTOP", "name": "Laptop", "price": 999, "stock": 50},
            {"sku": "MOUSE", "name": "Mouse", "price": 29, "stock": 200},
            {"sku": "KEYBOARD", "name": "Keyboard", "price": 79, "stock": 0},
        ]:
            send_recv(s, {"cmd": "insert", "collection": "products", "doc": prod})

        send_recv(s, {"cmd": "create_index", "collection": "accounts", "field": "account_id"})
        send_recv(s, {"cmd": "create_index", "collection": "products", "field": "sku"})

        # ════════════════════════════════════════════════════════════
        # Test 1: compile_oxiscript — verify compilation
        # ════════════════════════════════════════════════════════════

        print("TEST 1: compile_oxiscript")
        script = 'proc hello(name) { return {greeting: name} }'
        resp, ms = timed(s, {"cmd": "compile_oxiscript", "script": script})
        check("compile returns ok", resp.get("ok"), str(resp))
        data = get_data(resp)
        check("compiled name is hello", data.get("name") == "hello", str(data))
        check("compiled has steps", len(data.get("steps", [])) > 0)
        timings.append(("compile simple proc", ms))

        # ════════════════════════════════════════════════════════════
        # Test 2: Simple procedure — create and call
        # ════════════════════════════════════════════════════════════

        print("\nTEST 2: Simple procedure — create & call")
        script = '''
            proc get_balance(account_id) {
                let acc = find_one("accounts", {account_id: account_id})
                if acc == null {
                    abort "account not found"
                }
                return acc.balance
            }
        '''
        resp, ms = timed(s, {"cmd": "create_procedure", "script": script})
        check("create get_balance ok", resp.get("ok"), str(resp))
        timings.append(("create get_balance", ms))

        resp, ms = timed(s, {"cmd": "call_procedure", "name": "get_balance", "params": {"account_id": "alice"}})
        check("call get_balance ok", resp.get("ok"), str(resp))
        check("alice balance is 10000", get_data(resp) == 10000, str(get_data(resp)))
        timings.append(("call get_balance", ms))

        # ════════════════════════════════════════════════════════════
        # Test 3: Procedure with abort
        # ════════════════════════════════════════════════════════════

        print("\nTEST 3: Abort on invalid input")
        resp, ms = timed(s, {"cmd": "call_procedure", "name": "get_balance", "params": {"account_id": "nonexistent"}})
        check("abort returns error", not resp.get("ok"), str(resp))
        check("error message correct", "account not found" in resp.get("error", ""), str(resp))
        timings.append(("call get_balance (abort)", ms))

        # ════════════════════════════════════════════════════════════
        # Test 4: Transfer funds — multi-step with validation
        # ════════════════════════════════════════════════════════════

        print("\nTEST 4: Transfer funds (multi-step)")
        script = '''
            proc transfer(from, to, amount) {
                let sender = find_one("accounts", {account_id: from})
                let receiver = find_one("accounts", {account_id: to})

                if sender == null { abort "sender not found" }
                if receiver == null { abort "receiver not found" }
                if sender.balance < amount { abort "insufficient funds" }
                if amount <= 0 { abort "invalid amount" }

                // Combine all field updates into single update per doc (OCC requirement)
                update("accounts", {account_id: from}, {
                    $inc: {balance: -amount, total_sent: amount, tx_count: 1}
                })
                update("accounts", {account_id: to}, {
                    $inc: {balance: amount, tx_count: 1}
                })

                insert("transactions", {
                    from: from,
                    to: to,
                    amount: amount,
                    type: "transfer"
                })

                return {
                    status: "ok",
                    transferred: amount,
                    sender_balance: sender.balance - amount
                }
            }
        '''
        resp = send_recv(s, {"cmd": "create_procedure", "script": script})
        check("create transfer ok", resp.get("ok"), str(resp))

        # Transfer 1500 from alice to bob
        resp, ms = timed(s, {"cmd": "call_procedure", "name": "transfer", "params": {"from": "alice", "to": "bob", "amount": 1500}})
        check("transfer ok", resp.get("ok"), str(resp))
        data = get_data(resp) or {}
        check("transferred 1500", data.get("transferred") == 1500, str(data))
        timings.append(("transfer funds (7 steps)", ms))

        # Verify balances directly
        r = send_recv(s, {"cmd": "find_one", "collection": "accounts", "query": {"account_id": "bob"}})
        check("bob balance now 6500", get_data(r).get("balance") == 6500, str(get_data(r)))

        # Verify transaction log
        r = send_recv(s, {"cmd": "count", "collection": "transactions"})
        check("1 transaction logged", get_data(r).get("count") == 1, str(get_data(r)))

        # Transfer should fail — insufficient funds
        resp, ms = timed(s, {"cmd": "call_procedure", "name": "transfer", "params": {"from": "charlie", "to": "bob", "amount": 5000}})
        check("transfer fails for low balance", not resp.get("ok"))
        check("error is insufficient funds", "insufficient" in resp.get("error", ""))
        timings.append(("transfer funds (abort)", ms))

        # Verify charlie balance unchanged (rollback)
        r = send_recv(s, {"cmd": "find_one", "collection": "accounts", "query": {"account_id": "charlie"}})
        check("charlie balance unchanged (200)", get_data(r).get("balance") == 200, str(get_data(r)))

        # ════════════════════════════════════════════════════════════
        # Test 5: Procedure calling procedure
        # ════════════════════════════════════════════════════════════

        print("\nTEST 5: Procedure calling procedure")
        script = '''
            proc safe_withdraw(account_id, amount) {
                let balance = get_balance({account_id: account_id})
                if balance < amount {
                    abort "insufficient funds"
                }
                update("accounts", {account_id: account_id}, {
                    $inc: {balance: -amount, tx_count: 1}
                })
                insert("transactions", {
                    from: account_id,
                    to: "cash",
                    amount: amount,
                    type: "withdrawal"
                })
                return {withdrawn: amount, remaining: balance - amount}
            }
        '''
        resp = send_recv(s, {"cmd": "create_procedure", "script": script})
        check("create safe_withdraw ok", resp.get("ok"), str(resp))

        resp, ms = timed(s, {"cmd": "call_procedure", "name": "safe_withdraw", "params": {"account_id": "alice", "amount": 500}})
        check("safe_withdraw ok", resp.get("ok"), str(resp))
        data = get_data(resp) or {}
        check("withdrawn 500", data.get("withdrawn") == 500, str(data))
        timings.append(("safe_withdraw (calls get_balance)", ms))

        # Verify balance
        r = send_recv(s, {"cmd": "find_one", "collection": "accounts", "query": {"account_id": "alice"}})
        check("alice balance now 8000", get_data(r).get("balance") == 8000, str(get_data(r)))

        # ════════════════════════════════════════════════════════════
        # Test 6: E-commerce order — complex multi-collection
        # ════════════════════════════════════════════════════════════

        print("\nTEST 6: E-commerce place_order (multi-collection)")
        script = '''
            proc check_stock(sku) {
                let product = find_one("products", {sku: sku})
                if product == null {
                    abort "product not found"
                }
                return {sku: product.sku, name: product.name, price: product.price, stock: product.stock}
            }
        '''
        send_recv(s, {"cmd": "create_procedure", "script": script})

        script = '''
            proc place_order(account_id, sku, qty, total) {
                // Check product stock via another procedure
                let product = check_stock({sku: sku})

                if product.stock < qty {
                    abort "out of stock"
                }

                // Check buyer has enough funds
                let buyer = find_one("accounts", {account_id: account_id})
                if buyer == null {
                    abort "account not found"
                }

                if buyer.balance < total {
                    abort "insufficient funds"
                }

                // Deduct stock
                update("products", {sku: sku}, {$inc: {stock: -qty}})

                // Charge buyer
                update("accounts", {account_id: account_id}, {$inc: {balance: -total}})

                // Create order
                insert("orders", {
                    account_id: account_id,
                    sku: sku,
                    product_name: product.name,
                    qty: qty,
                    unit_price: product.price,
                    total: total,
                    status: "confirmed"
                })

                // Log it
                insert("logs", {
                    event: "order_placed",
                    account: account_id,
                    sku: sku,
                    total: total
                })

                return {
                    status: "confirmed",
                    product: product.name,
                    qty: qty,
                    total: total
                }
            }
        '''
        resp = send_recv(s, {"cmd": "create_procedure", "script": script})
        check("create place_order ok", resp.get("ok"), str(resp))

        # Place order: alice buys 2 mice (29 each = 58 total)
        resp, ms = timed(s, {"cmd": "call_procedure", "name": "place_order", "params": {"account_id": "alice", "sku": "MOUSE", "qty": 2, "total": 58}})
        check("place_order ok", resp.get("ok"), str(resp))
        data = get_data(resp) or {}
        check("order confirmed", data.get("status") == "confirmed", str(data))
        check("total is 58", data.get("total") == 58, str(data))
        check("product is Mouse", data.get("product") == "Mouse", str(data))
        timings.append(("place_order (calls check_stock, 8 steps)", ms))

        # Verify stock reduced
        r = send_recv(s, {"cmd": "find_one", "collection": "products", "query": {"sku": "MOUSE"}})
        check("mouse stock now 198", get_data(r).get("stock") == 198, str(get_data(r)))

        # Verify alice charged (8000 - 58 = 7942)
        r = send_recv(s, {"cmd": "find_one", "collection": "accounts", "query": {"account_id": "alice"}})
        check("alice balance reduced", get_data(r).get("balance") == 7942, str(get_data(r)))

        # Order out-of-stock product
        resp, ms = timed(s, {"cmd": "call_procedure", "name": "place_order", "params": {"account_id": "alice", "sku": "KEYBOARD", "qty": 1, "total": 79}})
        check("out of stock fails", not resp.get("ok"))
        check("error is out of stock", "out of stock" in resp.get("error", ""))
        timings.append(("place_order (out of stock abort)", ms))

        # ════════════════════════════════════════════════════════════
        # Test 7: Aggregation in procedure
        # ════════════════════════════════════════════════════════════

        print("\nTEST 7: Aggregation in procedure")
        # Add more transactions for aggregation
        for i in range(10):
            send_recv(s, {"cmd": "call_procedure", "name": "transfer", "params": {"from": "alice", "to": "bob", "amount": 10}})

        script = '''
            proc account_summary(account_id) {
                let acc = find_one("accounts", {account_id: account_id})
                let tx_count = count("transactions", {from: account_id})
                let recent = aggregate("transactions", [
                    {$match: {from: account_id}},
                    {$group: {_id: null, total: {$sum: "$amount"}, count: {$sum: 1}}}
                ])
                return {
                    name: acc.name,
                    balance: acc.balance,
                    transactions_sent: tx_count,
                    aggregate: recent
                }
            }
        '''
        resp = send_recv(s, {"cmd": "create_procedure", "script": script})
        check("create account_summary ok", resp.get("ok"), str(resp))

        resp, ms = timed(s, {"cmd": "call_procedure", "name": "account_summary", "params": {"account_id": "alice"}})
        check("account_summary ok", resp.get("ok"), str(resp))
        data = get_data(resp) or {}
        check("name is Alice", data.get("name") == "Alice", str(data))
        check("has balance", isinstance(data.get("balance"), (int, float)), str(data))
        check("has tx count", data.get("transactions_sent", 0) > 0, str(data))
        timings.append(("account_summary (find+count+aggregate)", ms))

        # ════════════════════════════════════════════════════════════
        # Test 8: list/get/delete procedures
        # ════════════════════════════════════════════════════════════

        print("\nTEST 8: Procedure management")
        resp = send_recv(s, {"cmd": "list_procedures"})
        check("list_procedures ok", resp.get("ok"))
        procs = get_data(resp)
        check("at least 5 procedures", len(procs) >= 5, f"got {len(procs)}: {procs}")

        resp = send_recv(s, {"cmd": "get_procedure", "name": "get_balance"})
        check("get_procedure ok", resp.get("ok"))
        check("has steps", len(get_data(resp).get("steps", [])) > 0)

        resp = send_recv(s, {"cmd": "delete_procedure", "name": "account_summary"})
        check("delete_procedure ok", resp.get("ok"))

        resp = send_recv(s, {"cmd": "call_procedure", "name": "account_summary", "params": {"account_id": "alice"}})
        check("deleted proc fails", not resp.get("ok"))

        # ════════════════════════════════════════════════════════════
        # Test 9: Batch performance — many calls
        # ════════════════════════════════════════════════════════════

        print("\nTEST 9: Batch performance")
        t0 = time.perf_counter()
        for i in range(100):
            send_recv(s, {"cmd": "call_procedure", "name": "get_balance", "params": {"account_id": "bob"}})
        batch_ms = (time.perf_counter() - t0) * 1000
        check("100 get_balance calls completed", True)
        timings.append(("100x get_balance (simple)", batch_ms))
        timings.append(("avg per get_balance call", batch_ms / 100))

        t0 = time.perf_counter()
        for i in range(50):
            send_recv(s, {"cmd": "call_procedure", "name": "transfer", "params": {"from": "bob", "to": "alice", "amount": 1}})
        batch_ms = (time.perf_counter() - t0) * 1000
        check("50 transfer calls completed", True)
        timings.append(("50x transfer (7-step)", batch_ms))
        timings.append(("avg per transfer call", batch_ms / 50))

        # ════════════════════════════════════════════════════════════
        # Summary
        # ════════════════════════════════════════════════════════════

        print(f"\n{'='*60}")
        print(f"Results: {passed} passed, {failed} failed")
        print(f"{'='*60}")

        print(f"\n{'─'*60}")
        print(f"{'Operation':<45} {'Time':>10}")
        print(f"{'─'*60}")
        for name, ms in timings:
            if ms < 1:
                print(f"  {name:<43} {ms:>8.3f} ms")
            else:
                print(f"  {name:<43} {ms:>8.1f} ms")

        print(f"{'─'*60}")

        s.close()
        sys.exit(0 if failed == 0 else 1)

    finally:
        if proc:
            try:
                proc.send_signal(signal.SIGTERM)
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
