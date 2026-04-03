#!/usr/bin/env julia
#
# OxiDB Julia Example — Stored Procedures & OxiScript
#
# Demonstrates creating, calling, and managing stored procedures
# using both JSON definitions and OxiScript syntax.
#
# Prerequisites:
#   1. Start oxidb-server on 127.0.0.1:4444
#   2. Run: cd examples/julia && julia --project=. stored_procedures_example.jl
#

using OxiDb

const DIV = "-" ^ 60

function main()
    client = connect_oxidb("127.0.0.1", 4444)
    println("Connected to OxiDB.\n")

    # Clean up
    for col in ["accounts", "transactions", "products", "orders", "alerts"]
        try; drop_collection(client, col); catch; end
    end
    for proc in ["get_balance", "transfer", "check_stock", "place_order", "classify", "greet"]
        try; delete_procedure(client, proc); catch; end
    end

    # ══════════════════════════════════════════════════════════════
    # 1. Compile OxiScript (without registering)
    # ══════════════════════════════════════════════════════════════
    println("1. Compile OxiScript")
    compiled = compile_oxiscript(client, """
        proc greet(name) {
            return {message: "hello", to: name}
        }
    """)
    println("   Compiled: $(compiled["name"])")
    println("   Steps: $(length(compiled["steps"]))")

    # ══════════════════════════════════════════════════════════════
    # 2. Simple procedure — get_balance
    # ══════════════════════════════════════════════════════════════
    println("\n2. Create & call get_balance")

    # Seed accounts
    for acc in [
        Dict("account_id" => "alice", "name" => "Alice", "balance" => 10000),
        Dict("account_id" => "bob",   "name" => "Bob",   "balance" => 5000),
        Dict("account_id" => "carol", "name" => "Carol", "balance" => 200),
    ]
        insert(client, "accounts", acc)
    end
    create_index(client, "accounts", "account_id")

    create_procedure(client; script="""
        proc get_balance(account_id) {
            let acc = find_one("accounts", {account_id: account_id})
            if acc == null {
                abort "account not found"
            }
            return acc.balance
        }
    """)

    balance = call_procedure(client, "get_balance"; params=Dict("account_id" => "alice"))
    println("   Alice's balance: $balance")

    # Test abort
    try
        call_procedure(client, "get_balance"; params=Dict("account_id" => "nobody"))
    catch e
        println("   Expected error: $(e.msg)")
    end

    # ══════════════════════════════════════════════════════════════
    # 3. Multi-step procedure — transfer funds
    # ══════════════════════════════════════════════════════════════
    println("\n3. Transfer funds")
    create_procedure(client; script="""
        proc transfer(from, to, amount) {
            let sender = find_one("accounts", {account_id: from})
            let receiver = find_one("accounts", {account_id: to})
            if sender == null { abort "sender not found" }
            if receiver == null { abort "receiver not found" }
            if sender.balance < amount { abort "insufficient funds" }

            update("accounts", {account_id: from}, {\$inc: {balance: -amount}})
            update("accounts", {account_id: to},   {\$inc: {balance: amount}})
            insert("transactions", {from: from, to: to, amount: amount, type: "transfer"})
            return {status: "ok", transferred: amount}
        }
    """)

    result = call_procedure(client, "transfer";
        params=Dict("from" => "alice", "to" => "bob", "amount" => 2000))
    println("   Transfer result: $result")

    alice = find_one(client, "accounts", Dict("account_id" => "alice"))
    bob   = find_one(client, "accounts", Dict("account_id" => "bob"))
    println("   Alice: $(alice["balance"]), Bob: $(bob["balance"])")

    # Insufficient funds — should abort and rollback
    try
        call_procedure(client, "transfer";
            params=Dict("from" => "carol", "to" => "bob", "amount" => 9999))
    catch e
        println("   Expected error: $(e.msg)")
    end
    carol = find_one(client, "accounts", Dict("account_id" => "carol"))
    println("   Carol balance unchanged: $(carol["balance"])")

    # ══════════════════════════════════════════════════════════════
    # 4. Procedure calling procedure — place_order
    # ══════════════════════════════════════════════════════════════
    println("\n4. Procedure calling procedure (place_order)")

    for prod in [
        Dict("sku" => "LAPTOP", "name" => "Laptop", "price" => 999, "stock" => 10),
        Dict("sku" => "MOUSE",  "name" => "Mouse",  "price" => 29,  "stock" => 100),
    ]
        insert(client, "products", prod)
    end
    create_index(client, "products", "sku")

    create_procedure(client; script="""
        proc check_stock(sku) {
            let product = find_one("products", {sku: sku})
            if product == null { abort "product not found" }
            return {sku: product.sku, name: product.name, price: product.price, stock: product.stock}
        }
    """)

    create_procedure(client; script="""
        proc place_order(account_id, sku, qty, total) {
            let product = check_stock({sku: sku})
            if product.stock < qty { abort "out of stock" }
            let buyer = find_one("accounts", {account_id: account_id})
            if buyer == null { abort "account not found" }
            if buyer.balance < total { abort "insufficient funds" }
            update("products", {sku: sku}, {\$inc: {stock: -qty}})
            update("accounts", {account_id: account_id}, {\$inc: {balance: -total}})
            insert("orders", {account_id: account_id, sku: sku, qty: qty, total: total, status: "confirmed"})
            return {status: "confirmed", product: product.name, qty: qty, total: total}
        }
    """)

    result = call_procedure(client, "place_order";
        params=Dict("account_id" => "bob", "sku" => "MOUSE", "qty" => 2, "total" => 58))
    println("   Order: $result")

    mouse = find_one(client, "products", Dict("sku" => "MOUSE"))
    println("   Mouse stock: $(mouse["stock"])")

    # ══════════════════════════════════════════════════════════════
    # 5. If-else-if chain — classify
    # ══════════════════════════════════════════════════════════════
    println("\n5. If-else-if chain (classify)")
    create_procedure(client; script="""
        proc classify(score) {
            if score >= 90 { return {grade: "A"} }
            else if score >= 70 { return {grade: "B"} }
            else if score >= 50 { return {grade: "C"} }
            else { return {grade: "F"} }
        }
    """)

    for score in [95, 80, 60, 30]
        result = call_procedure(client, "classify"; params=Dict("score" => score))
        println("   Score $score → Grade $(result["grade"])")
    end

    # ══════════════════════════════════════════════════════════════
    # 6. Procedure management
    # ══════════════════════════════════════════════════════════════
    println("\n6. Procedure management")
    procs = list_procedures(client)
    println("   Registered procedures: $procs")

    def = get_procedure(client, "classify")
    println("   classify has $(length(def["steps"])) steps")

    delete_procedure(client, "greet")
    println("   Deleted 'greet'")

    procs = list_procedures(client)
    println("   Remaining: $procs")

    # ══════════════════════════════════════════════════════════════
    # 7. Performance — batch procedure calls
    # ══════════════════════════════════════════════════════════════
    println("\n7. Performance")

    t0 = time()
    for _ in 1:100
        call_procedure(client, "get_balance"; params=Dict("account_id" => "alice"))
    end
    ms = (time() - t0) * 1000
    println("   100x get_balance: $(round(ms, digits=1)) ms ($(round(ms/100, digits=2)) ms/call)")

    t0 = time()
    for _ in 1:50
        call_procedure(client, "transfer";
            params=Dict("from" => "alice", "to" => "bob", "amount" => 1))
    end
    ms = (time() - t0) * 1000
    println("   50x transfer: $(round(ms, digits=1)) ms ($(round(ms/50, digits=2)) ms/call)")

    close(client)
    println("\nDone!")
end

main()
