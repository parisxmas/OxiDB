#!/usr/bin/env julia
# 08_oxiscript_procedure.jl — server-side stored procedure in OxiScript.
# One round-trip executes find + validate + update + insert atomically.

using OxiDb

const TRANSFER_SCRIPT = """
proc transfer(from, to, amount) {
    let s = find_one("accounts", {id: from})
    if s == null            { abort "sender not found" }
    if s.balance < amount   { abort "insufficient funds" }
    update("accounts", {id: from}, {\$inc: {balance: -amount}})
    update("accounts", {id: to},   {\$inc: {balance:  amount}})
    insert("ledger", {from: from, to: to, amount: amount})
    return {ok: true, sender_left: s.balance - amount}
}
"""

OxiDb.connect("127.0.0.1", 4444) do db
    delete(db, "accounts", Dict{String,Any}())
    delete(db, "ledger",   Dict{String,Any}())
    insert(db, "accounts", Dict("id" => "alice", "balance" => 1000))
    insert(db, "accounts", Dict("id" => "bob",   "balance" =>    0))

    exec(db, "create_procedure"; name = "transfer", script = TRANSFER_SCRIPT)

    result = exec(db, "call_procedure";
                  name   = "transfer",
                  params = Dict("from" => "alice", "to" => "bob", "amount" => 250))
    @show result   # → {ok: true, sender_left: 750}

    println(find_one(db, "accounts", Dict("id" => "alice")))
    println(find_one(db, "accounts", Dict("id" => "bob")))
end
