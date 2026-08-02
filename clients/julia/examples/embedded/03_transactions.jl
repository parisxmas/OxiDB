#!/usr/bin/env julia
# 03_transactions.jl — `transaction(db) do ... end` commits on success and
# rolls back automatically if the block throws. All-or-nothing.

using OxiDbEmbedded

db = open_db(mktempdir())
insert(db, "accounts", Dict("id" => "alice", "balance" => 500))
insert(db, "accounts", Dict("id" => "bob",   "balance" => 100))

function transfer!(db, from, to, amount)
    transaction(db) do
        balance = find_one(db, "accounts", Dict("id" => from))["balance"]
        balance < amount && error("insufficient funds: $from has \$$balance")
        update(db, "accounts", Dict("id" => from), Dict("\$inc" => Dict("balance" => -amount)))
        update(db, "accounts", Dict("id" => to),   Dict("\$inc" => Dict("balance" =>  amount)))
    end
end

transfer!(db, "alice", "bob", 200)                       # commits
try
    transfer!(db, "bob", "alice", 999)                   # throws -> rolled back
catch e
    println("rolled back: ", e isa OxiDbError ? e.msg : sprint(showerror, e))
end

for a in find(db, "accounts", Dict(); sort = Dict("id" => 1))
    println("  ", a["id"], ": \$", a["balance"])         # alice 300, bob 300
end

close(db)
