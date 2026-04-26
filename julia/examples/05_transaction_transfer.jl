#!/usr/bin/env julia
# 05_transaction_transfer.jl — atomic two-account transfer with rollback
# on insufficient funds. Either both balances change or neither does.

using OxiDb

function transfer!(db, from::AbstractString, to::AbstractString, amount::Real)
    tx_info = exec(db, "begin_tx")
    tx_id = tx_info["tx_id"]
    try
        sender = find_one(db, "accounts", Dict("id" => from))
        sender === nothing && error("sender not found")
        sender["balance"] < amount && error("insufficient funds")

        update(db, "accounts", Dict("id" => from),
                              Dict("\$inc" => Dict("balance" => -amount)))
        update(db, "accounts", Dict("id" => to),
                              Dict("\$inc" => Dict("balance" =>  amount)))
        insert(db, "ledger", Dict("from" => from, "to" => to, "amount" => amount))

        exec(db, "commit_tx")
        return :committed
    catch e
        try exec(db, "rollback_tx"); catch; end
        @warn "transfer rolled back" exception = e
        return :rolled_back
    end
end

OxiDb.connect("127.0.0.1", 4444) do db
    delete(db, "accounts", Dict{String,Any}())
    delete(db, "ledger",   Dict{String,Any}())
    insert(db, "accounts", Dict("id" => "alice", "balance" => 500))
    insert(db, "accounts", Dict("id" => "bob",   "balance" => 100))

    @show transfer!(db, "alice", "bob", 200)   # → :committed
    @show transfer!(db, "bob",   "alice", 999) # → :rolled_back

    for a in find(db, "accounts")
        println("  $(a["id"]): \$$(a["balance"])")
    end
end
