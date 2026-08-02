#!/usr/bin/env julia
# 02_bulk_load_and_index.jl — bulk insert 10 000 rows, build an index,
# then range-query in O(log n) by leaning on the index.

using OxiDb
using Random
Random.seed!(42)

OxiDb.connect("127.0.0.1", 4444) do db
    delete(db, "events", Dict{String,Any}())

    @info "Bulk insert 10 000 events"
    docs = [Dict(
        "user_id"  => rand(1:1000),
        "event"    => rand(["login", "click", "purchase", "logout"]),
        "amount"   => round(rand() * 1000, digits = 2),
        "ts"       => time() - rand(0:86_400),
    ) for _ in 1:10_000]

    @time insert_many(db, "events", docs)

    @info "Index user_id for fast lookup"
    exec(db, "create_index"; collection = "events", field = "user_id")

    @info "Range query — purchases by user 42"
    rows = find(db, "events";
                query = Dict("user_id" => 42, "event" => "purchase"),
                sort  = Dict("ts" => -1),
                limit = 5)
    println("Found ", length(rows), " purchases")
    foreach(r -> println("  ", r["amount"], " @ ", r["ts"]), rows)
end
