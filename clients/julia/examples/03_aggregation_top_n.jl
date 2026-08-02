#!/usr/bin/env julia
# 03_aggregation_top_n.jl — top-5 customers by spend last 30 days.
# Pipeline: $match → $group → $sort → $limit.

using OxiDb

OxiDb.connect("127.0.0.1", 4444) do db
    delete(db, "orders", Dict{String,Any}())

    insert_many(db, "orders", [
        Dict("customer" => "alice", "amount" => 120.50, "status" => "paid"),
        Dict("customer" => "bob",   "amount" =>  45.00, "status" => "paid"),
        Dict("customer" => "alice", "amount" => 999.00, "status" => "paid"),
        Dict("customer" => "carol", "amount" => 320.00, "status" => "paid"),
        Dict("customer" => "dan",   "amount" =>  10.00, "status" => "refunded"),
        Dict("customer" => "alice", "amount" =>  50.00, "status" => "paid"),
        Dict("customer" => "bob",   "amount" => 175.00, "status" => "paid"),
    ])

    top = aggregate(db, "orders", [
        Dict("\$match" => Dict("status" => "paid")),
        Dict("\$group" => Dict(
            "_id"   => "\$customer",
            "spend" => Dict("\$sum" => "\$amount"),
            "n"     => Dict("\$sum" => 1),
        )),
        Dict("\$sort"  => Dict("spend" => -1)),
        Dict("\$limit" => 5),
    ])

    println("Top customers:")
    for (i, row) in enumerate(top)
        println("  $i. $(row["_id"]): \$$(row["spend"]) ($(row["n"]) orders)")
    end
end
