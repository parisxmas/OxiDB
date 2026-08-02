#!/usr/bin/env julia
# 05_aggregation.jl — group / sum / sort with the aggregation pipeline,
# running entirely in-process.

using OxiDbEmbedded

db = open_db(mktempdir())
insert_many(db, "orders", [
    Dict("category" => "electronics", "amount" => 200, "status" => "completed"),
    Dict("category" => "books",       "amount" => 50,  "status" => "completed"),
    Dict("category" => "electronics", "amount" => 300, "status" => "completed"),
    Dict("category" => "books",       "amount" => 30,  "status" => "pending"),
    Dict("category" => "clothing",    "amount" => 100, "status" => "completed"),
])

println("revenue by category (completed orders only):")
for row in aggregate(db, "orders", [
    Dict("\$match" => Dict("status" => "completed")),
    Dict("\$group" => Dict("_id"   => "\$category",
                           "total" => Dict("\$sum" => "\$amount"),
                           "count" => Dict("\$sum" => 1))),
    Dict("\$sort"  => Dict("total" => -1)),
])
    println("  ", rpad(row["_id"], 12), " \$", row["total"], "  (", row["count"], " orders)")
end

close(db)
