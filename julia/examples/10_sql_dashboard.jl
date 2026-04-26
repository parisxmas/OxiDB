#!/usr/bin/env julia
# 10_sql_dashboard.jl — same data, queried with SQL.
# OxiDB speaks SQL alongside JSON; pick whichever fits the question.

using OxiDb
using Printf

OxiDb.connect("127.0.0.1", 4444) do db
    delete(db, "sales", Dict{String,Any}())

    insert_many(db, "sales", [
        Dict("region" => "EU", "category" => "books",       "amount" =>  29.99, "month" => "2026-01"),
        Dict("region" => "EU", "category" => "electronics", "amount" => 599.00, "month" => "2026-01"),
        Dict("region" => "US", "category" => "books",       "amount" =>  19.50, "month" => "2026-01"),
        Dict("region" => "US", "category" => "electronics", "amount" => 899.00, "month" => "2026-02"),
        Dict("region" => "EU", "category" => "books",       "amount" =>  39.00, "month" => "2026-02"),
        Dict("region" => "TR", "category" => "electronics", "amount" => 449.00, "month" => "2026-02"),
    ])

    rows = sql(db, """
        SELECT region, category, COUNT(*) AS n, SUM(amount) AS total
        FROM sales
        WHERE month >= '2026-01'
        GROUP BY region, category
        ORDER BY total DESC
        LIMIT 10
    """)

    @printf "%-8s %-15s %4s %12s\n" "REGION" "CATEGORY" "N" "TOTAL"
    println(repeat('-', 42))
    for r in rows
        @printf "%-8s %-15s %4d %12.2f\n" r["region"] r["category"] r["n"] r["total"]
    end
end
