#!/usr/bin/env julia
#
# OxiDB 1M Record Benchmark — compare query performance with and without composite index
#

import Pkg
const _pkg_path = joinpath(@__DIR__, "..", "..", "julia", "OxiDbEmbedded")
try
    using OxiDbEmbedded
catch
    Pkg.develop(path=_pkg_path)
    using OxiDbEmbedded
end

using Printf, Random

const CITIES = ["New York", "London", "Paris", "Berlin", "Tokyo",
                "Sydney", "Toronto", "Mumbai", "Shanghai", "Dubai"]
const BATCH_SIZE = 5000
const TOTAL = 1_000_000

function timed(f)
    t0 = time_ns()
    result = f()
    ms = (time_ns() - t0) / 1_000_000
    (result, ms)
end

fmt(ms) = ms < 1000 ? @sprintf("%.2fms", ms) : @sprintf("%.2fs", ms / 1000)

function main()
    db_path = mktempdir() * "/oxidb_bench_1m"
    println("Opening database at: $db_path")
    db = open_db(db_path)

    # ------------------------------------------------------------------
    # 1. Insert 1M records in batches
    # ------------------------------------------------------------------
    println("\n=== Inserting $TOTAL records (batch size $BATCH_SIZE) ===")
    rng = Random.MersenneTwister(42)

    t0 = time_ns()
    for batch_start in 1:BATCH_SIZE:TOTAL
        batch_end = min(batch_start + BATCH_SIZE - 1, TOTAL)
        docs = [Dict{String,Any}(
            "name"  => "user_$i",
            "age"   => rand(rng, 18:65),
            "city"  => CITIES[rand(rng, 1:length(CITIES))],
            "email" => "user_$i@example.com",
            "score" => rand(rng) * 100
        ) for i in batch_start:batch_end]
        insert_many(db, "users", docs)
        if batch_start % 100_000 < BATCH_SIZE
            elapsed = (time_ns() - t0) / 1_000_000_000
            @printf("  %d / %d (%.1fs)\n", batch_end, TOTAL, elapsed)
        end
    end
    insert_ms = (time_ns() - t0) / 1_000_000
    println("Insert complete: $(fmt(insert_ms))")

    n = count_docs(db, "users")
    println("Total docs: $n")

    # ------------------------------------------------------------------
    # 2. Queries WITHOUT index
    # ------------------------------------------------------------------
    println("\n=== Queries WITHOUT index ===")

    # Exact match on both fields (~2K results)
    r, ms = timed(() -> find(db, "users", Dict("city" => "Tokyo", "age" => 25)))
    @printf("  find(city=\"Tokyo\", age=25)      [%d docs]: %s\n", length(r), fmt(ms))

    # Narrow range (~5K results)
    r, ms = timed(() -> find(db, "users", Dict("city" => "London", "age" => Dict("\$gte" => 30, "\$lte" => 32))))
    @printf("  find(city=\"London\", age 30-32)  [%d docs]: %s\n", length(r), fmt(ms))

    # Sort + limit (10 results)
    r, ms = timed(() -> find(db, "users", Dict("city" => "Berlin"); sort=Dict("age" => -1), limit=10))
    @printf("  find(city=\"Berlin\", sort, lim10)[%d docs]: %s\n", length(r), fmt(ms))

    # Count exact
    r, ms = timed(() -> count_docs(db, "users", Dict("city" => "Dubai", "age" => 40)))
    @printf("  count(city=\"Dubai\", age=40)     [n=%d]:    %s\n", r, fmt(ms))

    # Count narrow range
    r, ms = timed(() -> count_docs(db, "users", Dict("city" => "Paris", "age" => Dict("\$gte" => 50, "\$lte" => 55))))
    @printf("  count(city=\"Paris\", age 50-55)  [n=%d]:  %s\n", r, fmt(ms))

    # find_one
    r, ms = timed(() -> find_one(db, "users", Dict("city" => "Sydney", "age" => 18)))
    @printf("  find_one(city=\"Sydney\", age=18)  [1 doc]:  %s\n", fmt(ms))

    # Aggregation — group by city (10 groups)
    _, ms = timed(() -> aggregate(db, "users", [
        Dict("\$group" => Dict("_id" => "\$city", "count" => Dict("\$sum" => 1))),
        Dict("\$sort" => Dict("count" => -1))
    ]))
    @printf("  aggregate(group by city)        [10 groups]: %s\n", fmt(ms))

    # Aggregation — match narrow + group
    _, ms = timed(() -> aggregate(db, "users", [
        Dict("\$match" => Dict("city" => "Tokyo", "age" => Dict("\$gte" => 60))),
        Dict("\$group" => Dict("_id" => nothing, "count" => Dict("\$sum" => 1), "avg_score" => Dict("\$avg" => "\$score")))
    ]))
    @printf("  aggregate(city=Tokyo,age>=60)   [1 group]:  %s\n", fmt(ms))

    # ------------------------------------------------------------------
    # 3. Create composite index on [city, age]
    # ------------------------------------------------------------------
    println("\n=== Creating composite index on [city, age] ===")
    _, ms = timed(() -> create_composite_index(db, "users", ["city", "age"]))
    @printf("  Index created: %s\n", fmt(ms))

    # ------------------------------------------------------------------
    # 4. Same queries WITH composite index
    # ------------------------------------------------------------------
    println("\n=== Queries WITH composite index [city, age] ===")

    r, ms = timed(() -> find(db, "users", Dict("city" => "Tokyo", "age" => 25)))
    @printf("  find(city=\"Tokyo\", age=25)      [%d docs]: %s\n", length(r), fmt(ms))

    r, ms = timed(() -> find(db, "users", Dict("city" => "London", "age" => Dict("\$gte" => 30, "\$lte" => 32))))
    @printf("  find(city=\"London\", age 30-32)  [%d docs]: %s\n", length(r), fmt(ms))

    r, ms = timed(() -> find(db, "users", Dict("city" => "Berlin"); sort=Dict("age" => -1), limit=10))
    @printf("  find(city=\"Berlin\", sort, lim10)[%d docs]: %s\n", length(r), fmt(ms))

    r, ms = timed(() -> count_docs(db, "users", Dict("city" => "Dubai", "age" => 40)))
    @printf("  count(city=\"Dubai\", age=40)     [n=%d]:    %s\n", r, fmt(ms))

    r, ms = timed(() -> count_docs(db, "users", Dict("city" => "Paris", "age" => Dict("\$gte" => 50, "\$lte" => 55))))
    @printf("  count(city=\"Paris\", age 50-55)  [n=%d]:  %s\n", r, fmt(ms))

    r, ms = timed(() -> find_one(db, "users", Dict("city" => "Sydney", "age" => 18)))
    @printf("  find_one(city=\"Sydney\", age=18)  [1 doc]:  %s\n", fmt(ms))

    _, ms = timed(() -> aggregate(db, "users", [
        Dict("\$group" => Dict("_id" => "\$city", "count" => Dict("\$sum" => 1))),
        Dict("\$sort" => Dict("count" => -1))
    ]))
    @printf("  aggregate(group by city)        [10 groups]: %s\n", fmt(ms))

    _, ms = timed(() -> aggregate(db, "users", [
        Dict("\$match" => Dict("city" => "Tokyo", "age" => Dict("\$gte" => 60))),
        Dict("\$group" => Dict("_id" => nothing, "count" => Dict("\$sum" => 1), "avg_score" => Dict("\$avg" => "\$score")))
    ]))
    @printf("  aggregate(city=Tokyo,age>=60)   [1 group]:  %s\n", fmt(ms))

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    println("\n=== Cleanup ===")
    drop_collection(db, "users")
    close(db)
    println("Done.")
end

main()
