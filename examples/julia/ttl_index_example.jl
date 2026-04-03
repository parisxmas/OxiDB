#!/usr/bin/env julia
#
# OxiDB Julia Example — TTL Indexes
#
# Demonstrates automatic document expiration using TTL indexes.
#
# Prerequisites:
#   1. Start oxidb-server on 127.0.0.1:4444
#   2. Run: cd examples/julia && julia --project=. ttl_index_example.jl
#

using OxiDb
using Dates

function main()
    client = connect_oxidb("127.0.0.1", 4444)
    println("Connected to OxiDB.\n")

    # Clean up
    try; drop_collection(client, "sessions"); catch; end
    try; drop_collection(client, "cache"); catch; end

    # ──────────────────────────────────────────────────────────────
    # 1. Create a TTL index — sessions expire after 30 seconds
    # ──────────────────────────────────────────────────────────────
    println("1. Creating TTL index on sessions.created_at (30s expiry)")
    create_ttl_index(client, "sessions", "created_at", 30)

    indexes = list_indexes(client, "sessions")
    for idx in indexes
        println("   Index: $(idx["name"]) ($(idx["index_type"]))")
        if haskey(idx, "expire_after_seconds") && idx["expire_after_seconds"] !== nothing
            println("          expireAfterSeconds = $(idx["expire_after_seconds"])")
        end
    end

    # ──────────────────────────────────────────────────────────────
    # 2. Insert sessions with timestamps
    # ──────────────────────────────────────────────────────────────
    println("\n2. Inserting sessions...")

    # Current session — should survive
    now_str = Dates.format(now(UTC), dateformat"yyyy-mm-ddTHH:MM:SSZ")
    insert(client, "sessions", Dict(
        "user" => "alice",
        "created_at" => now_str,
        "data" => Dict("theme" => "dark", "lang" => "en")
    ))
    println("   Inserted alice's session (now)")

    # Recent session — should also survive (within 30s)
    insert(client, "sessions", Dict(
        "user" => "bob",
        "created_at" => now_str,
        "data" => Dict("theme" => "light", "lang" => "de")
    ))
    println("   Inserted bob's session (now)")

    # ──────────────────────────────────────────────────────────────
    # 3. Check current state
    # ──────────────────────────────────────────────────────────────
    println("\n3. Current sessions:")
    n = count_docs(client, "sessions")
    println("   Total sessions: $n")

    docs = find(client, "sessions", Dict())
    for doc in docs
        println("   - $(doc["user"]): created_at=$(doc["created_at"])")
    end

    # ──────────────────────────────────────────────────────────────
    # 4. Cache example — very short TTL
    # ──────────────────────────────────────────────────────────────
    println("\n4. Cache with 60s TTL")
    create_ttl_index(client, "cache", "cached_at", 60)

    for i in 1:5
        insert(client, "cache", Dict(
            "key" => "item_$i",
            "value" => "data_$i",
            "cached_at" => now_str
        ))
    end
    println("   Inserted 5 cache entries")
    println("   Cache size: $(count_docs(client, "cache"))")

    # ──────────────────────────────────────────────────────────────
    # 5. Drop TTL index
    # ──────────────────────────────────────────────────────────────
    println("\n5. Dropping TTL index")
    drop_index(client, "sessions", "created_at_ttl")
    indexes = list_indexes(client, "sessions")
    ttl_count = count(idx -> idx["index_type"] == "ttl", indexes)
    println("   TTL indexes remaining: $ttl_count")

    # ──────────────────────────────────────────────────────────────
    # 6. SQL syntax
    # ──────────────────────────────────────────────────────────────
    println("\n6. TTL index via SQL")
    try; drop_collection(client, "logs"); catch; end
    sql(client, "CREATE TTL INDEX ON logs (timestamp) EXPIRE AFTER 86400")
    println("   Created TTL index via SQL (24h expiry)")

    indexes = list_indexes(client, "logs")
    for idx in indexes
        if idx["index_type"] == "ttl"
            println("   TTL index: $(idx["name"]), expire=$(idx["expire_after_seconds"])s")
        end
    end

    close(client)
    println("\nDone!")
end

main()
