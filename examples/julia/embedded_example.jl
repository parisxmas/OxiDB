#!/usr/bin/env julia
#
# OxiDB Embedded Julia Example — demonstrates every feature using the embedded
# database directly (no server needed).
#
# No prerequisites — just run:
#   julia embedded_example.jl
#
# The OxiDbEmbedded package auto-downloads the prebuilt native library on first run.
#

# Bootstrap: add the package from the local source tree
import Pkg
const _pkg_path = joinpath(@__DIR__, "..", "..", "julia", "OxiDbEmbedded")
try
    using OxiDbEmbedded
catch
    Pkg.develop(path=_pkg_path)
    using OxiDbEmbedded
end

using Printf

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

const DIVIDER = "=" ^ 60
fmt(ms) = @sprintf("%.2fms", ms)
section(title) = println("\n", DIVIDER, "\n  ", title, "\n", DIVIDER)

function timed(f)
    t0 = time_ns()
    result = f()
    ms = (time_ns() - t0) / 1_000_000
    (result, ms)
end

# ------------------------------------------------------------------
# Main demo
# ------------------------------------------------------------------

function main()
    db_path = mktempdir() * "/oxidb_julia_demo"
    println("Opening embedded database at: $db_path")
    db = open_db(db_path)

    # 1. Ping
    section("1. Ping")
    r, ms = timed(() -> ping(db))
    println("Ping: $r ($(fmt(ms)))")

    # 2. Collections
    section("2. Collection Management")
    create_collection(db, "users")
    create_collection(db, "orders")
    println("Collections: ", list_collections(db))

    # 3. Insert
    section("3. Insert Documents")
    r, ms = timed(() -> insert(db, "users", Dict(
        "name" => "Alice", "age" => 30, "email" => "alice@example.com",
        "city" => "New York", "tags" => ["admin", "premium"]
    )))
    println("Inserted Alice: $r ($(fmt(ms)))")

    users = [
        Dict("name" => "Bob",     "age" => 25, "email" => "bob@example.com",     "city" => "London",   "tags" => ["user"]),
        Dict("name" => "Charlie", "age" => 35, "email" => "charlie@example.com", "city" => "Paris",    "tags" => ["user", "premium"]),
        Dict("name" => "Diana",   "age" => 28, "email" => "diana@example.com",   "city" => "Berlin",   "tags" => ["user"]),
        Dict("name" => "Eve",     "age" => 42, "email" => "eve@example.com",     "city" => "Tokyo",    "tags" => ["admin"]),
        Dict("name" => "Frank",   "age" => 19, "email" => "frank@example.com",   "city" => "New York", "tags" => ["user"]),
    ]
    _, ms = timed(() -> insert_many(db, "users", users))
    println("Batch inserted $(length(users)) users ($(fmt(ms)))")

    # 4. Find
    section("4. Find Documents")
    docs = find(db, "users", Dict())
    println("All users ($(length(docs))):")
    for d in docs
        println("  ", d["name"], " age=", d["age"], " city=", d["city"])
    end

    println("\nUsers age >= 30:")
    for d in find(db, "users", Dict("age" => Dict("\$gte" => 30)))
        println("  ", d["name"], " age=", d["age"])
    end

    println("\nTop 3 oldest users:")
    for d in find(db, "users", Dict(); sort=Dict("age" => -1), limit=3)
        println("  ", d["name"], " age=", d["age"])
    end

    println("\nUsers in New York OR age < 20:")
    for d in find(db, "users", Dict("\$or" => [
        Dict("city" => "New York"),
        Dict("age" => Dict("\$lt" => 20))
    ]))
        println("  ", d["name"], " city=", d["city"], " age=", d["age"])
    end

    println("\nFindOne (name=Alice):")
    println("  ", find_one(db, "users", Dict("name" => "Alice")))

    # 5. Update
    section("5. Update Documents")

    update(db, "users", Dict("name" => "Alice"), Dict("\$set" => Dict("age" => 31, "status" => "active")))
    doc = find_one(db, "users", Dict("name" => "Alice"))
    println("After \$set: age=", doc["age"], " status=", doc["status"])

    update(db, "users", Dict("name" => "Alice"), Dict("\$inc" => Dict("age" => 1)))
    println("After \$inc: age=", find_one(db, "users", Dict("name" => "Alice"))["age"])

    update(db, "users", Dict("name" => "Alice"), Dict("\$unset" => Dict("status" => "")))
    println("After \$unset status: has status? ", haskey(find_one(db, "users", Dict("name" => "Alice")), "status"))

    update(db, "users", Dict("name" => "Alice"), Dict("\$push" => Dict("tags" => "vip")))
    println("After \$push 'vip': ", find_one(db, "users", Dict("name" => "Alice"))["tags"])

    update(db, "users", Dict("name" => "Alice"), Dict("\$pull" => Dict("tags" => "premium")))
    println("After \$pull 'premium': ", find_one(db, "users", Dict("name" => "Alice"))["tags"])

    update(db, "users", Dict("name" => "Alice"), Dict("\$addToSet" => Dict("tags" => "admin")))
    println("After \$addToSet 'admin' (no dup): ", find_one(db, "users", Dict("name" => "Alice"))["tags"])

    update(db, "users", Dict("name" => "Alice"), Dict("\$rename" => Dict("city" => "location")))
    println("After \$rename city->location: ", find_one(db, "users", Dict("name" => "Alice"))["location"])
    update(db, "users", Dict("name" => "Alice"), Dict("\$rename" => Dict("location" => "city")))

    update(db, "users", Dict("name" => "Alice"), Dict("\$currentDate" => Dict("last_login" => true)))
    println("After \$currentDate: last_login=", find_one(db, "users", Dict("name" => "Alice"))["last_login"])

    update(db, "users", Dict("name" => "Alice"), Dict("\$mul" => Dict("age" => 1)))
    println("After \$mul age*1: age=", find_one(db, "users", Dict("name" => "Alice"))["age"])

    update(db, "users", Dict("name" => "Alice"), Dict("\$min" => Dict("age" => 10)))
    println("After \$min(age, 10): age=", find_one(db, "users", Dict("name" => "Alice"))["age"])

    update(db, "users", Dict("name" => "Alice"), Dict("\$max" => Dict("age" => 32)))
    println("After \$max(age, 32): age=", find_one(db, "users", Dict("name" => "Alice"))["age"])

    # update_one (embedded only)
    update_one(db, "users", Dict("name" => "Bob"), Dict("\$set" => Dict("verified" => true)))
    println("After update_one Bob: verified=", find_one(db, "users", Dict("name" => "Bob"))["verified"])

    # 6. Count
    section("6. Count Documents")
    _, ms = timed(() -> count_docs(db, "users"))
    println("Total users: $(count_docs(db, "users")) ($(fmt(ms)))")

    # 7. Delete
    section("7. Delete Documents")
    insert(db, "users", Dict("name" => "Temp", "age" => 99))
    n_before = count_docs(db, "users")
    delete_one(db, "users", Dict("name" => "Temp"))
    println("Before delete_one: $n_before, after: $(count_docs(db, "users"))")

    insert(db, "users", Dict("name" => "Temp2", "age" => 98))
    n_before = count_docs(db, "users")
    delete(db, "users", Dict("name" => "Temp2"))
    println("Before delete: $n_before, after: $(count_docs(db, "users"))")

    # 8. Indexes
    section("8. Indexes")
    _, ms = timed(() -> create_index(db, "users", "name"))
    println("Created index on users.name ($(fmt(ms)))")
    create_unique_index(db, "users", "email")
    println("Created unique index on users.email")
    create_composite_index(db, "users", ["city", "age"])
    println("Created composite index on users.[city, age]")

    try
        insert(db, "users", Dict("name" => "Dup", "email" => "alice@example.com"))
        println("ERROR: should have thrown on duplicate email!")
    catch e
        println("Unique index enforced: ", e.msg)
    end

    # List indexes
    indexes = list_indexes(db, "users")
    println("Indexes on 'users': $(length(indexes))")
    for idx in indexes
        println("  ", idx)
    end

    # Drop index
    drop_index(db, "users", "name")
    println("Dropped index: name")
    indexes = list_indexes(db, "users")
    println("Remaining indexes: $(length(indexes))")

    # 9. Document Full-Text Search
    section("9. Document Full-Text Search")

    insert_many(db, "articles", [
        Dict("title" => "Getting Started with Rust",  "body" => "Rust is a systems programming language focused on safety, speed, and concurrency."),
        Dict("title" => "Go for Backend Services",    "body" => "Go excels at building fast, concurrent backend services and APIs."),
        Dict("title" => "Rust and WebAssembly",        "body" => "Rust compiles to WebAssembly for fast and safe web applications."),
        Dict("title" => "Database Design Patterns",    "body" => "Document databases store data as JSON documents, offering flexibility."),
        Dict("title" => "Building with Go and gRPC",   "body" => "gRPC and Go make a powerful combination for microservices."),
    ])
    println("Inserted 5 articles")

    create_text_index(db, "articles", ["title", "body"])
    println("Created text index on [title, body]")

    results = text_search(db, "articles", "Rust"; limit=10)
    println("TextSearch('Rust'): $(length(results)) results")
    for r in results
        println("  ", r["title"], " (score: ", r["_score"], ")")
    end

    results = text_search(db, "articles", "Go backend"; limit=10)
    println("TextSearch('Go backend'): $(length(results)) results")
    for r in results
        println("  ", r["title"], " (score: ", r["_score"], ")")
    end

    # 10. Aggregation Pipeline
    section("10. Aggregation Pipeline")

    orders = [
        Dict("customer" => "Alice",   "category" => "electronics", "amount" => 200, "status" => "completed"),
        Dict("customer" => "Bob",     "category" => "electronics", "amount" => 150, "status" => "completed"),
        Dict("customer" => "Charlie", "category" => "books",       "amount" => 50,  "status" => "completed"),
        Dict("customer" => "Alice",   "category" => "books",       "amount" => 30,  "status" => "pending"),
        Dict("customer" => "Diana",   "category" => "clothing",    "amount" => 100, "status" => "completed"),
        Dict("customer" => "Eve",     "category" => "electronics", "amount" => 300, "status" => "completed"),
        Dict("customer" => "Bob",     "category" => "clothing",    "amount" => 75,  "status" => "completed"),
    ]
    _, ms = timed(() -> insert_many(db, "orders", orders))
    println("Inserted $(length(orders)) orders ($(fmt(ms)))")

    println("\nRevenue by category (completed orders):")
    for r in aggregate(db, "orders", [
        Dict("\$match" => Dict("status" => "completed")),
        Dict("\$group" => Dict("_id" => "\$category", "total" => Dict("\$sum" => "\$amount"), "count" => Dict("\$sum" => 1))),
        Dict("\$sort"  => Dict("total" => -1))
    ])
        println("  ", r["_id"], ": \$", r["total"], " (", r["count"], " orders)")
    end

    println("\nGlobal stats:")
    for r in aggregate(db, "orders", [
        Dict("\$group" => Dict("_id" => nothing,
            "total_revenue" => Dict("\$sum" => "\$amount"),
            "avg_order" => Dict("\$avg" => "\$amount"),
            "max_order" => Dict("\$max" => "\$amount"),
            "min_order" => Dict("\$min" => "\$amount"),
            "order_count" => Dict("\$count" => Dict())
        ))
    ])
        println("  Total: \$", r["total_revenue"], "  Avg: \$", r["avg_order"],
                "  Max: \$", r["max_order"], "  Min: \$", r["min_order"],
                "  Count: ", r["order_count"])
    end

    println("\nOrders page 2 (skip 2, limit 2):")
    for r in aggregate(db, "orders", [
        Dict("\$sort" => Dict("amount" => -1)), Dict("\$skip" => 2), Dict("\$limit" => 2)
    ])
        println("  ", r["customer"], ": \$", r["amount"])
    end

    println("\nCompleted order count:")
    for r in aggregate(db, "orders", [
        Dict("\$match" => Dict("status" => "completed")), Dict("\$count" => "n")
    ])
        println("  ", r["n"], " completed orders")
    end

    println("\nProjected fields:")
    for r in aggregate(db, "orders", [
        Dict("\$project" => Dict("customer" => 1, "amount" => 1, "_id" => 0)), Dict("\$limit" => 3)
    ])
        println("  ", r)
    end

    println("\nWith 10% tax:")
    for r in aggregate(db, "orders", [
        Dict("\$addFields" => Dict("tax" => Dict("\$multiply" => ["\$amount", 0.1]))), Dict("\$limit" => 3)
    ])
        println("  ", r["customer"], ": amount=\$", r["amount"], " tax=\$", r["tax"])
    end

    println("\nLookup — join orders with products:")
    insert_many(db, "products", [
        Dict("name" => "electronics", "description" => "Gadgets and devices"),
        Dict("name" => "books",       "description" => "Reading materials"),
        Dict("name" => "clothing",    "description" => "Apparel and accessories"),
    ])
    for r in aggregate(db, "orders", [
        Dict("\$lookup" => Dict("from" => "products", "localField" => "category",
             "foreignField" => "name", "as" => "product_info")),
        Dict("\$limit" => 3)
    ])
        info = length(r["product_info"]) > 0 ? r["product_info"][1]["description"] : "N/A"
        println("  ", r["customer"], " bought ", r["category"], " -> ", info)
    end

    println("\nUnwind user tags:")
    for r in aggregate(db, "users", [
        Dict("\$unwind" => "\$tags"),
        Dict("\$group" => Dict("_id" => "\$tags", "count" => Dict("\$sum" => 1))),
        Dict("\$sort" => Dict("count" => -1))
    ])
        println("  tag '", r["_id"], "' appears ", r["count"], " times")
    end

    # 11. Transactions
    section("11. Transactions")
    println("Auto-commit transaction (debit + credit):")
    transaction(db) do
        insert(db, "ledger", Dict("action" => "debit",  "account" => "A", "amount" => 500))
        insert(db, "ledger", Dict("action" => "credit", "account" => "B", "amount" => 500))
    end
    for entry in find(db, "ledger", Dict())
        println("  ", entry["action"], " account=", entry["account"], " amount=", entry["amount"])
    end

    println("\nManual transaction (rolled back):")
    begin_tx(db)
    insert(db, "ledger", Dict("action" => "debit", "account" => "X", "amount" => 9999))
    rollback_tx(db)
    println("  Ledger count after rollback: $(count_docs(db, "ledger")) (should be 2)")

    # 12. Blob Storage
    section("12. Blob Storage")
    create_bucket(db, "files")
    create_bucket(db, "docs")
    println("Buckets: ", list_buckets(db))

    put_object(db, "files", "greeting.txt", Vector{UInt8}("Hello from Julia (embedded)!");
               content_type="text/plain", metadata=Dict("author" => "julia"))
    put_object(db, "files", "data.csv", Vector{UInt8}("name,age\nAlice,30\nBob,25");
               content_type="text/csv")
    put_object(db, "docs", "notes.txt", Vector{UInt8}("Julia is a fast language for scientific computing");
               content_type="text/plain")
    println("Uploaded 3 objects")

    data, meta = get_object(db, "files", "greeting.txt")
    println("Downloaded greeting.txt: \"", String(data), "\"")
    println("  size=", meta["size"], " content_type=", meta["content_type"])

    head = head_object(db, "files", "data.csv")
    println("Head data.csv: size=", head["size"], " etag=", head["etag"])

    println("Objects in 'files':")
    for o in list_objects(db, "files")
        println("  ", o["key"], " (", o["size"], " bytes)")
    end

    println("With prefix 'greet': ", length(list_objects(db, "files"; prefix="greet")), " found")

    delete_object(db, "files", "data.csv")
    println("After deleting data.csv: $(length(list_objects(db, "files"))) objects remain")

    # 13. Blob Full-Text Search
    section("13. Blob Full-Text Search")
    sleep(1)

    println("Search 'Julia':")
    for r in search(db, "Julia"; limit=10)
        println("  bucket=", r["bucket"], " key=", r["key"], " score=", r["score"])
    end

    println("Search 'Hello' in 'files':")
    for r in search(db, "Hello"; bucket="files", limit=10)
        println("  key=", r["key"], " score=", r["score"])
    end

    # 14. Compaction
    section("14. Compaction")
    for i in 1:20
        insert(db, "events", Dict("type" => "test", "seq" => i))
    end
    delete(db, "events", Dict("seq" => Dict("\$lte" => 10)))
    stats, ms = timed(() -> compact(db, "events"))
    println("Compaction ($(fmt(ms))):")
    println("  Old: ", stats["old_size"], "B → New: ", stats["new_size"], "B")
    println("  Kept: ", stats["docs_kept"], " docs, reclaimed: ", stats["old_size"] - stats["new_size"], "B")

    # Cleanup
    section("Cleanup")
    for col in ["users", "orders", "ledger", "products", "events", "articles"]
        try; drop_collection(db, col); catch; end
    end
    for bucket in ["files", "docs"]
        try
            for o in list_objects(db, bucket)
                delete_object(db, bucket, o["key"])
            end
            delete_bucket(db, bucket)
        catch; end
    end
    println("All collections and buckets cleaned up.")

    close(db)
    println("\nDone! All embedded features demonstrated successfully.")
end

main()
