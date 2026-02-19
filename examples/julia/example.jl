#!/usr/bin/env julia
#
# OxiDB Julia Example (TCP client) — demonstrates every feature via the database server.
#
# Prerequisites:
#   1. Start oxidb-server on 127.0.0.1:4444
#      Download prebuilt: https://github.com/parisxmas/OxiDB/releases/latest
#   2. Run: cd examples/julia && julia --project=. -e 'using Pkg; Pkg.instantiate()'
#   3. Run: julia --project=. example.jl
#

using OxiDb

const DIVIDER = "=" ^ 60

function section(title)
    println("\n", DIVIDER)
    println("  ", title)
    println(DIVIDER)
end

function main()
    println("Connecting to OxiDB...")
    client = connect_oxidb("127.0.0.1", 4444)

    # ------------------------------------------------------------------
    # 1. Ping
    # ------------------------------------------------------------------
    section("1. Ping")
    result = ping(client)
    println("Server says: ", result)

    # ------------------------------------------------------------------
    # 2. Collections
    # ------------------------------------------------------------------
    section("2. Collection Management")

    # Clean up from previous runs
    for col in ["users", "orders", "ledger", "products", "events", "articles"]
        try; drop_collection(client, col); catch; end
    end
    for bucket in ["files", "docs"]
        try; delete_bucket(client, bucket); catch; end
    end

    create_collection(client, "users")
    create_collection(client, "orders")
    cols = list_collections(client)
    println("Collections: ", cols)

    # ------------------------------------------------------------------
    # 3. Insert
    # ------------------------------------------------------------------
    section("3. Insert Documents")

    # Single insert
    result = insert(client, "users", Dict(
        "name" => "Alice", "age" => 30, "email" => "alice@example.com",
        "city" => "New York", "tags" => ["admin", "premium"]
    ))
    println("Inserted Alice: ", result)

    # Batch insert
    users = [
        Dict("name" => "Bob",     "age" => 25, "email" => "bob@example.com",     "city" => "London",  "tags" => ["user"]),
        Dict("name" => "Charlie", "age" => 35, "email" => "charlie@example.com", "city" => "Paris",   "tags" => ["user", "premium"]),
        Dict("name" => "Diana",   "age" => 28, "email" => "diana@example.com",   "city" => "Berlin",  "tags" => ["user"]),
        Dict("name" => "Eve",     "age" => 42, "email" => "eve@example.com",     "city" => "Tokyo",   "tags" => ["admin"]),
        Dict("name" => "Frank",   "age" => 19, "email" => "frank@example.com",   "city" => "New York","tags" => ["user"]),
    ]
    result = insert_many(client, "users", users)
    println("Batch inserted $(length(users)) users: ", result)

    # ------------------------------------------------------------------
    # 4. Find
    # ------------------------------------------------------------------
    section("4. Find Documents")

    # Find all
    docs = find(client, "users", Dict())
    println("All users ($(length(docs))):")
    for d in docs
        println("  ", d["name"], " age=", d["age"], " city=", d["city"])
    end

    # Find with filter
    println("\nUsers age >= 30:")
    docs = find(client, "users", Dict("age" => Dict("\$gte" => 30)))
    for d in docs
        println("  ", d["name"], " age=", d["age"])
    end

    # Find with sort, skip, limit
    println("\nTop 3 oldest users:")
    docs = find(client, "users", Dict(); sort=Dict("age" => -1), limit=3)
    for d in docs
        println("  ", d["name"], " age=", d["age"])
    end

    # Find with \$or
    println("\nUsers in New York OR age < 20:")
    docs = find(client, "users", Dict("\$or" => [
        Dict("city" => "New York"),
        Dict("age" => Dict("\$lt" => 20))
    ]))
    for d in docs
        println("  ", d["name"], " city=", d["city"], " age=", d["age"])
    end

    # FindOne
    println("\nFindOne (name=Alice):")
    doc = find_one(client, "users", Dict("name" => "Alice"))
    println("  ", doc)

    # ------------------------------------------------------------------
    # 5. Update & UpdateOne
    # ------------------------------------------------------------------
    section("5. Update Documents")

    # \$set
    update(client, "users", Dict("name" => "Alice"), Dict("\$set" => Dict("age" => 31, "status" => "active")))
    doc = find_one(client, "users", Dict("name" => "Alice"))
    println("After \$set (age=31, status=active): ", doc["age"], " ", doc["status"])

    # \$inc
    update(client, "users", Dict("name" => "Alice"), Dict("\$inc" => Dict("age" => 1)))
    doc = find_one(client, "users", Dict("name" => "Alice"))
    println("After \$inc (age+1): age=", doc["age"])

    # \$unset
    update(client, "users", Dict("name" => "Alice"), Dict("\$unset" => Dict("status" => "")))
    doc = find_one(client, "users", Dict("name" => "Alice"))
    println("After \$unset status: has status? ", haskey(doc, "status"))

    # \$push
    update(client, "users", Dict("name" => "Alice"), Dict("\$push" => Dict("tags" => "vip")))
    doc = find_one(client, "users", Dict("name" => "Alice"))
    println("After \$push 'vip' to tags: ", doc["tags"])

    # \$pull
    update(client, "users", Dict("name" => "Alice"), Dict("\$pull" => Dict("tags" => "premium")))
    doc = find_one(client, "users", Dict("name" => "Alice"))
    println("After \$pull 'premium' from tags: ", doc["tags"])

    # \$addToSet
    update(client, "users", Dict("name" => "Alice"), Dict("\$addToSet" => Dict("tags" => "admin")))
    doc = find_one(client, "users", Dict("name" => "Alice"))
    println("After \$addToSet 'admin' (no dup): ", doc["tags"])

    # \$rename
    update(client, "users", Dict("name" => "Alice"), Dict("\$rename" => Dict("city" => "location")))
    doc = find_one(client, "users", Dict("name" => "Alice"))
    println("After \$rename city->location: location=", doc["location"])
    # rename back
    update(client, "users", Dict("name" => "Alice"), Dict("\$rename" => Dict("location" => "city")))

    # \$currentDate
    update(client, "users", Dict("name" => "Alice"), Dict("\$currentDate" => Dict("last_login" => true)))
    doc = find_one(client, "users", Dict("name" => "Alice"))
    println("After \$currentDate: last_login=", doc["last_login"])

    # \$mul
    update(client, "users", Dict("name" => "Alice"), Dict("\$mul" => Dict("age" => 1)))
    doc = find_one(client, "users", Dict("name" => "Alice"))
    println("After \$mul age*1: age=", doc["age"])

    # \$min / \$max
    update(client, "users", Dict("name" => "Alice"), Dict("\$min" => Dict("age" => 10)))
    doc = find_one(client, "users", Dict("name" => "Alice"))
    println("After \$min(age, 10): age=", doc["age"])

    update(client, "users", Dict("name" => "Alice"), Dict("\$max" => Dict("age" => 32)))
    doc = find_one(client, "users", Dict("name" => "Alice"))
    println("After \$max(age, 32): age=", doc["age"])

    # update_one — only modifies the first match
    result = update_one(client, "users", Dict("name" => "Bob"), Dict("\$set" => Dict("verified" => true)))
    println("UpdateOne Bob -> verified: modified=", result["modified"])
    doc = find_one(client, "users", Dict("name" => "Bob"))
    println("  Bob verified=", doc["verified"])

    # ------------------------------------------------------------------
    # 6. Count
    # ------------------------------------------------------------------
    section("6. Count Documents")

    n = count_docs(client, "users")
    println("Total users: ", n)

    # ------------------------------------------------------------------
    # 7. Delete & DeleteOne
    # ------------------------------------------------------------------
    section("7. Delete Documents")

    insert(client, "users", Dict("name" => "Temp", "age" => 99))
    n_before = count_docs(client, "users")
    delete_one(client, "users", Dict("name" => "Temp"))
    n_after = count_docs(client, "users")
    println("DeleteOne: before=$n_before, after=$n_after")

    insert(client, "users", Dict("name" => "Temp2", "age" => 98))
    n_before = count_docs(client, "users")
    delete(client, "users", Dict("name" => "Temp2"))
    n_after = count_docs(client, "users")
    println("Delete:    before=$n_before, after=$n_after")

    # ------------------------------------------------------------------
    # 8. Indexes
    # ------------------------------------------------------------------
    section("8. Indexes")

    create_index(client, "users", "name")
    println("Created index on users.name")

    create_unique_index(client, "users", "email")
    println("Created unique index on users.email")

    create_composite_index(client, "users", ["city", "age"])
    println("Created composite index on users.[city, age]")

    # Verify unique index — duplicate email should fail
    try
        insert(client, "users", Dict("name" => "Dup", "email" => "alice@example.com"))
        println("ERROR: should have thrown on duplicate email!")
    catch e
        println("Unique index enforced: ", e.msg)
    end

    # List indexes
    indexes = list_indexes(client, "users")
    println("Indexes on 'users': $(length(indexes))")
    for idx in indexes
        println("  ", idx)
    end

    # Drop index
    drop_index(client, "users", "name")
    println("Dropped index: name")
    indexes = list_indexes(client, "users")
    println("Remaining indexes: $(length(indexes))")

    # ------------------------------------------------------------------
    # 9. Document Full-Text Search
    # ------------------------------------------------------------------
    section("9. Document Full-Text Search")

    insert_many(client, "articles", [
        Dict("title" => "Getting Started with Rust",  "body" => "Rust is a systems programming language focused on safety, speed, and concurrency."),
        Dict("title" => "Go for Backend Services",    "body" => "Go excels at building fast, concurrent backend services and APIs."),
        Dict("title" => "Rust and WebAssembly",        "body" => "Rust compiles to WebAssembly for fast and safe web applications."),
        Dict("title" => "Database Design Patterns",    "body" => "Document databases store data as JSON documents, offering flexibility."),
        Dict("title" => "Building with Go and gRPC",   "body" => "gRPC and Go make a powerful combination for microservices."),
    ])
    println("Inserted 5 articles")

    create_text_index(client, "articles", ["title", "body"])
    println("Created text index on [title, body]")

    results = text_search(client, "articles", "Rust"; limit=10)
    println("TextSearch('Rust'): $(length(results)) results")
    for r in results
        println("  ", r["title"], " (score: ", r["_score"], ")")
    end

    results = text_search(client, "articles", "Go backend"; limit=10)
    println("TextSearch('Go backend'): $(length(results)) results")
    for r in results
        println("  ", r["title"], " (score: ", r["_score"], ")")
    end

    # ------------------------------------------------------------------
    # 10. Aggregation Pipeline
    # ------------------------------------------------------------------
    section("10. Aggregation Pipeline")

    # Insert some orders
    orders = [
        Dict("customer" => "Alice",   "category" => "electronics", "amount" => 200, "status" => "completed"),
        Dict("customer" => "Bob",     "category" => "electronics", "amount" => 150, "status" => "completed"),
        Dict("customer" => "Charlie", "category" => "books",       "amount" => 50,  "status" => "completed"),
        Dict("customer" => "Alice",   "category" => "books",       "amount" => 30,  "status" => "pending"),
        Dict("customer" => "Diana",   "category" => "clothing",    "amount" => 100, "status" => "completed"),
        Dict("customer" => "Eve",     "category" => "electronics", "amount" => 300, "status" => "completed"),
        Dict("customer" => "Bob",     "category" => "clothing",    "amount" => 75,  "status" => "completed"),
    ]
    insert_many(client, "orders", orders)
    println("Inserted $(length(orders)) orders")

    # \$match + \$group + \$sort
    println("\nRevenue by category (completed orders):")
    results = aggregate(client, "orders", [
        Dict("\$match" => Dict("status" => "completed")),
        Dict("\$group" => Dict("_id" => "\$category", "total" => Dict("\$sum" => "\$amount"), "count" => Dict("\$sum" => 1))),
        Dict("\$sort"  => Dict("total" => -1))
    ])
    for r in results
        println("  ", r["_id"], ": \$", r["total"], " (", r["count"], " orders)")
    end

    # \$group with null _id (global aggregate)
    println("\nGlobal stats:")
    results = aggregate(client, "orders", [
        Dict("\$group" => Dict("_id" => nothing,
            "total_revenue" => Dict("\$sum" => "\$amount"),
            "avg_order" => Dict("\$avg" => "\$amount"),
            "max_order" => Dict("\$max" => "\$amount"),
            "min_order" => Dict("\$min" => "\$amount"),
            "order_count" => Dict("\$count" => Dict())
        ))
    ])
    for r in results
        println("  Total: \$", r["total_revenue"])
        println("  Avg:   \$", r["avg_order"])
        println("  Max:   \$", r["max_order"])
        println("  Min:   \$", r["min_order"])
        println("  Count: ", r["order_count"])
    end

    # \$skip + \$limit
    println("\nOrders page 2 (skip 2, limit 2):")
    results = aggregate(client, "orders", [
        Dict("\$sort" => Dict("amount" => -1)),
        Dict("\$skip" => 2),
        Dict("\$limit" => 2)
    ])
    for r in results
        println("  ", r["customer"], ": \$", r["amount"])
    end

    # \$count
    println("\nCount completed orders:")
    results = aggregate(client, "orders", [
        Dict("\$match" => Dict("status" => "completed")),
        Dict("\$count" => "completed_count")
    ])
    println("  ", results[1]["completed_count"], " completed orders")

    # \$project
    println("\nProjected fields (customer + amount only):")
    results = aggregate(client, "orders", [
        Dict("\$project" => Dict("customer" => 1, "amount" => 1, "_id" => 0)),
        Dict("\$limit" => 3)
    ])
    for r in results
        println("  ", r)
    end

    # \$addFields
    println("\nWith computed tax field (10%):")
    results = aggregate(client, "orders", [
        Dict("\$addFields" => Dict("tax" => Dict("\$multiply" => ["\$amount", 0.1]))),
        Dict("\$limit" => 3)
    ])
    for r in results
        println("  ", r["customer"], ": amount=\$", r["amount"], " tax=\$", r["tax"])
    end

    # \$lookup (cross-collection join)
    println("\nLookup — join orders with users:")
    # Insert products for the lookup
    insert_many(client, "products", [
        Dict("name" => "electronics", "description" => "Gadgets and devices"),
        Dict("name" => "books",       "description" => "Reading materials"),
        Dict("name" => "clothing",    "description" => "Apparel and accessories"),
    ])
    results = aggregate(client, "orders", [
        Dict("\$lookup" => Dict(
            "from" => "products",
            "localField" => "category",
            "foreignField" => "name",
            "as" => "product_info"
        )),
        Dict("\$limit" => 3)
    ])
    for r in results
        info = length(r["product_info"]) > 0 ? r["product_info"][1]["description"] : "N/A"
        println("  ", r["customer"], " bought ", r["category"], " -> ", info)
    end

    # \$unwind
    println("\nUnwind user tags:")
    results = aggregate(client, "users", [
        Dict("\$unwind" => "\$tags"),
        Dict("\$group" => Dict("_id" => "\$tags", "count" => Dict("\$sum" => 1))),
        Dict("\$sort" => Dict("count" => -1))
    ])
    for r in results
        println("  tag '", r["_id"], "' appears ", r["count"], " times")
    end

    # ------------------------------------------------------------------
    # 10. Transactions
    # ------------------------------------------------------------------
    section("11. Transactions")

    # Auto-commit transaction
    println("Auto-commit transaction (debit + credit):")
    transaction(client) do
        insert(client, "ledger", Dict("action" => "debit",  "account" => "A", "amount" => 500))
        insert(client, "ledger", Dict("action" => "credit", "account" => "B", "amount" => 500))
    end
    ledger = find(client, "ledger", Dict())
    for entry in ledger
        println("  ", entry["action"], " account=", entry["account"], " amount=", entry["amount"])
    end

    # Manual transaction with rollback
    println("\nManual transaction (rolled back):")
    begin_tx(client)
    insert(client, "ledger", Dict("action" => "debit", "account" => "X", "amount" => 9999))
    rollback_tx(client)
    n = count_docs(client, "ledger")
    println("  Ledger count after rollback: $n (should be 2)")

    # ------------------------------------------------------------------
    # 11. Blob Storage
    # ------------------------------------------------------------------
    section("12. Blob Storage")

    create_bucket(client, "files")
    create_bucket(client, "docs")
    buckets = list_buckets(client)
    println("Buckets: ", buckets)

    # Put objects
    put_object(client, "files", "greeting.txt", Vector{UInt8}("Hello from Julia!");
               content_type="text/plain", metadata=Dict("author" => "julia-example"))
    put_object(client, "files", "data.csv", Vector{UInt8}("name,age\nAlice,30\nBob,25");
               content_type="text/csv")
    put_object(client, "docs", "notes.txt", Vector{UInt8}("Julia is a fast language for scientific computing");
               content_type="text/plain")
    println("Uploaded 3 objects")

    # Get object
    data, meta = get_object(client, "files", "greeting.txt")
    println("Downloaded greeting.txt: \"", String(data), "\"")
    println("  Metadata: size=", meta["size"], " content_type=", meta["content_type"])

    # Head object
    head = head_object(client, "files", "data.csv")
    println("Head data.csv: size=", head["size"], " etag=", head["etag"])

    # List objects
    objs = list_objects(client, "files")
    println("Objects in 'files': ")
    for o in objs
        println("  ", o["key"], " (", o["size"], " bytes)")
    end

    # List with prefix
    objs = list_objects(client, "files"; prefix="greet", limit=10)
    println("Objects with prefix 'greet': ", length(objs), " found")

    # Delete object
    delete_object(client, "files", "data.csv")
    objs = list_objects(client, "files")
    println("After deleting data.csv: $(length(objs)) objects remain")

    # ------------------------------------------------------------------
    # 12. Full-Text Search
    # ------------------------------------------------------------------
    section("13. Blob Full-Text Search")

    # Wait for indexing
    sleep(1)

    results = search(client, "Julia"; limit=10)
    println("Search 'Julia':")
    for r in results
        println("  bucket=", r["bucket"], " key=", r["key"], " score=", r["score"])
    end

    results = search(client, "Hello"; bucket="files", limit=10)
    println("Search 'Hello' in 'files':")
    for r in results
        println("  key=", r["key"], " score=", r["score"])
    end

    # ------------------------------------------------------------------
    # 13. Compaction
    # ------------------------------------------------------------------
    section("14. Compaction")

    # Insert and delete some docs to create garbage
    for i in 1:20
        insert(client, "events", Dict("type" => "test", "seq" => i))
    end
    delete(client, "events", Dict("seq" => Dict("\$lte" => 10)))
    stats = compact(client, "events")
    println("Compaction stats:")
    println("  Old size: ", stats["old_size"], " bytes")
    println("  New size: ", stats["new_size"], " bytes")
    println("  Docs kept: ", stats["docs_kept"])
    println("  Reclaimed: ", stats["old_size"] - stats["new_size"], " bytes")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    section("Cleanup")

    for col in ["users", "orders", "ledger", "products", "events", "articles"]
        try; drop_collection(client, col); catch; end
    end
    for bucket in ["files", "docs"]
        try
            for o in list_objects(client, bucket)
                delete_object(client, bucket, o["key"])
            end
            delete_bucket(client, bucket)
        catch; end
    end
    println("All collections and buckets cleaned up.")

    close(client)
    println("\nDone! All features demonstrated successfully.")
end

main()
