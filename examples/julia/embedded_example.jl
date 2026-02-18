#!/usr/bin/env julia
#
# OxiDB Embedded Julia Example — demonstrates every feature using the embedded FFI
# library directly (no server needed).
#
# No prerequisites — just run:
#   julia embedded_example.jl
#
# The script auto-installs JSON3 and downloads the prebuilt native library on first run.
#

try
    using JSON3
catch
    import Pkg; Pkg.add("JSON3")
    using JSON3
end
using Base64
using Printf

# ------------------------------------------------------------------
# FFI library auto-download
# ------------------------------------------------------------------

const RELEASE_VERSION = "v0.6.0"
const RELEASE_BASE = "https://github.com/parisxmas/OxiDB/releases/download/$RELEASE_VERSION"
const LIB_DIR = joinpath(@__DIR__, "lib")

function get_lib_path()
    if Sys.isapple()
        lib_name = "liboxidb_embedded_ffi.dylib"
        if Sys.ARCH === :aarch64 || Sys.ARCH === :arm64
            tarball = "oxidb-embedded-ffi-macos-arm64.tar.gz"
        else
            error("Unsupported macOS architecture: $(Sys.ARCH). Only arm64 (Apple Silicon) is supported.")
        end
    elseif Sys.islinux()
        lib_name = "liboxidb_embedded_ffi.so"
        error("No prebuilt Linux binary available yet. Build from source: cargo build --release -p oxidb-embedded-ffi")
    elseif Sys.iswindows()
        lib_name = "oxidb_embedded_ffi.dll"
        error("No prebuilt Windows binary available yet. Build from source: cargo build --release -p oxidb-embedded-ffi")
    else
        error("Unsupported platform: $(Sys.KERNEL)")
    end

    lib_path = joinpath(LIB_DIR, lib_name)

    if !isfile(lib_path)
        println("Downloading prebuilt library ($tarball)...")
        mkpath(LIB_DIR)
        tarball_path = joinpath(LIB_DIR, tarball)
        url = "$RELEASE_BASE/$tarball"
        download(url, tarball_path)
        run(`tar xzf $tarball_path -C $LIB_DIR`)
        rm(tarball_path)
        println("Library extracted to $LIB_DIR")
    end

    if !isfile(lib_path)
        error("Library not found at $lib_path after extraction")
    end

    return lib_path
end

const LIB_PATH = get_lib_path()

function oxidb_open(path::AbstractString)
    handle = ccall((:oxidb_open, LIB_PATH), Ptr{Cvoid}, (Cstring,), path)
    handle == C_NULL && error("Failed to open database at: $path")
    handle
end

function oxidb_close(handle::Ptr{Cvoid})
    ccall((:oxidb_close, LIB_PATH), Cvoid, (Ptr{Cvoid},), handle)
end

function oxidb_execute(handle::Ptr{Cvoid}, json_cmd::AbstractString)
    result_ptr = ccall((:oxidb_execute, LIB_PATH), Cstring, (Ptr{Cvoid}, Cstring), handle, json_cmd)
    result_ptr == C_NULL && error("oxidb_execute returned NULL")
    result_str = unsafe_string(result_ptr)
    ccall((:oxidb_free_string, LIB_PATH), Cvoid, (Cstring,), result_ptr)
    result_str
end

# ------------------------------------------------------------------
# High-level helpers
# ------------------------------------------------------------------

struct OxiDbError <: Exception
    msg::String
end
Base.showerror(io::IO, e::OxiDbError) = print(io, "OxiDbError: ", e.msg)

struct TransactionConflictError <: Exception
    msg::String
end
Base.showerror(io::IO, e::TransactionConflictError) = print(io, "TransactionConflictError: ", e.msg)

mutable struct EmbeddedDB
    handle::Ptr{Cvoid}
end

function execute(db::EmbeddedDB, cmd::Dict)
    json = JSON3.write(cmd)
    t0 = time_ns()
    raw = oxidb_execute(db.handle, json)
    elapsed_ms = (time_ns() - t0) / 1_000_000
    resp = JSON3.read(raw, Dict{String,Any})
    if !get(resp, "ok", false)
        error_msg = get(resp, "error", "unknown error")
        if occursin("conflict", lowercase(error_msg))
            throw(TransactionConflictError(error_msg))
        end
        throw(OxiDbError(error_msg))
    end
    (data = get(resp, "data", nothing), ms = elapsed_ms)
end

# Convenience: execute and return just the data
function exec(db::EmbeddedDB, cmd::Dict)
    r = execute(db, cmd)
    r.data
end

function fmt(ms::Float64)
    @sprintf("%.2fms", ms)
end

# ------------------------------------------------------------------
# CRUD helpers
# ------------------------------------------------------------------

ping(db) = execute(db, Dict("cmd" => "ping"))

create_collection(db, name) = exec(db, Dict("cmd" => "create_collection", "collection" => name))
list_collections(db) = exec(db, Dict("cmd" => "list_collections"))
drop_collection(db, name) = exec(db, Dict("cmd" => "drop_collection", "collection" => name))

function insert(db::EmbeddedDB, collection, doc)
    exec(db, Dict("cmd" => "insert", "collection" => collection, "doc" => doc))
end

function insert_many(db::EmbeddedDB, collection, docs)
    exec(db, Dict("cmd" => "insert_many", "collection" => collection, "docs" => docs))
end

function find(db::EmbeddedDB, collection, query=Dict(); sort=nothing, skip=nothing, limit=nothing)
    payload = Dict{String,Any}("cmd" => "find", "collection" => collection, "query" => query)
    sort !== nothing && (payload["sort"] = sort)
    skip !== nothing && (payload["skip"] = skip)
    limit !== nothing && (payload["limit"] = limit)
    exec(db, payload)
end

function find_one(db::EmbeddedDB, collection, query=Dict())
    exec(db, Dict("cmd" => "find_one", "collection" => collection, "query" => query))
end

function update(db::EmbeddedDB, collection, query, update_doc)
    exec(db, Dict("cmd" => "update", "collection" => collection, "query" => query, "update" => update_doc))
end

function update_one(db::EmbeddedDB, collection, query, update_doc)
    exec(db, Dict("cmd" => "update_one", "collection" => collection, "query" => query, "update" => update_doc))
end

function delete(db::EmbeddedDB, collection, query)
    exec(db, Dict("cmd" => "delete", "collection" => collection, "query" => query))
end

function delete_one(db::EmbeddedDB, collection, query)
    exec(db, Dict("cmd" => "delete_one", "collection" => collection, "query" => query))
end

function count_docs(db::EmbeddedDB, collection, query=Dict())
    r = exec(db, Dict("cmd" => "count", "collection" => collection, "query" => query))
    r["count"]
end

create_index(db, collection, field) =
    exec(db, Dict("cmd" => "create_index", "collection" => collection, "field" => field))

create_unique_index(db, collection, field) =
    exec(db, Dict("cmd" => "create_unique_index", "collection" => collection, "field" => field))

create_composite_index(db, collection, fields) =
    exec(db, Dict("cmd" => "create_composite_index", "collection" => collection, "fields" => fields))

aggregate(db, collection, pipeline) =
    exec(db, Dict("cmd" => "aggregate", "collection" => collection, "pipeline" => pipeline))

compact(db, collection) =
    exec(db, Dict("cmd" => "compact", "collection" => collection))

begin_tx(db) = exec(db, Dict("cmd" => "begin_tx"))
commit_tx(db) = exec(db, Dict("cmd" => "commit_tx"))
rollback_tx(db) = exec(db, Dict("cmd" => "rollback_tx"))

function transaction(f, db::EmbeddedDB)
    begin_tx(db)
    try
        f()
        commit_tx(db)
    catch e
        try; rollback_tx(db); catch; end
        rethrow()
    end
end

create_bucket(db, bucket) = exec(db, Dict("cmd" => "create_bucket", "bucket" => bucket))
list_buckets(db) = exec(db, Dict("cmd" => "list_buckets"))
delete_bucket(db, bucket) = exec(db, Dict("cmd" => "delete_bucket", "bucket" => bucket))

function put_object(db::EmbeddedDB, bucket, key, data::Vector{UInt8};
                    content_type="application/octet-stream", metadata=nothing)
    payload = Dict{String,Any}(
        "cmd" => "put_object", "bucket" => bucket, "key" => key,
        "data" => base64encode(data), "content_type" => content_type
    )
    metadata !== nothing && (payload["metadata"] = metadata)
    exec(db, payload)
end

function get_object(db::EmbeddedDB, bucket, key)
    r = exec(db, Dict("cmd" => "get_object", "bucket" => bucket, "key" => key))
    (base64decode(r["content"]), r["metadata"])
end

head_object(db, bucket, key) =
    exec(db, Dict("cmd" => "head_object", "bucket" => bucket, "key" => key))

delete_object(db, bucket, key) =
    exec(db, Dict("cmd" => "delete_object", "bucket" => bucket, "key" => key))

function list_objects(db::EmbeddedDB, bucket; prefix=nothing, limit=nothing)
    payload = Dict{String,Any}("cmd" => "list_objects", "bucket" => bucket)
    prefix !== nothing && (payload["prefix"] = prefix)
    limit !== nothing && (payload["limit"] = limit)
    exec(db, payload)
end

function search(db::EmbeddedDB, query; bucket=nothing, limit=10)
    payload = Dict{String,Any}("cmd" => "search", "query" => query, "limit" => limit)
    bucket !== nothing && (payload["bucket"] = bucket)
    exec(db, payload)
end

# ------------------------------------------------------------------
# Main demo
# ------------------------------------------------------------------

const DIVIDER = "=" ^ 60

function section(title)
    println("\n", DIVIDER)
    println("  ", title)
    println(DIVIDER)
end

function main()
    db_path = mktempdir() * "/oxidb_julia_demo"
    println("Opening embedded database at: $db_path")
    handle = oxidb_open(db_path)
    db = EmbeddedDB(handle)

    # ------------------------------------------------------------------
    # 1. Ping
    # ------------------------------------------------------------------
    section("1. Ping")
    r = ping(db)
    println("Ping: $(r.data) ($(fmt(r.ms)))")

    # ------------------------------------------------------------------
    # 2. Collections
    # ------------------------------------------------------------------
    section("2. Collection Management")

    create_collection(db, "users")
    create_collection(db, "orders")
    cols = list_collections(db)
    println("Collections: ", cols)

    # ------------------------------------------------------------------
    # 3. Insert
    # ------------------------------------------------------------------
    section("3. Insert Documents")

    r = execute(db, Dict("cmd" => "insert", "collection" => "users", "doc" => Dict(
        "name" => "Alice", "age" => 30, "email" => "alice@example.com",
        "city" => "New York", "tags" => ["admin", "premium"]
    )))
    println("Inserted Alice: $(r.data) ($(fmt(r.ms)))")

    users = [
        Dict("name" => "Bob",     "age" => 25, "email" => "bob@example.com",     "city" => "London",   "tags" => ["user"]),
        Dict("name" => "Charlie", "age" => 35, "email" => "charlie@example.com", "city" => "Paris",    "tags" => ["user", "premium"]),
        Dict("name" => "Diana",   "age" => 28, "email" => "diana@example.com",   "city" => "Berlin",   "tags" => ["user"]),
        Dict("name" => "Eve",     "age" => 42, "email" => "eve@example.com",     "city" => "Tokyo",    "tags" => ["admin"]),
        Dict("name" => "Frank",   "age" => 19, "email" => "frank@example.com",   "city" => "New York", "tags" => ["user"]),
    ]
    r = execute(db, Dict("cmd" => "insert_many", "collection" => "users", "docs" => users))
    println("Batch inserted $(length(users)) users ($(fmt(r.ms)))")

    # ------------------------------------------------------------------
    # 4. Find
    # ------------------------------------------------------------------
    section("4. Find Documents")

    docs = find(db, "users", Dict())
    println("All users ($(length(docs))):")
    for d in docs
        println("  ", d["name"], " age=", d["age"], " city=", d["city"])
    end

    println("\nUsers age >= 30:")
    docs = find(db, "users", Dict("age" => Dict("\$gte" => 30)))
    for d in docs
        println("  ", d["name"], " age=", d["age"])
    end

    println("\nTop 3 oldest users:")
    docs = find(db, "users", Dict(); sort=Dict("age" => -1), limit=3)
    for d in docs
        println("  ", d["name"], " age=", d["age"])
    end

    println("\nUsers in New York OR age < 20:")
    docs = find(db, "users", Dict("\$or" => [
        Dict("city" => "New York"),
        Dict("age" => Dict("\$lt" => 20))
    ]))
    for d in docs
        println("  ", d["name"], " city=", d["city"], " age=", d["age"])
    end

    println("\nFindOne (name=Alice):")
    doc = find_one(db, "users", Dict("name" => "Alice"))
    println("  ", doc)

    # ------------------------------------------------------------------
    # 5. Update
    # ------------------------------------------------------------------
    section("5. Update Documents")

    update(db, "users", Dict("name" => "Alice"), Dict("\$set" => Dict("age" => 31, "status" => "active")))
    doc = find_one(db, "users", Dict("name" => "Alice"))
    println("After \$set (age=31, status=active): ", doc["age"], " ", doc["status"])

    update(db, "users", Dict("name" => "Alice"), Dict("\$inc" => Dict("age" => 1)))
    doc = find_one(db, "users", Dict("name" => "Alice"))
    println("After \$inc (age+1): age=", doc["age"])

    update(db, "users", Dict("name" => "Alice"), Dict("\$unset" => Dict("status" => "")))
    doc = find_one(db, "users", Dict("name" => "Alice"))
    println("After \$unset status: has status? ", haskey(doc, "status"))

    update(db, "users", Dict("name" => "Alice"), Dict("\$push" => Dict("tags" => "vip")))
    doc = find_one(db, "users", Dict("name" => "Alice"))
    println("After \$push 'vip' to tags: ", doc["tags"])

    update(db, "users", Dict("name" => "Alice"), Dict("\$pull" => Dict("tags" => "premium")))
    doc = find_one(db, "users", Dict("name" => "Alice"))
    println("After \$pull 'premium' from tags: ", doc["tags"])

    update(db, "users", Dict("name" => "Alice"), Dict("\$addToSet" => Dict("tags" => "admin")))
    doc = find_one(db, "users", Dict("name" => "Alice"))
    println("After \$addToSet 'admin' (no dup): ", doc["tags"])

    update(db, "users", Dict("name" => "Alice"), Dict("\$rename" => Dict("city" => "location")))
    doc = find_one(db, "users", Dict("name" => "Alice"))
    println("After \$rename city->location: location=", doc["location"])
    update(db, "users", Dict("name" => "Alice"), Dict("\$rename" => Dict("location" => "city")))

    update(db, "users", Dict("name" => "Alice"), Dict("\$currentDate" => Dict("last_login" => true)))
    doc = find_one(db, "users", Dict("name" => "Alice"))
    println("After \$currentDate: last_login=", doc["last_login"])

    update(db, "users", Dict("name" => "Alice"), Dict("\$mul" => Dict("age" => 1)))
    doc = find_one(db, "users", Dict("name" => "Alice"))
    println("After \$mul age*1: age=", doc["age"])

    update(db, "users", Dict("name" => "Alice"), Dict("\$min" => Dict("age" => 10)))
    doc = find_one(db, "users", Dict("name" => "Alice"))
    println("After \$min(age, 10): age=", doc["age"])

    update(db, "users", Dict("name" => "Alice"), Dict("\$max" => Dict("age" => 32)))
    doc = find_one(db, "users", Dict("name" => "Alice"))
    println("After \$max(age, 32): age=", doc["age"])

    # update_one (embedded only)
    update_one(db, "users", Dict("name" => "Bob"), Dict("\$set" => Dict("verified" => true)))
    doc = find_one(db, "users", Dict("name" => "Bob"))
    println("After update_one Bob verified: ", doc["verified"])

    # ------------------------------------------------------------------
    # 6. Count
    # ------------------------------------------------------------------
    section("6. Count Documents")

    r = execute(db, Dict("cmd" => "count", "collection" => "users", "query" => Dict()))
    println("Total users: $(r.data["count"]) ($(fmt(r.ms)))")

    # ------------------------------------------------------------------
    # 7. Delete
    # ------------------------------------------------------------------
    section("7. Delete Documents")

    insert(db, "users", Dict("name" => "Temp", "age" => 99))
    n_before = count_docs(db, "users")

    # delete_one (embedded only)
    delete_one(db, "users", Dict("name" => "Temp"))
    n_after = count_docs(db, "users")
    println("Before delete_one: $n_before, after: $n_after")

    insert(db, "users", Dict("name" => "Temp2", "age" => 98))
    n_before = count_docs(db, "users")
    delete(db, "users", Dict("name" => "Temp2"))
    n_after = count_docs(db, "users")
    println("Before delete: $n_before, after: $n_after")

    # ------------------------------------------------------------------
    # 8. Indexes
    # ------------------------------------------------------------------
    section("8. Indexes")

    r = execute(db, Dict("cmd" => "create_index", "collection" => "users", "field" => "name"))
    println("Created index on users.name ($(fmt(r.ms)))")

    create_unique_index(db, "users", "email")
    println("Created unique index on users.email")

    create_composite_index(db, "users", ["city", "age"])
    println("Created composite index on users.[city, age]")

    # Verify unique index
    try
        insert(db, "users", Dict("name" => "Dup", "email" => "alice@example.com"))
        println("ERROR: should have thrown on duplicate email!")
    catch e
        println("Unique index enforced: ", e.msg)
    end

    # ------------------------------------------------------------------
    # 9. Aggregation Pipeline
    # ------------------------------------------------------------------
    section("9. Aggregation Pipeline")

    orders = [
        Dict("customer" => "Alice",   "category" => "electronics", "amount" => 200, "status" => "completed"),
        Dict("customer" => "Bob",     "category" => "electronics", "amount" => 150, "status" => "completed"),
        Dict("customer" => "Charlie", "category" => "books",       "amount" => 50,  "status" => "completed"),
        Dict("customer" => "Alice",   "category" => "books",       "amount" => 30,  "status" => "pending"),
        Dict("customer" => "Diana",   "category" => "clothing",    "amount" => 100, "status" => "completed"),
        Dict("customer" => "Eve",     "category" => "electronics", "amount" => 300, "status" => "completed"),
        Dict("customer" => "Bob",     "category" => "clothing",    "amount" => 75,  "status" => "completed"),
    ]
    r = execute(db, Dict("cmd" => "insert_many", "collection" => "orders", "docs" => orders))
    println("Inserted $(length(orders)) orders ($(fmt(r.ms)))")

    # $match + $group + $sort
    println("\nRevenue by category (completed orders):")
    results = aggregate(db, "orders", [
        Dict("\$match" => Dict("status" => "completed")),
        Dict("\$group" => Dict("_id" => "\$category", "total" => Dict("\$sum" => "\$amount"), "count" => Dict("\$sum" => 1))),
        Dict("\$sort"  => Dict("total" => -1))
    ])
    for r in results
        println("  ", r["_id"], ": \$", r["total"], " (", r["count"], " orders)")
    end

    # $group with null _id (global aggregate)
    println("\nGlobal stats:")
    results = aggregate(db, "orders", [
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

    # $skip + $limit
    println("\nOrders page 2 (skip 2, limit 2):")
    results = aggregate(db, "orders", [
        Dict("\$sort" => Dict("amount" => -1)),
        Dict("\$skip" => 2),
        Dict("\$limit" => 2)
    ])
    for r in results
        println("  ", r["customer"], ": \$", r["amount"])
    end

    # $count
    println("\nCount completed orders:")
    results = aggregate(db, "orders", [
        Dict("\$match" => Dict("status" => "completed")),
        Dict("\$count" => "completed_count")
    ])
    println("  ", results[1]["completed_count"], " completed orders")

    # $project
    println("\nProjected fields (customer + amount only):")
    results = aggregate(db, "orders", [
        Dict("\$project" => Dict("customer" => 1, "amount" => 1, "_id" => 0)),
        Dict("\$limit" => 3)
    ])
    for r in results
        println("  ", r)
    end

    # $addFields
    println("\nWith computed tax field (10%):")
    results = aggregate(db, "orders", [
        Dict("\$addFields" => Dict("tax" => Dict("\$multiply" => ["\$amount", 0.1]))),
        Dict("\$limit" => 3)
    ])
    for r in results
        println("  ", r["customer"], ": amount=\$", r["amount"], " tax=\$", r["tax"])
    end

    # $lookup (cross-collection join)
    println("\nLookup — join orders with products:")
    insert_many(db, "products", [
        Dict("name" => "electronics", "description" => "Gadgets and devices"),
        Dict("name" => "books",       "description" => "Reading materials"),
        Dict("name" => "clothing",    "description" => "Apparel and accessories"),
    ])
    results = aggregate(db, "orders", [
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

    # $unwind
    println("\nUnwind user tags:")
    results = aggregate(db, "users", [
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
    section("10. Transactions")

    println("Auto-commit transaction (debit + credit):")
    transaction(db) do
        insert(db, "ledger", Dict("action" => "debit",  "account" => "A", "amount" => 500))
        insert(db, "ledger", Dict("action" => "credit", "account" => "B", "amount" => 500))
    end
    ledger = find(db, "ledger", Dict())
    for entry in ledger
        println("  ", entry["action"], " account=", entry["account"], " amount=", entry["amount"])
    end

    println("\nManual transaction (rolled back):")
    begin_tx(db)
    insert(db, "ledger", Dict("action" => "debit", "account" => "X", "amount" => 9999))
    rollback_tx(db)
    n = count_docs(db, "ledger")
    println("  Ledger count after rollback: $n (should be 2)")

    # ------------------------------------------------------------------
    # 11. Blob Storage
    # ------------------------------------------------------------------
    section("11. Blob Storage")

    create_bucket(db, "files")
    create_bucket(db, "docs")
    buckets = list_buckets(db)
    println("Buckets: ", buckets)

    put_object(db, "files", "greeting.txt", Vector{UInt8}("Hello from Julia (embedded)!");
               content_type="text/plain", metadata=Dict("author" => "julia-embedded-example"))
    put_object(db, "files", "data.csv", Vector{UInt8}("name,age\nAlice,30\nBob,25");
               content_type="text/csv")
    put_object(db, "docs", "notes.txt", Vector{UInt8}("Julia is a fast language for scientific computing");
               content_type="text/plain")
    println("Uploaded 3 objects")

    data, meta = get_object(db, "files", "greeting.txt")
    println("Downloaded greeting.txt: \"", String(data), "\"")
    println("  Metadata: size=", meta["size"], " content_type=", meta["content_type"])

    head = head_object(db, "files", "data.csv")
    println("Head data.csv: size=", head["size"], " etag=", head["etag"])

    objs = list_objects(db, "files")
    println("Objects in 'files': ")
    for o in objs
        println("  ", o["key"], " (", o["size"], " bytes)")
    end

    objs = list_objects(db, "files"; prefix="greet", limit=10)
    println("Objects with prefix 'greet': ", length(objs), " found")

    delete_object(db, "files", "data.csv")
    objs = list_objects(db, "files")
    println("After deleting data.csv: $(length(objs)) objects remain")

    # ------------------------------------------------------------------
    # 12. Full-Text Search
    # ------------------------------------------------------------------
    section("12. Full-Text Search")

    sleep(1)  # wait for background indexing

    results = search(db, "Julia"; limit=10)
    println("Search 'Julia':")
    for r in results
        println("  bucket=", r["bucket"], " key=", r["key"], " score=", r["score"])
    end

    results = search(db, "Hello"; bucket="files", limit=10)
    println("Search 'Hello' in 'files':")
    for r in results
        println("  key=", r["key"], " score=", r["score"])
    end

    # ------------------------------------------------------------------
    # 13. Compaction
    # ------------------------------------------------------------------
    section("13. Compaction")

    for i in 1:20
        insert(db, "events", Dict("type" => "test", "seq" => i))
    end
    delete(db, "events", Dict("seq" => Dict("\$lte" => 10)))
    r = execute(db, Dict("cmd" => "compact", "collection" => "events"))
    stats = r.data
    println("Compaction stats ($(fmt(r.ms))):")
    println("  Old size: ", stats["old_size"], " bytes")
    println("  New size: ", stats["new_size"], " bytes")
    println("  Docs kept: ", stats["docs_kept"])
    println("  Reclaimed: ", stats["old_size"] - stats["new_size"], " bytes")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    section("Cleanup")

    for col in ["users", "orders", "ledger", "products", "events"]
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

    oxidb_close(db.handle)
    println("\nDatabase closed. Done! All embedded features demonstrated successfully.")
end

main()
