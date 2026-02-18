"""
OxiDB Embedded — in-process document database for Julia (no server needed).

Uses the prebuilt `liboxidb_embedded_ffi` native library via `ccall`.
The library is automatically downloaded from GitHub Releases on first use.

# Usage

```julia
using OxiDbEmbedded

db = open_db("/tmp/mydb")

insert(db, "users", Dict("name" => "Alice", "age" => 30))
docs = find(db, "users", Dict("name" => "Alice"))
println(docs)

close(db)
```
"""
module OxiDbEmbedded

using Base64
using JSON3

export OxiDatabase, OxiDbError, TransactionConflictError,
       open_db,
       # Utility
       ping,
       # Collections
       create_collection, list_collections, drop_collection,
       # CRUD
       insert, insert_many, find, find_one,
       update, update_one, delete, delete_one,
       count_docs,
       # Indexes
       create_index, create_unique_index, create_composite_index,
       # Aggregation
       aggregate,
       # Compaction
       compact,
       # Transactions
       begin_tx, commit_tx, rollback_tx, transaction,
       # Blob storage
       create_bucket, list_buckets, delete_bucket,
       put_object, get_object, head_object, delete_object, list_objects,
       # FTS
       search

# ------------------------------------------------------------------
# Exceptions
# ------------------------------------------------------------------

struct OxiDbError <: Exception
    msg::String
end

Base.showerror(io::IO, e::OxiDbError) = print(io, "OxiDbError: ", e.msg)

struct TransactionConflictError <: Exception
    msg::String
end

Base.showerror(io::IO, e::TransactionConflictError) = print(io, "TransactionConflictError: ", e.msg)

# ------------------------------------------------------------------
# Library download
# ------------------------------------------------------------------

const _RELEASE_VERSION = "v0.6.0"
const _RELEASE_BASE = "https://github.com/parisxmas/OxiDB/releases/download/$(_RELEASE_VERSION)"
const _LIB_DIR = joinpath(@__DIR__, "..", "lib")

function _lib_path()
    if Sys.isapple()
        lib_name = "liboxidb_embedded_ffi.dylib"
        if Sys.ARCH === :aarch64 || Sys.ARCH === :arm64
            tarball = "oxidb-embedded-ffi-macos-arm64.tar.gz"
        else
            error("Unsupported macOS architecture: $(Sys.ARCH). Build from source: cargo build --release -p oxidb-embedded-ffi")
        end
    elseif Sys.islinux()
        lib_name = "liboxidb_embedded_ffi.so"
        error("No prebuilt Linux binary yet. Build from source: cargo build --release -p oxidb-embedded-ffi")
    elseif Sys.iswindows()
        lib_name = "oxidb_embedded_ffi.dll"
        tarball = "oxidb-embedded-ffi-windows-x86_64.tar.gz"
    else
        error("Unsupported platform: $(Sys.KERNEL)")
    end

    path = joinpath(_LIB_DIR, lib_name)

    if !isfile(path)
        @info "Downloading prebuilt OxiDB library ($tarball)..."
        mkpath(_LIB_DIR)
        tarball_path = joinpath(_LIB_DIR, tarball)
        download("$(_RELEASE_BASE)/$tarball", tarball_path)
        run(`tar xzf $tarball_path -C $(_LIB_DIR)`)
        rm(tarball_path)
        @info "Library installed to $(_LIB_DIR)"
    end

    isfile(path) || error("Library not found at $path after download")
    return path
end

const _LIB = _lib_path()

# ------------------------------------------------------------------
# Database handle
# ------------------------------------------------------------------

"""
    OxiDatabase

In-process OxiDB database handle. Thread-safe via internal locking.

Open with `open_db(path)`, close with `close(db)`.
"""
mutable struct OxiDatabase
    handle::Ptr{Cvoid}
    closed::Bool

    function OxiDatabase(handle::Ptr{Cvoid})
        db = new(handle, false)
        finalizer(db) do d
            d.closed || close(d)
        end
        db
    end
end

"""
    open_db(path; encryption_key_path=nothing) -> OxiDatabase

Open an embedded database at the given directory path.
Optionally provide an AES-256 encryption key file path.
"""
function open_db(path::AbstractString; encryption_key_path::Union{AbstractString,Nothing}=nothing)
    if encryption_key_path !== nothing
        handle = ccall((:oxidb_open_encrypted, _LIB), Ptr{Cvoid}, (Cstring, Cstring), path, encryption_key_path)
    else
        handle = ccall((:oxidb_open, _LIB), Ptr{Cvoid}, (Cstring,), path)
    end
    handle == C_NULL && throw(OxiDbError("failed to open database at: $path"))
    OxiDatabase(handle)
end

function Base.close(db::OxiDatabase)
    db.closed && return
    ccall((:oxidb_close, _LIB), Cvoid, (Ptr{Cvoid},), db.handle)
    db.closed = true
    nothing
end

# ------------------------------------------------------------------
# Low-level execute
# ------------------------------------------------------------------

function _execute(db::OxiDatabase, cmd::Dict)
    db.closed && throw(OxiDbError("database is closed"))
    json = JSON3.write(cmd)
    ptr = ccall((:oxidb_execute, _LIB), Cstring, (Ptr{Cvoid}, Cstring), db.handle, json)
    ptr == C_NULL && throw(OxiDbError("oxidb_execute returned NULL"))
    str = unsafe_string(ptr)
    ccall((:oxidb_free_string, _LIB), Cvoid, (Cstring,), ptr)
    JSON3.read(str, Dict{String,Any})
end

function _checked(db::OxiDatabase, cmd::Dict)
    resp = _execute(db, cmd)
    if !get(resp, "ok", false)
        error_msg = get(resp, "error", "unknown error")
        if occursin("conflict", lowercase(error_msg))
            throw(TransactionConflictError(error_msg))
        end
        throw(OxiDbError(error_msg))
    end
    get(resp, "data", nothing)
end

# ------------------------------------------------------------------
# Utility
# ------------------------------------------------------------------

"""
    ping(db) -> String

Ping the database. Returns "pong".
"""
ping(db::OxiDatabase) = _checked(db, Dict("cmd" => "ping"))

# ------------------------------------------------------------------
# Collection management
# ------------------------------------------------------------------

"""
    create_collection(db, name)

Explicitly create a collection. Collections are also auto-created on insert.
"""
create_collection(db::OxiDatabase, name::AbstractString) =
    _checked(db, Dict("cmd" => "create_collection", "collection" => name))

"""
    list_collections(db)

Return a list of collection names.
"""
list_collections(db::OxiDatabase) =
    _checked(db, Dict("cmd" => "list_collections"))

"""
    drop_collection(db, name)

Drop a collection and its data.
"""
drop_collection(db::OxiDatabase, name::AbstractString) =
    _checked(db, Dict("cmd" => "drop_collection", "collection" => name))

# ------------------------------------------------------------------
# CRUD
# ------------------------------------------------------------------

"""
    insert(db, collection, doc::Dict)

Insert a single document.
"""
function insert(db::OxiDatabase, collection::AbstractString, doc::Dict)
    _checked(db, Dict("cmd" => "insert", "collection" => collection, "doc" => doc))
end

"""
    insert_many(db, collection, docs::Vector)

Insert multiple documents.
"""
function insert_many(db::OxiDatabase, collection::AbstractString, docs::Vector)
    _checked(db, Dict("cmd" => "insert_many", "collection" => collection, "docs" => docs))
end

"""
    find(db, collection, query=Dict(); sort=nothing, skip=nothing, limit=nothing)

Find documents matching a query.
"""
function find(db::OxiDatabase, collection::AbstractString, query::Dict=Dict();
              sort=nothing, skip::Union{Integer,Nothing}=nothing,
              limit::Union{Integer,Nothing}=nothing)
    payload = Dict{String,Any}("cmd" => "find", "collection" => collection, "query" => query)
    sort !== nothing && (payload["sort"] = sort)
    skip !== nothing && (payload["skip"] = skip)
    limit !== nothing && (payload["limit"] = limit)
    _checked(db, payload)
end

"""
    find_one(db, collection, query=Dict())

Find a single document. Returns the document or nothing.
"""
function find_one(db::OxiDatabase, collection::AbstractString, query::Dict=Dict())
    _checked(db, Dict("cmd" => "find_one", "collection" => collection, "query" => query))
end

"""
    update(db, collection, query::Dict, update_doc::Dict)

Update all documents matching a query.
"""
function update(db::OxiDatabase, collection::AbstractString, query::Dict, update_doc::Dict)
    _checked(db, Dict("cmd" => "update", "collection" => collection,
                       "query" => query, "update" => update_doc))
end

"""
    update_one(db, collection, query::Dict, update_doc::Dict)

Update the first document matching a query (embedded only).
"""
function update_one(db::OxiDatabase, collection::AbstractString, query::Dict, update_doc::Dict)
    _checked(db, Dict("cmd" => "update_one", "collection" => collection,
                       "query" => query, "update" => update_doc))
end

"""
    delete(db, collection, query::Dict)

Delete all documents matching a query.
"""
function delete(db::OxiDatabase, collection::AbstractString, query::Dict)
    _checked(db, Dict("cmd" => "delete", "collection" => collection, "query" => query))
end

"""
    delete_one(db, collection, query::Dict)

Delete the first document matching a query (embedded only).
"""
function delete_one(db::OxiDatabase, collection::AbstractString, query::Dict)
    _checked(db, Dict("cmd" => "delete_one", "collection" => collection, "query" => query))
end

"""
    count_docs(db, collection, query=Dict()) -> Int

Count documents matching a query.
"""
function count_docs(db::OxiDatabase, collection::AbstractString, query::Dict=Dict())
    result = _checked(db, Dict("cmd" => "count", "collection" => collection, "query" => query))
    result["count"]
end

# ------------------------------------------------------------------
# Indexes
# ------------------------------------------------------------------

"""
    create_index(db, collection, field)

Create a non-unique index on a field.
"""
create_index(db::OxiDatabase, collection::AbstractString, field::AbstractString) =
    _checked(db, Dict("cmd" => "create_index", "collection" => collection, "field" => field))

"""
    create_unique_index(db, collection, field)

Create a unique index on a field.
"""
create_unique_index(db::OxiDatabase, collection::AbstractString, field::AbstractString) =
    _checked(db, Dict("cmd" => "create_unique_index", "collection" => collection, "field" => field))

"""
    create_composite_index(db, collection, fields)

Create a composite index on multiple fields.
"""
create_composite_index(db::OxiDatabase, collection::AbstractString, fields::Vector{<:AbstractString}) =
    _checked(db, Dict("cmd" => "create_composite_index", "collection" => collection, "fields" => fields))

# ------------------------------------------------------------------
# Aggregation
# ------------------------------------------------------------------

"""
    aggregate(db, collection, pipeline::Vector)

Run an aggregation pipeline. Returns list of result documents.
"""
aggregate(db::OxiDatabase, collection::AbstractString, pipeline::Vector) =
    _checked(db, Dict("cmd" => "aggregate", "collection" => collection, "pipeline" => pipeline))

# ------------------------------------------------------------------
# Compaction
# ------------------------------------------------------------------

"""
    compact(db, collection)

Compact a collection. Returns Dict with old_size, new_size, docs_kept.
"""
compact(db::OxiDatabase, collection::AbstractString) =
    _checked(db, Dict("cmd" => "compact", "collection" => collection))

# ------------------------------------------------------------------
# Transactions
# ------------------------------------------------------------------

"""
    begin_tx(db)

Begin a transaction.
"""
begin_tx(db::OxiDatabase) = _checked(db, Dict("cmd" => "begin_tx"))

"""
    commit_tx(db)

Commit the active transaction.
"""
commit_tx(db::OxiDatabase) = _checked(db, Dict("cmd" => "commit_tx"))

"""
    rollback_tx(db)

Rollback the active transaction.
"""
rollback_tx(db::OxiDatabase) = _checked(db, Dict("cmd" => "rollback_tx"))

"""
    transaction(f, db)

Execute `f` within a transaction. Auto-commits on success, auto-rolls back on exception.

# Example
```julia
transaction(db) do
    insert(db, "ledger", Dict("action" => "debit",  "amount" => 100))
    insert(db, "ledger", Dict("action" => "credit", "amount" => 100))
end
```
"""
function transaction(f, db::OxiDatabase)
    begin_tx(db)
    try
        f()
        commit_tx(db)
    catch e
        try; rollback_tx(db); catch; end
        rethrow()
    end
end

# ------------------------------------------------------------------
# Blob storage
# ------------------------------------------------------------------

"""
    create_bucket(db, bucket)

Create a blob storage bucket.
"""
create_bucket(db::OxiDatabase, bucket::AbstractString) =
    _checked(db, Dict("cmd" => "create_bucket", "bucket" => bucket))

"""
    list_buckets(db)

List all blob storage buckets.
"""
list_buckets(db::OxiDatabase) = _checked(db, Dict("cmd" => "list_buckets"))

"""
    delete_bucket(db, bucket)

Delete a blob storage bucket.
"""
delete_bucket(db::OxiDatabase, bucket::AbstractString) =
    _checked(db, Dict("cmd" => "delete_bucket", "bucket" => bucket))

"""
    put_object(db, bucket, key, data::Vector{UInt8}; content_type="application/octet-stream", metadata=nothing)

Upload a blob object. Data is base64-encoded automatically.
"""
function put_object(db::OxiDatabase, bucket::AbstractString, key::AbstractString,
                    data::Vector{UInt8};
                    content_type::AbstractString="application/octet-stream",
                    metadata::Union{Dict,Nothing}=nothing)
    payload = Dict{String,Any}(
        "cmd" => "put_object",
        "bucket" => bucket,
        "key" => key,
        "data" => base64encode(data),
        "content_type" => content_type
    )
    metadata !== nothing && (payload["metadata"] = metadata)
    _checked(db, payload)
end

"""
    get_object(db, bucket, key) -> (data::Vector{UInt8}, metadata::Dict)

Download a blob object. Returns (bytes, metadata).
"""
function get_object(db::OxiDatabase, bucket::AbstractString, key::AbstractString)
    result = _checked(db, Dict("cmd" => "get_object", "bucket" => bucket, "key" => key))
    data = base64decode(result["content"])
    (data, result["metadata"])
end

"""
    head_object(db, bucket, key)

Get blob object metadata without downloading the content.
"""
head_object(db::OxiDatabase, bucket::AbstractString, key::AbstractString) =
    _checked(db, Dict("cmd" => "head_object", "bucket" => bucket, "key" => key))

"""
    delete_object(db, bucket, key)

Delete a blob object.
"""
delete_object(db::OxiDatabase, bucket::AbstractString, key::AbstractString) =
    _checked(db, Dict("cmd" => "delete_object", "bucket" => bucket, "key" => key))

"""
    list_objects(db, bucket; prefix=nothing, limit=nothing)

List objects in a bucket.
"""
function list_objects(db::OxiDatabase, bucket::AbstractString;
                      prefix::Union{AbstractString,Nothing}=nothing,
                      limit::Union{Integer,Nothing}=nothing)
    payload = Dict{String,Any}("cmd" => "list_objects", "bucket" => bucket)
    prefix !== nothing && (payload["prefix"] = prefix)
    limit !== nothing && (payload["limit"] = limit)
    _checked(db, payload)
end

# ------------------------------------------------------------------
# Full-text search
# ------------------------------------------------------------------

"""
    search(db, query; bucket=nothing, limit=10)

Full-text search across blobs. Returns list of Dict with bucket, key, score.
"""
function search(db::OxiDatabase, query::AbstractString;
                bucket::Union{AbstractString,Nothing}=nothing,
                limit::Integer=10)
    payload = Dict{String,Any}("cmd" => "search", "query" => query, "limit" => limit)
    bucket !== nothing && (payload["bucket"] = bucket)
    _checked(db, payload)
end

end # module
