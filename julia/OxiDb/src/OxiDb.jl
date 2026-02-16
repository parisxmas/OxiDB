"""
OxiDB Julia client library.

Communicates with oxidb-server over TCP using the length-prefixed JSON protocol.

# Usage

```julia
using OxiDb

client = connect_oxidb("127.0.0.1", 4444)
insert(client, "users", Dict("name" => "Alice", "age" => 30))
docs = find(client, "users", Dict("name" => "Alice"))
close(client)
```
"""
module OxiDb

using Base64
using JSON3
using Sockets

export OxiDbClient, OxiDbError, TransactionConflictError,
       connect_oxidb,
       # Utility
       ping,
       # Collections
       create_collection, list_collections, drop_collection,
       # CRUD
       insert, insert_many, find, find_one, update, delete, count_docs,
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
# Client
# ------------------------------------------------------------------

"""
    OxiDbClient

TCP client for oxidb-server. Thread-safe via ReentrantLock.
Protocol: each message is [4-byte little-endian length][JSON payload].
"""
mutable struct OxiDbClient
    sock::TCPSocket
    lock::ReentrantLock

    function OxiDbClient(sock::TCPSocket)
        new(sock, ReentrantLock())
    end
end

"""
    connect_oxidb(host="127.0.0.1", port=4444) -> OxiDbClient

Connect to an oxidb-server instance.
"""
function connect_oxidb(host::AbstractString="127.0.0.1", port::Integer=4444)
    sock = Sockets.connect(host, port)
    OxiDbClient(sock)
end

function Base.close(client::OxiDbClient)
    close(client.sock)
end

# ------------------------------------------------------------------
# Low-level protocol
# ------------------------------------------------------------------

function _send_raw(client::OxiDbClient, data::Vector{UInt8})
    len = UInt32(length(data))
    len_bytes = reinterpret(UInt8, [htol(len)])
    write(client.sock, len_bytes)
    write(client.sock, data)
end

function _recv_raw(client::OxiDbClient)
    len_bytes = read(client.sock, 4)
    length(len_bytes) == 4 || throw(OxiDbError("connection closed by server"))
    len = ltoh(reinterpret(UInt32, len_bytes)[1])
    payload = read(client.sock, len)
    UInt32(length(payload)) == len || throw(OxiDbError("connection closed by server"))
    payload
end

function _request(client::OxiDbClient, payload::Dict)
    lock(client.lock) do
        json_bytes = Vector{UInt8}(JSON3.write(payload))
        _send_raw(client, json_bytes)
        resp_bytes = _recv_raw(client)
        JSON3.read(String(resp_bytes), Dict{String,Any})
    end
end

function _checked(client::OxiDbClient, payload::Dict)
    resp = _request(client, payload)
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
    ping(client) -> String

Ping the server. Returns "pong".
"""
ping(client::OxiDbClient) = _checked(client, Dict("cmd" => "ping"))

# ------------------------------------------------------------------
# Collection management
# ------------------------------------------------------------------

"""
    create_collection(client, name)

Explicitly create a collection. Collections are also auto-created on insert.
"""
create_collection(client::OxiDbClient, name::AbstractString) =
    _checked(client, Dict("cmd" => "create_collection", "collection" => name))

"""
    list_collections(client)

Return a list of collection names.
"""
list_collections(client::OxiDbClient) =
    _checked(client, Dict("cmd" => "list_collections"))

"""
    drop_collection(client, name)

Drop a collection and its data.
"""
drop_collection(client::OxiDbClient, name::AbstractString) =
    _checked(client, Dict("cmd" => "drop_collection", "collection" => name))

# ------------------------------------------------------------------
# CRUD
# ------------------------------------------------------------------

"""
    insert(client, collection, doc::Dict)

Insert a single document. Returns Dict("id" => ...) outside tx, "buffered" inside tx.
"""
function insert(client::OxiDbClient, collection::AbstractString, doc::Dict)
    _checked(client, Dict("cmd" => "insert", "collection" => collection, "doc" => doc))
end

"""
    insert_many(client, collection, docs::Vector)

Insert multiple documents.
"""
function insert_many(client::OxiDbClient, collection::AbstractString, docs::Vector)
    _checked(client, Dict("cmd" => "insert_many", "collection" => collection, "docs" => docs))
end

"""
    find(client, collection, query=Dict(); sort=nothing, skip=nothing, limit=nothing)

Find documents matching a query.
"""
function find(client::OxiDbClient, collection::AbstractString, query::Dict=Dict();
              sort=nothing, skip::Union{Integer,Nothing}=nothing,
              limit::Union{Integer,Nothing}=nothing)
    payload = Dict{String,Any}("cmd" => "find", "collection" => collection, "query" => query)
    sort !== nothing && (payload["sort"] = sort)
    skip !== nothing && (payload["skip"] = skip)
    limit !== nothing && (payload["limit"] = limit)
    _checked(client, payload)
end

"""
    find_one(client, collection, query=Dict())

Find a single document. Returns the document or nothing.
"""
function find_one(client::OxiDbClient, collection::AbstractString, query::Dict=Dict())
    _checked(client, Dict("cmd" => "find_one", "collection" => collection, "query" => query))
end

"""
    update(client, collection, query::Dict, update_doc::Dict)

Update documents matching a query.
"""
function update(client::OxiDbClient, collection::AbstractString, query::Dict, update_doc::Dict)
    _checked(client, Dict("cmd" => "update", "collection" => collection,
                           "query" => query, "update" => update_doc))
end

"""
    delete(client, collection, query::Dict)

Delete documents matching a query.
"""
function delete(client::OxiDbClient, collection::AbstractString, query::Dict)
    _checked(client, Dict("cmd" => "delete", "collection" => collection, "query" => query))
end

"""
    count_docs(client, collection, query=Dict()) -> Int

Count documents matching a query.
"""
function count_docs(client::OxiDbClient, collection::AbstractString, query::Dict=Dict())
    result = _checked(client, Dict("cmd" => "count", "collection" => collection, "query" => query))
    result["count"]
end

# ------------------------------------------------------------------
# Indexes
# ------------------------------------------------------------------

"""
    create_index(client, collection, field)

Create a non-unique index on a field.
"""
create_index(client::OxiDbClient, collection::AbstractString, field::AbstractString) =
    _checked(client, Dict("cmd" => "create_index", "collection" => collection, "field" => field))

"""
    create_unique_index(client, collection, field)

Create a unique index on a field.
"""
create_unique_index(client::OxiDbClient, collection::AbstractString, field::AbstractString) =
    _checked(client, Dict("cmd" => "create_unique_index", "collection" => collection, "field" => field))

"""
    create_composite_index(client, collection, fields)

Create a composite index on multiple fields.
"""
create_composite_index(client::OxiDbClient, collection::AbstractString, fields::Vector{<:AbstractString}) =
    _checked(client, Dict("cmd" => "create_composite_index", "collection" => collection, "fields" => fields))

# ------------------------------------------------------------------
# Aggregation
# ------------------------------------------------------------------

"""
    aggregate(client, collection, pipeline::Vector)

Run an aggregation pipeline. Returns list of result documents.
"""
aggregate(client::OxiDbClient, collection::AbstractString, pipeline::Vector) =
    _checked(client, Dict("cmd" => "aggregate", "collection" => collection, "pipeline" => pipeline))

# ------------------------------------------------------------------
# Compaction
# ------------------------------------------------------------------

"""
    compact(client, collection)

Compact a collection. Returns Dict with old_size, new_size, docs_kept.
"""
compact(client::OxiDbClient, collection::AbstractString) =
    _checked(client, Dict("cmd" => "compact", "collection" => collection))

# ------------------------------------------------------------------
# Transactions
# ------------------------------------------------------------------

"""
    begin_tx(client)

Begin a transaction on this connection. Returns Dict("tx_id" => ...).
"""
begin_tx(client::OxiDbClient) = _checked(client, Dict("cmd" => "begin_tx"))

"""
    commit_tx(client)

Commit the active transaction. Throws TransactionConflictError on OCC conflict.
"""
commit_tx(client::OxiDbClient) = _checked(client, Dict("cmd" => "commit_tx"))

"""
    rollback_tx(client)

Rollback the active transaction.
"""
rollback_tx(client::OxiDbClient) = _checked(client, Dict("cmd" => "rollback_tx"))

"""
    transaction(f, client)

Execute `f` within a transaction. Auto-commits on success, auto-rolls back on exception.

# Example
```julia
transaction(client) do
    insert(client, "col", Dict("x" => 1))
    update(client, "col", Dict("x" => 1), Dict("\\\$set" => Dict("x" => 2)))
end
```
"""
function transaction(f, client::OxiDbClient)
    begin_tx(client)
    try
        f()
        commit_tx(client)
    catch e
        try
            rollback_tx(client)
        catch _
            # rollback may fail if commit already failed
        end
        rethrow()
    end
end

# ------------------------------------------------------------------
# Blob storage
# ------------------------------------------------------------------

"""
    create_bucket(client, bucket)

Create a blob storage bucket.
"""
create_bucket(client::OxiDbClient, bucket::AbstractString) =
    _checked(client, Dict("cmd" => "create_bucket", "bucket" => bucket))

"""
    list_buckets(client)

List all blob storage buckets.
"""
list_buckets(client::OxiDbClient) = _checked(client, Dict("cmd" => "list_buckets"))

"""
    delete_bucket(client, bucket)

Delete a blob storage bucket.
"""
delete_bucket(client::OxiDbClient, bucket::AbstractString) =
    _checked(client, Dict("cmd" => "delete_bucket", "bucket" => bucket))

"""
    put_object(client, bucket, key, data::Vector{UInt8}; content_type="application/octet-stream", metadata=nothing)

Upload a blob object. Data is base64-encoded automatically.
"""
function put_object(client::OxiDbClient, bucket::AbstractString, key::AbstractString,
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
    _checked(client, payload)
end

"""
    get_object(client, bucket, key) -> (data::Vector{UInt8}, metadata::Dict)

Download a blob object. Returns (bytes, metadata).
"""
function get_object(client::OxiDbClient, bucket::AbstractString, key::AbstractString)
    result = _checked(client, Dict("cmd" => "get_object", "bucket" => bucket, "key" => key))
    data = base64decode(result["content"])
    (data, result["metadata"])
end

"""
    head_object(client, bucket, key)

Get blob object metadata without downloading the content.
"""
head_object(client::OxiDbClient, bucket::AbstractString, key::AbstractString) =
    _checked(client, Dict("cmd" => "head_object", "bucket" => bucket, "key" => key))

"""
    delete_object(client, bucket, key)

Delete a blob object.
"""
delete_object(client::OxiDbClient, bucket::AbstractString, key::AbstractString) =
    _checked(client, Dict("cmd" => "delete_object", "bucket" => bucket, "key" => key))

"""
    list_objects(client, bucket; prefix=nothing, limit=nothing)

List objects in a bucket.
"""
function list_objects(client::OxiDbClient, bucket::AbstractString;
                      prefix::Union{AbstractString,Nothing}=nothing,
                      limit::Union{Integer,Nothing}=nothing)
    payload = Dict{String,Any}("cmd" => "list_objects", "bucket" => bucket)
    prefix !== nothing && (payload["prefix"] = prefix)
    limit !== nothing && (payload["limit"] = limit)
    _checked(client, payload)
end

# ------------------------------------------------------------------
# Full-text search
# ------------------------------------------------------------------

"""
    search(client, query; bucket=nothing, limit=10)

Full-text search across blobs. Returns list of Dict with bucket, key, score.
"""
function search(client::OxiDbClient, query::AbstractString;
                bucket::Union{AbstractString,Nothing}=nothing,
                limit::Integer=10)
    payload = Dict{String,Any}("cmd" => "search", "query" => query, "limit" => limit)
    bucket !== nothing && (payload["bucket"] = bucket)
    _checked(client, payload)
end

end # module
