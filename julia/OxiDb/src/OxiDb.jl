"""
    OxiDb

Minimal Julia client for `oxidb-server`. JSON over TCP, length-prefixed
(`[u32 LE length][JSON]`). Thread-safe.

## Usage

```julia
using OxiDb

OxiDb.connect("127.0.0.1", 4444) do db
    insert(db, "users", Dict("name" => "Alice", "age" => 30))
    docs = find(db, "users"; query = Dict("age" => Dict("\$gte" => 18)))
    @show docs
end
```

For any command not covered by the convenience helpers below, use the
generic `exec` — every server command is reachable that way:

```julia
exec(db, "create_ttl_index";
     collection = "sessions", field = "created_at", expireAfterSeconds = 3600)

exec(db, "create_procedure"; name = "hi",
     script = "proc hi(n) { return {hi: n} }")
```
"""
module OxiDb

using JSON3
using Sockets
using Tables

export OxiDbClient, OxiDbError, OxiDbResult, exec, ping,
       insert, insert_many, find, find_one,
       update, update_one, delete, delete_one,
       count_docs, aggregate, sql

# ─── Errors ────────────────────────────────────────────────────────────────

struct OxiDbError <: Exception
    msg::String
end
Base.showerror(io::IO, e::OxiDbError) = print(io, "OxiDbError: ", e.msg)

# ─── Client ────────────────────────────────────────────────────────────────

mutable struct OxiDbClient
    sock::TCPSocket
    lock::ReentrantLock
end

OxiDbClient(sock::TCPSocket) = OxiDbClient(sock, ReentrantLock())

Base.show(io::IO, c::OxiDbClient) =
    print(io, "OxiDbClient(", isopen(c.sock) ? "open" : "closed", ")")

# ─── Query result wrapper ─────────────────────────────────────────────────

"""
    OxiDbResult <: AbstractVector{Any}

Return type of [`find`](@ref) and [`aggregate`](@ref). Walks like a
`Vector` of row `Dict`s — `length`, `getindex`, iteration all work — *and*
satisfies the [Tables.jl](https://github.com/JuliaData/Tables.jl)
row-access interface, so `DataFrames.DataFrame(result)`, `CSV.write(io,
result)`, `MLJ` pipelines, etc. accept it with no manual conversion.

```julia
rows = find(db, "users"; query = Dict("age" => Dict("\$gte" => 18)))
length(rows)               # n
rows[1]["name"]            # individual-row access still works
DataFrame(rows)            # ...and Tables.jl interop
```
"""
struct OxiDbResult <: AbstractVector{Any}
    rows::Vector{Any}
end
OxiDbResult(x::AbstractVector) = OxiDbResult(Vector{Any}(x))
OxiDbResult(::Nothing) = OxiDbResult(Any[])

Base.size(r::OxiDbResult) = size(r.rows)
Base.getindex(r::OxiDbResult, i::Integer) = r.rows[i]
Base.IndexStyle(::Type{<:OxiDbResult}) = IndexLinear()

# Tables.jl row-access — heterogeneous-schema row dicts are handled by
# Tables.dictrowtable (it merges keys across rows, fills missing with `missing`).
Tables.istable(::Type{<:OxiDbResult}) = true
Tables.rowaccess(::Type{<:OxiDbResult}) = true
Tables.rows(r::OxiDbResult) = Tables.rows(
    Tables.dictrowtable(Vector{Dict{String,Any}}(r.rows)))

"""
    connect(host="127.0.0.1", port=4444) -> OxiDbClient
    connect(f::Function, host="127.0.0.1", port=4444)

Open a TCP connection. The two-argument `do`-block form auto-closes
the connection when the block exits, even on error.
"""
connect(host::AbstractString = "127.0.0.1", port::Integer = 4444) =
    OxiDbClient(Sockets.connect(host, port))

function connect(f::Function, host::AbstractString = "127.0.0.1", port::Integer = 4444)
    c = connect(host, port)
    try f(c) finally close(c) end
end

Base.close(c::OxiDbClient) = close(c.sock)
Base.isopen(c::OxiDbClient) = isopen(c.sock)

# ─── Wire protocol (length-prefixed JSON) ──────────────────────────────────

function _request(c::OxiDbClient, payload::AbstractDict)
    body = Vector{UInt8}(JSON3.write(payload))
    lock(c.lock) do
        write(c.sock, htol(UInt32(length(body))))
        write(c.sock, body)
        n = ltoh(read(c.sock, UInt32))
        n == 0 && return Dict{String,Any}()
        JSON3.read(String(read(c.sock, Int(n))), Dict{String,Any})
    end
end

# ─── Generic command — the escape hatch for anything not below ─────────────

"""
    exec(client, cmd::AbstractString; kwargs...)

Send any command to the server. Returns the `data` field of the response,
or throws `OxiDbError` on failure. Pass command-specific fields as keyword
arguments — they're merged into the wire payload as-is.
"""
function exec(c::OxiDbClient, cmd::AbstractString; kwargs...)
    payload = Dict{String,Any}("cmd" => cmd)
    for (k, v) in kwargs
        payload[String(k)] = v
    end
    resp = _request(c, payload)
    get(resp, "ok", false) || throw(OxiDbError(get(resp, "error", "unknown error")))
    get(resp, "data", nothing)
end

# ─── Convenience: the everyday ops ─────────────────────────────────────────

ping(c::OxiDbClient) = exec(c, "ping")

insert(c::OxiDbClient, collection::AbstractString, doc::AbstractDict) =
    exec(c, "insert"; collection, doc)

insert_many(c::OxiDbClient, collection::AbstractString, docs::AbstractVector) =
    exec(c, "insert_many"; collection, docs)

function find(c::OxiDbClient, collection::AbstractString;
              query::AbstractDict = Dict{String,Any}(),
              sort = nothing, skip = nothing, limit = nothing)
    payload = Dict{String,Any}("cmd" => "find", "collection" => collection, "query" => query)
    sort  === nothing || (payload["sort"]  = sort)
    skip  === nothing || (payload["skip"]  = skip)
    limit === nothing || (payload["limit"] = limit)
    resp = _request(c, payload)
    get(resp, "ok", false) || throw(OxiDbError(get(resp, "error", "unknown error")))
    OxiDbResult(get(resp, "data", Any[]))
end

find_one(c::OxiDbClient, collection::AbstractString,
         query::AbstractDict = Dict{String,Any}()) =
    exec(c, "find_one"; collection, query)

update(c::OxiDbClient, collection::AbstractString,
       query::AbstractDict, update_doc::AbstractDict) =
    exec(c, "update"; collection, query, update = update_doc)

update_one(c::OxiDbClient, collection::AbstractString,
           query::AbstractDict, update_doc::AbstractDict) =
    exec(c, "update_one"; collection, query, update = update_doc)

delete(c::OxiDbClient, collection::AbstractString, query::AbstractDict) =
    exec(c, "delete"; collection, query)

delete_one(c::OxiDbClient, collection::AbstractString, query::AbstractDict) =
    exec(c, "delete_one"; collection, query)

"""
    count_docs(client, collection, query=Dict()) -> Int

`count_docs` rather than `count` so it doesn't collide with `Base.count`.
"""
function count_docs(c::OxiDbClient, collection::AbstractString,
                    query::AbstractDict = Dict{String,Any}())
    r = exec(c, "count"; collection, query)
    r isa AbstractDict ? Int(r["count"]) : Int(r)
end

aggregate(c::OxiDbClient, collection::AbstractString, pipeline::AbstractVector) =
    OxiDbResult(exec(c, "aggregate"; collection, pipeline))

sql(c::OxiDbClient, query::AbstractString) = exec(c, "sql"; query)

end # module
