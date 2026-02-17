# OxiDb.jl

Julia client for [OxiDB](https://github.com/parisxmas/OxiDB) document database.

Communicates with `oxidb-server` over TCP using the length-prefixed JSON protocol. Only dependency is `JSON3`.

## Requirements

- Julia 1.6+
- A running `oxidb-server` instance (see [main README](../../README.md#installation))

## Installation

```julia
using Pkg
Pkg.develop(path="julia/OxiDb")
```

## Quick Start

```julia
using OxiDb

client = connect_oxidb("127.0.0.1", 4444)

insert(client, "users", Dict("name" => "Alice", "age" => 30))
docs = find(client, "users", Dict("name" => "Alice"))
println(docs)
# [Dict("_id" => 1, "_version" => 1, "name" => "Alice", "age" => 30)]

close(client)
```

## API Reference

### Connection

```julia
client = connect_oxidb("127.0.0.1", 4444)  # connect
close(client)                                # disconnect
```

### CRUD

| Function | Description |
|----------|-------------|
| `insert(client, collection, doc)` | Insert a document, returns `Dict("id" => ...)` |
| `insert_many(client, collection, docs)` | Insert multiple documents |
| `find(client, collection, query; sort, skip, limit)` | Find matching documents |
| `find_one(client, collection, query)` | Find first matching document |
| `update(client, collection, query, update)` | Update matching documents |
| `delete(client, collection, query)` | Delete matching documents |
| `count_docs(client, collection, query)` | Count matching documents |

```julia
# Insert
insert(client, "users", Dict("name" => "Alice", "age" => 30))
insert_many(client, "users", [
    Dict("name" => "Bob", "age" => 25),
    Dict("name" => "Charlie", "age" => 35)
])

# Find with options
docs = find(client, "users", Dict("age" => Dict("\$gte" => 18)))
docs = find(client, "users", Dict(); sort=Dict("age" => 1), skip=0, limit=10)
doc  = find_one(client, "users", Dict("name" => "Alice"))

# Update
update(client, "users", Dict("name" => "Alice"), Dict("\$set" => Dict("age" => 31)))

# Delete
delete(client, "users", Dict("name" => "Charlie"))

# Count
n = count_docs(client, "users")
```

### Collections & Indexes

| Function | Description |
|----------|-------------|
| `create_collection(client, name)` | Explicitly create a collection |
| `list_collections(client)` | List all collection names |
| `drop_collection(client, name)` | Drop a collection and its data |
| `create_index(client, collection, field)` | Create a non-unique index |
| `create_unique_index(client, collection, field)` | Create a unique index |
| `create_composite_index(client, collection, fields)` | Create a multi-field index |

```julia
create_collection(client, "orders")
cols = list_collections(client)
drop_collection(client, "orders")

create_index(client, "users", "name")
create_unique_index(client, "users", "email")
create_composite_index(client, "users", ["name", "age"])
```

### Aggregation

```julia
results = aggregate(client, "orders", [
    Dict("\$match" => Dict("status" => "completed")),
    Dict("\$group" => Dict("_id" => "\$category", "total" => Dict("\$sum" => "\$amount"))),
    Dict("\$sort" => Dict("total" => -1)),
    Dict("\$limit" => 10)
])
```

**Supported stages:** `$match`, `$group`, `$sort`, `$skip`, `$limit`, `$project`, `$count`, `$unwind`, `$addFields`, `$lookup`

**Accumulators:** `$sum`, `$avg`, `$min`, `$max`, `$count`, `$first`, `$last`, `$push`

### Transactions

```julia
# Auto-commit on success, auto-rollback on exception
transaction(client) do
    insert(client, "ledger", Dict("action" => "debit",  "amount" => 100))
    insert(client, "ledger", Dict("action" => "credit", "amount" => 100))
end

# Manual control
begin_tx(client)
insert(client, "ledger", Dict("action" => "refund", "amount" => 50))
commit_tx(client)   # or rollback_tx(client)
```

### Blob Storage

```julia
# Buckets
create_bucket(client, "files")
list_buckets(client)
delete_bucket(client, "files")

# Objects
put_object(client, "files", "hello.txt", Vector{UInt8}("Hello!");
           content_type="text/plain", metadata=Dict("author" => "julia"))
data, meta = get_object(client, "files", "hello.txt")
head = head_object(client, "files", "hello.txt")
objs = list_objects(client, "files"; prefix="hello", limit=10)
delete_object(client, "files", "hello.txt")
```

### Full-Text Search

```julia
results = search(client, "hello world"; bucket="files", limit=10)
# Returns: [Dict("bucket" => "files", "key" => "doc.txt", "score" => 2.45), ...]
```

### Compaction

```julia
stats = compact(client, "users")
# Returns: Dict("old_size" => 4096, "new_size" => 2048, "docs_kept" => 10)
```

## Error Handling

```julia
try
    insert(client, "users", Dict("email" => "duplicate@test.com"))
catch e
    if e isa OxiDbError
        println("Database error: ", e.msg)
    elseif e isa TransactionConflictError
        println("OCC conflict: ", e.msg)
    end
end
```

## Exported Symbols

```julia
# Types
OxiDbClient, OxiDbError, TransactionConflictError

# Connection
connect_oxidb

# CRUD
insert, insert_many, find, find_one, update, delete, count_docs

# Collections
create_collection, list_collections, drop_collection

# Indexes
create_index, create_unique_index, create_composite_index

# Aggregation & Compaction
aggregate, compact

# Transactions
begin_tx, commit_tx, rollback_tx, transaction

# Blob Storage
create_bucket, list_buckets, delete_bucket,
put_object, get_object, head_object, delete_object, list_objects

# Search
search

# Utility
ping
```

## Running Tests

```bash
# Start the server
./oxidb-server

# Run tests
cd julia/OxiDb
julia --project=. test/runtests.jl
```

## License

See [LICENSE](../../LICENSE) for details.
