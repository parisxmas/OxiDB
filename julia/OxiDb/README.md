# OxiDb.jl

Julia client for [OxiDB](https://github.com/parisxmas/OxiDB) document database. Two packages:

| Package | Mode | Server needed? |
|---------|------|----------------|
| **`OxiDbEmbedded`** | In-process via FFI | No |
| **`OxiDb`** | TCP client | Yes |

Both share the same API (insert, find, update, delete, aggregate, transactions, blobs, FTS).

## Requirements

- Julia 1.6+

## Quick Start — Embedded (recommended)

No server, no compilation. The prebuilt native library is downloaded automatically.

```julia
using Pkg
Pkg.develop(path="julia/OxiDbEmbedded")
```

```julia
using OxiDbEmbedded

db = open_db("/tmp/mydb")

insert(db, "users", Dict("name" => "Alice", "age" => 30))
docs = find(db, "users", Dict("name" => "Alice"))
println(docs)

close(db)
```

Or run the full demo:

```bash
julia examples/julia/embedded_example.jl
```

### Supported platforms (prebuilt)

| Platform | Architecture | Status |
|----------|-------------|--------|
| macOS | arm64 (Apple Silicon) | Prebuilt available |
| Linux | x86_64 | Prebuilt available |
| Windows | x86_64 | Prebuilt available |
| macOS | x86_64 | Build from source |

Build from source: `cargo build --release -p oxidb-embedded-ffi`

## Quick Start — TCP Client

Requires a running `oxidb-server` (see [main README](../../README.md#installation)).

```julia
using Pkg
Pkg.develop(path="julia/OxiDb")
```

```julia
using OxiDb

client = connect_oxidb("127.0.0.1", 4444)

insert(client, "users", Dict("name" => "Alice", "age" => 30))
docs = find(client, "users", Dict("name" => "Alice"))
println(docs)

close(client)
```

## API Reference

Both packages export the same functions. Replace `db`/`client` interchangeably.

### Open / Connect

```julia
# Embedded
db = open_db("/tmp/mydb")
db = open_db("/tmp/mydb"; encryption_key_path="/path/to/key")
close(db)

# TCP client
client = connect_oxidb("127.0.0.1", 4444)
close(client)
```

### CRUD

| Function | Description |
|----------|-------------|
| `insert(db, collection, doc)` | Insert a document |
| `insert_many(db, collection, docs)` | Insert multiple documents |
| `find(db, collection, query; sort, skip, limit)` | Find matching documents |
| `find_one(db, collection, query)` | Find first matching document |
| `update(db, collection, query, update)` | Update matching documents |
| `update_one(db, collection, query, update)` | Update first match |
| `delete(db, collection, query)` | Delete matching documents |
| `delete_one(db, collection, query)` | Delete first match |
| `count_docs(db, collection, query)` | Count matching documents |

```julia
insert(db, "users", Dict("name" => "Alice", "age" => 30))
insert_many(db, "users", [
    Dict("name" => "Bob", "age" => 25),
    Dict("name" => "Charlie", "age" => 35)
])

docs = find(db, "users", Dict("age" => Dict("\$gte" => 18)))
docs = find(db, "users", Dict(); sort=Dict("age" => 1), skip=0, limit=10)
doc  = find_one(db, "users", Dict("name" => "Alice"))

update(db, "users", Dict("name" => "Alice"), Dict("\$set" => Dict("age" => 31)))
delete(db, "users", Dict("name" => "Charlie"))
n = count_docs(db, "users")
```

### Collections & Indexes

```julia
create_collection(db, "orders")
list_collections(db)
drop_collection(db, "orders")

create_index(db, "users", "name")
create_unique_index(db, "users", "email")
create_composite_index(db, "users", ["name", "age"])
create_text_index(db, "users", ["name", "bio"])

indexes = list_indexes(db, "users")
drop_index(db, "users", "name")
```

### Document Full-Text Search

```julia
# Create a text index on fields you want to search
create_text_index(db, "articles", ["title", "body"])

# Search returns matching documents with _score field, sorted by relevance
results = text_search(db, "articles", "rust programming"; limit=10)
for doc in results
    println("$(doc["title"]) (score: $(doc["_score"]))")
end
```

### Aggregation

```julia
results = aggregate(db, "orders", [
    Dict("\$match" => Dict("status" => "completed")),
    Dict("\$group" => Dict("_id" => "\$category", "total" => Dict("\$sum" => "\$amount"))),
    Dict("\$sort" => Dict("total" => -1)),
    Dict("\$limit" => 10)
])
```

**Stages:** `$match`, `$group`, `$sort`, `$skip`, `$limit`, `$project`, `$count`, `$unwind`, `$addFields`, `$lookup`

**Accumulators:** `$sum`, `$avg`, `$min`, `$max`, `$count`, `$first`, `$last`, `$push`

### Transactions

```julia
# Auto-commit on success, auto-rollback on exception
transaction(db) do
    insert(db, "ledger", Dict("action" => "debit",  "amount" => 100))
    insert(db, "ledger", Dict("action" => "credit", "amount" => 100))
end

# Manual control
begin_tx(db)
insert(db, "ledger", Dict("action" => "refund", "amount" => 50))
commit_tx(db)   # or rollback_tx(db)
```

### Blob Storage

```julia
create_bucket(db, "files")
list_buckets(db)

put_object(db, "files", "hello.txt", Vector{UInt8}("Hello!");
           content_type="text/plain", metadata=Dict("author" => "julia"))
data, meta = get_object(db, "files", "hello.txt")
head = head_object(db, "files", "hello.txt")
objs = list_objects(db, "files"; prefix="hello", limit=10)

delete_object(db, "files", "hello.txt")
delete_bucket(db, "files")
```

### Full-Text Search

```julia
results = search(db, "hello world"; bucket="files", limit=10)
# Returns: [Dict("bucket" => "files", "key" => "doc.txt", "score" => 2.45), ...]
```

### Compaction

```julia
stats = compact(db, "users")
# Returns: Dict("old_size" => 4096, "new_size" => 2048, "docs_kept" => 10)
```

## Error Handling

```julia
try
    insert(db, "users", Dict("email" => "duplicate@test.com"))
catch e
    if e isa OxiDbError
        println("Database error: ", e.msg)
    elseif e isa TransactionConflictError
        println("OCC conflict: ", e.msg)
    end
end
```

## Running Tests

```bash
# Start the server (for TCP client tests)
./oxidb-server

cd julia/OxiDb
julia --project=. test/runtests.jl
```

## License

See [LICENSE](../../LICENSE) for details.
