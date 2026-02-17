# OxiDB .NET Client

.NET client for [OxiDB](https://github.com/parisxmas/OxiDB) document database.

Wraps the C FFI library (`oxidb-client-ffi`) via P/Invoke. Communicates with `oxidb-server` over the native Rust FFI layer.

## Requirements

- .NET 8+
- A running `oxidb-server` instance (see [main README](../../README.md#installation))
- The `oxidb-client-ffi` shared library built for your platform

## Installation

1. Build the FFI shared library:

```bash
cargo build --release -p oxidb-client-ffi
# Produces: target/release/liboxidb_client_ffi.dylib (macOS)
#           target/release/liboxidb_client_ffi.so    (Linux)
#           target/release/oxidb_client_ffi.dll      (Windows)
```

2. Reference the `OxiDb.Client` project and ensure the shared library is in your library path.

## Quick Start

```csharp
using OxiDb.Client;

using var db = OxiDbClient.Connect("127.0.0.1", 4444);

db.Insert("users", "{\"name\":\"Alice\",\"age\":30}");
var docs = db.Find("users", "{\"name\":\"Alice\"}");
Console.WriteLine(docs);
```

## API Reference

### Connection

```csharp
using var db = OxiDbClient.Connect(host: "127.0.0.1", port: 4444);
// Automatically disposed at end of scope
```

### CRUD

| Method | Description |
|--------|-------------|
| `Insert(collection, docJson)` | Insert a document |
| `InsertMany(collection, docsJson)` | Insert multiple documents |
| `Find(collection, queryJson)` / `Find(collection, Filter)` | Find documents |
| `FindOne(collection, queryJson)` / `FindOne(collection, Filter)` | Find first document |
| `Update(collection, queryJson, updateJson)` / `Update(collection, Filter, UpdateDef)` | Update documents |
| `Delete(collection, queryJson)` / `Delete(collection, Filter)` | Delete documents |
| `Count(collection)` | Count documents |

```csharp
// Insert
db.Insert("users", "{\"name\":\"Alice\",\"age\":30}");
db.InsertMany("users", "[{\"name\":\"Bob\"},{\"name\":\"Charlie\"}]");

// Find with Filter builder
var docs = db.Find("users", Filter.Gte("age", 18));
var doc = db.FindOne("users", Filter.Eq("name", "Alice"));

// Update with UpdateDef builder
db.Update("users", Filter.Eq("name", "Alice"), UpdateDef.Set("age", 31));

// Delete
db.Delete("users", Filter.Eq("name", "Charlie"));

// Count
var count = db.Count("users");
```

### Collections & Indexes

```csharp
db.ListCollections();
db.DropCollection("orders");

db.CreateIndex("users", "name");
db.CreateCompositeIndex("users", "[\"name\", \"age\"]");
```

### Aggregation

```csharp
var results = db.Aggregate("orders", """
    [
        {"$match": {"status": "completed"}},
        {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
        {"$sort": {"total": -1}}
    ]
""");
```

### Transactions

```csharp
db.BeginTransaction();
db.Insert("ledger", "{\"action\":\"debit\",\"amount\":100}");
db.Insert("ledger", "{\"action\":\"credit\",\"amount\":100}");
db.CommitTransaction();   // or db.RollbackTransaction()
```

### Blob Storage

```csharp
db.CreateBucket("files");
db.ListBuckets();
db.DeleteBucket("files");

db.PutObject("files", "hello.txt", Convert.ToBase64String(data), "text/plain");
var obj = db.GetObject("files", "hello.txt");
var head = db.HeadObject("files", "hello.txt");
var objs = db.ListObjects("files", prefix: "hello", limit: 10);
db.DeleteObject("files", "hello.txt");
```

### Full-Text Search

```csharp
var results = db.Search("hello world", bucket: "files", limit: 10);
```

## Error Handling

```csharp
try
{
    db.Insert("users", json);
}
catch (OxiDbException e)
{
    Console.WriteLine($"Database error: {e.Message}");
}
```

## License

See [LICENSE](../../LICENSE) for details.
