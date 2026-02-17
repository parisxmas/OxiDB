# OxiDB Go Client

Go client for [OxiDB](https://github.com/parisxmas/OxiDB) document database.

Zero dependencies — uses only the Go standard library. Communicates with `oxidb-server` over TCP using the length-prefixed JSON protocol.

## Requirements

- Go 1.21+
- A running `oxidb-server` instance (see [main README](../../README.md#installation))

## Installation

```bash
go get github.com/parisxmas/OxiDB/go/oxidb
```

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/parisxmas/OxiDB/go/oxidb"
)

func main() {
    client, _ := oxidb.ConnectDefault() // 127.0.0.1:4444
    defer client.Close()

    client.Insert("users", map[string]any{"name": "Alice", "age": 30})
    docs, _ := client.Find("users", map[string]any{"name": "Alice"}, nil)
    fmt.Println(docs)
    // [map[_id:1 _version:1 name:Alice age:30]]
}
```

## API Reference

### Connection

```go
// With defaults (127.0.0.1:4444, 5s timeout)
client, err := oxidb.ConnectDefault()

// With custom settings
client, err := oxidb.Connect("10.0.0.1", 4444, 10*time.Second)

client.Close()
```

### CRUD

| Method | Description |
|--------|-------------|
| `Insert(collection, doc)` | Insert a document, returns `map[string]any` |
| `InsertMany(collection, docs)` | Insert multiple documents |
| `Find(collection, query, opts)` | Find matching documents |
| `FindOne(collection, query)` | Find first matching document or `nil` |
| `Update(collection, query, update)` | Update matching documents |
| `Delete(collection, query)` | Delete matching documents |
| `Count(collection, query)` | Count matching documents |

```go
// Insert
client.Insert("users", map[string]any{"name": "Alice", "age": 30})
client.InsertMany("users", []map[string]any{
    {"name": "Bob", "age": 25},
    {"name": "Charlie", "age": 35},
})

// Find with options
docs, _ := client.Find("users", map[string]any{"name": "Alice"}, nil)
limit := 10
docs, _ = client.Find("users", map[string]any{}, &oxidb.FindOptions{
    Sort: map[string]any{"age": 1}, Limit: &limit,
})
doc, _ := client.FindOne("users", map[string]any{"name": "Alice"})

// Update
client.Update("users", map[string]any{"name": "Alice"},
    map[string]any{"$set": map[string]any{"age": 31}})

// Delete
client.Delete("users", map[string]any{"name": "Charlie"})

// Count
n, _ := client.Count("users", map[string]any{})
```

### Collections & Indexes

```go
client.CreateCollection("orders")
cols, _ := client.ListCollections()
client.DropCollection("orders")

client.CreateIndex("users", "name")
client.CreateUniqueIndex("users", "email")
client.CreateCompositeIndex("users", []string{"name", "age"})
```

### Aggregation

```go
results, _ := client.Aggregate("orders", []map[string]any{
    {"$match": map[string]any{"status": "completed"}},
    {"$group": map[string]any{"_id": "$category", "total": map[string]any{"$sum": "$amount"}}},
    {"$sort": map[string]any{"total": -1}},
    {"$limit": 10},
})
```

**Supported stages:** `$match`, `$group`, `$sort`, `$skip`, `$limit`, `$project`, `$count`, `$unwind`, `$addFields`, `$lookup`

### Transactions

```go
// Auto-commit on success, auto-rollback on error
client.WithTransaction(func() error {
    client.Insert("ledger", map[string]any{"action": "debit", "amount": 100})
    client.Insert("ledger", map[string]any{"action": "credit", "amount": 100})
    return nil
})

// Manual control
client.BeginTx()
client.Insert("ledger", map[string]any{"action": "refund", "amount": 50})
client.CommitTx()   // or client.RollbackTx()
```

### Blob Storage

```go
// Buckets
client.CreateBucket("files")
client.ListBuckets()
client.DeleteBucket("files")

// Objects
client.PutObject("files", "hello.txt", []byte("Hello!"), "text/plain",
    map[string]string{"author": "go"})
data, meta, _ := client.GetObject("files", "hello.txt")
head, _ := client.HeadObject("files", "hello.txt")
prefix := "hello"
limit := 10
objs, _ := client.ListObjects("files", &prefix, &limit)
client.DeleteObject("files", "hello.txt")
```

### Full-Text Search

```go
results, _ := client.Search("hello world", nil, 10)

// Filter by bucket
bucket := "files"
results, _ = client.Search("hello world", &bucket, 10)
```

### Compaction

```go
stats, _ := client.Compact("users")
// stats["old_size"], stats["new_size"], stats["docs_kept"]
```

## Error Handling

```go
import "errors"

_, err := client.Insert("users", doc)
if err != nil {
    var conflict *oxidb.TransactionConflictError
    if errors.As(err, &conflict) {
        fmt.Println("OCC conflict:", conflict.Msg)
    }
    var dbErr *oxidb.Error
    if errors.As(err, &dbErr) {
        fmt.Println("Database error:", dbErr.Msg)
    }
}
```

## Running Tests

```bash
# Start the server
./oxidb-server

# Run tests
cd go/oxidb
go test -v -count=1 ./...
```

## License

See [LICENSE](../../LICENSE) for details.
