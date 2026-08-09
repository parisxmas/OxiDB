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
client, err := oxidb.Connect("192.0.2.1", 4444, 10*time.Second)

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

### Stored Procedures

```go
// Create from JSON definition
client.CreateProcedure("greet", map[string]any{
    "body": "return 'Hello, ' + params.name;",
})

// Create from OxiScript
client.CreateProcedureFromScript("proc greet(name) { return 'Hello, ' + name; }")

// Call a procedure
result, _ := client.CallProcedure("greet", map[string]any{"name": "Alice"})

// List, get, delete
names, _ := client.ListProcedures()
def, _ := client.GetProcedure("greet")
client.DeleteProcedure("greet")

// Compile OxiScript without creating
compiled, _ := client.CompileOxiScript("proc test() { return 1; }")
```

### TTL Indexes

```go
// Auto-expire documents 3600 seconds after the "created_at" field value
client.CreateTTLIndex("sessions", "created_at", 3600)
```

### Retention Policies

```go
// Keep logs for 30 days (auto-deletes older documents)
client.SetRetention("_gelf_logs", 30)

policy, _ := client.GetRetention("_gelf_logs")
all, _ := client.ListRetentions()
client.DeleteRetention("_gelf_logs")
```

### Alerting

```go
// Create an alert that fires when error count exceeds threshold
client.CreateAlert("high-errors", "logs",
    map[string]any{
        "type": "count_threshold", "query": map[string]any{"level": map[string]any{"$lte": 3}},
        "window": "5m", "threshold": 100, "operator": "gte",
    },
    []map[string]any{
        {"type": "webhook", "url": "https://hooks.example.com/alert"},
        {"type": "stderr"},
    },
    300, // cooldown seconds
)

alert, _ := client.GetAlert("high-errors")
alerts, _ := client.ListAlerts()
result, _ := client.TestAlert("high-errors") // dry-run
history, _ := client.ListAlertHistory()
client.DeleteAlert("high-errors")
```

### Text Extraction

```go
// Extract text from a blob (PDF, DOCX, HTML, etc.)
text, _ := client.ExtractText("files", "report.pdf")
```

### Backup & Restore

```go
info, _ := client.Backup("/tmp/oxidb-backup")
// info["path"], info["size_bytes"], info["collections"]

info, _ = client.Restore("/tmp/oxidb-backup", "/tmp/oxidb-restored")
```

### SQL Dialect

```go
// Set SQL dialect for the session
client.SetDialect("postgresql") // mysql, postgresql, mssql, generic
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
