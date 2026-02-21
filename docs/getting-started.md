# Getting Started

OxiDB is a fast, embeddable document database engine written in Rust. It supports both SQL and MongoDB-style queries, and can run as an embedded library or a standalone TCP server.

## Installation

### Pre-built Binaries

Download the latest binary for your platform from the [GitHub Releases](https://github.com/nicklasxyz/OxiDB/releases) page.

### Build from Source

Requirements: Rust 1.70+

```bash
# Clone the repository
git clone https://github.com/nicklasxyz/OxiDB.git
cd OxiDB

# Build the server
cargo build --release -p oxidb-server

# The binary is at target/release/oxidb-server
```

To build with OCR support for full-text search on images:

```bash
cargo build --workspace --features ocr
```

### Docker

```bash
docker compose up -d
```

## Starting the Server

Run the server with default settings:

```bash
./oxidb-server
```

The server listens on `127.0.0.1:4444` by default and stores data in `./oxidb_data`.

### Configuration via Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OXIDB_ADDR` | `127.0.0.1:4444` | TCP listen address |
| `OXIDB_DATA` | `./oxidb_data` | Data directory |
| `OXIDB_POOL_SIZE` | `4` | Worker thread count |
| `OXIDB_IDLE_TIMEOUT` | `30` | Connection idle timeout in seconds (0 = no timeout) |

Example with custom settings:

```bash
OXIDB_ADDR=0.0.0.0:5555 OXIDB_DATA=/var/lib/oxidb OXIDB_POOL_SIZE=8 ./oxidb-server
```

For security, TLS, authentication, and clustering options, see the [Server Configuration](server.md) guide.

## Protocol Overview

OxiDB uses a length-prefixed JSON protocol over TCP. Each message is:

```
[4 bytes: payload length as u32 little-endian][JSON payload]
```

Maximum message size is 16 MiB.

Responses have the format:

```json
{"ok": true, "data": ...}
```

or on error:

```json
{"ok": false, "error": "error message"}
```

## First Operations

### Insert a Document

```json
{"command": "insert", "collection": "users", "doc": {"name": "Alice", "age": 30}}
```

Response:

```json
{"ok": true, "data": {"id": 1}}
```

Collections are auto-created on first insert -- no need to create them explicitly.

### Find Documents

```json
{"command": "find", "collection": "users", "query": {"name": "Alice"}}
```

Response:

```json
{"ok": true, "data": [{"_id": 1, "name": "Alice", "age": 30}]}
```

### Update a Document

```json
{"command": "update", "collection": "users", "query": {"name": "Alice"}, "update": {"$set": {"age": 31}}}
```

### Delete a Document

```json
{"command": "delete", "collection": "users", "query": {"name": "Alice"}}
```

## Client Library Quickstart

### Python

```python
from oxidb import OxiDbClient

with OxiDbClient(host="127.0.0.1", port=4444) as client:
    client.insert("users", {"name": "Alice", "age": 30})
    users = client.find("users", {"name": "Alice"})
    print(users)
```

### Go

```go
package main

import (
    "fmt"
    "github.com/nicklasxyz/OxiDB/go/oxidb"
)

func main() {
    client, err := oxidb.ConnectDefault()
    if err != nil {
        panic(err)
    }
    defer client.Close()

    client.Insert("users", map[string]any{"name": "Alice", "age": 30})
    users, _ := client.Find("users", map[string]any{"name": "Alice"}, nil)
    fmt.Println(users)
}
```

### Java (Spring Boot)

```java
@Autowired
private OxiDbClient db;

public void example() {
    db.insert("users", Map.of("name", "Alice", "age", 30));
    JsonNode users = db.find("users", Map.of("name", "Alice"));
    System.out.println(users);
}
```

### Julia

```julia
using OxiDb

client = connect_oxidb("127.0.0.1", 4444)
insert(client, "users", Dict("name" => "Alice", "age" => 30))
users = find(client, "users", Dict("name" => "Alice"))
println(users)
close(client)
```

### .NET

```csharp
using var db = OxiDbClient.Connect(host: "127.0.0.1", port: 4444);

db.Insert("users", """{"name": "Alice", "age": 30}""");
var users = db.Find("users", """{"name": "Alice"}""");
Console.WriteLine(users);
```

### Swift

```swift
let db = try OxiDBClient.connect(host: "127.0.0.1", port: 4444)

try db.insert(collection: "users", document: ["name": "Alice", "age": 30])
let users = try db.find(collection: "users", query: ["name": "Alice"])
print(users)

db.disconnect()
```

## Next Steps

- [Querying Documents](queries.md) -- query operators, sorting, pagination
- [Updating Documents](updates.md) -- field and array update operators
- [Indexes](indexes.md) -- improve query performance
- [Transactions](transactions.md) -- multi-document ACID transactions
- [Aggregation](aggregation.md) -- data analytics pipelines
- [SQL](sql.md) -- SQL query language support
- [Client Libraries](client-libraries.md) -- detailed per-language reference
