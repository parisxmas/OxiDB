# Client Libraries

OxiDB provides official client libraries for Python, Go, Java (Spring Boot), Julia, .NET, and Swift. All clients communicate with the server using the length-prefixed JSON [protocol](protocol-reference.md) over TCP.

## Python

### Installation

Copy the single-file client into your project:

```bash
cp python/oxidb.py your_project/
```

Requirements: Python 3.7+. No external dependencies.

### Connection

```python
from oxidb import OxiDbClient

# Default: 127.0.0.1:4444
client = OxiDbClient()

# Custom host/port
client = OxiDbClient(host="10.0.0.1", port=5555, timeout=10.0)

# Context manager (auto-closes)
with OxiDbClient() as client:
    client.ping()
```

### Error Handling

```python
from oxidb import OxiDbError, TransactionConflictError

try:
    client.insert("users", {"name": "Alice"})
except TransactionConflictError:
    # OCC conflict during transaction commit
    pass
except OxiDbError as e:
    # General database error
    print(f"Error: {e}")
```

### Transaction Helper

```python
with client.transaction():
    client.insert("ledger", {"action": "debit", "amount": 100})
    client.insert("ledger", {"action": "credit", "amount": 100})
# Auto-committed on success, auto-rolled back on exception
```

### Blob Handling

The Python client automatically base64-encodes data on `put_object` and base64-decodes on `get_object`:

```python
# put_object accepts bytes
client.put_object("images", "photo.jpg", open("photo.jpg", "rb").read(),
                  content_type="image/jpeg")

# get_object returns (bytes, metadata_dict)
data, meta = client.get_object("images", "photo.jpg")
```

### API Methods

| Method | Description |
|--------|-------------|
| `ping()` | Health check |
| `create_collection(name)` | Create collection |
| `list_collections()` | List collections |
| `drop_collection(name)` | Drop collection |
| `insert(collection, doc)` | Insert document |
| `insert_many(collection, docs)` | Insert multiple documents |
| `find(collection, query, *, sort, skip, limit)` | Find documents |
| `find_one(collection, query)` | Find single document |
| `update(collection, query, update)` | Update matching documents |
| `update_one(collection, query, update)` | Update first match |
| `delete(collection, query)` | Delete matching documents |
| `delete_one(collection, query)` | Delete first match |
| `count(collection, query)` | Count documents |
| `create_index(collection, field)` | Create field index |
| `create_unique_index(collection, field)` | Create unique index |
| `create_composite_index(collection, fields)` | Create composite index |
| `create_text_index(collection, fields)` | Create text index |
| `list_indexes(collection)` | List indexes |
| `drop_index(collection, index)` | Drop index |
| `text_search(collection, query, limit=10)` | Full-text search on collection |
| `create_vector_index(collection, field, dimension, metric="cosine")` | Create vector index |
| `vector_search(collection, field, vector, limit=10)` | Vector similarity search |
| `aggregate(collection, pipeline)` | Run aggregation pipeline |
| `compact(collection)` | Compact collection storage |
| `begin_tx()` | Begin transaction |
| `commit_tx()` | Commit transaction |
| `rollback_tx()` | Rollback transaction |
| `transaction()` | Transaction context manager |
| `create_bucket(bucket)` | Create blob bucket |
| `list_buckets()` | List buckets |
| `delete_bucket(bucket)` | Delete bucket |
| `put_object(bucket, key, data, content_type, metadata)` | Store object |
| `get_object(bucket, key)` | Retrieve object |
| `head_object(bucket, key)` | Get object metadata |
| `delete_object(bucket, key)` | Delete object |
| `list_objects(bucket, prefix, limit)` | List objects |
| `search(query, bucket, limit)` | Search blobs (FTS) |
| `sql(query)` | Execute SQL |
| `create_schedule(name, procedure, cron, every, params, enabled)` | Create schedule |
| `list_schedules()` | List schedules |
| `get_schedule(name)` | Get schedule |
| `delete_schedule(name)` | Delete schedule |
| `enable_schedule(name)` | Enable schedule |
| `disable_schedule(name)` | Disable schedule |

---

## Go

### Installation

```bash
go get github.com/nicklasxyz/OxiDB/go/oxidb
```

Requirements: Go 1.21+. No external dependencies.

### Connection

```go
import "github.com/nicklasxyz/OxiDB/go/oxidb"

// Default: 127.0.0.1:4444, 5s timeout
client, err := oxidb.ConnectDefault()
if err != nil {
    panic(err)
}
defer client.Close()

// Custom
client, err := oxidb.Connect("10.0.0.1", 5555, 10*time.Second)
```

### Error Handling

```go
import "errors"

var conflict *oxidb.TransactionConflictError
var dbErr *oxidb.Error

if errors.As(err, &conflict) {
    // OCC conflict
} else if errors.As(err, &dbErr) {
    // General error
}
```

### Transaction Helper

```go
err := client.WithTransaction(func() error {
    client.Insert("ledger", map[string]any{"action": "debit", "amount": 100})
    client.Insert("ledger", map[string]any{"action": "credit", "amount": 100})
    return nil
})
```

### Blob Handling

The Go client automatically base64-encodes on `PutObject` and base64-decodes on `GetObject`:

```go
data, _ := os.ReadFile("photo.jpg")
client.PutObject("images", "photo.jpg", data, "image/jpeg", nil)

content, meta, _ := client.GetObject("images", "photo.jpg")
os.WriteFile("out.jpg", content, 0644)
```

### Find Options

```go
opts := &oxidb.FindOptions{
    Sort:  map[string]any{"name": 1},
    Skip:  0,
    Limit: 10,
}
results, _ := client.Find("users", map[string]any{}, opts)
```

### API Methods

| Method | Description |
|--------|-------------|
| `Ping()` | Health check |
| `CreateCollection(name)` | Create collection |
| `ListCollections()` | List collections |
| `DropCollection(name)` | Drop collection |
| `Insert(collection, doc)` | Insert document |
| `InsertMany(collection, docs)` | Insert multiple documents |
| `Find(collection, query, opts)` | Find documents |
| `FindOne(collection, query)` | Find single document |
| `Update(collection, query, update)` | Update matching documents |
| `UpdateOne(collection, query, update)` | Update first match |
| `Delete(collection, query)` | Delete matching documents |
| `DeleteOne(collection, query)` | Delete first match |
| `Count(collection, query)` | Count documents |
| `CreateIndex(collection, field)` | Create field index |
| `CreateUniqueIndex(collection, field)` | Create unique index |
| `CreateCompositeIndex(collection, fields)` | Create composite index |
| `CreateTextIndex(collection, fields)` | Create text index |
| `ListIndexes(collection)` | List indexes |
| `DropIndex(collection, index)` | Drop index |
| `TextSearch(collection, query, limit)` | Full-text search |
| `CreateVectorIndex(collection, field, dimension, metric)` | Create vector index |
| `VectorSearch(collection, field, vector, limit)` | Vector similarity search |
| `Aggregate(collection, pipeline)` | Run aggregation |
| `Compact(collection)` | Compact storage |
| `BeginTx()` | Begin transaction |
| `CommitTx()` | Commit transaction |
| `RollbackTx()` | Rollback transaction |
| `WithTransaction(fn)` | Transaction callback |
| `CreateBucket(bucket)` | Create bucket |
| `ListBuckets()` | List buckets |
| `DeleteBucket(bucket)` | Delete bucket |
| `PutObject(bucket, key, data, contentType, metadata)` | Store object |
| `GetObject(bucket, key)` | Retrieve object |
| `HeadObject(bucket, key)` | Get metadata |
| `DeleteObject(bucket, key)` | Delete object |
| `ListObjects(bucket, prefix, limit)` | List objects |
| `Search(query, bucket, limit)` | Search blobs |
| `SQL(query)` | Execute SQL |
| `CreateSchedule(name, procedure, opts)` | Create schedule |
| `ListSchedules()` | List schedules |
| `GetSchedule(name)` | Get schedule |
| `DeleteSchedule(name)` | Delete schedule |
| `EnableSchedule(name)` | Enable schedule |
| `DisableSchedule(name)` | Disable schedule |

---

## Java / Spring Boot

### Installation

Build and install the starter:

```bash
cd oxidb-spring-boot-starter
mvn clean install
```

Add to your `pom.xml`:

```xml
<dependency>
    <groupId>com.oxidb</groupId>
    <artifactId>oxidb-spring-boot-starter</artifactId>
    <version>0.2.0</version>
</dependency>
```

Requirements: Java 17+, Spring Boot 3.x.

### Configuration

In `application.properties`:

```properties
oxidb.host=127.0.0.1
oxidb.port=4444
oxidb.timeout-ms=5000
```

### Connection

```java
// Auto-configured bean (recommended)
@Autowired
private OxiDbClient db;

// Manual connection
OxiDbClient db = new OxiDbClient("127.0.0.1", 4444, 5000);
db.close();  // AutoCloseable
```

### Error Handling

```java
import com.oxidb.spring.OxiDbException;
import com.oxidb.spring.TransactionConflictException;

try {
    db.commitTx();
} catch (TransactionConflictException e) {
    // OCC conflict
} catch (OxiDbException e) {
    // General error
}
```

### Transaction Helper

```java
db.withTransaction(() -> {
    db.insert("ledger", Map.of("action", "debit", "amount", 100));
    db.insert("ledger", Map.of("action", "credit", "amount", 100));
});
```

### Blob Handling

`PutObject` accepts `byte[]` and auto-encodes to base64. Use `decodeObjectContent()` to decode `GetObject` results:

```java
byte[] data = Files.readAllBytes(Path.of("photo.jpg"));
db.putObject("images", "photo.jpg", data, "image/jpeg", null);

JsonNode obj = db.getObject("images", "photo.jpg");
byte[] content = db.decodeObjectContent(obj);
```

### API Methods

All methods return `JsonNode` (Jackson) unless noted. Methods accept both `Map<String, Object>` and raw JSON strings.

| Method | Description |
|--------|-------------|
| `ping()` | Health check |
| `createCollection(name)` | Create collection |
| `listCollections()` | List collections |
| `dropCollection(name)` | Drop collection |
| `insert(collection, doc)` | Insert document |
| `insertMany(collection, docs)` | Insert multiple documents |
| `find(collection, query)` | Find documents |
| `find(collection, query, sort, skip, limit)` | Find with options |
| `findOne(collection, query)` | Find single document |
| `update(collection, query, update)` | Update matching documents |
| `updateOne(collection, query, update)` | Update first match |
| `delete(collection, query)` | Delete matching documents |
| `deleteOne(collection, query)` | Delete first match |
| `count(collection, query)` | Count documents (returns `int`) |
| `createIndex(collection, field)` | Create field index |
| `createUniqueIndex(collection, field)` | Create unique index |
| `createCompositeIndex(collection, fields)` | Create composite index |
| `createTextIndex(collection, fields)` | Create text index |
| `listIndexes(collection)` | List indexes |
| `dropIndex(collection, index)` | Drop index |
| `textSearch(collection, query, limit)` | Full-text search |
| `createVectorIndex(collection, field, dimension, metric)` | Create vector index |
| `vectorSearch(collection, field, vector, limit)` | Vector similarity search |
| `aggregate(collection, pipeline)` | Run aggregation |
| `compact(collection)` | Compact storage |
| `beginTx()` | Begin transaction |
| `commitTx()` | Commit transaction |
| `rollbackTx()` | Rollback transaction |
| `withTransaction(action)` | Transaction callback |
| `createBucket(bucket)` | Create bucket |
| `listBuckets()` | List buckets |
| `deleteBucket(bucket)` | Delete bucket |
| `putObject(bucket, key, data, contentType, metadata)` | Store object |
| `getObject(bucket, key)` | Retrieve object |
| `decodeObjectContent(result)` | Decode base64 content (returns `byte[]`) |
| `headObject(bucket, key)` | Get metadata |
| `deleteObject(bucket, key)` | Delete object |
| `listObjects(bucket, prefix, limit)` | List objects |
| `search(query, bucket, limit)` | Search blobs |
| `sql(query)` | Execute SQL |
| `createSchedule(name, procedure, cron, params, enabled)` | Create cron schedule |
| `createScheduleInterval(name, procedure, every, params, enabled)` | Create interval schedule |
| `listSchedules()` | List schedules |
| `getSchedule(name)` | Get schedule |
| `deleteSchedule(name)` | Delete schedule |
| `enableSchedule(name)` | Enable schedule |
| `disableSchedule(name)` | Disable schedule |

---

## Julia

### Installation

```julia
using Pkg
Pkg.develop(path="julia/OxiDb")
```

For embedded mode (no server required):

```julia
Pkg.develop(path="julia/OxiDbEmbedded")
```

Requirements: Julia 1.6+.

### Connection

```julia
using OxiDb

# TCP client
client = connect_oxidb("127.0.0.1", 4444)
close(client)

# Embedded (no server)
using OxiDbEmbedded
db = open_db("/tmp/mydb")
db = open_db("/tmp/mydb"; encryption_key_path="/path/to/key")  # with encryption
close(db)
```

Both TCP and embedded clients expose the same API.

### Error Handling

```julia
try
    commit_tx(client)
catch e
    if e isa TransactionConflictError
        # OCC conflict
    elseif e isa OxiDbError
        # General error
    end
end
```

### Transaction Helper

```julia
transaction(client) do
    insert(client, "ledger", Dict("action" => "debit", "amount" => 100))
    insert(client, "ledger", Dict("action" => "credit", "amount" => 100))
end
```

### Blob Handling

The Julia client auto-encodes/decodes base64:

```julia
data = read("photo.jpg")
put_object(client, "images", "photo.jpg", data; content_type="image/jpeg")

content, meta = get_object(client, "images", "photo.jpg")
write("out.jpg", content)
```

### API Methods

| Method | Description |
|--------|-------------|
| `ping(client)` | Health check |
| `create_collection(client, name)` | Create collection |
| `list_collections(client)` | List collections |
| `drop_collection(client, name)` | Drop collection |
| `insert(client, collection, doc)` | Insert document |
| `insert_many(client, collection, docs)` | Insert multiple documents |
| `find(client, collection, query; sort, skip, limit)` | Find documents |
| `find_one(client, collection, query)` | Find single document |
| `update(client, collection, query, update_doc)` | Update matching documents |
| `update_one(client, collection, query, update_doc)` | Update first match |
| `delete(client, collection, query)` | Delete matching documents |
| `delete_one(client, collection, query)` | Delete first match |
| `count_docs(client, collection, query)` | Count documents |
| `create_index(client, collection, field)` | Create field index |
| `create_unique_index(client, collection, field)` | Create unique index |
| `create_composite_index(client, collection, fields)` | Create composite index |
| `create_text_index(client, collection, fields)` | Create text index |
| `list_indexes(client, collection)` | List indexes |
| `drop_index(client, collection, index)` | Drop index |
| `text_search(client, collection, query; limit)` | Full-text search |
| `create_vector_index(client, collection, field, dimension; metric)` | Create vector index |
| `vector_search(client, collection, field, vector; limit)` | Vector similarity search |
| `aggregate(client, collection, pipeline)` | Run aggregation |
| `compact(client, collection)` | Compact storage |
| `begin_tx(client)` | Begin transaction |
| `commit_tx(client)` | Commit transaction |
| `rollback_tx(client)` | Rollback transaction |
| `transaction(f, client)` | Transaction callback |
| `create_bucket(client, bucket)` | Create bucket |
| `list_buckets(client)` | List buckets |
| `delete_bucket(client, bucket)` | Delete bucket |
| `put_object(client, bucket, key, data; content_type, metadata)` | Store object |
| `get_object(client, bucket, key)` | Retrieve object |
| `head_object(client, bucket, key)` | Get metadata |
| `delete_object(client, bucket, key)` | Delete object |
| `list_objects(client, bucket; prefix, limit)` | List objects |
| `search(client, query; bucket, limit)` | Search blobs |
| `sql(client, query)` | Execute SQL |
| `create_schedule(client, name, procedure; cron, every, params, enabled)` | Create schedule |
| `list_schedules(client)` | List schedules |
| `get_schedule(client, name)` | Get schedule |
| `delete_schedule(client, name)` | Delete schedule |
| `enable_schedule(client, name)` | Enable schedule |
| `disable_schedule(client, name)` | Disable schedule |

---

## .NET

### Installation

Build the C FFI library:

```bash
cargo build --release -p oxidb-client-ffi
```

This produces `liboxidb_client_ffi.dylib` (macOS), `.so` (Linux), or `.dll` (Windows). Place it where your .NET application can find it.

Requirements: .NET 8+. Uses P/Invoke to the native FFI library.

### Connection

```csharp
using var db = OxiDbClient.Connect(host: "127.0.0.1", port: 4444);
// IDisposable -- auto-closes on dispose
```

### Error Handling

```csharp
try
{
    db.Insert("users", """{"name": "Alice"}""");
}
catch (OxiDbException e)
{
    Console.WriteLine($"Error: {e.Message}");
}
```

### Transactions

The .NET client uses manual transaction management:

```csharp
db.BeginTransaction();
try
{
    db.Insert("ledger", """{"action": "debit", "amount": 100}""");
    db.Insert("ledger", """{"action": "credit", "amount": 100}""");
    db.CommitTransaction();
}
catch
{
    db.RollbackTransaction();
    throw;
}
```

### Filter Builder

Build queries with a fluent API instead of raw JSON:

```csharp
var query = Filter.And(
    Filter.Gte("age", 18),
    Filter.Eq("status", "active")
);
var users = db.Find("users", query);

// Operator overloads
var combined = Filter.Eq("a", 1) & Filter.Gt("b", 2);  // AND
var either = Filter.Eq("a", 1) | Filter.Eq("a", 2);     // OR
```

### UpdateDef Builder

Build updates with a fluent API:

```csharp
var update = UpdateDef.Set("status", "active")
           + UpdateDef.Inc("login_count", 1)
           + UpdateDef.CurrentDate("last_login");
db.Update("users", Filter.Eq("name", "Alice"), update);
```

Available methods: `Set`, `Unset`, `Inc`, `Mul`, `Min`, `Max`, `Rename`, `CurrentDate`, `Push`, `Pull`, `AddToSet`, `PopFirst`, `PopLast`.

### Blob Handling

The .NET client requires manual base64 encoding/decoding:

```csharp
var base64 = Convert.ToBase64String(File.ReadAllBytes("photo.jpg"));
db.PutObject("images", "photo.jpg", base64, "image/jpeg", null);
```

### API Methods

All methods return `JsonDocument`.

| Method | Description |
|--------|-------------|
| `Ping()` | Health check |
| `CreateCollection(collection)` | Create collection |
| `ListCollections()` | List collections |
| `DropCollection(collection)` | Drop collection |
| `Insert(collection, docJson)` | Insert document |
| `InsertMany(collection, docsJson)` | Insert multiple documents |
| `Find(collection, queryJson)` / `Find(collection, Filter)` | Find documents |
| `FindOne(collection, queryJson)` / `FindOne(collection, Filter)` | Find single document |
| `Update(collection, queryJson, updateJson)` / `Update(collection, Filter, UpdateDef)` | Update documents |
| `UpdateOne(collection, queryJson, updateJson)` / `UpdateOne(collection, Filter, UpdateDef)` | Update first match |
| `Delete(collection, queryJson)` / `Delete(collection, Filter)` | Delete documents |
| `DeleteOne(collection, queryJson)` / `DeleteOne(collection, Filter)` | Delete first match |
| `Count(collection)` | Count documents |
| `CreateIndex(collection, field)` | Create field index |
| `CreateUniqueIndex(collection, field)` | Create unique index |
| `CreateCompositeIndex(collection, fieldsJson)` | Create composite index |
| `CreateTextIndex(collection, fieldsJson)` | Create text index |
| `ListIndexes(collection)` | List indexes |
| `DropIndex(collection, index)` | Drop index |
| `TextSearch(collection, query, limit)` | Full-text search |
| `CreateVectorIndex(collection, field, dimension, metric)` | Create vector index |
| `VectorSearch(collection, field, vectorJson, limit)` | Vector similarity search |
| `Aggregate(collection, pipelineJson)` | Run aggregation |
| `Compact(collection)` | Compact storage |
| `BeginTransaction()` | Begin transaction |
| `CommitTransaction()` | Commit transaction |
| `RollbackTransaction()` | Rollback transaction |
| `CreateBucket(bucket)` | Create bucket |
| `ListBuckets()` | List buckets |
| `DeleteBucket(bucket)` | Delete bucket |
| `PutObject(bucket, key, dataB64, contentType, metadataJson)` | Store object |
| `GetObject(bucket, key)` | Retrieve object |
| `HeadObject(bucket, key)` | Get metadata |
| `DeleteObject(bucket, key)` | Delete object |
| `ListObjects(bucket, prefix, limit)` | List objects |
| `Search(query, bucket, limit)` | Search blobs |
| `Sql(query)` | Execute SQL |
| `CreateSchedule(scheduleJson)` | Create schedule |
| `ListSchedules()` | List schedules |
| `GetSchedule(name)` | Get schedule |
| `DeleteSchedule(name)` | Delete schedule |
| `EnableSchedule(name)` | Enable schedule |
| `DisableSchedule(name)` | Disable schedule |

---

## Swift

### Installation

Via Swift Package Manager with the prebuilt XCFramework, or build from source:

```bash
cargo build --release -p oxidb-client-ffi
# or for embedded:
cargo build --release -p oxidb-embedded-ffi
```

Requirements: Swift 5.9+, macOS 13+ / iOS 16+.

### Two Modes

**Client mode** (TCP connection to server):

```swift
let client = try OxiDBClient.connect(host: "127.0.0.1", port: 4444)
// ...
client.disconnect()
```

**Embedded mode** (in-process, no server):

```swift
let db = try OxiDBDatabase.open(path: "/path/to/mydb")
// With encryption:
let db = try OxiDBDatabase.open(path: "/path/to/mydb", encryptionKeyPath: "/path/to/key")
db.close()
```

Both modes expose the same API.

### Error Handling

```swift
do {
    try db.insert(collection: "users", document: ["name": "Alice"])
} catch OxiDBError.transactionConflict(let msg) {
    // OCC conflict
} catch OxiDBError.operationFailed(let msg) {
    // General error
} catch OxiDBError.connectionFailed {
    // Connection error
} catch OxiDBError.databaseOpenFailed {
    // Failed to open embedded database
}
```

### Transaction Helper

```swift
try db.transaction {
    try db.insert(collection: "ledger", document: ["action": "debit", "amount": 100])
    try db.insert(collection: "ledger", document: ["action": "credit", "amount": 100])
}
```

### Mutation Observers (Embedded Mode)

```swift
// Callback-based
let id = db.addMutationObserver { event in
    print(event.operation, event.collection, event.timestamp, event.metadata)
}
db.removeMutationObserver(id)

// AsyncStream
for await event in db.mutationEvents() {
    print(event)
}
```

### Blob Handling

The Swift client requires manual base64 encoding:

```swift
let fileData = try Data(contentsOf: URL(fileURLWithPath: "photo.jpg"))
let base64 = fileData.base64EncodedString()
try db.putObject(bucket: "images", key: "photo.jpg",
                 dataBase64: base64, contentType: "image/jpeg", metadata: nil)
```

### API Methods

All methods return `[String: Any]` or `[[String: Any]]`.

| Method | Description |
|--------|-------------|
| `ping()` | Health check |
| `createCollection(_:)` | Create collection |
| `listCollections()` | List collections |
| `dropCollection(_:)` | Drop collection |
| `insert(collection:document:)` | Insert document |
| `insertMany(collection:documents:)` | Insert multiple documents |
| `find(collection:query:)` | Find documents |
| `findOne(collection:query:)` | Find single document |
| `update(collection:query:update:)` | Update documents |
| `updateOne(collection:query:update:)` | Update first match |
| `delete(collection:query:)` | Delete documents |
| `deleteOne(collection:query:)` | Delete first match |
| `count(collection:)` | Count documents |
| `createIndex(collection:field:)` | Create field index |
| `createUniqueIndex(collection:field:)` | Create unique index |
| `createCompositeIndex(collection:fields:)` | Create composite index |
| `createTextIndex(collection:fields:)` | Create text index |
| `listIndexes(collection:)` | List indexes |
| `dropIndex(collection:index:)` | Drop index |
| `textSearch(collection:query:limit:)` | Full-text search |
| `createVectorIndex(collection:field:dimension:metric:)` | Create vector index |
| `vectorSearch(collection:field:vector:limit:)` | Vector similarity search |
| `aggregate(collection:pipeline:)` | Run aggregation |
| `compact(collection:)` | Compact storage |
| `beginTransaction()` | Begin transaction |
| `commitTransaction()` | Commit transaction |
| `rollbackTransaction()` | Rollback transaction |
| `transaction(_:)` | Transaction callback |
| `createBucket(_:)` | Create bucket |
| `listBuckets()` | List buckets |
| `deleteBucket(_:)` | Delete bucket |
| `putObject(bucket:key:dataBase64:contentType:metadata:)` | Store object |
| `getObject(bucket:key:)` | Retrieve object |
| `headObject(bucket:key:)` | Get metadata |
| `deleteObject(bucket:key:)` | Delete object |
| `listObjects(bucket:prefix:limit:)` | List objects |
| `search(query:bucket:limit:)` | Search blobs |
| `sql(query:)` | Execute SQL |
| `createSchedule(definition:)` | Create schedule |
| `listSchedules()` | List schedules |
| `getSchedule(name:)` | Get schedule |
| `deleteSchedule(name:)` | Delete schedule |
| `enableSchedule(name:)` | Enable schedule |
| `disableSchedule(name:)` | Disable schedule |

## See Also

- [Getting Started](getting-started.md) -- quickstart examples
- [Protocol Reference](protocol-reference.md) -- raw TCP protocol details
