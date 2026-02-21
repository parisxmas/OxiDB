# Indexes

Indexes improve query performance by enabling direct lookups instead of full collection scans. OxiDB supports field indexes, unique indexes, composite indexes, and text indexes, all backed by in-memory BTreeMap structures with persistent cache files.

## Field Index

A field index speeds up queries on a single field. It supports equality, range, and `$in` operators.

```json
{"command": "create_index", "collection": "users", "field": "email"}
```

After creating the index, queries like `{"email": "alice@example.com"}` use a direct BTreeMap lookup instead of scanning every document.

### Index-optimized operations

- **Equality** (`$eq`): Direct key lookup
- **Range** (`$gt`, `$gte`, `$lt`, `$lte`): BTreeMap range scan
- **`$in`**: Multiple key lookups
- **Sort**: BTreeMap iteration is O(limit) instead of O(n log n)
- **Count**: Returns set size without loading documents

## Unique Index

A unique index enforces a uniqueness constraint on a field. Inserts or updates that would create a duplicate value are rejected.

```json
{"command": "create_unique_index", "collection": "users", "field": "email"}
```

Attempting to insert a document with a duplicate value returns an error:

```json
{"ok": false, "error": "unique index violation on field 'email'"}
```

## Composite Index

A composite index covers multiple fields and supports prefix scans. The field order matters -- queries that match a prefix of the indexed fields can use the index.

```json
{"command": "create_composite_index", "collection": "orders", "fields": ["customer_id", "status", "date"]}
```

Response:

```json
{"ok": true, "data": {"index": "customer_id_status_date"}}
```

This index accelerates queries on:
- `{"customer_id": "c1"}` (first field only)
- `{"customer_id": "c1", "status": "shipped"}` (first two fields)
- `{"customer_id": "c1", "status": "shipped", "date": "2025-01-01"}` (all fields)

But not on `{"status": "shipped"}` alone (skips the prefix).

## Text Index

A text index enables full-text search on string fields. Multiple fields can be included.

```json
{"command": "create_text_index", "collection": "articles", "fields": ["title", "body"]}
```

Query with text search:

```json
{"command": "text_search", "collection": "articles", "query": "rust database", "limit": 10}
```

Results are ranked by TF-IDF score and include a `_score` field:

```json
{"ok": true, "data": [{"_id": 3, "title": "Building with Rust", "_score": 0.85}, ...]}
```

## Listing Indexes

```json
{"command": "list_indexes", "collection": "users"}
```

## Dropping Indexes

```json
{"command": "drop_index", "collection": "users", "index": "email"}
```

For composite indexes, use the combined name returned at creation time (e.g., `"customer_id_status_date"`).

## Value Ordering

OxiDB enforces a cross-type ordering for index values:

```
Null < Bool < Number < DateTime < String
```

Within each type, values are ordered naturally (numeric order, lexicographic for strings, chronological for dates).

## Automatic Date Detection

String values matching ISO 8601, RFC 3339, or `YYYY-MM-DD` format are automatically stored as epoch milliseconds in the index. This enables correct chronological ordering and range queries on date fields.

For example, inserting `"2025-03-15T10:30:00Z"` stores the value as a DateTime index entry, so range queries like `{"date": {"$gte": "2025-01-01", "$lt": "2026-01-01"}}` work correctly.

## Vector Index

A vector index enables k-nearest-neighbor (KNN) similarity search on embedding fields. See [Vector Search](vector-search.md) for a full guide.

```json
{"command": "create_vector_index", "collection": "articles", "field": "embedding", "dimension": 384, "metric": "cosine"}
```

Supported distance metrics: `cosine` (default), `euclidean`, `dot_product`.

Query with vector search:

```json
{"command": "vector_search", "collection": "articles", "field": "embedding", "vector": [0.1, 0.2, ...], "limit": 10}
```

Results include `_similarity` (0-1, higher is better) and `_distance` fields:

```json
{"ok": true, "data": [{"_id": 3, "title": "...", "_similarity": 0.95, "_distance": 0.05}, ...]}
```

For collections under 1000 vectors, exact (flat) search is used. For larger collections, an HNSW (Hierarchical Navigable Small World) graph provides fast approximate search.

## Persistent Index Cache

Indexes are persisted as binary files (`.fidx` for field indexes, `.cidx` for composite indexes, `.vidx` for vector indexes) and reloaded on startup, avoiding full rebuild from the document store.

## Client Examples

### Python

```python
# Field index
client.create_index("users", "email")

# Unique index
client.create_unique_index("users", "username")

# Composite index
client.create_composite_index("orders", ["customer_id", "status"])

# Text index
client.create_text_index("articles", ["title", "body"])

# Text search
results = client.text_search("articles", "rust database", limit=5)

# List and drop
indexes = client.list_indexes("users")
client.drop_index("users", "email")
```

### Go

```go
// Field index
client.CreateIndex("users", "email")

// Unique index
client.CreateUniqueIndex("users", "username")

// Composite index
client.CreateCompositeIndex("orders", []string{"customer_id", "status"})

// Text index
client.CreateTextIndex("articles", []string{"title", "body"})

// Text search
results, _ := client.TextSearch("articles", "rust database", 5)

// List and drop
indexes, _ := client.ListIndexes("users")
client.DropIndex("users", "email")
```

### Java

```java
// Field index
db.createIndex("users", "email");

// Unique index
db.createUniqueIndex("users", "username");

// Composite index
db.createCompositeIndex("orders", List.of("customer_id", "status"));

// Text index
db.createTextIndex("articles", List.of("title", "body"));

// Text search
JsonNode results = db.textSearch("articles", "rust database", 5);

// List and drop
JsonNode indexes = db.listIndexes("users");
db.dropIndex("users", "email");
```

### Julia

```julia
# Field index
create_index(client, "users", "email")

# Unique index
create_unique_index(client, "users", "username")

# Composite index
create_composite_index(client, "orders", ["customer_id", "status"])

# Text index
create_text_index(client, "articles", ["title", "body"])

# Text search
results = text_search(client, "articles", "rust database"; limit=5)

# List and drop
indexes = list_indexes(client, "users")
drop_index(client, "users", "email")
```

### .NET

```csharp
// Field index
db.CreateIndex("users", "email");

// Unique index
db.CreateUniqueIndex("users", "username");

// Composite index
db.CreateCompositeIndex("orders", """["customer_id", "status"]""");

// Text index
db.CreateTextIndex("articles", """["title", "body"]""");

// Text search
var results = db.TextSearch("articles", "rust database", 5);

// List and drop
var indexes = db.ListIndexes("users");
db.DropIndex("users", "email");
```

### Swift

```swift
// Field index
try db.createIndex(collection: "users", field: "email")

// Unique index
try db.createUniqueIndex(collection: "users", field: "username")

// Composite index
try db.createCompositeIndex(collection: "orders", fields: ["customer_id", "status"])

// Text index
try db.createTextIndex(collection: "articles", fields: ["title", "body"])

// Text search
let results = try db.textSearch(collection: "articles", query: "rust database", limit: 5)

// List and drop
let indexes = try db.listIndexes(collection: "users")
try db.dropIndex(collection: "users", index: "email")
```

## See Also

- [Vector Search](vector-search.md) -- full guide to vector similarity search
- [Querying Documents](queries.md) -- operators that benefit from indexes
- [Aggregation](aggregation.md) -- `$match` stage uses indexes
- [SQL](sql.md) -- `CREATE INDEX` DDL statement
