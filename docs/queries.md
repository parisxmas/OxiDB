# Querying Documents

OxiDB supports JSON-based queries with comparison operators, logical combinators, dot notation for nested fields, and sort/skip/limit options.

## Basic Queries

The simplest query matches documents where a field equals a value:

```json
{"command": "find", "collection": "users", "query": {"name": "Alice"}}
```

This is shorthand for the `$eq` operator:

```json
{"query": {"name": {"$eq": "Alice"}}}
```

An empty query `{}` matches all documents.

## Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$eq` | Equal to | `{"age": {"$eq": 30}}` |
| `$ne` | Not equal to | `{"status": {"$ne": "inactive"}}` |
| `$gt` | Greater than | `{"age": {"$gt": 18}}` |
| `$gte` | Greater than or equal | `{"score": {"$gte": 90}}` |
| `$lt` | Less than | `{"price": {"$lt": 100}}` |
| `$lte` | Less than or equal | `{"quantity": {"$lte": 0}}` |
| `$in` | Value in array | `{"status": {"$in": ["active", "pending"]}}` |
| `$exists` | Field exists | `{"email": {"$exists": true}}` |
| `$regex` | Regular expression | `{"name": {"$regex": "^Al", "$options": "i"}}` |

The `$regex` operator supports an optional `$options` field. Use `"i"` for case-insensitive matching.

## Logical Operators

Combine multiple conditions with `$and` and `$or`:

```json
{
  "$and": [
    {"age": {"$gte": 18}},
    {"status": "active"}
  ]
}
```

```json
{
  "$or": [
    {"role": "admin"},
    {"role": "moderator"}
  ]
}
```

Multiple conditions on the same level are implicitly `$and`:

```json
{"age": {"$gte": 18}, "status": "active"}
```

## Dot Notation

Query nested fields using dot notation:

```json
{"address.city": "Berlin"}
```

```json
{"settings.notifications.email": true}
```

## Find Options

### Sort

Sort results by one or more fields. Use `1` for ascending, `-1` for descending:

```json
{
  "command": "find",
  "collection": "users",
  "query": {},
  "sort": {"age": -1, "name": 1}
}
```

When the sort field has a [field index](indexes.md), OxiDB uses index-backed sorting which is O(limit) instead of O(n log n).

### Skip and Limit

Paginate results with `skip` and `limit`:

```json
{
  "command": "find",
  "collection": "products",
  "query": {"category": "electronics"},
  "sort": {"price": 1},
  "skip": 20,
  "limit": 10
}
```

## find_one

Returns a single matching document (or null if none found):

```json
{"command": "find_one", "collection": "users", "query": {"email": "alice@example.com"}}
```

Response:

```json
{"ok": true, "data": {"_id": 1, "email": "alice@example.com", "name": "Alice"}}
```

## count

Count matching documents:

```json
{"command": "count", "collection": "users", "query": {"status": "active"}}
```

Response:

```json
{"ok": true, "data": 42}
```

With an empty query `{}` or no query, counts all documents. When the field being queried has an index, OxiDB returns the set size directly without touching documents.

## Index-Backed Queries

The following operators benefit from [indexes](indexes.md):

- `$eq` -- direct BTreeMap lookup
- `$gt`, `$gte`, `$lt`, `$lte` -- BTreeMap range scan
- `$in` -- multiple BTreeMap lookups
- Sort on indexed field -- BTreeMap iteration

See the [Indexes](indexes.md) guide for how to create indexes.

## Client Examples

### Python

```python
from oxidb import OxiDbClient

with OxiDbClient() as client:
    # Basic find
    users = client.find("users", {"status": "active"})

    # With operators
    adults = client.find("users", {"age": {"$gte": 18}})

    # With sort, skip, limit
    page = client.find("users", {}, sort={"name": 1}, skip=0, limit=10)

    # find_one
    user = client.find_one("users", {"email": "alice@example.com"})

    # count
    n = client.count("users", {"status": "active"})
```

### Go

```go
// Basic find
users, _ := client.Find("users", map[string]any{"status": "active"}, nil)

// With options
opts := &oxidb.FindOptions{
    Sort:  map[string]any{"name": 1},
    Skip:  0,
    Limit: 10,
}
page, _ := client.Find("users", map[string]any{}, opts)

// find_one
user, _ := client.FindOne("users", map[string]any{"email": "alice@example.com"})

// count
n, _ := client.Count("users", map[string]any{"status": "active"})
```

### Java

```java
// Basic find
JsonNode users = db.find("users", Map.of("status", "active"));

// With sort, skip, limit
JsonNode page = db.find("users", Map.of(), Map.of("name", 1), 0, 10);

// find_one
JsonNode user = db.findOne("users", Map.of("email", "alice@example.com"));

// count
int n = db.count("users", Map.of("status", "active"));
```

### Julia

```julia
# Basic find
users = find(client, "users", Dict("status" => "active"))

# With sort, skip, limit
page = find(client, "users", Dict(); sort=Dict("name" => 1), skip=0, limit=10)

# find_one
user = find_one(client, "users", Dict("email" => "alice@example.com"))

# count
n = count_docs(client, "users", Dict("status" => "active"))
```

### .NET

```csharp
// Basic find
var users = db.Find("users", """{"status": "active"}""");

// With Filter builder
var adults = db.Find("users", Filter.Gte("age", 18));

// Combine filters
var query = Filter.And(Filter.Gte("age", 18), Filter.Eq("status", "active"));
var result = db.Find("users", query);

// find_one
var user = db.FindOne("users", """{"email": "alice@example.com"}""");

// count
var count = db.Count("users");
```

### Swift

```swift
// Basic find
let users = try db.find(collection: "users", query: ["status": "active"])

// With operators
let adults = try db.find(collection: "users", query: ["age": ["$gte": 18]])

// find_one
let user = try db.findOne(collection: "users", query: ["email": "alice@example.com"])

// count
let n = try db.count(collection: "users")
```
