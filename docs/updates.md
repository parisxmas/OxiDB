# Updating Documents

OxiDB supports update operators for modifying documents. Updates are applied atomically per document.

## Field Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$set` | Set a field value | `{"$set": {"name": "Bob"}}` |
| `$unset` | Remove a field | `{"$unset": {"temp_field": ""}}` |
| `$inc` | Increment numeric field | `{"$inc": {"views": 1}}` |
| `$mul` | Multiply numeric field | `{"$mul": {"price": 1.1}}` |
| `$min` | Set to value if less than current | `{"$min": {"low_score": 50}}` |
| `$max` | Set to value if greater than current | `{"$max": {"high_score": 99}}` |
| `$rename` | Rename a field | `{"$rename": {"old_name": "new_name"}}` |
| `$currentDate` | Set field to current timestamp | `{"$currentDate": {"updated_at": true}}` |

`$currentDate` sets the field to the current date/time as an ISO 8601 string.

## Array Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$push` | Append value to array | `{"$push": {"tags": "new_tag"}}` |
| `$pull` | Remove matching values from array | `{"$pull": {"tags": "old_tag"}}` |
| `$addToSet` | Append only if not already present | `{"$addToSet": {"tags": "unique_tag"}}` |
| `$pop` | Remove first (`-1`) or last (`1`) element | `{"$pop": {"queue": 1}}` |

`$push` creates the array if the field does not exist. `$pull` removes all occurrences of the matching value.

## Combining Operators

Multiple operators can be used in a single update:

```json
{
  "$set": {"status": "processed"},
  "$inc": {"process_count": 1},
  "$currentDate": {"processed_at": true}
}
```

## Dot Notation

Update nested fields using dot notation:

```json
{"$set": {"address.city": "Berlin"}}
```

```json
{"$inc": {"stats.login_count": 1}}
```

## update vs update_one

### update

Updates **all** documents matching the query. Returns the count of modified documents.

```json
{
  "command": "update",
  "collection": "users",
  "query": {"status": "trial"},
  "update": {"$set": {"status": "expired"}}
}
```

Response:

```json
{"ok": true, "data": {"modified": 15}}
```

### update_one

Updates only the **first** matching document and stops (early termination). Returns the count of modified documents (0 or 1).

```json
{
  "command": "update_one",
  "collection": "users",
  "query": {"email": "alice@example.com"},
  "update": {"$set": {"verified": true}}
}
```

Response:

```json
{"ok": true, "data": {"modified": 1}}
```

## Client Examples

### Python

```python
# Field operators
client.update("users", {"name": "Alice"}, {
    "$set": {"status": "active"},
    "$inc": {"login_count": 1},
    "$currentDate": {"last_login": True}
})

# Array operators
client.update("posts", {"_id": 1}, {"$push": {"tags": "rust"}})
client.update("posts", {"_id": 1}, {"$pull": {"tags": "draft"}})
client.update("posts", {"_id": 1}, {"$addToSet": {"tags": "featured"}})

# update_one
client.update_one("users", {"email": "bob@example.com"}, {"$set": {"verified": True}})
```

### Go

```go
// Field operators
client.Update("users", map[string]any{"name": "Alice"}, map[string]any{
    "$set": map[string]any{"status": "active"},
    "$inc": map[string]any{"login_count": 1},
    "$currentDate": map[string]any{"last_login": true},
})

// Array operators
client.Update("posts", map[string]any{"_id": 1}, map[string]any{
    "$push": map[string]any{"tags": "rust"},
})

// update_one
client.UpdateOne("users", map[string]any{"email": "bob@example.com"}, map[string]any{
    "$set": map[string]any{"verified": true},
})
```

### Java

```java
// Field operators
db.update("users", Map.of("name", "Alice"), Map.of(
    "$set", Map.of("status", "active"),
    "$inc", Map.of("login_count", 1),
    "$currentDate", Map.of("last_login", true)
));

// Array operators
db.update("posts", Map.of("_id", 1), Map.of("$push", Map.of("tags", "rust")));

// update_one
db.updateOne("users",
    Map.of("email", "bob@example.com"),
    Map.of("$set", Map.of("verified", true))
);
```

### Julia

```julia
# Field operators
update(client, "users", Dict("name" => "Alice"), Dict(
    "\$set" => Dict("status" => "active"),
    "\$inc" => Dict("login_count" => 1),
    "\$currentDate" => Dict("last_login" => true)
))

# Array operators
update(client, "posts", Dict("_id" => 1), Dict("\$push" => Dict("tags" => "rust")))

# update_one
update_one(client, "users",
    Dict("email" => "bob@example.com"),
    Dict("\$set" => Dict("verified" => true))
)
```

### .NET

```csharp
// With UpdateDef builder
var upd = UpdateDef.Set("status", "active")
        + UpdateDef.Inc("login_count", 1)
        + UpdateDef.CurrentDate("last_login");
db.Update("users", Filter.Eq("name", "Alice"), upd);

// Array operators
db.Update("posts", Filter.Eq("_id", 1), UpdateDef.Push("tags", "rust"));
db.Update("posts", Filter.Eq("_id", 1), UpdateDef.Pull("tags", "draft"));

// update_one
db.UpdateOne("users",
    Filter.Eq("email", "bob@example.com"),
    UpdateDef.Set("verified", true));
```

### Swift

```swift
// Field operators
try db.update(collection: "users", query: ["name": "Alice"], update: [
    "$set": ["status": "active"],
    "$inc": ["login_count": 1],
    "$currentDate": ["last_login": true]
])

// Array operators
try db.update(collection: "posts", query: ["_id": 1], update: ["$push": ["tags": "rust"]])

// update_one
try db.updateOne(collection: "users",
    query: ["email": "bob@example.com"],
    update: ["$set": ["verified": true]])
```

## See Also

- [Querying Documents](queries.md) -- query syntax used in the `query` parameter
- [Transactions](transactions.md) -- transactional updates across multiple documents
- [Indexes](indexes.md) -- improve update query performance
