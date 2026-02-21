# Stored Procedures

Stored procedures are JSON-defined multi-step workflows that execute atomically within a transaction. They allow you to define reusable server-side logic such as balance transfers, conditional updates, and multi-collection operations.

## Concept

A procedure consists of:
- **name**: Unique identifier
- **params**: Array of parameter names the caller must provide
- **steps**: Array of operations to execute sequentially

All steps run inside an implicit transaction. If any step fails or an `abort` step is reached, the transaction is rolled back automatically.

## Creating a Procedure

```json
{
  "command": "create_procedure",
  "name": "transfer_funds",
  "params": ["from_account", "to_account", "amount"],
  "steps": [
    {
      "type": "find_one",
      "collection": "accounts",
      "query": {"name": "$param.from_account"},
      "as": "sender"
    },
    {
      "type": "if",
      "condition": {"$lt": ["$sender.balance", "$param.amount"]},
      "then": [
        {"type": "abort", "message": "Insufficient funds"}
      ]
    },
    {
      "type": "update",
      "collection": "accounts",
      "query": {"name": "$param.from_account"},
      "update": {"$inc": {"balance": {"$multiply": ["$param.amount", -1]}}}
    },
    {
      "type": "update",
      "collection": "accounts",
      "query": {"name": "$param.to_account"},
      "update": {"$inc": {"balance": "$param.amount"}}
    },
    {
      "type": "return",
      "value": {"status": "ok", "transferred": "$param.amount"}
    }
  ]
}
```

## Step Types

| Step Type | Description | Fields |
|-----------|-------------|--------|
| `find` | Query documents, store result array | `collection`, `query`, `as` |
| `find_one` | Get single document | `collection`, `query`, `as` |
| `insert` | Insert a document | `collection`, `doc` |
| `update` | Update matching documents | `collection`, `query`, `update` |
| `delete` | Delete matching documents | `collection`, `query` |
| `count` | Count matching documents | `collection`, `query`, `as` |
| `aggregate` | Run aggregation pipeline | `collection`, `pipeline`, `as` |
| `set` | Set a variable to a value | `var`, `value` |
| `if` | Conditional branching | `condition`, `then`, `else` |
| `abort` | Abort with error message | `message` |
| `return` | Return a result value | `value` |

### find / find_one

```json
{
  "type": "find",
  "collection": "orders",
  "query": {"customer_id": "$param.customer_id"},
  "as": "orders"
}
```

The result is stored in the variable specified by `as`. `find` stores an array, `find_one` stores a single document (or null).

### insert

```json
{
  "type": "insert",
  "collection": "audit_log",
  "doc": {"action": "transfer", "from": "$param.from_account", "amount": "$param.amount"}
}
```

### update / delete

```json
{
  "type": "update",
  "collection": "accounts",
  "query": {"name": "$param.account"},
  "update": {"$inc": {"balance": "$param.amount"}}
}
```

### count

```json
{
  "type": "count",
  "collection": "orders",
  "query": {"status": "pending"},
  "as": "pending_count"
}
```

### set

Set a variable for use in subsequent steps:

```json
{"type": "set", "var": "tax_rate", "value": 0.2}
```

### if / else

```json
{
  "type": "if",
  "condition": {"$gte": ["$sender.balance", "$param.amount"]},
  "then": [
    {"type": "update", "collection": "accounts", "query": {"name": "$param.from_account"}, "update": {"$inc": {"balance": -100}}}
  ],
  "else": [
    {"type": "abort", "message": "Insufficient funds"}
  ]
}
```

### abort

Stops execution and rolls back the transaction:

```json
{"type": "abort", "message": "Validation failed: negative amount"}
```

### return

Returns a custom value to the caller and commits the transaction:

```json
{"type": "return", "value": {"status": "ok", "new_balance": "$sender.balance"}}
```

## Variable Resolution

Variables in step fields are resolved using these prefixes:

| Prefix | Description | Example |
|--------|-------------|---------|
| `$param.name` | Procedure parameter | `$param.amount` |
| `$param.user.field` | Nested parameter access | `$param.user.email` |
| `$varname` | Variable set by `as` or `set` | `$sender` |
| `$varname.field` | Nested variable access | `$sender.balance` |

Array indexing is supported: `$orders.0.amount` accesses the first order's amount.

## Condition Expressions

Conditions used in `if` steps support these operators:

| Operator | Description | Example |
|----------|-------------|---------|
| `$eq` | Equal | `{"$eq": ["$a", "$b"]}` |
| `$ne` | Not equal | `{"$ne": ["$status", "closed"]}` |
| `$gt` | Greater than | `{"$gt": ["$balance", 0]}` |
| `$gte` | Greater than or equal | `{"$gte": ["$balance", "$param.amount"]}` |
| `$lt` | Less than | `{"$lt": ["$balance", "$param.amount"]}` |
| `$lte` | Less than or equal | `{"$lte": ["$count", 100]}` |
| `$and` | Logical AND | `{"$and": [cond1, cond2]}` |
| `$or` | Logical OR | `{"$or": [cond1, cond2]}` |
| `$not` | Logical NOT | `{"$not": cond}` |

## Managing Procedures

### List Procedures

```json
{"command": "list_procedures"}
```

### Get Procedure Definition

```json
{"command": "get_procedure", "name": "transfer_funds"}
```

### Delete Procedure

```json
{"command": "delete_procedure", "name": "transfer_funds"}
```

### Call Procedure

```json
{
  "command": "call_procedure",
  "name": "transfer_funds",
  "params": {"from_account": "Alice", "to_account": "Bob", "amount": 50}
}
```

## Client Examples

### Python

```python
# Create procedure
client.insert("_procedures", {
    "name": "transfer_funds",
    "params": ["from_account", "to_account", "amount"],
    "steps": [
        {"type": "find_one", "collection": "accounts", "query": {"name": "$param.from_account"}, "as": "sender"},
        {"type": "if", "condition": {"$lt": ["$sender.balance", "$param.amount"]},
         "then": [{"type": "abort", "message": "Insufficient funds"}]},
        {"type": "update", "collection": "accounts", "query": {"name": "$param.from_account"},
         "update": {"$inc": {"balance": {"$multiply": ["$param.amount", -1]}}}},
        {"type": "update", "collection": "accounts", "query": {"name": "$param.to_account"},
         "update": {"$inc": {"balance": "$param.amount"}}},
        {"type": "return", "value": {"status": "ok"}}
    ]
})

# Call procedure
result = client._send({
    "command": "call_procedure",
    "name": "transfer_funds",
    "params": {"from_account": "Alice", "to_account": "Bob", "amount": 50}
})

# List procedures
procedures = client._send({"command": "list_procedures"})

# Delete procedure
client._send({"command": "delete_procedure", "name": "transfer_funds"})
```

### Go

```go
// Call procedure (via raw send or protocol message)
result, _ := client.Send(map[string]any{
    "command": "call_procedure",
    "name":    "transfer_funds",
    "params":  map[string]any{"from_account": "Alice", "to_account": "Bob", "amount": 50},
})
```

### Java

```java
// Call procedure via raw command or protocol
JsonNode result = db.sql(""); // Use protocol-level call
```

### Julia

```julia
# Procedures are managed via protocol-level commands
```

### .NET

```csharp
// Procedures are managed via protocol-level commands
```

### Swift

```swift
// Procedures are managed via protocol-level commands
```

## See Also

- [Transactions](transactions.md) -- procedures automatically wrap steps in a transaction
- [Scheduler](scheduler.md) -- run procedures on a schedule
- [Protocol Reference](protocol-reference.md) -- raw command format for procedure management
