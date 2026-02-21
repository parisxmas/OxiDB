# Transactions

OxiDB supports multi-document, multi-collection ACID transactions using Optimistic Concurrency Control (OCC). Transactions provide snapshot isolation -- reads see a consistent view, and conflicts are detected at commit time.

## How OCC Works

1. **Begin**: A transaction starts and gets a unique ID. A snapshot of document versions is recorded as reads occur.
2. **Operations**: All writes (insert, update, delete) are buffered in memory. Reads within the transaction see buffered writes overlaid on the snapshot.
3. **Commit**: At commit time, OxiDB validates that no document read during the transaction was modified by another transaction. If validation passes, all buffered writes are applied atomically. If a conflict is detected, the transaction is aborted and a conflict error is returned.
4. **Rollback**: Discards all buffered writes with no side effects.

Deadlocks are prevented by acquiring collection locks in sorted order (using a BTreeSet).

## Manual Transaction Flow

### Protocol

```json
{"command": "begin_tx"}
```

Response:

```json
{"ok": true, "data": {"tx_id": "tx_1"}}
```

After beginning a transaction, all subsequent operations on the same connection run within the transaction context:

```json
{"command": "insert", "collection": "accounts", "doc": {"name": "Alice", "balance": 100}}
{"command": "update", "collection": "accounts", "query": {"name": "Bob"}, "update": {"$inc": {"balance": -50}}}
{"command": "commit_tx"}
```

To discard:

```json
{"command": "rollback_tx"}
```

## Transaction-Aware Operations

The following operations participate in transactions (use the transaction's read/write sets):

| Operation | In Transaction |
|-----------|---------------|
| `insert` | Buffered until commit |
| `insert_many` | Buffered until commit |
| `update` | Buffered until commit, reads tracked |
| `delete` | Buffered until commit, reads tracked |
| `find` | Reads from snapshot + buffered writes |

## Non-Transactional Operations

These operations execute immediately regardless of transaction state:

- `find_one`
- `update_one`
- `delete_one`

Use the multi-document variants (`find`, `update`, `delete`) for transactional consistency.

## Conflict Handling

When a commit fails due to a version conflict, a `TransactionConflictError` is raised. The standard retry pattern is:

```python
from oxidb import OxiDbClient, TransactionConflictError

with OxiDbClient() as client:
    for attempt in range(3):
        try:
            with client.transaction():
                alice = client.find("accounts", {"name": "Alice"})
                if alice[0]["balance"] >= 50:
                    client.update("accounts", {"name": "Alice"}, {"$inc": {"balance": -50}})
                    client.update("accounts", {"name": "Bob"}, {"$inc": {"balance": 50}})
            break  # success
        except TransactionConflictError:
            continue  # retry
```

## Client Examples

### Python

```python
# Context manager (recommended)
with client.transaction():
    client.insert("ledger", {"action": "debit", "amount": 100})
    client.insert("ledger", {"action": "credit", "amount": 100})
# Auto-committed here, or auto-rolled back on exception

# Manual
client.begin_tx()
try:
    client.insert("ledger", {"action": "debit", "amount": 100})
    client.insert("ledger", {"action": "credit", "amount": 100})
    client.commit_tx()
except Exception:
    client.rollback_tx()
    raise
```

### Go

```go
// Callback helper (recommended)
err := client.WithTransaction(func() error {
    client.Insert("ledger", map[string]any{"action": "debit", "amount": 100})
    client.Insert("ledger", map[string]any{"action": "credit", "amount": 100})
    return nil
})

// Manual
client.BeginTx()
_, err := client.Insert("ledger", map[string]any{"action": "debit", "amount": 100})
if err != nil {
    client.RollbackTx()
    return err
}
client.Insert("ledger", map[string]any{"action": "credit", "amount": 100})
client.CommitTx()
```

Error handling:

```go
import "errors"

var conflict *oxidb.TransactionConflictError
if errors.As(err, &conflict) {
    // OCC conflict -- retry
}
```

### Java

```java
// Callback helper (recommended)
db.withTransaction(() -> {
    db.insert("ledger", Map.of("action", "debit", "amount", 100));
    db.insert("ledger", Map.of("action", "credit", "amount", 100));
});

// Manual
db.beginTx();
try {
    db.insert("ledger", Map.of("action", "debit", "amount", 100));
    db.insert("ledger", Map.of("action", "credit", "amount", 100));
    db.commitTx();
} catch (Exception e) {
    db.rollbackTx();
    throw e;
}
```

Error handling:

```java
try {
    db.commitTx();
} catch (TransactionConflictException e) {
    // OCC conflict -- retry
} catch (OxiDbException e) {
    // Other error
}
```

### Julia

```julia
# Block form (recommended)
transaction(client) do
    insert(client, "ledger", Dict("action" => "debit", "amount" => 100))
    insert(client, "ledger", Dict("action" => "credit", "amount" => 100))
end

# Manual
begin_tx(client)
try
    insert(client, "ledger", Dict("action" => "debit", "amount" => 100))
    insert(client, "ledger", Dict("action" => "credit", "amount" => 100))
    commit_tx(client)
catch e
    rollback_tx(client)
    rethrow()
end
```

Error handling:

```julia
try
    commit_tx(client)
catch e
    if e isa TransactionConflictError
        # OCC conflict -- retry
    end
end
```

### .NET

```csharp
// Manual (no built-in helper)
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

### Swift

```swift
// Block form (recommended)
try db.transaction {
    try db.insert(collection: "ledger", document: ["action": "debit", "amount": 100])
    try db.insert(collection: "ledger", document: ["action": "credit", "amount": 100])
}

// Manual
try db.beginTransaction()
do {
    try db.insert(collection: "ledger", document: ["action": "debit", "amount": 100])
    try db.insert(collection: "ledger", document: ["action": "credit", "amount": 100])
    try db.commitTransaction()
} catch {
    try db.rollbackTransaction()
    throw error
}
```

Error handling:

```swift
do {
    try db.commitTransaction()
} catch OxiDBError.transactionConflict(let msg) {
    // OCC conflict -- retry
}
```

## See Also

- [Stored Procedures](stored-procedures.md) -- procedures automatically wrap steps in a transaction
- [Server Configuration](server.md) -- WAL and crash recovery details
