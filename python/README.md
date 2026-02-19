# OxiDB Python Client

Python client for [OxiDB](https://github.com/parisxmas/OxiDB) document database.

Zero external dependencies — uses only the Python standard library. Communicates with `oxidb-server` over TCP using the length-prefixed JSON protocol.

## Requirements

- Python 3.7+
- A running `oxidb-server` instance (see [main README](../README.md#installation))

## Installation

Copy the single file into your project:

```bash
cp oxidb.py your_project/
```

## Quick Start

```python
from oxidb import OxiDbClient

db = OxiDbClient("127.0.0.1", 4444)

db.insert("users", {"name": "Alice", "age": 30})
docs = db.find("users", {"name": "Alice"})
print(docs)
# [{'_id': 1, '_version': 1, 'name': 'Alice', 'age': 30}]

db.close()
```

Or use as a context manager:

```python
with OxiDbClient("127.0.0.1", 4444) as db:
    db.insert("users", {"name": "Bob", "age": 25})
```

## API Reference

### Connection

```python
client = OxiDbClient(host="127.0.0.1", port=4444, timeout=5.0)
client.close()

# or as context manager:
with OxiDbClient() as client:
    ...
```

### CRUD

| Method | Description |
|--------|-------------|
| `insert(collection, doc)` | Insert a document, returns `{"id": ...}` |
| `insert_many(collection, docs)` | Insert multiple documents |
| `find(collection, query, *, sort, skip, limit)` | Find matching documents |
| `find_one(collection, query)` | Find first matching document or `None` |
| `update(collection, query, update)` | Update all matching documents |
| `update_one(collection, query, update)` | Update first matching document |
| `delete(collection, query)` | Delete all matching documents |
| `delete_one(collection, query)` | Delete first matching document |
| `count(collection, query)` | Count matching documents |

```python
# Insert
db.insert("users", {"name": "Alice", "age": 30})
db.insert_many("users", [
    {"name": "Bob", "age": 25},
    {"name": "Charlie", "age": 35},
])

# Find with options
docs = db.find("users", {"age": {"$gte": 18}})
docs = db.find("users", {}, sort={"age": 1}, skip=0, limit=10)
doc  = db.find_one("users", {"name": "Alice"})

# Update
db.update("users", {"name": "Alice"}, {"$set": {"age": 31}})

# Delete
db.delete("users", {"name": "Charlie"})

# Count
n = db.count("users")
```

### Collections & Indexes

```python
db.create_collection("orders")
cols = db.list_collections()
db.drop_collection("orders")

db.create_index("users", "name")
db.create_unique_index("users", "email")
db.create_composite_index("users", ["name", "age"])
db.create_text_index("articles", ["title", "body"])

indexes = db.list_indexes("users")
db.drop_index("users", "name")
```

### Document Full-Text Search

```python
# Create a text index on fields you want to search
db.create_text_index("articles", ["title", "body"])

# Search returns matching documents with _score field, sorted by relevance
results = db.text_search("articles", "rust programming", limit=10)
for doc in results:
    print(f"{doc['title']} (score: {doc['_score']})")
```

### Aggregation

```python
results = db.aggregate("orders", [
    {"$match": {"status": "completed"}},
    {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
    {"$sort": {"total": -1}},
    {"$limit": 10},
])
```

**Supported stages:** `$match`, `$group`, `$sort`, `$skip`, `$limit`, `$project`, `$count`, `$unwind`, `$addFields`, `$lookup`

**Accumulators:** `$sum`, `$avg`, `$min`, `$max`, `$count`, `$first`, `$last`, `$push`

### Transactions

```python
# Auto-commit on success, auto-rollback on exception
with db.transaction():
    db.insert("ledger", {"action": "debit",  "amount": 100})
    db.insert("ledger", {"action": "credit", "amount": 100})

# Manual control
db.begin_tx()
db.insert("ledger", {"action": "refund", "amount": 50})
db.commit_tx()   # or db.rollback_tx()
```

### Blob Storage

```python
# Buckets
db.create_bucket("files")
db.list_buckets()
db.delete_bucket("files")

# Objects
db.put_object("files", "hello.txt", b"Hello!",
              content_type="text/plain", metadata={"author": "py"})
data, meta = db.get_object("files", "hello.txt")
head = db.head_object("files", "hello.txt")
objs = db.list_objects("files", prefix="hello", limit=10)
db.delete_object("files", "hello.txt")
```

### Full-Text Search

```python
results = db.search("hello world", bucket="files", limit=10)
# Returns: [{"bucket": "files", "key": "doc.txt", "score": 2.45}, ...]
```

### Compaction

```python
stats = db.compact("users")
# Returns: {"old_size": 4096, "new_size": 2048, "docs_kept": 10}
```

## Error Handling

```python
from oxidb import OxiDbError, TransactionConflictError

try:
    db.insert("users", {"email": "duplicate@test.com"})
except TransactionConflictError as e:
    print(f"OCC conflict: {e}")
except OxiDbError as e:
    print(f"Database error: {e}")
```

## License

See [LICENSE](../LICENSE) for details.
