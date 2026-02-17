# OxiDB Ruby Client

Ruby client for [OxiDB](https://github.com/parisxmas/OxiDB) document database.

Zero dependencies — uses only the Ruby standard library. Communicates with `oxidb-server` over TCP using the length-prefixed JSON protocol.

## Requirements

- Ruby 3.0+
- A running `oxidb-server` instance (see [main README](../README.md#installation))

## Installation

Copy the single file into your project:

```bash
cp lib/oxidb.rb your_project/
```

Or install as a gem from source:

```bash
gem build oxidb.gemspec
gem install oxidb-0.1.0.gem
```

## Quick Start

```ruby
require_relative "oxidb"

db = OxiDb::Client.new("127.0.0.1", 4444)

db.insert("users", { "name" => "Alice", "age" => 30 })
docs = db.find("users", { "name" => "Alice" })
puts docs.inspect
# [{"_id"=>1, "_version"=>1, "name"=>"Alice", "age"=>30}]

db.close
```

Or use with a block:

```ruby
OxiDb::Client.open("127.0.0.1", 4444) do |db|
  db.insert("users", { "name" => "Bob", "age" => 25 })
end
```

## API Reference

### Connection

```ruby
db = OxiDb::Client.new(host = "127.0.0.1", port = 4444, timeout: 5)
db.close

# or with a block (auto-closes):
OxiDb::Client.open("127.0.0.1", 4444) { |db| ... }
```

### CRUD

| Method | Description |
|--------|-------------|
| `insert(collection, doc)` | Insert a document, returns `{"id" => ...}` |
| `insert_many(collection, docs)` | Insert multiple documents |
| `find(collection, query, sort:, skip:, limit:)` | Find matching documents |
| `find_one(collection, query)` | Find first matching document or `nil` |
| `update(collection, query, update)` | Update matching documents |
| `delete(collection, query)` | Delete matching documents |
| `count(collection, query)` | Count matching documents |

```ruby
# Insert
db.insert("users", { "name" => "Alice", "age" => 30 })
db.insert_many("users", [
  { "name" => "Bob", "age" => 25 },
  { "name" => "Charlie", "age" => 35 }
])

# Find with options
docs = db.find("users", { "age" => { "$gte" => 18 } })
docs = db.find("users", {}, sort: { "age" => 1 }, skip: 0, limit: 10)
doc  = db.find_one("users", { "name" => "Alice" })

# Update
db.update("users", { "name" => "Alice" }, { "$set" => { "age" => 31 } })

# Delete
db.delete("users", { "name" => "Charlie" })

# Count
n = db.count("users")
```

### Collections & Indexes

```ruby
db.create_collection("orders")
db.list_collections
db.drop_collection("orders")

db.create_index("users", "name")
db.create_unique_index("users", "email")
db.create_composite_index("users", ["name", "age"])
```

### Aggregation

```ruby
results = db.aggregate("orders", [
  { "$match" => { "status" => "completed" } },
  { "$group" => { "_id" => "$category", "total" => { "$sum" => "$amount" } } },
  { "$sort"  => { "total" => -1 } },
  { "$limit" => 10 }
])
```

**Supported stages:** `$match`, `$group`, `$sort`, `$skip`, `$limit`, `$project`, `$count`, `$unwind`, `$addFields`, `$lookup`

### Transactions

```ruby
# Auto-commit on success, auto-rollback on exception
db.transaction do
  db.insert("ledger", { "action" => "debit",  "amount" => 100 })
  db.insert("ledger", { "action" => "credit", "amount" => 100 })
end

# Manual control
db.begin_tx
db.insert("ledger", { "action" => "refund", "amount" => 50 })
db.commit_tx   # or db.rollback_tx
```

### Blob Storage

```ruby
# Buckets
db.create_bucket("files")
db.list_buckets
db.delete_bucket("files")

# Objects
db.put_object("files", "hello.txt", "Hello!",
              content_type: "text/plain", metadata: { "author" => "ruby" })
data, meta = db.get_object("files", "hello.txt")
head = db.head_object("files", "hello.txt")
objs = db.list_objects("files", prefix: "hello", limit: 10)
db.delete_object("files", "hello.txt")
```

### Full-Text Search

```ruby
results = db.search("hello world", bucket: "files", limit: 10)
# => [{"bucket"=>"files", "key"=>"doc.txt", "score"=>2.45}, ...]
```

### Compaction

```ruby
stats = db.compact("users")
# => {"old_size"=>4096, "new_size"=>2048, "docs_kept"=>10}
```

## Error Handling

```ruby
begin
  db.insert("users", { "email" => "duplicate@test.com" })
rescue OxiDb::TransactionConflictError => e
  puts "OCC conflict: #{e.message}"
rescue OxiDb::Error => e
  puts "Database error: #{e.message}"
end
```

## Running Tests

```bash
# Start the server
./oxidb-server

# Run tests
cd ruby
ruby test/test_oxidb.rb
```

## License

See [LICENSE](../LICENSE) for details.
