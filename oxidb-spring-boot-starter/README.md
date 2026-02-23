# OxiDB Spring Boot Starter

Spring Boot auto-configuration starter for [OxiDB](https://github.com/parisxmas/OxiDB) document database.

Provides an auto-configured `OxiDbClient` bean that communicates with `oxidb-server` over TCP using the length-prefixed JSON protocol. Uses Jackson for JSON serialization.

## Requirements

- Java 17+
- Spring Boot 3.x
- A running `oxidb-server` instance (see [main README](../README.md#installation))

## Installation

Build and install the starter to your local Maven repository:

```bash
cd oxidb-spring-boot-starter
mvn clean install
```

Add to your project's `pom.xml`:

```xml
<dependency>
    <groupId>com.oxidb</groupId>
    <artifactId>oxidb-spring-boot-starter</artifactId>
    <version>0.2.0</version>
</dependency>
```

## Configuration

Add to `application.properties`:

```properties
oxidb.host=127.0.0.1
oxidb.port=4444
oxidb.timeout-ms=5000
```

All properties are optional and default to the values shown above.

## Quick Start

```java
import com.oxidb.spring.OxiDbClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class UserController {

    @Autowired
    private OxiDbClient db;

    @PostMapping("/users")
    public Object createUser(@RequestBody String json) {
        return db.insert("users", json);
    }

    @GetMapping("/users")
    public Object listUsers() {
        return db.find("users", Map.of());
    }
}
```

## API Reference

### CRUD

| Method | Description |
|--------|-------------|
| `insert(collection, doc)` / `insert(collection, jsonString)` | Insert a document |
| `insertMany(collection, docs)` | Insert multiple documents |
| `find(collection, query)` / `find(collection, query, sort, skip, limit)` | Find documents |
| `find(collection, jsonQueryString)` | Find with JSON string query |
| `findOne(collection, query)` | Find first matching document |
| `update(collection, query, update)` | Update documents |
| `delete(collection, query)` / `delete(collection, jsonString)` | Delete documents |
| `count(collection, query)` / `count(collection)` | Count documents |

```java
// Insert (Map or JSON string)
db.insert("users", Map.of("name", "Alice", "age", 30));
db.insert("users", "{\"name\":\"Bob\",\"age\":25}");
db.insertMany("users", List.of(
    Map.of("name", "Charlie", "age", 35),
    Map.of("name", "Diana", "age", 28)
));

// Find with options
JsonNode docs = db.find("users", Map.of("age", Map.of("$gte", 18)));
JsonNode docs2 = db.find("users", Map.of(), Map.of("age", 1), 0, 10);
JsonNode doc = db.findOne("users", Map.of("name", "Alice"));

// Update
db.update("users", Map.of("name", "Alice"), Map.of("$set", Map.of("age", 31)));

// Delete
db.delete("users", Map.of("name", "Charlie"));

// Count
int n = db.count("users");
```

### Collections & Indexes

```java
db.createCollection("orders");
db.listCollections();
db.dropCollection("orders");

db.createIndex("users", "name");
db.createUniqueIndex("users", "email");
db.createCompositeIndex("users", List.of("name", "age"));
```

### Aggregation

```java
// Using JSON string
JsonNode results = db.aggregate("orders", """
    [
        {"$match": {"status": "completed"}},
        {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
        {"$sort": {"total": -1}},
        {"$limit": 10}
    ]
""");

// Using List<Map>
JsonNode results2 = db.aggregate("orders", List.of(
    Map.of("$group", Map.of("_id", null, "total", Map.of("$sum", "$amount")))
));
```

**Supported stages:** `$match`, `$group`, `$sort`, `$skip`, `$limit`, `$project`, `$count`, `$unwind`, `$addFields`, `$lookup`

### Transactions

```java
// Auto-commit on success, auto-rollback on exception
db.withTransaction(() -> {
    db.insert("ledger", Map.of("action", "debit",  "amount", 100));
    db.insert("ledger", Map.of("action", "credit", "amount", 100));
});

// Manual control
db.beginTx();
db.insert("ledger", Map.of("action", "refund", "amount", 50));
db.commitTx();   // or db.rollbackTx()
```

### Blob Storage

```java
// Buckets
db.createBucket("files");
db.listBuckets();
db.deleteBucket("files");

// Objects
db.putObject("files", "hello.txt", "Hello!".getBytes(), "text/plain",
             Map.of("author", "java"));
JsonNode obj = db.getObject("files", "hello.txt");
byte[] content = db.decodeObjectContent(obj); // base64 -> bytes
JsonNode head = db.headObject("files", "hello.txt");
JsonNode objs = db.listObjects("files", "hello", 10);
db.deleteObject("files", "hello.txt");
```

### Full-Text Search

```java
JsonNode results = db.search("hello world", "files", 10);
JsonNode all = db.search("hello world"); // all buckets, limit 10
```

### Compaction

```java
JsonNode stats = db.compact("users");
// stats has: old_size, new_size, docs_kept
```

### Ping

```java
db.ping(); // returns "pong"
```

## Error Handling

```java
import com.oxidb.spring.OxiDbException;
import com.oxidb.spring.TransactionConflictException;

try {
    db.commitTx();
} catch (TransactionConflictException e) {
    System.out.println("OCC conflict: " + e.getMessage());
} catch (OxiDbException e) {
    System.out.println("Database error: " + e.getMessage());
}
```

## Auto-Configuration

The starter provides:

- `OxiDbProperties` — `@ConfigurationProperties(prefix = "oxidb")`
- `OxiDbAutoConfiguration` — creates `OxiDbClient` bean with `@ConditionalOnMissingBean`
- Registered via `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`

You can override the auto-configured bean by defining your own `OxiDbClient` bean.

## Example App

See [`examples/spring-boot`](../examples/spring-boot) for a full working REST app with endpoints for CRUD, transactions, blobs, and full-text search.

## License

See [LICENSE](../LICENSE) for details.
