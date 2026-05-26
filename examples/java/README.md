# OxiDB Java Quickstart

Minimal plain-Java example showing how to consume the
`com.oxidb:oxidb-client:0.28.18` artifact.

Walks through: connect → HELLO handshake → CRUD → typed deserialization
→ Query builder → async (`CompletableFuture`) → streaming → typed
exception handling.

## Run

1. **Start oxidb-server** (e.g. from the repo root):
   ```bash
   ./target/release/oxidb-server
   ```
2. **Run the example:**
   ```bash
   cd examples/java
   mvn -q compile exec:java
   ```

Optional env overrides:
- `OXIDB_HOST` (default `127.0.0.1`)
- `OXIDB_PORT` (default `4444`)

## Expected output (abridged)

```
Connected to oxidb-server 0.28.18 (wire v1, stable surface v1.0)
  features: [fts, blobs, txn, rbac, tls, encryption_at_rest, audit, scram_sha_256, indexes, aggregation]
  auth methods: [anonymous]

Inserted Alice with _id=1
Inserted 4 more with ids=[2, 3, 4, 5]

Total users: 5

Active adult engineers (2):
  - Alice (30)
  - Dave (42)

Users in their 20s (2): [Alice, Bob]
Engineering OR Sales: 5

Found Alice: User[_id=1, name=Alice, age=30, active=true, department=Engineering]
Async result: 3 engineers

Reactivated Carol: modified=1 doc(s)

Streaming all users (batch=2):
  [1] Alice
  [2] Bob
  [3] Carol
  [4] Dave
  [5] Eve

Caught typed exception: NotFound — no such collection 'no_such_collection_zzz'

Cleanup: deleted 5 docs.
Done.
```

## What it demonstrates

| Section | API used |
|---|---|
| 1. HELLO | `client.hello("myapp/1.0")` → `HelloResponse` with version/features |
| 2. Clean slate | `client.delete(coll, Map.of())` with typed `NotFound` catch |
| 3. Single insert | `client.insertReturningId(coll, doc)` → `long` |
| 4. Bulk insert | `client.insertManyReturningIds(coll, list)` → `long[]` |
| 5. Count | `client.count(coll, null)` |
| 6. Compound query | `Query.and(Query.eq, Query.gte, Query.eq)` + typed `find(..., User.class)` |
| 7. Range query | `Query.range("age", 20, 30)` |
| 8. `$in` query | `Query.in("department", List.of("Engineering", "Sales"))` |
| 9. findOne | `client.findOne(coll, query, User.class)` returns `T` or `null` |
| 10. Async | `client.findAsync(...).join()` |
| 11. Update | `client.update(coll, query, Map.of("$set", ...))` |
| 12. Streaming | `for (var u : client.stream(coll, query, sort, batchSize, User.class))` |
| 13. Typed exceptions | `catch (OxiDbException.OxiDbNotFoundException nf)` |

## Related examples

- [`examples/spring-boot/`](../spring-boot/) — Spring Boot REST API using the same client
