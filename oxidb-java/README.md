# OxiDB Java Client

Pure-Java native document client for [OxiDB](https://oxidb.baltavista.com).
TCP transport over the OxiWire binary protocol. Java 17+.

```xml
<dependency>
    <groupId>com.oxidb</groupId>
    <artifactId>oxidb-client</artifactId>
    <version>0.28.18</version>
</dependency>
```

OxiDB is a document database — MongoDB-style JSON queries via this
client are the canonical API. (Earlier versions shipped an experimental
SQL surface via JDBC; SQL has been removed and the JDBC driver is no
longer maintained.)

## 60-second start

```java
import com.oxidb.client.OxiDbClient;
import com.oxidb.client.Query;
import com.oxidb.client.HelloResponse;

import java.util.Map;
import java.util.List;

try (OxiDbClient client = OxiDbClient.connect("127.0.0.1", 4444)) {

    // Verify server version + features at connect time
    // (HELLO handshake — wire v0.28.13+, ADR-0003 Phase 2).
    HelloResponse hello = client.hello("myapp/1.0");
    System.out.println("Connected to " + hello.name() + " " + hello.version()
                       + " (wire v" + hello.wireVersion() + ")");

    // Insert a doc and get the auto-assigned id.
    long aliceId = client.insertReturningId("users",
        Map.of("name", "Alice", "age", 30, "active", true));

    // Find with a strongly-typed Query.
    List<Map<String, Object>> adults = client.find("users",
        Query.and(Query.gte("age", 18), Query.eq("active", true)));

    // Typed deserialization via Jackson.
    public record User(long _id, String name, int age, boolean active) {}
    List<User> typedAdults = client.find("users",
        Query.and(Query.gte("age", 18), Query.eq("active", true)),
        User.class);

    // Stream millions of rows without materialising everything.
    var sort = Map.<String, Object>of("_id", 1);
    for (User u : client.stream("users", null, sort, 1000, User.class)) {
        // process one at a time
    }
}
```

## Async

Every blocking method has a `CompletableFuture` variant:

```java
client.findAsync("users", Query.eq("active", true), User.class)
    .thenAccept(users -> System.out.println("Got " + users.size() + " users"));

client.helloAsync("myapp/1.0").join();
```

## Query builder

For runtime-constructed queries, prefer `Query.*` over hand-built maps —
type-safe and autocomplete-friendly:

```java
Query.eq("status", "active");
Query.ne("dept", "Sales");
Query.gt("age", 30);   Query.gte("age", 18);
Query.lt("age", 65);   Query.lte("age", 64);
Query.in("country", List.of("US", "UK", "JP"));
Query.exists("email", true);
Query.regex("name", "^A");
Query.and(Query.eq("active", true), Query.gte("age", 50));
Query.or(Query.eq("city", "NY"), Query.eq("city", "London"));
Query.range("salary", 50_000, 100_000);   // [50k, 100k)
```

These return `Map<String, Object>`, so they compose with hand-built maps:

```java
Map<String, Object> custom = new HashMap<>(Query.eq("dept", "Engineering"));
custom.put("salary", Map.of("$gte", 80_000));
```

## Exception hierarchy

The base is `OxiDbException` (unchecked — `RuntimeException`). Specific
subclasses cover the common failure modes:

| Type | Trigger |
|---|---|
| `OxiDbException.OxiDbDuplicateKeyException` | Write violated a unique index |
| `OxiDbException.OxiDbTransactionConflictException` | OCC commit conflict; retry the whole tx |
| `OxiDbException.OxiDbAuthenticationException` | SCRAM / RBAC failure |
| `OxiDbException.OxiDbNotFoundException` | Collection / index / user missing |
| `OxiDbException.OxiDbImmutableException` | Write hit a WORM lock |
| `OxiDbException.OxiDbConnectionException` | Wire-level (closed / EOF / dial failed) |
| `OxiDbException.OxiDbProtocolException` | OxiWire decode failure — usually a wire-version mismatch |

Catch `OxiDbException` if you don't care which:

```java
try {
    client.insertReturningId("users", duplicate);
} catch (OxiDbException.OxiDbDuplicateKeyException dup) {
    // handle just dupes
} catch (OxiDbException other) {
    // anything else
}
```

## Thread safety

Each `OxiDbClient` instance is thread-safe — an internal `ReentrantLock`
serialises requests on the single socket. For high concurrency, use a
pool of instances (one per worker thread, or a `BlockingQueue`-backed
pool). HikariCP-style pooling is on the roadmap below.

## Roadmap

The artifacts published at **v0.28.18** ship the native document client
driver. The list below tracks bigger-scope work for upcoming releases:

| Item | Status |
|---|---|
| Spring Data `OxiDbRepository<T, ID>` integration | Planned — separate module `oxidb-spring-data` |
| Reactive Streams / Project Reactor flavour | Planned — `oxidb-client-reactive` |
| Quarkus extension | Planned — `oxidb-quarkus-extension` |
| Kotlin extensions (coroutines, DSL) | Planned — `oxidb-client-kotlin` |
| Connection pool (`OxiDbConnectionPool`) | Planned — same artifact, ~v0.29 |
| `IAsyncEnumerable`-equivalent via `Flow.Publisher` | Planned — `oxidb-client-reactive` |
| Compile-time query annotation processor | Planned — `oxidb-processor` |
| Bean-validation integration (`@NotNull`, ...) | Planned |

Sequencing follows demand. File a GitHub issue if any of these unblocks
you on a real project — that bumps priority.

## Maven Central publishing

Local publish-ready build:

```bash
cd oxidb-java
mvn -P release deploy
```

Requires:
- GPG key for artifact signing (set in `~/.gnupg/`)
- Maven Central credentials in `~/.m2/settings.xml`:

```xml
<server>
    <id>central</id>
    <username>YOUR_USER</username>
    <password>YOUR_TOKEN</password>
</server>
```

The release profile bundles sources jar, javadoc jar, GPG signatures,
and uploads via the official `central-publishing-maven-plugin`.

## License

AGPL-3.0-only. Commercial licensing available — see
[COMMERCIAL-LICENSE.md](../COMMERCIAL-LICENSE.md) in the repo root.
