# OxiDB Spring Boot Example

Spring Boot 3.4 REST API backed by OxiDB. Demonstrates manual `@Bean`
wiring of `com.oxidb.client.OxiDbClient` — no auto-configuring starter
required.

> The dedicated `oxidb-spring-boot-starter` is on the roadmap
> (see [`oxidb-java/README.md`](../../oxidb-java/README.md#roadmap)).
> Until it ships, the pattern below is the canonical way to use OxiDB
> from Spring Boot, and the autoconfigure module will fundamentally
> just generate this `@Bean` for you.

## Run

```bash
# in one terminal:
./oxidb-server

# in another:
cd examples/spring-boot
mvn -q spring-boot:run
```

Default port: `8080` (Spring) → `4444` (OxiDB).

Override via env or `application.properties`:
- `oxidb.host` (default `127.0.0.1`)
- `oxidb.port` (default `4444`)
- `server.port` (default `8080`)

## REST endpoints

| Method | Path | Body | Description |
|---|---|---|---|
| `GET` | `/ping` | — | Server ping |
| `GET` | `/hello` | — | Full HELLO response (version, features, auth methods) |
| `POST` | `/{collection}` | JSON doc | Insert; returns `{id}` |
| `GET` | `/{collection}?query={json}` | — | Find with optional JSON query |
| `GET` | `/{collection}/count` | — | Count all in the collection |
| `PATCH` | `/{collection}` | `{"query": ..., "update": ...}` | Update matching docs |
| `DELETE` | `/{collection}` | JSON query | Delete matching docs |
| `GET` | `/{collection}/typed/{name}` | — | Typed `findOne` example (returns `User` record or 404) |

## Sample session

```bash
# Verify connectivity
curl http://localhost:8080/ping
# → {"data":"pong"}

curl http://localhost:8080/hello
# → {"name":"oxidb-server","version":"0.28.18","wire_version":1,"stable_surface_version":"1.0",...}

# Insert a user
curl -X POST http://localhost:8080/users \
  -H 'Content-Type: application/json' \
  -d '{"name":"Alice","age":30,"active":true}'
# → {"id":1}

# Find all active users (JSON query as query param)
curl 'http://localhost:8080/users?query={"active":true}'
# → [{"_id":1,"name":"Alice","age":30,"active":true}]

# Typed findOne
curl http://localhost:8080/users/typed/Alice
# → {"_id":1,"name":"Alice","age":30,"active":true}

# 404 on missing
curl -i http://localhost:8080/users/typed/Nobody
# → HTTP/1.1 404 Not Found

# Update
curl -X PATCH http://localhost:8080/users \
  -H 'Content-Type: application/json' \
  -d '{"query":{"name":"Alice"},"update":{"$inc":{"age":1}}}'
# → {"modified":1}

# Count
curl http://localhost:8080/users/count
# → {"count":1}

# Cleanup
curl -X DELETE http://localhost:8080/users \
  -H 'Content-Type: application/json' -d '{}'
# → {"deleted":1}
```

## Exception → HTTP status mapping

The controller maps OxiDB's typed exceptions onto standard HTTP codes:

| Exception | HTTP status |
|---|---|
| `OxiDbNotFoundException` | 404 |
| `OxiDbDuplicateKeyException` | 409 |
| `OxiDbException` (everything else) | 500 |

Spring's `@ExceptionHandler` mechanism does the routing — see
`OxiDbController` for the patterns.

## Why no starter?

The `oxidb-spring-boot-starter` would add `@AutoConfiguration` so the
`OxiDbClient` bean lands automatically when the JAR is on the classpath.
For learning the integration it's actually clearer to show the manual
wiring once, then collapse it into autoconfigure when the starter ships.
