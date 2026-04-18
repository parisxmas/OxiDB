# Changelog

## v0.25.1

### Query Engine

- Add `$not` operator — negate any field condition; missing fields return true (MongoDB-compatible)
- Add `$nor` top-level operator — match documents where none of the conditions are true
- Add `$all` operator — array must contain all specified values
- Add `$size` operator — match arrays with exact length
- Add `$type` operator — match by JSON type (string, number, bool, array, object, null, int)
- Add `$mod` operator — modulo arithmetic on numeric fields (`[divisor, remainder]`)
- Add `$expr` top-level operator — cross-field comparisons (`{"$expr": {"$gt": ["$sold", "$stock"]}}`)
- Add `$elemMatch` operator — match array elements against sub-queries with AND semantics
- Refactor `matches_doc` and `matches_value` into shared `eval_field_op` helper

### Go Client

- Add stored procedure methods: `CreateProcedure`, `CreateProcedureFromScript`, `CallProcedure`, `ListProcedures`, `GetProcedure`, `DeleteProcedure`, `CompileOxiScript`
- Add `CreateTTLIndex` for automatic document expiration
- Add retention policy methods: `SetRetention`, `GetRetention`, `DeleteRetention`, `ListRetentions`
- Add alerting methods: `CreateAlert`, `GetAlert`, `DeleteAlert`, `ListAlerts`, `TestAlert`, `ListAlertHistory`
- Add `ExtractText` for blob text extraction (PDF, DOCX, HTML)
- Add `Backup` and `Restore` for full database backup/restore
- Add `SetDialect` for SQL dialect switching (mysql, postgresql, mssql, generic)

### Tests

- 107 query engine tests (53 new), including 15 real-world scenario tests covering fraud detection, loan eligibility, matchmaking, property search, supply chain, content management, and more

## v0.25.0

- Bump all workspace crates to v0.25.0
- Add Python client retention, alerting, and TTL methods
- Add `oxidb-tail` TUI log viewer with table columns, stats toggle, and keyboard shortcuts

## v0.24.0

- Add WebAssembly support — OxiDB runs in the browser
- Add JavaScript/TypeScript SDK (`oxidb` npm package) — zero dependencies, REST + WebSocket
- Add JWT authentication for REST and WebSocket APIs
- Add WebSocket server with real-time subscriptions
- Add per-document security rules (Firebase-style access control)
- Add TTL indexes with automatic document expiration
- Add REST HTTP API with CORS and 64-thread pool

## v0.23.1

- Add TTL indexes, REST HTTP API, OxiScript tests, Julia client updates

## v0.23.0

- Add stored procedures (OxiScript and JavaScript)
- Add cron scheduling for procedures
- Add multi-database support (create, drop, use, list)
- Add SQL dialect support (MySQL, PostgreSQL, MSSQL, Generic)

## v0.22.2

- Add GELF ingestion, retention policies, and alerting system
- Add GELF chunked reassembly and FTS stemming with accent normalization
- Add GPU-accelerated vector search via wgpu compute shaders

## v0.22.0

- Add OxiMem in-memory key-value layer (RESP protocol, redis-cli compatible)
- Add MQTT v3.1.1 protocol with cross-protocol pub/sub
- Add sorted sets (ZADD, ZRANGE, etc.)

## v0.20.4

- Add vector similarity search (cosine, euclidean, dot product)
- Add pipeline command batching

## v0.20.0

- Add S3-compatible blob storage with full-text search
- Add backup and restore commands

## v0.19.0

- Add SCRAM-SHA-256 authentication
- Add RBAC (Admin, ReadWrite, Read roles)
- Add TLS support
- Add audit logging

## v0.18.0

- Initial public release
- Core document engine with append-only storage, WAL, field indexes
- SQL and JSON query support
- ACID transactions with OCC
- Aggregation pipeline
- Full-text search
- Python, Go, .NET, PHP, Swift, Julia client libraries
