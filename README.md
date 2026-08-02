<p align="center">
  <img src="logo.png" alt="OxiDB" width="500">
</p>

<p align="center">One database process, several shapes of data: JSON documents, SQL tables, time series, a Redis-compatible key-value store, S3-compatible object storage, and MQTT/AMQP messaging — behind one binary, with no configuration to start.</p>

<p align="center"><strong>Clients:</strong> <a href="python/">Python</a> · <a href="go/">Go</a> · <a href="dotnet/">.NET</a> · <a href="oxidb-js/">JS/TS</a> · <a href="julia/">Julia</a> · <a href="swift/">Swift</a> · <a href="php/">PHP</a> · <a href="oxidb-client-ffi/">C FFI</a> · <a href="oxidb-wasm/">WASM</a> · <a href="oxidb-vscode/">VS Code</a></p>

---

> ⚠️ **WARNING — pre-1.0 stability.** OxiDB is under active pre-1.0 development. The on-disk data format, the wire/server protocol, the client SDK surface, and the JSON query language are all subject to **breaking changes between releases** with no migration path or backward-compatibility guarantee. Pin a specific version, expect to dump-and-reload on upgrade, and treat any production-like use as experimental until a `1.0` release explicitly commits to stability. The test suite protects against regressions *within* a version, not breaking changes *across* versions.

**Adoption status:** pre-1.0. Path to 1.0 in [ADR-0003](docs/decisions/0003-1.0-stability-scope.md); release policy in [ADR-0004](docs/decisions/0004-phase-0-answers.md). All architectural decisions: [`docs/decisions/`](docs/decisions/).

## Install

Download a binary from [Releases](https://github.com/parisxmas/OxiDB/releases) — macOS (arm64/x86_64), Linux (x86_64/arm64), Windows:

```bash
tar xzf oxidb-server-*.tar.gz
./oxidb-server
```

Or build it, or run it in Docker:

```bash
cargo run --release --package oxidb-server     # needs Rust 1.70+
docker compose up -d
```

It listens on `127.0.0.1:4444` and writes to `./oxidb_data/`. Nothing else is required — every surface below is off until you give it a port.

```bash
echo '{"cmd":"insert","collection":"users","document":{"name":"Ada","age":36}}' | nc 127.0.0.1 4444
```

## One engine, several shapes

Each surface is a first-class citizen, not an adapter: they share the process, the durability machinery and the security model, and each is disabled until enabled.

| Surface | Enable with | What it is |
|---|---|---|
| **Documents** | on by default | Schemaless JSON, MongoDB-style query and aggregation — plus geospatial and graph queries |
| **SQL** | `OXIDB_SQL=1` | A relational engine with its own files — tables, joins, transactions, an EF Core provider |
| **PostgreSQL wire** | `OXIDB_PG_PORT` | The PostgreSQL v3 protocol against the SQL engine — psql, psycopg, JDBC, Npgsql and DBeaver connect unmodified |
| **Time series** | `OXIDB_TSDB=1` | Gorilla-compressed series, retention by block, continuous rollups |
| **Key-value** | `OXIDB_OXIMEM_PORT` | RESP wire protocol — real Redis clients, 50+ commands |
| **Objects** | `OXIDB_S3_PORT` | S3 API — `aws-cli`, `boto3` and the MinIO SDKs work unmodified |
| **Messaging** | `OXIDB_MQTT_PORT`, `OXIDB_AMQP_PORT` | MQTT 3.1.1 and AMQP 0-9-1 — `pika` and the RabbitMQ clients work unmodified |
| **Logs** | `OXIDB_GELF_PORT`, `OXIDB_MSGPACK_PORT` | Structured log ingestion with retention and alerting |
| **HTTP** | `OXIDB_HTTP_PORT`, `OXIDB_WS_PORT` | REST, a PostgREST-compatible surface, and realtime over WebSocket |

Databases are isolated ([ADR-0012](docs/decisions/0012-multi-database.md)): one process hosts many, each with its own directory, and every surface takes a `db` selector.

## Documents

Collections are created on first insert. No schema, no migration step.

**Queries** — `$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$in` `$nin` `$exists` `$regex` `$elemMatch` `$all` `$size` `$not` `$type` `$mod` `$and` `$or` `$nor` `$expr`

**Updates** — `$set` `$unset` `$inc` `$mul` `$min` `$max` `$rename` `$currentDate` `$push` `$pull` `$addToSet` `$pop`, with dot notation for nested fields

**Aggregation** — `$match` `$group` `$sort` `$skip` `$limit` `$project` `$count` `$unwind` `$addFields` `$lookup` `$facet` `$replaceRoot` `$out`, plus stages built for time-shaped data: `$setWindowFields` (document *and* time frames), `$dateHistogram` (bucketed, with empty buckets filled), `$ohlcv` (ticks to candles), `$densify` and `$fill` (gap generation, LOCF/linear/constant)

**Indexes** — field, unique, composite, full-text, vector, and TTL, all with automatic backfill. The planner picks by estimated selectivity, sorts straight off a B-tree when one covers the order, and answers a covered `$group` without reading documents at all.

**Full-text search** — BM25 ranking with Lucene's defaults, Snowball stemming in 18 languages, `<mark>` snippets, and text extracted from HTML, XML, JSON, PDF, DOCX, XLSX and images (OCR, `--features ocr`) — for stored files as well as documents.

**Vector search** — k-NN with cosine, Euclidean and dot-product metrics; exact for small collections, HNSW for large, optional GPU acceleration through wgpu (`--features gpu`).

**Geospatial** — `$geoWithin` (`$centerSphere`, `$box`) and `$near`/`$nearSphere` with meter distances and nearest-first ordering, over GeoJSON points, `[lon, lat]` pairs or `{lat, lon}` objects. Backed by a geohash index whose every candidate is verified against the live document — the index can be generous but never wrong. Shapes that cannot be answered correctly (planar `$center`, polygons) are refused by name rather than answered approximately.

**Graph** — `$graphLookup` (breadth-first traversal issuing one `$in` per frontier, so an index on the connect field serves the whole walk; cycle-safe, prunable mid-traversal) and `$shortestPath` (Dijkstra over an edge collection with lazily fetched adjacency) run inside the aggregation pipeline. Ceilings err loudly — never a silent partial answer.

**Transactions** — optimistic concurrency with a three-phase commit; writes buffer until commit and collections lock in a fixed order, so deadlock is not possible. The exact guarantee and its anomaly scorecard are written down in [`docs/isolation.md`](docs/isolation.md) and pinned by tests. An abandoned transaction expires (`OXIDB_TX_MAX_IDLE_SECS`, default 300 s): a client that vanishes mid-transaction cannot park state — or `FOR UPDATE` locks — on the server forever, and a late touch is told `TransactionExpired`, not "not found".

**Read snapshots** ([ADR-0017](docs/decisions/0017-mvcc-lite-read-snapshots.md)) — aggregation is snapshot-consistent by default, so a report can never see half a transfer, and `snapshot_begin`…`snapshot_end` gives an explicit point-in-time view. The write path is untouched: with no snapshot open the cost is one atomic load per write.

## SQL

A second engine ([ADR-0010](docs/decisions/0010-sql-engine-crate.md)) with entirely separate files — a collection name and a table name can never collide.

DDL and DML, `INNER`/`LEFT`/`RIGHT`/`FULL` joins, `GROUP BY`/`HAVING`, secondary indexes, parameterised statements and interactive transactions. Analytics: CTEs including `WITH RECURSIVE`, set operations, `LATERAL` joins, `DISTINCT ON`, ordered-set aggregates, window functions, and calendar-correct date arithmetic.

**Constraints are enforced, not just parsed** — composite `PRIMARY KEY`, `UNIQUE` (declared or `CREATE UNIQUE INDEX`, validated against existing rows), single-column `FOREIGN KEY` with `ON DELETE` `CASCADE`/`SET NULL`/`RESTRICT`, `NOT NULL`, `VARCHAR(n)` length, and integer width (`SMALLINT` out of range is an error, never a silent widening). What cannot be enforced is refused by name.

**Disk-first by default** — rows, primary keys and every index are mapped files bounded by the checkpoint interval, not by row count: a warm 1.2M-row, index-heavy database costs ~39 MB of process memory, and the sparse row index costs 0.69 bytes per row. Measured against stock PostgreSQL 18 on identical data, 5 of 8 query workloads run at parity ([`docs/query-benchmark.md`](docs/query-benchmark.md)). **Group commit** lets concurrent writers share fsyncs — a flat ~266 writes/s at any concurrency becomes ~1.2k/s at 16 connections.

`ALTER TABLE ADD/DROP COLUMN` is metadata-only — O(1) on a table of any size, with the physical rewrite folded into a later checkpoint.

**Stored procedures** come in two languages ([ADR-0014](docs/decisions/0014-cobra-stored-procedures.md)): SQL text, and compiled **Cobra** bytecode run by a VM with a fuel cap and determinism checked at creation, so a call replicates safely.

**Entity Framework Core** — a full provider with migrations and scaffolding. It passes the official EF Core relational specification suite, 3832 of 3832, across all twelve Northwind suites. It runs **embedded** too: `UseOxiDb("Path=./mydata")` runs the whole EF Core stack in-process with no server, SQLite-style — the same `DbContext` points at a server by changing one connection string. And EF Core also works over the **unmodified Npgsql provider** through the PostgreSQL wire port.

## Time series

An InfluxDB-shaped engine where each series is a Gorilla-compressed column — delta-of-delta timestamps, XOR floats — partitioned into sealed blocks, so retention drops whole blocks instead of deleting points.

Typed fields (float, integer, boolean, string). Aggregations include mean, sum, min, max, count, first, last, distinct, plus `rate` for counters and interpolated percentiles. Continuous rollups materialise completed buckets into derived measurements with a crash-safe watermark, so a restart never double-counts. Writes accept InfluxDB line protocol.

## Talking to it

**Wire protocol** — length-prefixed JSON over TCP, or the compact OxiWire binary framing. One request shape for every engine.

**PostgreSQL wire** ([ADR-0023](docs/decisions/0023-postgres-wire-protocol.md)) — `OXIDB_PG_PORT` speaks the PostgreSQL v3 protocol against the SQL engine, verified with real drivers rather than the spec: **psql 18, psycopg 3, Npgsql in its default mode, pgjdbc** (including `DatabaseMetaData` introspection) **and DBeaver** connect unmodified, over the same SCRAM accounts and TLS as the native port. System-catalog queries are answered with PostgreSQL's real column sets filled from OxiDB's schema; what cannot be answered truthfully is refused by name, because an empty result would be believed.

**REST** — JSON over HTTP for documents, aggregation, indexes, procedures, SQL and storage.

**PostgREST-compatible** ([ADR-0019](docs/decisions/0019-postgrest-rest-surface.md)) — `/rest/v1/{table}` speaks PostgREST's URL grammar, so `@supabase/postgrest-js` and other PostgREST clients work **unmodified**. Filters, `or`/`and` trees, ordering, pagination, resource embedding, and full CRUD, over documents, SQL tables *and* time series.

**Realtime** — WebSocket subscriptions with change streams (`watch`/`unwatch`, filtered, resumable by token), JWT authentication, and row-level security rules evaluated per document.

**S3** — Put/Get/Head/Delete/Copy, ListObjectsV2 with prefixes and continuation, multipart upload, range and conditional reads, tagging, lifecycle expiry, SSE-S3/SSE-C, and SigV4 including presigned URLs.

**RESP** — strings, hashes, lists, sets, sorted sets, pub/sub, `MULTI`/`EXEC`/`WATCH`, Lua via `EVAL`, and persistence.

**MQTT 3.1.1** — publish/subscribe with honest QoS 1 and 2 ([ADR-0015](docs/decisions/0015-durable-mqtt-qos.md)): a `PUBACK` means the message is on disk, retained messages and sessions survive a restart, and the guarantee is the one the level actually promises rather than a best effort.

**AMQP 0-9-1** ([ADR-0016](docs/decisions/0016-amqp-protocol.md)) — exchanges (direct, fanout, topic), queues, bindings, acknowledgements, prefetch and publisher confirms, spoken well enough that `pika`, the .NET client and the Go client connect unmodified. Durable queues survive `SIGKILL`. Messages bridge between AMQP and MQTT, so a sensor publishing MQTT can be consumed by a worker speaking AMQP.

## Logs and alerting

The database is also a place to put logs, without another system to run.

**GELF** (`OXIDB_GELF_PORT`) receives Graylog-format messages over UDP, reassembles chunked ones, and indexes every field it finds — Elasticsearch-style dynamic mapping — so arbitrary structured queries work immediately. **MessagePack** (`OXIDB_MSGPACK_PORT`) is the cheaper sibling: a compact binary frame, and deliberately *no* per-field indexing, because a log stream is append-only and rarely queried by arbitrary field. Both batch their writes and take their own collection and retention.

**Retention** is per collection — `set_retention` with a day count, swept by the same TTL machinery that expires documents. **Alerting** evaluates count and aggregation thresholds in the background, fires webhooks with a cooldown, and records what it did in `_alert_history`.

The server can also *emit* to a GELF or MessagePack endpoint (`OXIDB_GELF_ADDR`, `OXIDB_MSGPACK_ADDR`), including to itself — which is how a single process ends up storing its own request log.

## Durability

Every write goes through a WAL with CRC32 checksums, and a commit is acknowledged only after the WAL fsync, the data fsync and the commit-log fsync. Documents live in an mmap'd data file with a small resident offset index, so memory does not scale with the retention window; set `OXIDB_DISK_FIRST=0` for the older all-resident mode.

The WAL checkpoints online — it seals rather than truncates, so a checkpoint never races a writer — and a transaction's writes are invisible on disk until its commit point, which is what makes a transaction spanning several collections all-or-nothing after a crash. The SQL and time-series engines checkpoint by writing a whole new generation and promoting it with a single atomic `MANIFEST` rename — a crash on either side of the rename leaves the previous generation in force; unchanged tables are hard-linked rather than rewritten.

**Point-in-time recovery** (opt-in, `OXIDB_PITR`) stamps every durable WAL record with a global sequence number and a wall clock, archives sealed segments with a self-healing manifest, and rebuilds to any GSN, timestamp or `Latest` on top of a base backup — with transactionally consistent cuts. **Backup and restore** are low-lock for the SQL and time-series engines: the slow compression runs with the lock released.

Crash behaviour is not assumed. `SIGKILL` drills, a Jepsen-style bank that kills the process mid-commit and checks conservation, byte-offset kill matrices and fsync fault injection all live in-tree and run with `cargo test -- --ignored`.

## Security

TLS, SCRAM-SHA-256 authentication, role-based access control (Admin / ReadWrite / Read), audit logging with rotation and compression, and AES-256-GCM encryption at rest with per-record nonces. On the HTTP surfaces: JWT verification and per-collection security rules, including row-level expressions and per-identity rate limits.

Nine fuzz targets live in [`fuzz/`](fuzz/), four mutation-based and two differential against the canonical `redis-rs` and `pgwire` implementations.

## Scale

**Raft replication** — multi-node clustering via OpenRaft with leader election, sub-second failover and HAProxy-compatible health checks. SQL writes replicate too; read-only statements run node-locally.

**Sharding** — the OxiPool proxy routes by CRC32 of a per-collection shard key: targeted when the key is present, scatter-gather with merge when it is not, broadcast for DDL. Cross-shard transactions are detected and refused rather than half-applied.

**Observability** — `explain` returns the real plan with timings, a slow-query profiler records anything past `OXIDB_SLOW_QUERY_MS`, and Prometheus metrics are exposed on the REST listener.

## Tools

A CLI with an interactive shell (embedded or client mode), a VS Code extension with a collection browser and query editor, [OxiDB Studio](oxidb-app/) — a Tauri desktop app with a SQL editor and data grid — and a WASM build of the document engine that runs in a browser with OPFS persistence. The [geo globe demo](https://oxidb.baltavista.com/demo/geo/) is that WASM build live: 10,000 cities, `$near`/`$geoWithin` and `$shortestPath` routing, entirely in the browser.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `OXIDB_ADDR` | `127.0.0.1:4444` | Wire protocol listen address |
| `OXIDB_DATA` | `./oxidb_data` | Data directory |
| `OXIDB_POOL_SIZE` | `4` | Worker threads |
| `OXIDB_DISK_FIRST` | `1` | Documents in an mmap'd file; `0` keeps them resident |
| `OXIDB_SQL` / `OXIDB_TSDB` | off | Enable the SQL / time-series engines |
| `OXIDB_SQL_DISK_FIRST` | `1` | SQL rows and indexes as mapped files; `0` keeps them resident |
| `OXIDB_PG_PORT` | off | PostgreSQL wire listener (needs `OXIDB_SQL=1`) |
| `OXIDB_DOC` | on | `0` runs the server without the document engine (SQL/TSDB only) |
| `OXIDB_TX_MAX_IDLE_SECS` | `300` | Idle interactive transactions roll back; `0` = never |
| `OXIDB_HTTP_PORT` / `OXIDB_WS_PORT` | off | REST and realtime listeners |
| `OXIDB_S3_PORT` / `OXIDB_OXIMEM_PORT` | off | S3 and RESP listeners |
| `OXIDB_MQTT_PORT` / `OXIDB_AMQP_PORT` | off | Messaging listeners |
| `OXIDB_GELF_PORT` / `OXIDB_MSGPACK_PORT` | off | Log ingestion listeners |
| `OXIDB_AUTH` / `OXIDB_JWT_SECRET` | off | SCRAM authentication / JWT verification |
| `OXIDB_TLS_CERT` / `OXIDB_TLS_KEY` | — | TLS certificate and key |
| `OXIDB_ENCRYPTION_KEY` | — | Path to a 32-byte key for encryption at rest |
| `OXIDB_PITR` | off | Point-in-time recovery |
| `OXIDB_AUDIT` | off | Audit log |
| `OXIDB_SLOW_QUERY_MS` | off | Record queries slower than this |
| `OXIDB_NODE_ID` / `OXIDB_RAFT_PEERS` | — | Raft cluster membership |

More knobs than these exist — the server prints every one it read with `--verbose`, and the docs index is [`docs/`](docs/README.md).

## Book (Turkish)

A full-length Turkish book, **_OxiDB Doküman Veritabanı_** — a ground-up, code-free explanation of document databases and then how OxiDB works — lives in [`books/belge-veritabanlari/`](books/belge-veritabanlari/). ~116 pages, with 27 hand-drawn diagrams.

📖 **[Download the PDF](https://github.com/parisxmas/OxiDB/raw/master/books/belge-veritabanlari/belge-veritabanlari.pdf)**

## License

As of **v0.40.0**, OxiDB is **source-available** — see [LICENSE](LICENSE). Read it, modify it, and run it in production for your own applications and business, free, at any scale.

Two things need a commercial license: **offering OxiDB as a service** to third parties, and **distributing it** — on its own or embedded in your product. See [COMMERCIAL-LICENSE.md](COMMERCIAL-LICENSE.md) for terms and contact.

The thin TCP client libraries are MIT-licensed, redistribution included — shipping one inside your application needs no license.

Each prior release keeps the license it was published with: early releases `MIT OR Apache-2.0`, through v0.32.x `AGPL-3.0-only`, and v0.33.0–v0.39.x proprietary.

### Contribution

By submitting a contribution you agree it may be distributed under the source-available and commercial licenses above — see [COMMERCIAL-LICENSE.md](COMMERCIAL-LICENSE.md#6-contributions).
