# ADR-0023: PostgreSQL wire protocol as a separate listener

**Status:** Accepted — v1 (startup + SCRAM auth, simple and extended query,
transactions, SQLSTATE mapping, session/catalog interception, the driver type
catalog and JDBC's `DatabaseMetaData`) landed & tested 2026-07-28, verified
against real `psql` 18, `psycopg` 3.3, **Npgsql 8.0** and **pgjdbc 42.7**.
**Supersedes:** —
**Related:** [ADR-0010](0010-sql-engine.md) (the engine this exposes),
[ADR-0012](0012-multi-database.md) (the `database` startup parameter resolves
through the same registry),
[ADR-0013](0013-dotnet-ef-core.md) (the other "speak someone else's protocol"
decision),
[ADR-0016](0016-amqp-protocol.md) (the precedent: a stateful binary protocol
added as an independent listener),
[`oxidb-server/src/pg/`](../../oxidb-server/src/pg/) (the implementation),
[`docs/postgres-wire.md`](../postgres-wire.md) (the user-facing reference).

## Context

OxiDB's SQL engine can be reached three ways: OxiWire (length-prefixed JSON
over TCP), the REST surface, and the PostgREST-compatible surface. All three
need an OxiDB-specific client, or at least an OxiDB-specific URL. The .NET
story is the exception, and only because ADR-0013 wrote a whole EF Core
provider to get it.

The generic version of that problem is the wire protocol itself. An enormous
amount of software speaks the PostgreSQL v3 frontend/backend protocol — `psql`,
psycopg, pgjdbc, npgsql, every ORM and BI tool built on them. Speaking it means
that software connects to OxiDB with a changed connection string and nothing
else: no driver to write, no dialect to register, no shim to maintain.

The question this ADR answers: **can OxiDB serve the PostgreSQL wire protocol
without disturbing its own protocol, and what is the honest boundary of a first
version?**

Three properties of the existing codebase make the answer cheap:

1. **Listeners are already independent.** The server hosts nine of them
   (OxiWire, RESP, MQTT, AMQP, S3, REST, WebSocket, GELF, MsgPack), each opt-in
   behind its own `OXIDB_*_PORT`, each a thread per connection. AMQP 0-9-1
   (~1500 lines) is the proof that a stateful binary protocol can be added
   without touching the native path.
2. **SCRAM-SHA-256 already exists server-side**, with stored per-user verifiers.
   PostgreSQL carries SCRAM inside a SASL envelope, but the messages *inside*
   that envelope are the RFC 5802 ones `scram.rs` already speaks.
3. **The engine already has the shapes the protocol needs**: `$N` parameters,
   per-connection interactive transactions parked between requests, one result
   per statement, and statically-known column types.

## Decision

Add `oxidb-server/src/pg/` — codec, auth, session, catalog, types, errors — and
one optional listener (`OXIDB_PG_PORT`, off by default). **Nothing in the
OxiWire path changes.** The listener reaches the same per-database engines
through `sql_bridge`, the same accounts through `auth`, and the same verifiers
through `scram`.

Four decisions inside that shape the result:

### A portal runs once, at Describe or Execute — whichever comes first

PostgreSQL describes a portal without executing it. This server cannot: the
engine reports a result's shape by producing it. Answering `Describe(portal)`
with `NoData` (the honest "I don't know yet") breaks psycopg, which reads its
column metadata from exactly there.

So a portal executes on first touch and buffers its output; `Execute` streams
that buffer, honouring the row limit. The only visible difference from
PostgreSQL is that a statement's error surfaces one message earlier — still
inside the same batch, still before the `Sync` that ends it.

### Session and catalog statements never reach the engine

`SET extra_float_digits = 3` is sent by every client before it will talk at
all, and the engine's parser rejects `SET` outright. `SET`, `RESET`, `SHOW`,
`DISCARD`, `version()`, `current_database()` and friends are answered by
`pg/catalog.rs`.

`\dt` and `\l` are answered from the engine's own catalog. **Everything else in
`pg_catalog` is refused with `0A000` naming what is unsupported**, rather than
answered with an empty result — an empty result is believed. (This was not
theoretical: answering psql's `\d <table>` probe with the table list made psql
fail on a column it expected, which is how the refusal got written.)

### Errors are SQLSTATEs first, messages second

Clients recover on the code, not the text: psycopg raises `UniqueViolation` for
`23505` and `UndefinedTable` for `42P01` regardless of wording. Every
`SqlError` variant maps to the code PostgreSQL would use, and the engine's own
message is carried through unchanged.

The failed-transaction state is part of this. The engine rolls a transaction
back on a statement error; PostgreSQL keeps it poisoned until `ROLLBACK` and
answers `COMMIT` with `ROLLBACK`. The session emulates that, because psycopg's
`with conn.transaction():` recovery depends on it.

### The type catalog is answered; per-table introspection is not

The original scope stopped at psql + psycopg on the assumption that npgsql and
pgjdbc needed a queryable `pg_catalog`. Probing the real drivers showed that
was wrong in a useful way:

- **pgjdbc already worked** for queries, prepared statements, transactions and
  SQLSTATEs. Only `DatabaseMetaData` failed.
- **Npgsql already worked** in full, given a connection-string flag
  (`Server Compatibility Mode=NoTypeLoading`) — and what it wanted without the
  flag was the **type** catalog, not the table catalog.

That distinction is the decision. The type catalog's content is *static*: the
fixed list of types this server can produce, with their real PostgreSQL OIDs so
each driver's existing handler applies unchanged. It is answered from a
constant, and the two follow-up queries (composite fields, enum labels) are
answered **empty — which is true**: this server has no composite types and no
enums. Per-table metadata is not static and is still refused, so nothing here
invents an answer it does not have.

Executing these queries for real would be the *harder* path, not the easier
one: they use `~`, `::regclass`, `pg_get_expr()` and window functions the
engine would have to grow first.

The same reasoning extends to JDBC's `DatabaseMetaData`: those queries are
unanswerable as written (they use `information_schema._pg_expandarray`,
`pg_get_indexdef`, `generate_series`, `current_schemas`), but the *questions*
are ones the engine's catalog answers directly — which tables, which columns,
which keys, which indexes. So each is matched and answered from `list_tables`,
`table_def`, `list_indexes` and `Table::foreign_keys`.

The cost is that the match is per-driver and per-version text, and a driver
upgrade can change it. That is why the fallback is a refusal rather than an
empty result — a stale match fails loudly.

**Matching must be keyed on a marker unique to the query, not on the tables it
reads.** Getting this wrong is the one way this design produces a *silently*
wrong answer, and it did during development: `getIndexInfo` matched a broad
"mentions `pg_class`" rule and came back holding the **table list** under index
column names. A caller cannot tell that from a correct answer. Every matcher is
now keyed on an alias only that call uses (`self_referencing_col_name`,
`index_qualifier`, `elemtypoid`, `fkcolumn_name`), and `tests/pg_wire.rs` pins
the property that no catalog query is ever answered in another one's shape.

## Consequences

**What this buys.** `psql -h host -p 5432 -U user -d oxidb` works: DDL, CRUD,
transactions, `\dt`, `\l`, errors rendered as PostgreSQL renders them. psycopg
connects with an unchanged connection string, binds parameters server-side,
raises typed exceptions, and recovers from a poisoned transaction. Both are
pinned by tests — `pg_wire.rs` at the byte level, `pg_e2e.rs` through the real
driver.

**What it costs.** A tenth protocol to keep working. The mitigation is that it
owns no state: every request is a call into machinery that already existed, so
the failure modes are the engine's, not a second implementation's.

**What is deliberately missing** (each refused with an error naming it):
arbitrary catalog queries — so psql's `\d <table>`, `pg_dump` and BI tools;
`COPY`; `LISTEN`/`NOTIFY`; `DECLARE … CURSOR`; query cancellation; real schemas
and `information_schema`; server-side parameter type inference.

**A lesson about scoping from documentation.** Every prediction here about
which drivers would work was wrong until the drivers were actually run: pgjdbc
and Npgsql were both far closer to working than the protocol documentation
suggested, and Npgsql's *observed* failure (`unsupported sql: function
version()`) pointed at a statement that was already handled — the real cause
was a multi-statement batch mixing intercepted and engine statements. A
logging proxy that dumped the exact bytes found it in minutes; reasoning about
it did not.

**Cluster mode.** The listener is not started in cluster mode, as no optional
listener is. The write path refuses writes when Raft is active anyway, because
a write accepted here would not replicate — the guard costs one already-cached
parse and removes a way to diverge a replica.

**A follow-on this makes cheap.** If the catalog is ever wanted, it is additive
and isolated: `pg/catalog.rs` grows, or the tables get materialized in the
engine, and nothing else in the listener changes.
