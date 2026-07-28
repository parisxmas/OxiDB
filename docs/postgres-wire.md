# PostgreSQL Wire Protocol

OxiDB speaks the **PostgreSQL v3 frontend/backend protocol** on its own port, so
PostgreSQL clients connect to it unmodified. The listener serves the
[SQL engine](sql.md); it is a separate listener end to end and changes nothing
about the native OxiWire port.

## Client support

Verified against the real drivers, not inferred:

| Client | Status |
|---|---|
| **psql** 18 | Queries, DDL, transactions, `\dt`, `\l`. `\d <table>` is refused (see Limitations). |
| **psycopg** 3.3 | Everything tested works: extended protocol, server-side parameters, `executemany`, typed exceptions, transaction recovery, `fetchmany`. |
| **Npgsql** 8.0 | Everything tested works, including the type catalog it loads on connect — no `Server Compatibility Mode` flag needed. |
| **pgjdbc** 42.7 | Everything tested works, including `DatabaseMetaData`: `getTables`, `getColumns`, `getPrimaryKeys` (composite keys included), `getIndexInfo`, `getImportedKeys`, `getTypeInfo`, `getSchemas`. |
| pg_dump, BI tools | Not supported. |

## Enabling

```bash
OXIDB_SQL=1 OXIDB_PG_PORT=5432 oxidb-server
```

| Variable | Default | Meaning |
|---|---|---|
| `OXIDB_PG_PORT` | `0` (off) | Port to listen on. `5432` makes existing connection strings work unchanged. |
| `OXIDB_SQL` | off | **Required** — this port serves the SQL engine. Without it a connection is refused with a message saying so. |
| `OXIDB_TLS_CERT` / `OXIDB_TLS_KEY` | unset | When set, `sslmode=require` works; otherwise TLS is declined and `sslmode=prefer` continues in plaintext. |
| `OXIDB_AUTH` | off | When on, clients authenticate with SCRAM-SHA-256 against the same accounts as the native port. |

```bash
psql -h 127.0.0.1 -p 5432 -U admin -d oxidb
psycopg.connect("host=127.0.0.1 port=5432 user=admin dbname=oxidb")
```

The `database` in the connection string selects an OxiDB database
([ADR-0012](decisions/)); it is opened on first use exactly as the native port
opens it, and both ports see the same data.

## Authentication

With `OXIDB_AUTH` on, the server offers **SCRAM-SHA-256** — the same verifiers
(salt, iteration count, stored key, server key) the native port checks, so an
account works on both without being re-created. Authentication is mutual: the
client verifies the server's signature too.

An account created before SCRAM verifiers existed has none to check a proof
against, and is offered cleartext password instead (use TLS, or reset the
password to generate a verifier). A failed login always answers
`28P01 password authentication failed`, whatever went wrong — a distinct "no
such user" would let anyone enumerate accounts.

Roles carry over: a `read` role may only run SELECT/SHOW, and a write is
refused with `42501 insufficient_privilege`, the same gate the native port
applies.

## What works

- **Simple and extended query protocols** — `psql` uses the first, drivers the
  second. Parameters (`$1`, or `%s` as psycopg spells it) bind server-side.
- **Transactions**: `BEGIN`/`COMMIT`/`ROLLBACK`, with the transaction-status
  byte clients depend on (`I` idle, `T` in a transaction, `E` failed). After a
  failed statement the transaction is poisoned until `ROLLBACK`, and `COMMIT`
  answers `ROLLBACK` — PostgreSQL's behaviour, which is what makes psycopg's
  `with conn.transaction():` recover correctly.
- **Errors as SQLSTATEs**, so drivers raise the right exception class:
  `23505` unique violation, `23503` foreign key, `23502` not-null, `42P01`
  undefined table, `42703` undefined column, `42601` syntax, `0A000`
  unsupported, `55P03` lock timeout, `42501` permission denied.
- **Row limits** — `fetchmany` suspends and resumes the portal.
- `SET`/`RESET`/`SHOW`/`DISCARD`, `SELECT version()`, `current_database()`,
  `current_user`, `current_schema()`.
- **Mixed batches** — one simple-query message may carry both statements this
  server answers itself and statements the engine runs, in any order. (Npgsql
  opens with exactly that.)
- **The type catalog** a driver loads on connect (`pg_type` and friends),
  answered from the fixed list of types this server has.
- **JDBC `DatabaseMetaData`** — tables, columns (with `VARCHAR(n)` lengths),
  primary keys (every part of a composite one), indexes, foreign keys, types
  and schemas, all built from the engine's own catalog.
- **psql `\dt` and `\l`**, answered from the engine's own catalog.

## Type mapping

| SQL engine | PostgreSQL type (OID) | Notes |
|---|---|---|
| `INT` | `int8` (20) | The engine's integer is 64-bit; calling it `int4` would truncate. |
| `DOUBLE` | `float8` (701) | `NaN`/`Infinity` use PostgreSQL's spelling. |
| `TEXT` | `text` (25) | |
| `BOOL` | `bool` (16) | `t`/`f` in text format. |
| `TIMESTAMP` | `timestamp` (1114) | Epoch milliseconds, rendered ISO. |
| `BLOB` | `bytea` (17) | Hex (`\x…`) format. |
| `DECIMAL` | `numeric` (1700) | Exact, as text. |

A column whose type the engine cannot infer statically is described from the
first non-NULL value in the result, falling back to `text`.

Results are sent in text format unless the client asks for binary, which is
supported for `bool`, `int2/4/8`, `float4/8`, `text` and `bytea`. Binary
`numeric` and `timestamp` are refused by name rather than mis-encoded.

## Limitations

This is a v1 aimed at `psql` and `psycopg`. What is missing is refused with an
error naming it — never answered with a plausible-looking empty result.

- **The system catalogs are not queryable.** What is answered is a fixed set of
  *known questions* — the type catalog, JDBC's `DatabaseMetaData` calls, psql's
  `\dt` and `\l` — each matched on a marker unique to it and answered from the
  engine's own catalog. An arbitrary `pg_catalog` or `information_schema` query,
  including psql's `\d <table>`, is refused with `0A000` pointing at
  `DESCRIBE <table>`, `SHOW TABLES` and `SHOW INDEXES`. Nothing is answered
  with an empty result, which would be believed.

  The consequence to know: matching is **per-driver and per-version text**. A
  driver upgrade that rewrites one of these queries loses the match and gets
  the refusal — loudly, which is the intended failure. It will not quietly
  return the wrong answer.
- No `COPY`, `LISTEN`/`NOTIFY`, or `DECLARE ... CURSOR` (psycopg's *named*
  server-side cursors; unnamed cursors and `fetchmany` do work).
- No query cancellation: `CancelRequest` is accepted and ignored.
- No real schemas. A qualified name resolves to its last part, so
  `public.users` and `users` are the same table.
- Prepared-statement parameter types are not inferred: a parameter the client
  leaves unspecified is described as `text`, and the engine coerces it per
  target column.
- The listener is not started in cluster mode (like every other optional
  listener). The write path refuses writes if Raft is ever active, because they
  would not replicate.

## See also

- [SQL Reference](sql.md) — the dialect this port speaks
- [Server Configuration](server.md) — all environment variables
