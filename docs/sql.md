# SQL Engine

OxiDB can mount a **standalone relational SQL engine** alongside the document
engine in the same server (ADR-0010). It is a real SQL engine with typed
tables, its own storage, WAL, crash recovery, secondary indexes, and
transactions — not a translation layer over document collections.

The two engines share **no state**:

- SQL tables and document collections live in different namespaces — a
  collection named `users` and a SQL table named `users` never collide.
- The SQL engine owns entirely separate files under `${OXIDB_DATA}/sql`
  (override with `OXIDB_SQL_DATA`).
- It is **off by default** and costs nothing when unused.

## Enabling

```bash
OXIDB_SQL=1 oxidb-server
```

| Variable | Default | Description |
|----------|---------|-------------|
| `OXIDB_SQL` | off | Set to `1`/`true`/`yes`/`on` to enable the SQL engine |
| `OXIDB_SQL_DATA` | `${OXIDB_DATA}/sql` | SQL engine data directory |

## Wire Protocol

Send a request with `engine: "sql"` and the reserved `sql` command
(requires the **ReadWrite** role when auth is enabled):

```json
{ "engine": "sql", "cmd": "sql", "sql": "SELECT * FROM users WHERE id = $1", "params": [1] }
```

`params` is optional and binds `?` / `$N` placeholders left-to-right.
Requests without an `engine` field (or with `engine: "doc"`) are served by the
document engine exactly as before.

The response contains **one result per statement**:

```json
{ "ok": true, "data": [ { "columns": ["id", "name"], "rows": [[1, "ada"]] } ] }
```

| Statement | Result shape |
|-----------|--------------|
| `SELECT` | `{"columns": [...], "rows": [[cell, ...], ...]}` |
| `INSERT` / `UPDATE` / `DELETE` | `{"affected": N}` |
| `CREATE` / `DROP` (table or index) | `{"ddl": true}` |
| `BEGIN` / `COMMIT` / `ROLLBACK` | `{"transaction": true}` |

Cells are JSON scalars; `TIMESTAMP` values are epoch milliseconds.

### REST

With the HTTP API enabled (`OXIDB_HTTP_PORT`), the same engine is reachable at
`POST /api/sql` (write-level role when JWT auth is enabled):

```json
{ "sql": "SELECT name FROM users WHERE id = ?", "params": [1] }
```

→ `{ "results": [ { "columns": ["name"], "rows": [["ada"]] } ] }` — or HTTP
400 with `{"error": "..."}`.

## Client Libraries

```python
# Python (oxidb >= 0.27)
db.sql("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
db.sql("INSERT INTO users VALUES (?, ?)", [1, "ada"])
[result] = db.sql("SELECT name FROM users WHERE id = $1", [1])
```

```go
// Go
_, err := c.Sql("INSERT INTO users VALUES (?, ?)", 1, "ada")
res, err := c.Sql("SELECT name FROM users WHERE id = $1", 1)
// res[0].Columns, res[0].Rows
```

```csharp
// .NET (OxiDb.Client.Tcp)
await client.SqlAsync("INSERT INTO users VALUES (?, ?)", new object?[] { 1, "ada" });
var results = await client.SqlAsync("SELECT name FROM users WHERE id = $1", new object?[] { 1 });
```

```javascript
// JS (oxidb >= 0.25, via REST)
await db.sql("INSERT INTO users VALUES (?, ?)", [1, "ada"]);
const [res] = await db.sql("SELECT name FROM users WHERE id = $1", [1]);
```

The embedded clients have no SQL surface — the engine is server-side.

## Data Types

| SQL type (aliases) | Stored as |
|--------------------|-----------|
| `INT` (`INTEGER`, `BIGINT`, `SMALLINT`, `TINYINT`) | 64-bit integer |
| `DOUBLE` (`DOUBLE PRECISION`, `FLOAT`, `REAL`) | 64-bit float |
| `TEXT` (`VARCHAR`, `CHAR`, `STRING`, `NVARCHAR`) | UTF-8 string |
| `BOOL` (`BOOLEAN`) | boolean |
| `TIMESTAMP` (`DATETIME`) | epoch milliseconds (64-bit integer) |

Comparisons use SQL three-valued logic; `NULL` never equals anything
(`IS NULL` / `IS NOT NULL` test for it).

## DDL

```sql
CREATE TABLE users (
  id    INT PRIMARY KEY,      -- PRIMARY KEY implies NOT NULL (no uniqueness check in v1)
  name  TEXT NOT NULL,
  age   INT
);
CREATE TABLE IF NOT EXISTS users (...);
DROP TABLE users;
DROP TABLE IF EXISTS users;

CREATE INDEX idx_users_age ON users (age);   -- single-column secondary index
CREATE INDEX IF NOT EXISTS idx_users_age ON users (age);
DROP INDEX idx_users_age;
DROP INDEX IF EXISTS idx_users_age;
```

Secondary indexes serve `WHERE column = value` equality seeks on single-table
SELECTs and are maintained automatically on writes.

## DML

```sql
INSERT INTO users VALUES (1, 'ada', 36);
INSERT INTO users (id, name) VALUES (2, 'bob'), (3, 'eve');  -- multi-row = one fsync, atomic
UPDATE users SET age = age + 1 WHERE id = 1;
DELETE FROM users WHERE age IS NULL;
```

A multi-row `INSERT` is durably applied as a single WAL batch: one fsync for
the whole statement, and all rows are validated before any is applied.

## SELECT

```sql
SELECT * FROM users;
SELECT name AS n, age FROM users WHERE age >= 18 AND name <> 'root'
ORDER BY age DESC, n ASC LIMIT 10;
```

- `WHERE` with `AND`/`OR`/`NOT`, comparisons, arithmetic, `IS [NOT] NULL`
- `ORDER BY` on columns, expressions, or projection aliases; `ASC`/`DESC`
- `LIMIT n`

### Joins

`INNER`, `LEFT`, `RIGHT`, and `FULL` joins, chained to any depth:

```sql
SELECT r.name, SUM(it.qty * p.price) AS rev
FROM regions r
JOIN customers c ON c.region_id = r.id
JOIN orders o    ON o.customer_id = c.id
JOIN items it    ON it.order_id = o.id
JOIN products p  ON p.id = it.product_id
GROUP BY r.name
HAVING SUM(it.qty * p.price) > 1000
ORDER BY rev DESC;
```

Equi-joins (`a.x = b.y` conjuncts in the `ON`) execute as hash joins — or as
direct-address array joins for densely-packed integer keys — with late
materialization; non-equi `ON` conditions fall back to nested loops. See
`oxidb-sql/BENCHMARKS.md` for the executor design and a differential benchmark
against PostgreSQL 15.

### Aggregates

`COUNT(*)`, `COUNT(expr)`, `SUM`, `AVG`, `MIN`, `MAX`, with `GROUP BY`
(expressions allowed) and `HAVING`.

## Parameters

`?` placeholders bind left-to-right; `$1`, `$2`, … bind by position:

```json
{ "engine": "sql", "cmd": "sql",
  "sql": "SELECT * FROM users WHERE age > ? AND name <> ?",
  "params": [18, "root"] }
```

## Transactions

```sql
BEGIN;
INSERT INTO accounts VALUES (1, 100);
UPDATE accounts SET balance = balance - 10 WHERE id = 1;
COMMIT;   -- or ROLLBACK;
```

Writes inside a transaction are buffered (with read-your-writes) and flushed
atomically as one WAL batch on `COMMIT`. Transaction control statements must
appear in the same request string in v1. An unmatched `BEGIN` at the end of a
request is rolled back.

## v1 Limitations

- No implicit `INT` → `DOUBLE` coercion — write `5.0` for a `DOUBLE` column
- No SQL timestamp literal — `TIMESTAMP` columns are populated via parameters
  (epoch ms) or the programmatic API
- `PRIMARY KEY` implies `NOT NULL` only; uniqueness is not enforced
- Single-column secondary indexes
- No `OFFSET`, subqueries, `UNION`, views, or window functions
- Transactions are single-writer and their reads are not index-accelerated
- Node-local: SQL statements are **not** Raft-replicated in cluster mode
- No read-only SQL role: the `sql` command requires **ReadWrite**

## See Also

- [Server Configuration](server.md) — environment variables
- [Protocol Reference](protocol-reference.md) — wire command reference
- [Transactions](transactions.md) — document engine transactions (separate mechanism)
