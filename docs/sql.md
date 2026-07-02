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

Send a request with `engine: "sql"` and the reserved `sql` command. With auth
enabled, the **ReadWrite** role has full access; the **Read** role may execute
SELECT statements only (any write or DDL is denied per statement):

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

The **embedded** clients (Python `oxidb-embedded`, .NET `OxiDb.Client.Embedded`)
have the same `sql()` surface: the engine opens lazily under
`<data dir>/sql` on the first call — no env var needed in embedded mode.

### Cluster mode

SQL **writes** (any statement that isn't a SELECT) are replicated through
Raft: the SQL string and params are appended to the Raft log and re-executed
on every node. SELECT-only requests run node-locally. All cluster nodes must
run with `OXIDB_SQL=1`.

## Data Types

| SQL type (aliases) | Stored as |
|--------------------|-----------|
| `INT` (`INTEGER`, `BIGINT`, `SMALLINT`, `TINYINT`) | 64-bit integer |
| `DOUBLE` (`DOUBLE PRECISION`, `FLOAT`, `REAL`) | 64-bit float |
| `TEXT` (`VARCHAR`, `CHAR`, `STRING`, `NVARCHAR`) | UTF-8 string |
| `BOOL` (`BOOLEAN`) | boolean |
| `TIMESTAMP` (`DATETIME`) | epoch milliseconds (64-bit integer) |

Integer values implicitly widen into `DOUBLE` columns (`INSERT ... VALUES (5)`
stores `5.0`) and into `TIMESTAMP` columns (taken as epoch milliseconds).
Timestamp literals are also supported:

```sql
INSERT INTO events VALUES (TIMESTAMP '2026-01-02 03:04:05');
SELECT * FROM events WHERE ts >= TIMESTAMP '2026-01-01';   -- also 'YYYY-MM-DDTHH:MM:SS[.fff][Z|±HH:MM]'
```

Comparisons use SQL three-valued logic; `NULL` never equals anything
(`IS NULL` / `IS NOT NULL` test for it).

## DDL

```sql
CREATE TABLE users (
  id    INT PRIMARY KEY,      -- NOT NULL + uniqueness enforced on every write
  name  TEXT NOT NULL,
  age   INT
);
CREATE TABLE IF NOT EXISTS users (...);
DROP TABLE users;
DROP TABLE IF EXISTS users;

CREATE INDEX idx_users_age ON users (age);           -- single-column index
CREATE INDEX idx_users_geo ON users (country, city); -- multi-column index
CREATE INDEX IF NOT EXISTS idx_users_age ON users (age);
DROP INDEX idx_users_age;
DROP INDEX IF EXISTS idx_users_age;
```

A duplicate `PRIMARY KEY` value is rejected with a `duplicate key` error —
on plain INSERTs, multi-row INSERTs (checked across the whole batch before
anything is applied), UPDATEs, and inside transactions.

Secondary indexes serve equality seeks on single-table SELECTs (a
multi-column index applies when the WHERE clause has `col = value` conjuncts
for **all** of its columns) and are maintained automatically on writes.

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
ORDER BY age DESC, n ASC LIMIT 10 OFFSET 20;
```

- `WHERE` with `AND`/`OR`/`NOT`, comparisons, arithmetic, `IS [NOT] NULL`,
  `[NOT] IN (v1, v2, ...)`
- `ORDER BY` on columns, expressions, or projection aliases; `ASC`/`DESC`
- `LIMIT n` / `OFFSET n`

### UNION

```sql
SELECT name FROM customers
UNION            -- distinct; UNION ALL keeps duplicates
SELECT name FROM suppliers
ORDER BY 1 LIMIT 10 OFFSET 5;   -- outer clauses apply to the combined result
```

`UNION` arms must have the same column count; the output takes the left arm's
column names. The outer `ORDER BY` uses output column names or 1-based
positions. (`EXCEPT` / `INTERSECT` are not supported.)

### Subqueries

```sql
-- Uncorrelated: evaluated once per statement.
SELECT id FROM orders WHERE total > (SELECT AVG(total) FROM orders);
SELECT id FROM orders WHERE customer_id IN (SELECT id FROM vip);

-- Correlated: references to the enclosing query's tables re-execute the
-- subquery per outer row (inner names shadow outer ones, per SQL scoping).
SELECT id FROM emp e
WHERE salary = (SELECT MAX(salary) FROM emp x WHERE x.dept = e.dept);
```

A scalar subquery must return one column and at most one row (zero rows =
`NULL`); `IN (SELECT ...)` takes a one-column result. Correlation reaches one
level up and also works in UPDATE/DELETE (the target table is the outer
scope); correlated subqueries are not allowed inside aggregated queries or
window functions.

### Views

```sql
CREATE VIEW region_totals AS
  SELECT region, SUM(amount) AS total FROM sales GROUP BY region;
CREATE OR REPLACE VIEW region_totals AS SELECT ...;
SELECT * FROM region_totals WHERE total > 100;   -- filter/join like a table
DROP VIEW region_totals;
```

A view stores its SELECT and re-executes it whenever referenced (always-fresh
results). The body is validated by a trial run at creation. Views are
read-only and share the table namespace (no collisions).

### Window functions

```sql
SELECT dept, salary,
       ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn,
       RANK()       OVER (PARTITION BY dept ORDER BY salary DESC) AS rk,
       SUM(salary)  OVER (PARTITION BY dept)                      AS dept_total,
       SUM(salary)  OVER (PARTITION BY dept ORDER BY salary)      AS running
FROM emp;
```

`ROW_NUMBER`, `RANK`, `DENSE_RANK`, and the aggregates
(`COUNT/SUM/AVG/MIN/MAX`) over a window. Without `ORDER BY` in the window an
aggregate covers the whole partition; with it, the aggregate is cumulative
and peer rows (equal sort keys) share the value — the standard default frame.
Window functions are allowed in the SELECT list (and its ORDER BY aliases)
of non-aggregated queries; to filter on a window result, wrap it in a view or
subselect-free outer pattern. Explicit frames (`ROWS BETWEEN ...`) are not
supported.

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

## Limitations

- Derived tables (`FROM (SELECT ...)`), `EXCEPT`, `INTERSECT`, `DISTINCT`,
  and explicit window frames are not supported
- Correlated subqueries reach one level up and are not allowed inside
  aggregated queries or window functions
- Plain `UNIQUE` column constraints are accepted but not enforced
  (`PRIMARY KEY` **is** enforced)
- Transactions are single-writer and their reads are not index-accelerated;
  `CREATE/DROP VIEW` are not allowed inside a transaction
- No cross-engine transactions (document + SQL) — see ADR-0011 for the
  proposed design

## See Also

- [Server Configuration](server.md) — environment variables
- [Protocol Reference](protocol-reference.md) — wire command reference
- [Transactions](transactions.md) — document engine transactions (separate mechanism)
