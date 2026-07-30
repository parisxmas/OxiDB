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
| `OXIDB_DOC` | **on** | Set to `0` to run the server **without the document engine** — SQL/TSDB only. The one engine switch that is on by default. See below. |
| `OXIDB_SQL_DATA` | `${OXIDB_DATA}/sql` | SQL engine data directory |
| `OXIDB_SQL_SYNC` | `full` | WAL durability: `full` = true storage flush per commit (survives power loss); `data` = OS-cache-level sync (PostgreSQL's default class, several times faster) |
| `OXIDB_SQL_DISK_FIRST` | off | Keep table data on disk (mmap'd last-checkpoint snapshot) with only post-checkpoint changes in RAM, instead of holding every row resident. Same on-disk format either way — a database can be reopened in either mode. Indexes and the PRIMARY KEY map stay in RAM. |
| `OXIDB_SQL_CHECKPOINT_BYTES` | 64 MiB | Auto-checkpoint when the live WAL exceeds this many bytes: folds the WAL into per-table `.rdat` snapshots and truncates it (bounds restart replay time, and bounds the RAM overlay in disk-first mode). `0` disables auto-checkpointing. |
| `OXIDB_SQL_REPLAY_FOLD_OPS` | `rows / 24`, at least 50 000 | Row operations replayed between folds when opening a disk-first database — which is what sets the open-time memory peak, since a replayed record stays in RAM until a fold moves it to disk. Derived from the database's own row count by default, because both sides of the trade are per-row: a fixed interval holds the peak to 1.7× the steady state at 1.2M rows but spends 9 extra seconds reaching 1.3× at 9.6M, where the transient is already small next to what the database costs to run. Set this to override; `0` never folds mid-replay (fastest open, largest peak). See [the memory benchmark](pg-memory-benchmark.md). |

At 1M rows (4 columns, PK), disk-first cuts resident memory roughly in half
(272 → 143 MB) and opens faster; full scans pay a decode cost (11 → 43 ms).
Mapped snapshot pages are clean file pages the OS can evict under memory
pressure, so the effective floor is lower than RSS suggests.

### Memory at startup

Two things the engine deliberately does *not* do when it opens a database:

- **Secondary indexes are built on first use, not at open.** An index that
  exists in the catalog costs a column list until a query needs it, at which
  point it is built from the current rows. Writes to a table skip any index that
  is not built yet — it will see them when it is built — so this trades startup
  memory and time for one scan on the first query that seeks. `CREATE INDEX`
  itself still builds immediately: that is work the caller asked for.
- **In disk-first mode, a replayed WAL tail is folded at open.** Records past
  the last checkpoint replay into the in-memory overlay, and only a checkpoint
  moves them into the mmap'd snapshot — so without this a restart inherited the
  previous process's pending WAL as resident memory and held it until the next
  write. Measured at 1M rows, a 55 MB tail cost 60 MB of overlay. The replay
  therefore folds periodically, which is what bounds the open-time peak: it is a
  straight line in pending operations (~370 bytes each), so the fold interval
  *is* the peak. Because the steady state is per-row too, that interval scales
  with the row count rather than being fixed, which holds the peak near 1.65× the
  steady state at any size.

The steady state used to be unbounded too — about 33 bytes a row, essentially all
of it the `.rdat` row-offset index, so 100M rows would have cost ~3.1 GB of
anonymous memory. That index is now **sparse**: one entry per 32 records, with a
walk of record headers to reach a specific row, which is 0.69 bytes a row (~69 MB
at 100M). Sequential readers use a cursor and never walk, so scans are unaffected.
Disk-first mode now holds no per-row structure at all — rows, secondary indexes,
primary keys, `UNIQUE` columns and the row index are all mapped files. Measured in
[the memory benchmark](pg-memory-benchmark.md).

A checkpoint **reuses the files of any table that has not changed** since the
generation it is based on, hard-linking them into the new generation instead of
writing them again. Without that, a checkpoint cost the whole database however
little had changed (~1s per 1.2M rows here), which is what made folding often
enough to bound the peak unaffordable. Both are measured in
[the memory benchmark](pg-memory-benchmark.md), which also
sets out honestly where OxiDB still loses to PostgreSQL: index and primary keys
are held in RAM, so a workload that uses every index pays for every index, while
PostgreSQL never exceeds its `shared_buffers` cap.

### SQL-only servers (`OXIDB_DOC=0`)

The document engine is the server's default and starts unconditionally. Setting
`OXIDB_DOC=0` turns it off, leaving a SQL (and/or time-series) server:

```bash
OXIDB_DOC=0 OXIDB_SQL=1 OXIDB_PG_PORT=5432 oxidb-server
```

What changes: no document data directory is created (`$OXIDB_DATA` gets only
`sql/`), no per-database TTL-eviction or alert threads run, no scheduler, and
every document command is refused with an error naming the switch — rather than
being served from a store the operator asked not to have. `ping` still answers,
so health checks keep working. Measured on an idle server: **9.8 MB RSS and 8
threads, against 14.2 MB and 23 threads** with the document engine on.

Two configurations are refused at startup rather than half-served:

- **No engine at all** — `OXIDB_DOC=0` without `OXIDB_SQL=1` or `OXIDB_TSDB=1`
  would leave nothing to serve.
- **A document-backed listener** — REST, WebSocket, S3, MQTT, AMQP, GELF,
  MessagePack and OxiMem all store their state in document collections. The
  error names each offending variable and why. `OXIDB_ADDR` (OxiWire, for SQL
  and TSDB requests) and `OXIDB_PG_PORT` both work without documents.

Cluster mode is not supported with `OXIDB_DOC=0` — Raft replicates document
operations through the same log, so a SQL-only cluster is separate work, and the
server says so instead of starting half-gated.

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
| `SELECT` | `{"columns": [...], "types": [...], "rows": [[cell, ...], ...]}` — `types` holds statically-known column types (`"INT"`/`"DOUBLE"`/`"TEXT"`/`"BOOL"`/`"TIMESTAMP"`, `null` = unknown) |
| `INSERT` / `UPDATE` / `DELETE` | `{"affected": N}` — INSERTs that assigned AUTO_INCREMENT values add `"last_insert_id"` |
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
| `BIGINT` (`INT8`, or a bare integer type) | 64-bit integer |
| `INT` (`INTEGER`, `INT4`) | 64-bit integer, **range-checked to 32 bits** |
| `SMALLINT` (`INT2`) | 64-bit integer, **range-checked to 16 bits** |
| `TINYINT` | 64-bit integer, **range-checked to 8 bits** |
| `DOUBLE` (`DOUBLE PRECISION`, `FLOAT`, `REAL`) | 64-bit float |
| `TEXT` (`VARCHAR`, `CHAR`, `STRING`, `NVARCHAR`) | UTF-8 string |
| `BOOL` (`BOOLEAN`) | boolean |
| `TIMESTAMP` (`DATETIME`) | epoch milliseconds (64-bit integer) |
| `DECIMAL` (`NUMERIC`) | 64-bit float (precision/scale accepted, ignored) |
| `BLOB` (`BYTEA`, `BINARY`, `VARBINARY`) | raw bytes (base64 on the JSON wire) |

**Integer widths are a constraint, not a storage format.** Every integer is
stored as a 64-bit value whatever it was declared, so `SMALLINT` saves no
space — but a value outside the declared range is *refused* on write rather
than silently widened, so a column's declared type stays true of its contents:

```sql
CREATE TABLE t (small SMALLINT);
INSERT INTO t VALUES (32767);   -- ok
INSERT INTO t VALUES (32768);   -- ERROR: value 32768 is out of range for
                                -- column "small" declared SMALLINT (-32768..=32767)
```

`ALTER TABLE ... ALTER COLUMN ... TYPE SMALLINT` checks every stored value
before it changes anything, so narrowing either succeeds completely or leaves
the column untouched. A column declared `BIGINT` — or with no width, including
one from a database created before widths were recorded — keeps the full
64-bit range.

Integer values implicitly widen into `DOUBLE` columns (`INSERT ... VALUES (5)`
stores `5.0`) and into `TIMESTAMP` columns (taken as epoch milliseconds).
Timestamp literals are also supported:

```sql
INSERT INTO events VALUES (TIMESTAMP '2026-01-02 03:04:05');
SELECT * FROM events WHERE ts >= TIMESTAMP '2026-01-01';   -- also 'YYYY-MM-DDTHH:MM:SS[.fff][Z|±HH:MM]'
```

Comparisons use SQL three-valued logic; `NULL` never equals anything
(`IS NULL` / `IS NOT NULL` test for it). Scalar functions and operators:

```sql
SELECT COALESCE(nickname, name, 'anon') FROM users;   -- first non-NULL (short-circuits)
SELECT IFNULL(nickname, name) FROM users;             -- two-argument spelling
SELECT total / NULLIF(count, 0) FROM stats;           -- NULL when equal (÷0 guard)
SELECT grp, COALESCE(SUM(amt), 0) FROM k LEFT JOIN v ... GROUP BY grp;

SELECT CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END FROM exams;
SELECT CASE status WHEN 1 THEN 'open' WHEN 2 THEN 'closed' END FROM tickets;  -- simple form
SELECT * FROM users WHERE name LIKE 'a%';             -- %, _, [NOT] LIKE, ESCAPE 'c'
SELECT CAST(id AS TEXT), CAST('42' AS INT) FROM t;    -- NULL passes; bad parses error
SELECT UPPER(s), LOWER(s), LENGTH(s), SUBSTRING(s, 1, 3), TRIM(s), LTRIM(s), RTRIM(s),
       REPLACE(s, 'a', 'o'), CONCAT(a, '-', b), a || b, ABS(n) FROM t;
SELECT DISTINCT city FROM users;                      -- dedup after ORDER BY, before LIMIT
SELECT name FROM p WHERE EXISTS (SELECT 1 FROM orders o WHERE o.p_id = p.id);
```

String functions are character-based and NULL-propagating (`CONCAT`/`||`
return NULL if any input is NULL); `LIKE` is case-sensitive. `EXISTS`
supports correlation, including an aggregated body
(`EXISTS (SELECT SUM(x) ... HAVING ...)`).

## DDL

```sql
CREATE TABLE users (
  id    INT PRIMARY KEY,      -- NOT NULL + uniqueness enforced on every write
  name  TEXT NOT NULL,
  email TEXT UNIQUE,          -- enforced (NULLs exempt, SQL-standard)
  age   INT DEFAULT 0         -- literal defaults fill omitted INSERT columns
);
CREATE TABLE IF NOT EXISTS users (...);
DROP TABLE users;
DROP TABLE IF EXISTS users;

-- Table-level constraints (the shape EF Core migrations and pg_dump emit).
CREATE TABLE orders (
  id      INT NOT NULL AUTO_INCREMENT,
  email   TEXT,
  CONSTRAINT pk_orders PRIMARY KEY (id),
  CONSTRAINT uq_email  UNIQUE (email)
);

-- A composite PRIMARY KEY names several columns. Uniqueness is over the whole
-- tuple: two rows may share `student` or `course`, never both. Every member
-- column is implicitly NOT NULL, and none of them can later be dropped or
-- retyped. Table-level UNIQUE still takes a single column.
CREATE TABLE enrolment (
  student INT,
  course  TEXT,
  grade   INT,
  CONSTRAINT pk_enrolment PRIMARY KEY (student, course)
);

ALTER TABLE users ADD COLUMN city TEXT DEFAULT 'n/a';  -- one operation per statement
ALTER TABLE users DROP COLUMN city;                    -- blocked while an index needs it
ALTER TABLE users RENAME COLUMN name TO full_name;

CREATE INDEX idx_users_age ON users (age);           -- single-column index
CREATE INDEX idx_users_geo ON users (country, city); -- multi-column index
CREATE INDEX IF NOT EXISTS idx_users_age ON users (age);
DROP INDEX idx_users_age;
DROP INDEX IF EXISTS idx_users_age;
```

`AUTO_INCREMENT` (also `AUTOINCREMENT` and PostgreSQL's
`GENERATED ... AS IDENTITY`) is supported on an `INT PRIMARY KEY` column:

```sql
CREATE TABLE users (
  id   INT PRIMARY KEY AUTO_INCREMENT,
  name TEXT NOT NULL
);
INSERT INTO users (name) VALUES ('ada'), ('bob');  -- ids 1, 2 assigned
INSERT INTO users VALUES (NULL, 'eve');            -- NULL also draws from the counter
```

Omitted (or `NULL`) values draw sequential values from a per-table counter;
the INSERT's result carries the last one as `last_insert_id`. Explicit
values are allowed and push the counter past themselves. The counter is
seeded from `max(id) + 1` at startup (SQLite-default semantics: ids of
rows deleted from the top can be reused after a restart); rolled-back
transactions leave gaps, as in every SQL engine.

A duplicate `PRIMARY KEY` value is rejected with a `duplicate key` error —
on plain INSERTs, multi-row INSERTs (checked across the whole batch before
anything is applied), UPDATEs, and inside transactions. A composite key
collides only when every member column matches.

Secondary indexes serve equality seeks on single-table SELECTs (a
multi-column index applies when the WHERE clause has `col = value` conjuncts
for **all** of its columns) and are maintained automatically on writes.

### Databases (ADR-0012)

```sql
CREATE DATABASE crm;             -- admin-only; replicated in cluster mode
CREATE DATABASE IF NOT EXISTS crm;
DROP DATABASE crm;               -- admin-only; drops document + SQL data
DROP DATABASE IF EXISTS crm;
SHOW DATABASES;                  -- columns: database
USE crm;                         -- session default for subsequent requests
```

Each database has its own SQL engine (default at `OXIDB_SQL_DATA`, others
at `${OXIDB_DATA}/<name>/sql`). Database statements must be sent alone —
they cannot be mixed with other statements in one batch. The equivalent
wire commands are `create_database` / `drop_database` / `list_databases` /
`use_db`, and any request can target a database explicitly with a `db`
field (REST: `?db=<name>`).

### Users (wire-protocol user store)

```sql
CREATE USER ali WITH PASSWORD 'gizli' ROLE readwrite;  -- role: admin|readwrite|read (default read)
ALTER USER ali WITH PASSWORD 'yeni';                   -- and/or: ALTER USER ali ROLE admin
DROP USER [IF EXISTS] ali;
SHOW USERS;                                            -- columns: user, role, db_roles
GRANT readwrite ON DATABASE crm TO ali;                -- per-database role override
REVOKE ALL ON DATABASE crm FROM ali;
```

These manage the SCRAM user store (`_auth/users.json`) and are Admin-only,
exactly like the wire commands (`create_user` / `update_user` / `drop_user` /
`list_users` / `grant_db_role` / `revoke_db_role`) they mirror. They require
authentication to be enabled (`OXIDB_AUTH=1`) and must be sent alone (not
mixed with other statements). Not available over REST — REST authenticates
with JWT against a different user system.

### Introspection

```sql
SHOW TABLES;                -- columns: table, rows
SHOW VIEWS;                 -- columns: view, definition
SHOW INDEXES;               -- columns: index, table, columns
SHOW INDEXES FROM users;    -- only that table's indexes
DESCRIBE users;             -- columns: column, type, nullable, primary_key
SHOW COLUMNS FROM users;    -- same as DESCRIBE
```

Introspection statements are read-only: the `read` role may run them, and in
cluster mode they are served node-locally (not Raft-replicated). Inside a
transaction they see the transaction's own uncommitted DDL.

## DML

```sql
INSERT INTO users VALUES (1, 'ada', 36);
INSERT INTO users (id, name) VALUES (2, 'bob'), (3, 'eve');  -- multi-row = one fsync, atomic
UPDATE users SET age = age + 1 WHERE id = 1;
DELETE FROM users WHERE age IS NULL;

-- RETURNING projects the touched rows back as a result set (PostgreSQL-style):
INSERT INTO users (name) VALUES ('ada') RETURNING id;   -- read generated keys
UPDATE users SET age = age + 1 WHERE id = 1 RETURNING age;  -- post-update values
DELETE FROM users WHERE age IS NULL RETURNING *;            -- the deleted rows
```

A multi-row `INSERT` is durably applied as a single WAL batch: one fsync for
the whole statement, and all rows are validated before any is applied.
`RETURNING` works inside transactions and is how ADO.NET/EF Core read
generated keys and count affected rows.

## SELECT

```sql
SELECT * FROM users;
SELECT name AS n, age FROM users WHERE age >= 18 AND name <> 'root'
ORDER BY age DESC, n ASC LIMIT 10 OFFSET 20;
```

- `WHERE` with `AND`/`OR`/`NOT`, comparisons, arithmetic, `IS [NOT] NULL`,
  `[NOT] IN (v1, v2, ...)`
- `ORDER BY` on columns, expressions, or projection aliases; `ASC`/`DESC`
- `LIMIT n` / `OFFSET n` — literals or bind parameters (`LIMIT $1 OFFSET $2`)
- Derived tables: `FROM (SELECT ...) AS x`, also as a JOIN side; the alias
  is required, and the subquery may use bind parameters

### Set operations

```sql
SELECT name FROM customers
UNION            -- distinct; UNION ALL keeps duplicates
SELECT name FROM suppliers
ORDER BY 1 LIMIT 10 OFFSET 5;   -- outer clauses apply to the combined result
```

`UNION`, `EXCEPT` and `INTERSECT`, each with an `ALL` (bag-semantics) form,
and standard precedence — `INTERSECT` binds tighter than the other two. Arms
must have the same column count; the output takes the left arm's column
names. The outer `ORDER BY` uses output column names or 1-based positions.

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
`NULL`); `IN (SELECT ...)` takes a one-column result. Correlation reaches any
depth of nesting (through subqueries, derived tables and `VALUES`), works in
UPDATE/DELETE (the target table is the outer scope), and is allowed inside
aggregate arguments, grouped queries (correlating on the group key), `HAVING`,
and window-function arguments.

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

`INNER`, `LEFT`, `RIGHT`, `FULL` and `CROSS` joins, chained to any depth,
plus `[LEFT] JOIN LATERAL (SELECT ...) x ON ...` — a derived table that may
reference the rows to its left, re-executed per left row (`RIGHT`/`FULL`
LATERAL are rejected):

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
(expressions allowed) and `HAVING`. `DISTINCT` inside an aggregate
(`COUNT(DISTINCT x)`), `mode() WITHIN GROUP (ORDER BY x)`, and
`SELECT DISTINCT ON (key) ...` (first row per key, by the `ORDER BY`) are
supported.

## Stored Procedures

SQL-native stored procedures (distinct from the document engine's
[JSON/OxiScript procedures](stored-procedures.md)): a named, parameterized
batch of DML/SELECT statements, stored in the catalog and executed
atomically.

```sql
CREATE PROCEDURE yatir(kime TEXT, tutar DOUBLE) AS BEGIN
  UPDATE hesap SET bakiye = bakiye + tutar WHERE ad = kime;
  SELECT bakiye FROM hesap WHERE ad = kime;
END;

CALL yatir('ali', 25);        -- result = the LAST statement's result set
CALL yatir($1, $2);           -- arguments can be bind parameters

CREATE OR ALTER PROCEDURE yatir(...) AS BEGIN ... END;  -- replace
DROP PROCEDURE [IF EXISTS] yatir;
SHOW PROCEDURES;              -- name, params, stored definition
```

- **Parameters by name**: the body references parameters as plain
  identifiers. At creation they are rewritten to `$N` placeholders —
  expression positions only, so INSERT column lists and qualified
  `table.col` references are untouched. In expression position a parameter
  **shadows** a column of the same name (qualify the column to reach it).
- **Atomic**: a top-level `CALL` runs in an implicit transaction (any
  statement failing rolls the whole call back); inside an open transaction
  it joins it.
- **Body surface (v1)**: DML + SELECT only — no DDL, no transaction
  control, no nested `CALL`, no `$N` placeholders of the body's own.
  Arguments are coerced to the declared types (INT widens to
  DOUBLE/TIMESTAMP; type mismatches name the offending parameter).
- Procedures live in the catalog (their own namespace), are WAL-logged,
  and survive restarts. In cluster mode `CREATE/DROP PROCEDURE` and `CALL`
  replicate like any other write statement.

A 1000+ line join/math-heavy example lives at
`oxidb-sql/tests/data/complex_procedure.sql` (exercised by
`t_procedures_stress.rs`).

## Parameters

`?` placeholders bind left-to-right; `$1`, `$2`, … bind by position:

```json
{ "engine": "sql", "cmd": "sql",
  "sql": "SELECT * FROM users WHERE age > ? AND name <> ?",
  "params": [18, "root"] }
```

## Transactions

Transactions are **interactive** (ADR-0013 Phase B): `BEGIN` opens a
transaction that spans requests on the same connection — run any number of
statements across round-trips (reads see your own buffered writes; other
connections see nothing until commit), then `COMMIT` or `ROLLBACK`.
Savepoints give partial rollback:

```sql
BEGIN;
INSERT INTO accounts VALUES (1, 100);
SAVEPOINT a;
UPDATE accounts SET balance = balance - 10 WHERE id = 1;
ROLLBACK TO SAVEPOINT a;   -- the update is undone; savepoint a survives
RELEASE SAVEPOINT a;       -- forget the savepoint, keep the data
COMMIT;                    -- one atomic WAL batch, single fsync
```

A statement error aborts the transaction (auto-rollback, PostgreSQL-style
strictness without the "must ROLLBACK first" limbo); disconnecting rolls an
open transaction back; a transaction is bound to the database it began on.

Embedded/one-shot callers using `execute()` keep the old batch-scoped
contract: an unmatched `BEGIN` at the end of the request is rolled back.

Other writers may work on the same table while a transaction is open. Its
row ids and `AUTO_INCREMENT` values are reserved from the engine as it
buffers each write, so nothing it wrote can be handed to — or overwritten by
— a concurrent transaction or autocommit statement; and its uniqueness
constraints are re-checked at `COMMIT`, so a key another writer took in the
meantime **fails the commit** (duplicate-key error, nothing applied) instead
of producing two rows with one key. Retry the transaction.

`SELECT ... FOR UPDATE` takes real row locks that exclude other transactions'
writes to those rows until commit/rollback (`OXIDB_SQL_LOCK_TIMEOUT_MS`,
default 5000, bounds the wait and turns a deadlock into an error); plain
`UPDATE`s exclude each other the same way.

### Group commit

Concurrent writers **share their WAL flush**. A write appends its record and
applies it under the engine lock, releases the lock, and only then fsyncs; one
writer is elected to perform the flush and everyone whose record was already on
disk when it began is covered by it. The engine lock is therefore never held
across an fsync, so writers overlap instead of queueing behind each other.

The acknowledgement rule is unchanged: a statement returns to its caller only
after a flush that covers its own WAL sequence — nothing is acknowledged before
it is durable. What *is* newly observable is that another connection can read a
write in the window between its apply and its flush; a crash there loses it, and
the writer never got an acknowledgement. This is the same window PostgreSQL has,
and the same one the document engine's group commit already accepts.

The gain starts at about four concurrent writers. A flush may only claim records
that were on disk when it started — one appended midway through may or may not
have been included, so it is not credited — and with one or two writers no group
accumulates during the previous flush. Measured on the
[wire benchmark](wire-benchmark.md): flat ~266 writes/sec at every concurrency
before, ~1.2k/sec at 16 connections after, with p50 settling near two flush times
instead of growing linearly with the connection count.

A checkpoint also satisfies waiting writers: it fsyncs the snapshot it folded
those records into, which is the same data made durable a different way — and it
must, since the WAL they would otherwise flush may have just been truncated.

**Cluster mode**: interactive transactions work — statements run on the
leader and a lone `COMMIT` replicates the buffered writes through Raft as
one atomic entry applied on every node. Self-contained `BEGIN..COMMIT`
batches also replicate whole. `BEGIN`/`COMMIT` must each be the only
statement in their request in cluster mode.

The commit-time uniqueness re-check above is **single-node only**: a
replicated commit was already agreed by the cluster, and every node has to
apply exactly the agreed ops or diverge, so the apply path never second-
guesses them. Two transactions racing for the same key on a leader can
therefore both commit there. Row-id and `AUTO_INCREMENT` reservation is
unaffected (the leader reserves as it buffers), so no write is lost.

## Limitations

**Constraints.** Some constraint syntax is accepted and then *not* enforced —
those cases are called out here because a silently-ignored constraint is worse
than a rejected one:

- `CREATE UNIQUE INDEX` creates an ordinary index: the `UNIQUE` is **not
  enforced**. Use a column-level `UNIQUE` (or `PRIMARY KEY`) to enforce it
- A table-level `UNIQUE` naming several columns is accepted and **not
  enforced**; single-column `UNIQUE` is enforced (NULLs exempt, SQL-standard).
  `PRIMARY KEY` may be composite and is fully enforced
- `FOREIGN KEY` is enforced for single-column keys — a child INSERT/UPDATE
  must find its parent, and a parent DELETE honours `ON DELETE NO ACTION` /
  `RESTRICT` / `CASCADE` / `SET NULL`. Multi-column FKs and `ON UPDATE`
  actions are accepted and **not enforced**, and a single-column FK cannot
  reference a table whose primary key is composite
- `CHECK` constraints are rejected at parse time
- A primary key or unique constraint can only be declared with the table:
  `ALTER TABLE ... ADD/DROP CONSTRAINT` is not supported

**Queries.** Not supported: explicit window frames (`ROWS`/`RANGE BETWEEN
...`; the standard default frame is what you get), `GROUPING SETS` / `ROLLUP`
/ `CUBE`, `MERGE`, and `TRUNCATE`. `INSERT ... ON CONFLICT` parses but the
conflict clause is **ignored**, so a conflicting row raises a duplicate-key
error rather than being skipped or updated — check first, or `UPDATE` then
`INSERT`.

**Transactions.**

- A transaction that cannot commit its writes fails at `COMMIT` rather than
  earlier: uniqueness is re-checked against the committed state at that
  point, so if another writer took one of its keys while it was open, the
  commit is refused with a duplicate-key error and nothing of it lands.
  Retry the transaction
- Row ids and `AUTO_INCREMENT` values are reserved when a transaction
  buffers the insert, not at commit, so a transaction that rolls back leaves
  a gap — normal sequence behaviour, and the price of never handing two
  writers the same value
- `ALTER TABLE`, `CREATE/DROP VIEW` and `CREATE/DROP PROCEDURE` are not
  allowed inside a transaction
- No cross-engine transactions: a document-engine write and a SQL write are
  each durable on their own, never one atomic unit

## See Also

- [PostgreSQL Wire Protocol](postgres-wire.md) — reach this engine with `psql`
  and `psycopg`, unmodified (`OXIDB_PG_PORT`)
- [Server Configuration](server.md) — environment variables
- [Protocol Reference](protocol-reference.md) — wire command reference
- [Transactions](transactions.md) — document engine transactions (separate mechanism)
