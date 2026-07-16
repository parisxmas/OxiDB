import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "SQL Engine",
  description:
    "OxiDB's standalone relational SQL engine — DDL, DML, joins, CTEs, window functions, transactions, instant online ALTER TABLE, an EF Core provider, and backup/restore.",
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg> SQL Engine</h2>
    <p class="section-desc">A full relational SQL engine mounted alongside the document engine in the same process. It owns entirely separate files &mdash; a collection name and a table name never collide. Off by default; zero cost when unused.</p>

    <h3>Enable it</h3>
    <pre><code class="lang-bash"><span class="co"># Server: set OXIDB_SQL=1</span>
OXIDB_SQL=1 ./oxidb-server

<span class="co"># SQL data lives under &lt;OXIDB_DATA&gt;/sql (override with OXIDB_SQL_DATA)</span></code></pre>

    <h3>Send SQL over the wire</h3>
    <p>Every request is length-prefixed JSON. A SQL request carries <code>"engine": "sql"</code>:</p>
    <pre><code class="lang-json">{ "engine": "sql", "cmd": "sql", "sql": "SELECT * FROM users WHERE age &gt; ?", "params": [18] }</code></pre>
    <p><code>params</code> binds <code>?</code> / <code>$N</code> placeholders left-to-right. The reply is <code>{ok, data:[ ...one result per statement... ]}</code>.</p>

    <h3>DDL &mdash; tables, types, constraints</h3>
    <pre><code class="lang-sql">CREATE TABLE users (
  id     INT PRIMARY KEY AUTO_INCREMENT,
  name   TEXT NOT NULL,
  email  TEXT UNIQUE,
  age    INT DEFAULT 0,
  joined TIMESTAMP,
  price  DECIMAL(10,2)
);

CREATE INDEX idx_age ON users (age);
CREATE UNIQUE INDEX idx_email ON users (email);</code></pre>
    <p>Types: <code>INT</code>, <code>DOUBLE</code>, <code>DECIMAL(p,s)</code> (exact), <code>TEXT</code>, <code>BLOB</code>, <code>BOOL</code>, <code>TIMESTAMP</code> (epoch-ms, ISO-8601 auto-detected).</p>

    <h3>Instant, online <code>ALTER TABLE</code></h3>
    <p>Adding or dropping a column is <strong>O(1)</strong> &mdash; metadata-only, no row rewrite, no downtime. Works on a 500M-row live table without blocking. A later checkpoint reclaims a dropped column's space.</p>
    <pre><code class="lang-sql">ALTER TABLE users ADD COLUMN score INT DEFAULT 0;   <span class="co">-- instant</span>
ALTER TABLE users DROP COLUMN score;                 <span class="co">-- instant</span>
ALTER TABLE users RENAME COLUMN name TO full_name;</code></pre>

    <h3>DML</h3>
    <pre><code class="lang-sql">INSERT INTO users (name, email, age) VALUES ('Ada', 'ada@x.io', 36);
INSERT INTO users (name, age) VALUES ('Bob', 25), ('Eve', 30);   <span class="co">-- multi-row</span>

UPDATE users SET age = age + 1 WHERE name = 'Ada';
DELETE FROM users WHERE age &lt; 18;

INSERT INTO users (name) VALUES ('Kim') RETURNING id, name;       <span class="co">-- RETURNING</span></code></pre>

    <h3>SELECT &mdash; joins, aggregates, windows</h3>
    <pre><code class="lang-sql"><span class="co">-- INNER / LEFT / RIGHT / FULL / CROSS / LATERAL joins</span>
SELECT u.name, o.total
FROM users u
LEFT JOIN orders o ON o.user_id = u.id;

<span class="co">-- GROUP BY + HAVING</span>
SELECT age, COUNT(*), AVG(price)
FROM users GROUP BY age HAVING COUNT(*) &gt; 1;

<span class="co">-- Window functions</span>
SELECT name, age,
       ROW_NUMBER() OVER (ORDER BY age DESC) AS rank,
       AVG(age) OVER () AS avg_age
FROM users;

<span class="co">-- DISTINCT ON (argmax), aggregate DISTINCT, LIMIT/OFFSET</span>
SELECT DISTINCT ON (age) name, age FROM users ORDER BY age, name;</code></pre>

    <h3>CTEs, recursion &amp; set operations</h3>
    <pre><code class="lang-sql">WITH adults AS (SELECT * FROM users WHERE age &gt;= 18)
SELECT COUNT(*) FROM adults;

WITH RECURSIVE nums(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM nums WHERE n &lt; 10
)
SELECT * FROM nums;

SELECT id FROM a UNION SELECT id FROM b;      <span class="co">-- also EXCEPT / INTERSECT (+ ALL)</span></code></pre>

    <h3>Transactions</h3>
    <p>Per-engine transactions over one connection (session). Buffered until commit.</p>
    <pre><code class="lang-sql">BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;   <span class="co">-- or ROLLBACK; SAVEPOINT / ROLLBACK TO also supported</span></code></pre>

    <h3>Sequences &amp; stored procedures</h3>
    <pre><code class="lang-sql">CREATE SEQUENCE order_seq START WITH 1000;
SELECT NEXT VALUE FOR order_seq;               <span class="co">-- EF Core HiLo keys</span>

CREATE PROCEDURE give_raise AS BEGIN
  UPDATE users SET age = age + 1;
END;
CALL give_raise();</code></pre>

    <h3>EF Core &amp; ADO.NET (.NET)</h3>
    <p>A full EF Core provider passes all <strong>3832 official EF Core relational specification tests</strong> and beats PostgreSQL across the EF Core benchmark. Migrations, scaffolding, LINQ, and <code>ExecuteUpdate</code>/<code>ExecuteDelete</code> all work.</p>
    <pre><code class="lang-csharp">// EF Core
options.UseOxiDb("Host=127.0.0.1;Port=4444");
var adults = db.Users.Where(u =&gt; u.Age &gt;= 18).OrderBy(u =&gt; u.Name).ToList();
db.Database.Migrate();

// ADO.NET / Dapper
using var conn = new OxiDbConnection("Host=127.0.0.1;Port=4444");
var rows = conn.Query&lt;User&gt;("SELECT * FROM users WHERE age &gt; @a", new { a = 18 });</code></pre>
    <pre><code class="lang-bash">dotnet add package OxiDb.EntityFrameworkCore
dotnet add package OxiDb.Data          <span class="co"># ADO.NET, Dapper-ready</span></code></pre>

    <h3>Backup &amp; restore</h3>
    <p>The SQL engine has its own consistent, <strong>low-lock online backup</strong> &mdash; the archive compresses with the engine lock released, so queries and writes keep running. Admin-only.</p>
    <pre><code class="lang-json">{ "engine": "sql", "cmd": "backup",  "path": "/backups/sql.tar.gz" }
{ "engine": "sql", "cmd": "restore", "archive": "/backups/sql.tar.gz", "target": "/data/restored" }</code></pre>

    <h3>Durability</h3>
    <p>Crash-atomic checkpoints via a MANIFEST + generations: each checkpoint writes a whole new generation and promotes it with a single atomic rename, so catalog and snapshots can never disagree after a crash. A CRC'd WAL bridges the rest. Node-local (not Raft-replicated) in v1.</p>
  </div>
</section>` }} />
}
