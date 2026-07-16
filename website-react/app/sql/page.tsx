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

    <h3>Sequences</h3>
    <pre><code class="lang-sql">CREATE SEQUENCE order_seq START WITH 1000;
SELECT NEXT VALUE FOR order_seq;               <span class="co">-- EF Core HiLo keys</span></code></pre>

    <h3>Stored procedures &mdash; two languages</h3>
    <p>Procedures come in two flavours (ADR-0014). Both are registered with <code>CREATE PROCEDURE</code>, invoked with <code>CALL</code>, run inside the caller's transaction, and replicate under Raft.</p>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Language</th><th>Body</th><th>Best for</th></tr></thead>
        <tbody>
          <tr><td><strong>SQL text</strong></td><td><code>AS BEGIN &hellip; END</code>, re-parsed per <code>CALL</code></td><td>Zero toolchain &mdash; just SQL statements</td></tr>
          <tr><td><strong>Cobra</strong></td><td>Compiled <code>.cobrac</code> bytecode, run by the in-server VM</td><td>Real control flow &mdash; loops, branches, locals</td></tr>
        </tbody>
      </table>
    </div>

    <h4>SQL-text procedures</h4>
    <pre><code class="lang-sql">CREATE PROCEDURE give_raise(pct INT) AS BEGIN
  UPDATE users SET salary = salary + salary * pct / 100;
END;
CALL give_raise(5);</code></pre>

    <h3>Cobra &mdash; compiled procedures</h3>
    <p><strong>Cobra</strong> is the compiled procedure language. You author a small program, compile it to portable <code>.cobrac</code> bytecode, and the server runs it on a built-in Rust VM &mdash; no toolchain on the server, no <code>cgo</code>, no sidecar. The program defines <code>run(db, &hellip;params)</code>; <code>db.query</code> / <code>db.execute</code> go through the same executor, so they <strong>join the <code>CALL</code>'s transaction</strong> and every write is atomic with the rest.</p>
    <pre><code class="lang-python"><span class="co"># transfer.cobra — real logic, then compiled to bytecode</span>
<span class="kw">def</span> run(db, from_id, to_id, amount):
    db.execute(<span class="str">"UPDATE accounts SET balance = balance - ? WHERE id = ?"</span>, [amount, from_id])
    db.execute(<span class="str">"UPDATE accounts SET balance = balance + ? WHERE id = ?"</span>, [amount, to_id])
    rows = db.query(<span class="str">"SELECT balance FROM accounts WHERE id = ?"</span>, [from_id])
    print(<span class="str">"remaining:"</span>, rows[<span class="num">0</span>][<span class="str">"balance"</span>])   <span class="co"># print() -> query notices</span>
    <span class="kw">return</span> rows                                   <span class="co"># list of dicts -> a result set</span></code></pre>
    <pre><code class="lang-bash"><span class="co"># 1. compile to portable bytecode, then base64 it</span>
cobra build --portable transfer.cobra transfer.cobrac
B64=$(base64 -i transfer.cobrac)</code></pre>
    <pre><code class="lang-sql"><span class="co">-- 2. register the compiled procedure and call it</span>
CREATE PROCEDURE transfer(from_id INT, to_id INT, amount DECIMAL)
  LANGUAGE COBRA AS '&lt;base64 of transfer.cobrac&gt;';

CALL transfer(1, 2, 100);      <span class="co">-- both UPDATEs + the SELECT run in ONE transaction</span>
SHOW PROCEDURES;               <span class="co">-- lists each proc with its language column</span></code></pre>
    <p>Return shaping: a returned list-of-dicts becomes a table, a single dict becomes one row, a scalar becomes a one-column result; anything <code>print</code>ed comes back as <strong>notices</strong> alongside the rows. Cobra procedures are <strong>deterministic by construction</strong> &mdash; async, imports, and all I/O are rejected at <code>CREATE</code> time and a 100M-instruction fuel cap bounds every <code>CALL</code>, so a procedure replicates identically on every Raft node.</p>

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
