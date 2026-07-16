import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Stored Procedures — SQL & Cobra",
  description:
    "OxiDB's SQL engine has two stored-procedure languages: zero-toolchain SQL-text bodies and compiled Cobra bytecode run by an in-server VM. Both CALL inside the caller's transaction and replicate under Raft.",
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg> Stored Procedures</h2>
    <p class="section-desc">The SQL engine (ADR-0014) runs procedures in <strong>two languages</strong>. Both are registered with <code>CREATE PROCEDURE</code>, invoked with <code>CALL</code>, run <strong>inside the caller's transaction</strong>, and replicate under Raft. Pick SQL text for plain statements, Cobra when you need real control flow.</p>

    <div class="table-wrap">
      <table>
        <thead><tr><th>Language</th><th>Body</th><th>Toolchain</th><th>Best for</th></tr></thead>
        <tbody>
          <tr><td><strong>SQL text</strong></td><td><code>AS BEGIN &hellip; END</code>, re-parsed per <code>CALL</code></td><td>None</td><td>A sequence of SQL statements</td></tr>
          <tr><td><strong>Cobra</strong></td><td>Compiled <code>.cobrac</code> bytecode, run by the in-server VM</td><td><code>cobra build</code> at author time</td><td>Loops, branches, locals, computed results</td></tr>
        </tbody>
      </table>
    </div>

    <h3>SQL-text procedures</h3>
    <p>The zero-toolchain path &mdash; the body is a block of SQL statements, re-parsed on every <code>CALL</code>. Parameters are referenced by name.</p>
    <pre><code class="lang-sql">CREATE PROCEDURE give_raise(pct INT) AS BEGIN
  UPDATE users SET salary = salary + salary * pct / 100;
  INSERT INTO audit(event) VALUES ('raise applied');
END;

CALL give_raise(5);</code></pre>

    <h3>Managing procedures</h3>
    <pre><code class="lang-sql">CREATE OR ALTER PROCEDURE give_raise(pct INT) AS BEGIN
  UPDATE users SET salary = salary + salary * pct / 100;
END;                          <span class="co">-- redefine in place</span>

SHOW PROCEDURES;              <span class="co">-- name, params, language, definition</span>
DROP PROCEDURE give_raise;</code></pre>

    <h3>Procedures run in the caller's transaction</h3>
    <p>Every statement a procedure runs is part of the surrounding transaction, so a failure rolls the whole thing back &mdash; the procedure is atomic with the work around it.</p>
    <pre><code class="lang-sql">BEGIN;
CALL give_raise(5);          <span class="co">-- its UPDATE + INSERT join this transaction</span>
UPDATE settings SET last_raise = NOW();
COMMIT;                       <span class="co">-- all-or-nothing</span></code></pre>

    <h2 style="margin-top:44px"><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/></svg> Cobra &mdash; compiled procedures</h2>
    <p class="section-desc"><strong><a href="https://cobralang.baltavista.com/" target="_blank" rel="noopener">Cobra</a></strong> is the compiled procedure language. You author a small program, compile it to portable <code>.cobrac</code> bytecode, and the server executes it on a built-in Rust VM &mdash; no toolchain on the server, no <code>cgo</code>, no sidecar process. The program defines <code>run(db, &hellip;params)</code>; the <code>db</code> handle goes through the same executor as ordinary SQL.</p>
    <p>Cobra is a full general-purpose language with its own docs, tooling, and playground at <a href="https://cobralang.baltavista.com/" target="_blank" rel="noopener">cobralang.baltavista.com</a>.</p>

    <h3>Write the procedure</h3>
    <p><code>db.execute(sql[, params])</code> runs a DML statement and returns the affected-row count; <code>db.query(sql[, params])</code> runs a <code>SELECT</code> and returns a list of row dicts. Both <strong>join the <code>CALL</code>'s transaction</strong>. Anything <code>print</code>ed is returned to the client as <strong>notices</strong>.</p>
    <pre><code class="lang-python"><span class="co"># transfer.cobra — real logic, compiled to bytecode</span>
<span class="kw">def</span> run(db, from_id, to_id, amount):
    <span class="kw">if</span> amount &lt;= 0:
        <span class="kw">raise</span> <span class="str">"amount must be positive"</span>       <span class="co"># aborts the CALL + its transaction</span>

    db.execute(<span class="str">"UPDATE accounts SET balance = balance - ? WHERE id = ?"</span>, [amount, from_id])
    db.execute(<span class="str">"UPDATE accounts SET balance = balance + ? WHERE id = ?"</span>, [amount, to_id])

    rows = db.query(<span class="str">"SELECT balance FROM accounts WHERE id = ?"</span>, [from_id])
    print(<span class="str">"remaining:"</span>, rows[<span class="num">0</span>][<span class="str">"balance"</span>])       <span class="co"># -> a notice</span>
    <span class="kw">return</span> rows                                    <span class="co"># list of dicts -> a result set</span></code></pre>

    <h3>Compile &amp; register</h3>
    <pre><code class="lang-bash"><span class="co"># compile to portable bytecode, then base64 it</span>
cobra build --portable transfer.cobra transfer.cobrac
B64=$(base64 -i transfer.cobrac)</code></pre>
    <pre><code class="lang-sql"><span class="co">-- register the compiled procedure (param types declared in SQL)</span>
CREATE PROCEDURE transfer(from_id INT, to_id INT, amount DECIMAL)
  LANGUAGE COBRA AS '&lt;base64 of transfer.cobrac&gt;';

CALL transfer(1, 2, 100);      <span class="co">-- both UPDATEs + the SELECT run in ONE transaction</span>
SHOW PROCEDURES;               <span class="co">-- the language column reads COBRA</span></code></pre>

    <h3>Return shaping</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Cobra <code>return</code></th><th>Wire result</th></tr></thead>
        <tbody>
          <tr><td>list of dicts</td><td>A table (column union, first-seen order; missing keys &rarr; NULL)</td></tr>
          <tr><td>a single dict</td><td>One row</td></tr>
          <tr><td>a scalar / other list</td><td>A single <code>value</code> column</td></tr>
          <tr><td>nothing / null</td><td>Empty result set</td></tr>
        </tbody>
      </table>
    </div>
    <p>Everything <code>print</code>ed comes back as <strong>notices</strong> attached to the result, so a procedure can report progress without polluting its return set.</p>

    <h3>Deterministic &amp; replication-safe</h3>
    <p>Cobra procedures are <strong>deterministic by construction</strong>. At <code>CREATE</code> time the bytecode is validated &mdash; async, imports, and every form of I/O (network, files, clocks, randomness, channels) are rejected. Each <code>CALL</code> runs under a <strong>100M-instruction fuel cap</strong>, so a runaway procedure cannot stall the server. Because a procedure can only touch the database through <code>db</code> and has no other side effects, it produces identical results on every Raft node &mdash; <code>CREATE</code>, <code>CALL</code>, and <code>DROP</code> all replicate safely.</p>

    <p>For the rest of the SQL surface &mdash; joins, CTEs, window functions, transactions &mdash; see the <a href="/sql/">SQL Reference</a>.</p>
  </div>
</section>` }} />
}
