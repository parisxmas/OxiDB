import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "How the SQL Engine Works",
  description:
    "A walk through OxiDB's SQL engine internals: the life of a query, the buffered-overlay transaction model, row locks, generation-based crash-atomic checkpoints, instant O(1) ALTER TABLE, and what it deliberately does not do (MVCC).",
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg> How the SQL Engine Works</h2>

    <p class="section-desc">OxiDB's SQL engine is a <strong>standalone relational engine</strong> — its own crate, its own files, zero shared state with the document engine. This article walks through what actually happens between <code>SELECT</code> arriving on the wire and rows coming back: parsing, execution, storage, transactions, locking, crash recovery — and the things the engine deliberately does <em>not</em> do. Everything here describes the code as it ships; where there is a trade-off, the trade-off is stated.</p>

    <h3>The shape of the thing</h3>
    <p>The engine mounts beside the document engine in the same server process, off by default (<code>OXIDB_SQL=1</code>). A request tagged <code>engine: "sql"</code> routes to it; everything else takes the document path untouched. A SQL table and a document collection can share a name and never meet — they live in different directories with different formats.</p>
    <pre><code class="lang-json">{ "engine": "sql", "cmd": "sql",
  "sql": "SELECT * FROM products WHERE id = $1",
  "params": [42] }</code></pre>
    <p>Each database gets its own engine instance (the default at <code>\${OXIDB_DATA}/sql</code>, named databases at <code>\${OXIDB_DATA}/&lt;name&gt;/sql</code>). One instance is a single shared handle: a mutex-guarded core holding the catalog, the tables, and the WAL, plus a session-transaction registry and a row-lock table beside it.</p>

    <h3>Life of a query</h3>
    <p><strong>1. Parse.</strong> The SQL text goes through <code>sqlparser</code>'s generic grammar, and the resulting syntax tree is translated into the engine's own logical AST. Translation is where honesty is enforced: anything the executor cannot actually do — an unsupported locking clause, an exotic query body — is rejected <em>by name</em> at this stage rather than accepted and quietly mis-executed. A few shapes the grammar cannot express (EF Core's <code>CREATE SEQUENCE</code>, <code>SELECT NEXT VALUE FOR</code>, <code>SHOW INDEXES</code>) are recognized as raw text before the parser sees them.</p>
    <p><strong>2. Cache.</strong> Text &rarr; AST is a pure function, so parsed statements are cached (up to 512 texts, whole-map drop past that). Applications loop over a small set of parameterized texts; parsing costs more than cloning an AST, so repeat statements skip the parser entirely. Parameters (<code>?</code> / <code>$N</code>) bind at execution, not parse, which is what makes the cache safe.</p>
    <p><strong>3. Rewrite at parse time.</strong> Non-recursive CTEs are inlined as derived tables — a CTE referenced twice is inlined twice, and the executor never knows it existed. A <code>WITH RECURSIVE</code> CTE is split into its anchor and step arms and materialized later by fixpoint iteration: the step re-runs with the CTE bound to the previous round's rows until nothing new appears, with guards at 1M iterations / 10M rows so a cyclic step terminates.</p>
    <p><strong>4. Execute.</strong> The executor is written against a small <code>Store</code> trait — scan, point-lookup, insert, update, delete, lock — with <strong>two implementations</strong>: the engine itself (autocommit: every operation applies and logs immediately) and a transaction (everything buffers in an overlay; more below). The same executor code serves both, which is why transactional and autocommit SQL cannot drift apart semantically.</p>

    <h3>Finding rows without scanning everything</h3>
    <p>There is no cost-based optimizer, and that is a deliberate size-and-predictability choice. Instead there is a short list of mechanical pruning rules that cover the shapes real applications send:</p>
    <ul>
      <li><strong>Equality conjuncts use indexes.</strong> A WHERE clause is decomposed into its top-level <code>AND</code>ed equality tests; if a secondary index (or the primary key map) covers them, the scan collapses to an index lookup.</li>
      <li><strong>Index-nested-loop joins.</strong> When the outer side of a join is small and the inner side has an index on the join key, the engine probes the index per outer row instead of scanning the inner table — with a hard cap on the outer size so the strategy can never be worse than the scan it replaces.</li>
      <li><strong>Short-circuit logic.</strong> <code>AND</code>/<code>OR</code> stop evaluating when the answer is decided; <code>STARTS_WITH</code>/<code>ENDS_WITH</code> compare borrowed bytes in place (they exist because ordinal affix tests otherwise rendered as per-row <code>SUBSTRING</code> + <code>LENGTH</code>).</li>
    </ul>
    <p>Expression evaluation itself is a borrowed-value tree walk. A stack-machine expression compiler was built, measured, and <strong>reverted</strong>: against an evaluator that already avoids cloning, compilation was net-negative. The lesson stuck — the engine optimizes what profiling shows, not what folklore suggests.</p>

    <h3>Where rows live</h3>
    <p>Every table is a <code>TableState</code>: rows addressed by an internal row id, a primary-key map for point lookups, and secondary indexes rebuilt at open. Rows are held one of two ways:</p>
    <ul>
      <li><strong>Resident</strong> (default): all rows in memory, reads are direct.</li>
      <li><strong>Disk-first</strong> (<code>OXIDB_SQL_DISK_FIRST=1</code>): the bulk of each table stays in its last-checkpoint snapshot file, memory-mapped; only rows changed since that checkpoint live in RAM. Resident memory tracks the write rate, not the table size.</li>
    </ul>

    <h3>Durability: a WAL and a MANIFEST</h3>
    <p>The write path is conventional and boring on purpose. Every mutation appends a CRC-framed, sequence-numbered record to a single live WAL file. A transaction's operations travel as <em>one</em> batch record — one append, one fsync, all-or-nothing on recovery.</p>
    <p>Checkpoints are where it gets interesting. A checkpoint writes a <strong>whole new generation</strong> into its own directory — <code>gen.N/</code> holding a <code>catalog.json</code> and one row-snapshot file per table — fsyncs it all, and then <em>promotes</em> it by atomically renaming a tiny <code>MANIFEST</code> file recording <code>{generation, wal_seq}</code> into place. That rename is the single commit point:</p>
    <ul>
      <li>Crash <em>before</em> the rename &rarr; the previous MANIFEST, and therefore the previous intact generation, is still in force. The half-written <code>gen.N/</code> is swept at the next open.</li>
      <li>Crash <em>after</em> &rarr; the new generation is live, and recovery replays only WAL records past the recorded <code>wal_seq</code> watermark — so a not-yet-truncated WAL can never double-apply a checkpointed, non-idempotent operation like <code>ALTER TABLE</code>.</li>
    </ul>
    <p>Because the catalog and the row snapshots switch together, their arities can never disagree after a crash — the failure mode the old overwrite-in-place layout actually had. One deliberate exception: <strong>sequences</strong> live in their own <code>sequences.json</code>, saved on every <code>NEXT VALUE FOR</code> — far more often than a checkpoint — because a handed-out sequence value must never be re-issued, even if the transaction that took it rolled back.</p>

    <h3>Transactions: an overlay, not a version store</h3>
    <p>A transaction buffers <em>everything</em> — row changes, created/dropped tables, index DDL, uniqueness state — in an in-memory overlay over the committed engine state. Its own reads see the overlay first (read-your-writes); the engine's committed state is untouched until commit. <code>COMMIT</code> hands the buffered operations to the engine as one atomic WAL batch; dropping the transaction (explicit <code>ROLLBACK</code>, a failed statement, a vanished connection) discards the overlay. <code>SAVEPOINT</code> is a snapshot of the overlay's data, and rolling back to one restores it without touching the engine.</p>
    <p>Interactive transactions survive across wire calls by being <em>parked</em>: between requests the overlay sits in a registry keyed by a session id, and the next statement resumes it. Uniqueness checks probe the engine's persistent maps <em>through</em> the overlay, so their cost scales with the transaction's writes, never with table size.</p>

    <h3>Row locks and <code>SELECT ... FOR UPDATE</code></h3>
    <p>Concurrency control is pessimistic and lock-based. A Condvar lock table maps <code>(table, row id)</code> to an owner — an open transaction, or an ephemeral autocommit statement:</p>
    <ul>
      <li><code>SELECT ... FOR UPDATE</code> locks every matched row until commit/rollback, re-evaluating the match to a fixpoint (the matched set can change while waiting on a contended row). Only a plain single-table SELECT qualifies; joins, aggregates, <code>DISTINCT</code>, set operations, views and derived tables are refused by name — a locking clause that does not lock is worse than none.</li>
      <li><strong>Plain <code>UPDATE</code>/<code>DELETE</code> take the same locks</strong> before mutating, so two concurrent read-modify-write transactions on one row serialize instead of losing a write.</li>
      <li>Waiting always happens with the engine mutex <em>released</em> — the lock holder needs that mutex to commit and let the waiter proceed. Deadlocks are not detected; they resolve as a lock timeout on one side (<code>OXIDB_SQL_LOCK_TIMEOUT_MS</code>, default 5000), which aborts that statement's transaction.</li>
    </ul>

    <h3>What isolation you actually get</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Anomaly</th><th>Possible?</th><th>Why</th></tr></thead>
        <tbody>
          <tr><td>Dirty read</td><td><strong>No</strong></td><td>Uncommitted writes exist only in the writer's private overlay.</td></tr>
          <tr><td>Lost update</td><td><strong>No</strong></td><td>Writers lock their rows; concurrent read-modify-write serializes.</td></tr>
          <tr><td>Non-repeatable read</td><td>Yes</td><td>Base reads always see the latest committed state. Re-read under <code>FOR UPDATE</code> if it matters.</td></tr>
          <tr><td>Phantom</td><td>Yes</td><td>There are no range or table locks.</td></tr>
        </tbody>
      </table>
    </div>
    <p>That is <strong>READ COMMITTED</strong> — the same default contract PostgreSQL ships with — plus pessimistic upgrades where you ask for them. There is <strong>no MVCC</strong>: no row version chains, no snapshot timestamps, no vacuum. This is a considered position, not an omission. The engine's one non-negotiable asset is provable simplicity — a buffered overlay committed as one WAL batch is something crash tests can corner — and its readers already never block on writers, which is the benefit MVCC is usually bought for. Snapshot isolation would cost version-chain memory, visibility rules and garbage collection, for a guarantee its real workloads (EF Core applications, short transactions) have not asked for.</p>

    <h3>Instant <code>ALTER TABLE</code> — the 500M-row problem</h3>
    <p><code>ADD COLUMN</code> and <code>DROP COLUMN</code> are <strong>O(1)</strong>: no row rewrite, no checkpoint, no downtime, regardless of table size. The trick is a split between physical and logical schema. The catalog stores the physical truth; the executor only ever sees a logical view:</p>
    <ul>
      <li><strong>ADD</strong> appends a slot to the physical schema. Old rows, which are physically short, read back padded with the new column's default. Nothing on disk moves.</li>
      <li><strong>DROP</strong> tombstones the column in place (the same idea as Postgres's <code>attisdropped</code>). The physical cell stays in every row; reads project it out, writes fill it with a placeholder.</li>
      <li>Durability rides on the WAL record alone. The next <em>ordinary</em> checkpoint — which rewrites every row anyway — compacts: rows are rewritten to the live columns and the tombstones leave the catalog. The deferred O(n) cost is folded into work that was already being paid.</li>
    </ul>

    <h3>Stored procedures: two languages, one rule</h3>
    <p><code>CREATE PROCEDURE ... AS BEGIN ... END</code> stores SQL text, re-parsed per <code>CALL</code> — zero toolchain. <code>LANGUAGE COBRA</code> stores compiled bytecode run by an in-server VM: the procedure defines <code>run(db, ...params)</code>, its queries join the CALL's transaction, and <strong>determinism is validated at CREATE</strong> — async, imports and I/O are rejected up front, with a 100M-instruction fuel cap at runtime. The rule behind both: a CALL must be safe to replicate, so a procedure may compute, read and write — and nothing else.</p>

    <h3>In a cluster</h3>
    <p>SQL writes replicate through Raft <em>as statements</em>: the server classifies each statement by parsing it, and anything that is not read-only ships to the group. <code>SELECT ... FOR UPDATE</code> deliberately classifies as a <strong>write</strong> — routed to a replica, its lock would be theater. Read-only SQL runs node-locally (a replica, when the pool has one). The engine is <strong>not sharded</strong>: oxipool shards by collection and shard key, which a SQL statement does not have, so SQL routes to one backend rather than scattering.</p>

    <h3>Backup without stopping</h3>
    <p><code>backup</code> holds the engine lock for two O(1) moments — pin the committed generation, note the WAL length, unpin at the end — and runs the slow tar/compress with the lock <em>released</em>. A pinned generation is safe from GC and freezes WAL truncation, so the archive (a synthesized MANIFEST, the pinned <code>gen.N/</code>, a stable WAL prefix, <code>sequences.json</code>) is crash-consistent as of the pin instant while writes and auto-checkpoints continue around it.</p>

    <h3>Proof over promises</h3>
    <p>The engine's conformance claim is not self-graded: it runs <strong>the official EF Core relational specification suite</strong> — all twelve Northwind suites, over the wire — at <strong>3832/3832 green</strong>. Where behavior had to be pinned down (case-insensitive <code>LIKE</code>, <code>COLLATE</code>, <code>VALUES</code> table constructors, multi-level correlated subqueries), the spec run is what forced the decision. And where it competes, it measures: against PostgreSQL over the same EF Core provider it wins most benchmark shapes — with the loss analysis published alongside the wins, including the one that turned out to be a bias in the benchmark harness rather than the engine.</p>

    <p>Reference of the SQL surface itself — DDL, joins, CTEs, window functions, set operations — lives on the <a href="/sql/">SQL page</a>. The transaction wire protocol and session semantics are on the <a href="/server/">server page</a>.</p>
  </div>
</section>` }} />
}
