import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Changelog",
  description: `All notable changes to OxiDB, organized by version.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg> Changelog</h2>
    <p class="section-desc">All notable changes to OxiDB, organized by version.</p>

    <!-- v0.28.12 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.28.12</h3>
        <span class="version-date">2026-05-24</span>
        <span class="version-badge latest">latest</span>
      </div>

      <div class="change-group">
        <h4 class="change-type added">Added</h4>
        <ul>
          <li>
            <strong>Audit log rotation</strong> &mdash; <code>RotationPolicy</code> in <code>oxidb-server/src/audit.rs</code> supports size-based (<code>OXIDB_AUDIT_MAX_BYTES</code>), age-based (<code>OXIDB_AUDIT_MAX_AGE_SECS</code>), and calendar-aligned UTC rotation (<code>OXIDB_AUDIT_CALENDAR=hourly|daily</code>), with optional gzip compression of rotated files (<code>OXIDB_AUDIT_COMPRESS=true</code>). Wired into both standalone and cluster modes.
          </li>
          <li>
            <strong>CERN-grade testing program</strong> &mdash; 9 cargo-fuzz targets (RESP, pg_wire, OxiWire, MsgPack, differential vs Redis &amp; Postgres), OSS-Fuzz integration scaffolding, coverage reporting, ACID isolation-anomaly suite, HEP-shaped scale workload, encrypted-backup DR drill, upgrade-chain fixture corpus, and 39 authn/authz/SCRAM/canonicalisation/audit attack patterns &mdash; all rejected by the server.
          </li>
          <li>
            <strong>Format version headers</strong> &mdash; OXTX for <code>_tx_commit_log</code>, OXWA for <code>.wal</code>, OXBT for <code>.btree</code>; explicit <code>format_version</code> in blob <code>.meta</code> JSON. Establishes the 1.0 on-disk-format contract.
          </li>
        </ul>
      </div>

      <div class="change-group">
        <h4 class="change-type fixed">Fixed</h4>
        <ul>
          <li>
            <strong>Unauthenticated DoS bugs</strong> found by fuzzing &mdash; RESP multi-byte UTF-8 line splitter panic, RESP CR-truncation + allocator-bomb, pg_wire message length unbounded allocation (now capped at 16 MiB), pg_wire i16-overflow + empty-body panic, OxiWire array/map pre-allocation now bounded by remaining bytes. Server versions &lt; 0.28.3 vulnerable.
          </li>
        </ul>
      </div>

      <div class="change-group">
        <h4 class="change-type changed">Changed</h4>
        <ul>
          <li>
            <strong>Julia client surface</strong> &mdash; <code>find</code> / <code>aggregate</code> now return a <code>Tables.jl</code>-compatible row collection (DataFrames, CSV, MLJ, GLM accept it directly). SQL exports removed from Julia clients &mdash; OxiDB is a document database; Tables.jl covers the data-frame integration story.
          </li>
        </ul>
      </div>
    </div>

    <!-- v0.25.3 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.25.3</h3>
        <span class="version-date">2026-04-25</span>
      </div>

      <div class="change-group">
        <h4 class="change-type changed">Changed</h4>
        <ul>
          <li>
            <strong>Raft persistence: O(1) per mutation</strong> &mdash; rewrote <code>oxidb-server/src/raft/log_store.rs</code> to split state into a small <code>raft_meta.json</code> (vote / committed / sm_data) and an append-only <code>raft_log.jsonl</code> (one entry per line).
          </li>
          <li>
            <strong>Append-only log writes</strong> &mdash; <code>append_to_log</code> is now a single line append per entry instead of rewriting the entire log; <code>delete_conflict_logs_since</code> and <code>purge_logs_upto</code> rewrite only on those rare events.
          </li>
          <li>
            <strong>Transparent migration</strong> from the v0.25.2 single-file <code>raft_state.json</code> on first boot.
          </li>
          <li>
            <strong>Unblocked 1M-record load tests</strong> under failover &mdash; 22.4 s end-to-end, 44,701 rec/s avg, zero records lost. The previous single-file snapshot stalled the cluster at ~52% complete due to 14 MB-per-mutation rewrites.
          </li>
        </ul>
      </div>
    </div>

    <!-- v0.25.2 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.25.2</h3>
        <span class="version-date">2026-04-25</span>
      </div>

      <div class="change-group">
        <h4 class="change-type added">Added</h4>
        <ul>
          <li>
            <strong>Persistent Raft state for cluster mode</strong> &mdash; <code>OxiDbStore</code> in <code>oxidb-server/src/raft/log_store.rs</code> was previously in-memory only; nodes that restarted came back as <code>Learner term=0</code> and lost cluster membership, breaking failover scenarios.
          </li>
          <li>
            <strong>New <code>OxiDbStore::open(db, &amp;data_dir)</code> constructor</strong> &mdash; loads existing Raft state on startup; <code>OxiDbStore::new(db)</code> retained as in-memory variant for tests.
          </li>
          <li>
            <strong>Atomic write-through</strong> on every mutation &mdash; <code>save_vote</code>, <code>save_committed</code>, <code>append_to_log</code>, <code>delete_conflict_logs_since</code>, <code>purge_logs_upto</code>, <code>apply_to_state_machine</code>, <code>install_snapshot</code>.
          </li>
          <li>
            <strong>ShardReplicaRealWorldTest harness</strong> &mdash; 14-service docker-compose: 9 oxidb-server nodes (3 Raft groups), 3 per-shard oxipool master/replica routers, 1 top-tier shard-routing oxipool, Go API tier, one-shot cluster-init bootstrapper.
          </li>
          <li>
            <strong>End-to-end test suites</strong> &mdash; Go smoke harness (5 assertions), Python integration tests (8 cases: CRUD + sharding + aggregation), Python failover scenarios (5: network partition, follower down, recovery catch-up, two followers down, leader down), parameterized load test with mid-stream failover (validated against 10K, 100K, 1M record loads).
          </li>
        </ul>
      </div>
    </div>

    <!-- v0.25.1 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.25.1</h3>
        <span class="version-date">2026-04-18</span>
      </div>

      <div class="change-group">
        <h4 class="change-type added">Added</h4>
        <ul>
          <li>
            <strong>Eight new query operators</strong> &mdash; <code>$not</code>, <code>$nor</code>, <code>$all</code>, <code>$size</code>, <code>$type</code>, <code>$mod</code>, <code>$expr</code>, <code>$elemMatch</code>.
          </li>
          <li>
            <strong><code>$not</code> field operator</strong> &mdash; negate any field condition; missing fields evaluate to true (MongoDB-compatible).
          </li>
          <li>
            <strong><code>$nor</code> top-level operator</strong> &mdash; match documents where none of the listed conditions are true.
          </li>
          <li>
            <strong><code>$all</code> array operator</strong> &mdash; array must contain all specified values.
          </li>
          <li>
            <strong><code>$size</code> operator</strong> &mdash; match arrays with an exact length.
          </li>
          <li>
            <strong><code>$type</code> operator</strong> &mdash; match by JSON type (<code>string</code>, <code>number</code>, <code>bool</code>, <code>array</code>, <code>object</code>, <code>null</code>, <code>int</code>).
          </li>
          <li>
            <strong><code>$mod</code> operator</strong> &mdash; modulo arithmetic on numeric fields (<code>[divisor, remainder]</code>).
          </li>
          <li>
            <strong><code>$expr</code> top-level operator</strong> &mdash; cross-field comparisons, e.g. <code>{"$expr": {"$gt": ["$sold", "$stock"]}}</code>.
          </li>
          <li>
            <strong><code>$elemMatch</code> operator</strong> &mdash; match array elements against sub-queries with AND semantics.
          </li>
          <li>
            <strong>Go client additions</strong> &mdash; stored procedures (<code>CreateProcedure</code>, <code>CallProcedure</code>, <code>ListProcedures</code>...), <code>CreateTTLIndex</code>, retention policies, alerting methods, <code>ExtractText</code>, <code>Backup</code>/<code>Restore</code>, <code>SetDialect</code>.
          </li>
        </ul>
      </div>

      <div class="change-group">
        <h4 class="change-type fixed">Fixed</h4>
        <ul>
          <li>
            <strong>Array dot-notation in <code>$set</code> / <code>$inc</code> / <code>$unset</code></strong> &mdash; <code>variants.0.stock</code> no longer corrupts arrays.
          </li>
        </ul>
      </div>

      <div class="change-group">
        <h4 class="change-type changed">Changed</h4>
        <ul>
          <li>
            <strong>Refactored <code>matches_doc</code> and <code>matches_value</code></strong> into a shared <code>eval_field_op</code> helper.
          </li>
        </ul>
      </div>
    </div>

    <!-- v0.24.0 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.24.0</h3>
        <span class="version-date">2026-04-10</span>
      </div>

      <div class="change-group">
        <h4 class="change-type added">Added</h4>
        <ul>
          <li>
            <strong>WebAssembly support</strong> -- New <code>oxidb-wasm</code> crate compiles OxiDB to <code>wasm32</code> and runs entirely in the browser.
          </li>
          <li>
            <strong>In-memory browser mode</strong> -- No server needed. JSON queries, SQL, and aggregation all work client-side in the browser.
          </li>
          <li>
            <strong>wasm-bindgen API</strong> -- Full JavaScript API surface: <code>init</code>, <code>insert</code>, <code>find</code>, <code>update</code>, <code>delete</code>, <code>count</code>, <code>sql</code>, <code>aggregate</code>.
          </li>
          <li>
            <strong>~1.5 MB gzipped WASM binary</strong> -- Compact binary size suitable for production web applications.
          </li>
          <li>
            <strong>TypeScript types included</strong> -- Full type definitions shipped with the WASM package for editor autocompletion and type safety.
          </li>
          <li>
            <strong>Cross-platform lock shim (<code>src/locks.rs</code>)</strong> -- Uses <code>parking_lot</code> on native targets, spin locks on <code>wasm32</code>.
          </li>
        </ul>
      </div>

      <div class="change-group">
        <h4 class="change-type changed">Changed</h4>
        <ul>
          <li>
            <strong>Native-only dependencies made target-specific</strong> -- <code>rayon</code>, <code>memmap2</code>, <code>zstd</code>, and other native-only crates moved to target-specific dependencies to enable WASM compilation.
          </li>
          <li>
            <strong><code>#[cfg(not(target_arch = "wasm32"))]</code> guards throughout core engine</strong> -- Platform-incompatible code paths conditionally compiled out for the WASM target.
          </li>
        </ul>
      </div>
    </div>

    <!-- v0.18.0 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.18.0</h3>
        <span class="version-date">2026-03-05</span>
      </div>

      <div class="change-group">
        <h4 class="change-type added">Added</h4>
        <ul>
          <li>
            <strong>OxiWire binary protocol</strong> -- Custom wire format with 1-byte type tags, 4-byte LE lengths, 8-byte LE numbers. Magic byte <code>0xDB</code>. Replaces MsgPack for all request/response paths.
            <span class="commit-ref">Encoder + decoder in Rust and Go</span>
          </li>
          <li>
            <strong>.NET EF Core provider</strong> -- Full Entity Framework Core support with LINQ queries, transactions, and both TCP and embedded modes.
            <span class="commit-ref">d7d5a05</span>
          </li>
          <li>
            <strong>.NET NuGet packages</strong> -- <code>OxiDb.Client.Tcp</code>, <code>OxiDb.Client.Embedded</code>, <code>OxiDb.EntityFrameworkCore</code>.
            <span class="commit-ref">d7d5a05</span>
          </li>
          <li>
            <strong>Composite index tests</strong> -- 9 subtests covering exact match, prefix match, count, sort, update, delete, aggregate, drop, and triple-field composite indexes in Go.
          </li>
          <li>
            <strong>Parallel OxiWire serialization</strong> -- Result sets >= 5,000 docs are serialized across up to 8 CPU cores. Chunk-based, zero per-doc allocation.
          </li>
          <li>
            <strong>OxiDB vs MongoDB benchmark suite</strong> -- 22 tests across 7 categories. Score: OxiDB 19 -- MongoDB 1.
          </li>
          <li>
            <strong>OxiDB vs PostgreSQL benchmark suite</strong> -- 20 tests comparing document workloads. Score: OxiDB 10 -- PostgreSQL 10.
            <span class="commit-ref">d7d5a05</span>
          </li>
          <li>
            <strong>OxiDB vs SQLite benchmark</strong> -- 100K document embedded benchmark.
            <span class="commit-ref">ce8db5f</span>
          </li>
        </ul>
      </div>

      <div class="change-group">
        <h4 class="change-type changed">Changed</h4>
        <ul>
          <li>
            <strong>Aggregation indexed-path threshold</strong> -- Changed from 10% to 50% selectivity. Indexed aggregation path now preferred when candidate set is less than 50% of collection size.
          </li>
          <li>
            <strong>Go client rewritten for OxiWire</strong> -- All requests/responses use OxiWire binary format. MsgPack dependency removed entirely.
          </li>
          <li>
            <strong>Pipeline handler updated</strong> -- Sub-responses decoded from OxiWire and re-encoded for composite pipeline responses.
          </li>
        </ul>
      </div>

      <div class="change-group">
        <h4 class="change-type removed">Removed</h4>
        <ul>
          <li>
            <strong>MsgPack support</strong> -- Removed from server (Rust), Go client, and all benchmark tests. OxiWire is the sole binary protocol.
          </li>
          <li>
            <strong><code>github.com/vmihailenco/msgpack/v5</code></strong> -- Removed from Go module dependencies.
          </li>
        </ul>
      </div>
    </div>

    <!-- v0.17.0 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.17.0</h3>
        <span class="version-date">2026-02-23</span>
      </div>

      <div class="change-group">
        <h4 class="change-type added">Added</h4>
        <ul>
          <li>
            <strong>LRU document cache</strong> -- Per-collection in-memory cache with configurable capacity. JSON deserialized once, then <code>Arc</code>-refcounted.
            <span class="commit-ref">0997d8e</span>
          </li>
          <li>
            <strong>Streaming scan for non-indexed finds</strong> -- Avoids loading all documents into memory for large unindexed queries.
            <span class="commit-ref">39cd803</span>
          </li>
          <li>
            <strong>Lock-free pread</strong> -- Separate read-only file handle uses <code>pread</code> for concurrent reads without locking the write path.
            <span class="commit-ref">63093a3</span>
          </li>
          <li>
            <strong>Sorted-offset batch reads</strong> -- Indexed finds sort offsets before reading to minimize disk seeks.
            <span class="commit-ref">63093a3</span>
          </li>
          <li>
            <strong>Zero-decode aggregation</strong> -- Extract only needed fields from raw JSONB, skip full document deserialization.
            <span class="commit-ref">4a6f696</span>
          </li>
          <li>
            <strong>Batch pread for indexed $match aggregations</strong> -- Combine pread with zero-decode for indexed aggregation paths.
            <span class="commit-ref">4b610ea</span>
          </li>
          <li>
            <strong>Zero-decode index creation</strong> -- Extract only <code>_id</code> and the indexed field from raw JSONB during index build.
            <span class="commit-ref">beb3f49</span>
          </li>
          <li>
            <strong>DocIdSet optimization</strong> -- Inline storage for single-document index entries saves ~80 bytes per entry.
            <span class="commit-ref">4897f86</span>
          </li>
          <li>
            <strong>Zero-decode filter for unindexed scans</strong> -- JSONB keypath extraction avoids full JSON parse on scan.
            <span class="commit-ref">7b9c639</span>
          </li>
          <li>
            <strong>Parallel segmented scan</strong> -- Large unindexed queries split across CPU cores for parallel processing.
            <span class="commit-ref">b339e5a</span>
          </li>
          <li>
            <strong>Index-only count for aggregations</strong> -- <code>$group</code> with <code>$sum: 1</code> on indexed fields returns set size without touching documents.
            <span class="commit-ref">b339e5a</span>
          </li>
        </ul>
      </div>

      <div class="change-group">
        <h4 class="change-type changed">Changed</h4>
        <ul>
          <li>
            <strong>Memory consumption reduced</strong> -- Skip bulk cache during insert, drop unused <code>Value</code> clones, use <code>DocIdSet</code> instead of <code>BTreeSet</code> for single-entry indexes.
            <span class="commit-ref">4897f86</span>
          </li>
          <li>
            <strong>Streaming I/O throughout</strong> -- Replaced collect-then-process patterns with streaming iterators for finds, aggregations, and index creation.
          </li>
        </ul>
      </div>
    </div>

    <!-- v0.16.0 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.16.0</h3>
        <span class="version-date">2026-02-23</span>
      </div>

      <div class="change-group">
        <h4 class="change-type added">Added</h4>
        <ul>
          <li><strong>Core document database engine</strong> -- Append-only storage, WAL with CRC32 checksums, per-collection locking.</li>
          <li><strong>JSON query language</strong> -- $eq, $ne, $gt, $gte, $lt, $lte, $in, $exists, $regex, $and, $or. Dot notation for nested fields.</li>
          <li><strong>Update operators</strong> -- $set, $unset, $inc, $mul, $min, $max, $rename, $currentDate, $push, $pull, $addToSet, $pop.</li>
          <li><strong>Aggregation pipeline</strong> -- $match, $group, $sort, $project, $limit, $skip, $unwind, $addFields, $lookup, $count. Accumulators: $sum, $avg, $min, $max, $count, $first, $last, $push.</li>
          <li><strong>Single-field, unique, and composite indexes</strong> -- BTreeMap-backed with index-only count and index-backed sort.</li>
          <li><strong>Full-text search</strong> -- TF-IDF ranking with HTML, XML, JSON, PDF, DOCX, XLSX, and OCR support.</li>
          <li><strong>Vector search</strong> -- HNSW index with cosine, euclidean, and dot product distance metrics.</li>
          <li><strong>ACID transactions</strong> -- OCC with 3-phase commit, per-document versioning, deadlock-free sorted locking.</li>
          <li><strong>SQL support</strong> -- SELECT, INSERT, UPDATE, DELETE, CREATE/DROP INDEX, CREATE/DROP TABLE, JOINs, GROUP BY, aggregate functions.</li>
          <li><strong>Blob storage</strong> -- S3-style bucket/object API with metadata, ETags, content types.</li>
          <li><strong>Encryption at rest</strong> -- AES-256-GCM with random 12-byte nonce per document.</li>
          <li><strong>Zstd compression</strong> -- Level 3, transparent per-document, thread-local context reuse.</li>
          <li><strong>Change streams</strong> -- Watch collections for insert/update/delete events. Resumable with 4096-event replay buffer.</li>
          <li><strong>Stored procedures</strong> -- Named multi-step operations with parameter substitution.</li>
          <li><strong>Scheduled tasks</strong> -- Background job scheduling with enable/disable control.</li>
          <li><strong>Multi-database support</strong> -- Isolated databases within a single server instance.</li>
          <li><strong>Backup & restore</strong> -- Compressed full backups with all data, indexes, and metadata.</li>
          <li><strong>TCP server</strong> -- Length-prefixed JSON over TCP (max 16 MiB). Tokio-based async runtime.</li>
          <li><strong>SCRAM-SHA-256 authentication</strong> -- Salted challenge-response, no plaintext passwords on wire.</li>
          <li><strong>RBAC</strong> -- Admin, ReadWrite, Read roles with per-command authorization.</li>
          <li><strong>TLS/SSL</strong> -- Certificate-based encryption for all traffic.</li>
          <li><strong>Audit logging</strong> -- GELF format for centralized logging.</li>
          <li><strong>Raft clustering</strong> -- Multi-node replication via openraft (optional <code>cluster</code> feature flag).</li>
          <li><strong>Client libraries</strong> -- Python, Go, Julia, .NET (TCP + Embedded), Swift (C FFI).</li>
          <li><strong>C FFI</strong> -- <code>oxidb-client-ffi</code> (cdylib) and <code>oxidb-embedded-ffi</code> (staticlib + cdylib) for language bindings.</li>
        </ul>
      </div>
    </div>

  </div>
</section>` }} />
}