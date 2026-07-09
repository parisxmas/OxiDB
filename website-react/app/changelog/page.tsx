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

    <!-- v0.34.7 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.34.7</h3>
        <span class="version-date">2026-07-09</span>
        <span class="version-badge latest">latest</span>
      </div>
      <div class="change-group">
        <h4 class="change-type added">Added &mdash; full MQTT 3.1.1 broker</h4>
        <ul>
          <li><strong>Topic wildcards</strong> &mdash; <code>+</code> (one level) and <code>#</code> (subtree) filters, backed by the OxiMem pattern-subscriber layer.</li>
          <li><strong>Retained messages</strong> &mdash; last-known-value delivery to new subscribers; empty retained payload clears.</li>
          <li><strong>QoS</strong> &mdash; QoS&nbsp;1 delivery with packet ids; inbound QoS&nbsp;2 completes the PUBREC/PUBREL/PUBCOMP handshake.</li>
          <li><strong>Last Will &amp; Testament</strong> &mdash; published on abnormal disconnect or keepalive expiry (1.5&times; enforced).</li>
          <li><strong>Auth</strong> &mdash; <code>OXIDB_MQTT_USER</code>/<code>OXIDB_MQTT_PASSWORD</code> require matching CONNECT credentials.</li>
          <li>Wire-test suite speaking raw MQTT bytes (6 tests).</li>
        </ul>
      </div>
    </div>

    <!-- v0.34.6 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.34.6</h3>
        <span class="version-date">2026-07-09</span>
      </div>
      <div class="change-group">
        <h4 class="change-type added">Added &mdash; S3 API</h4>
        <ul>
          <li><strong>ListObjectsV2 continuation tokens</strong> &mdash; <code>aws s3 ls</code> pages correctly over large buckets.</li>
          <li><strong>Lifecycle expiration</strong> &mdash; <code>?lifecycle</code> Days rules per bucket with a background sweeper.</li>
          <li>SigV4 wire-test suite under <code>cargo test</code>: signed roundtrip, corrupted-signature 403, multipart assembly, batch delete.</li>
        </ul>
      </div>
    </div>

    <!-- v0.34.2-v0.34.5 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.34.2 &ndash; v0.34.5</h3>
        <span class="version-date">2026-07-09</span>
      </div>
      <div class="change-group">
        <h4 class="change-type added">Added &mdash; OxiMem becomes a full Redis-class store</h4>
        <ul>
          <li><strong>Transactions</strong> &mdash; MULTI/EXEC/DISCARD/WATCH/UNWATCH with O(1) version-counter WATCH and Redis EXECABORT semantics.</li>
          <li><strong>Server-side scripting</strong> &mdash; EVAL/EVALSHA/SCRIPT (Lua 5.4) with KEYS/ARGV, <code>redis.call</code>, <code>cjson</code>, <code>redis.sha1hex</code>; atomic, busy-script time limit, SCRIPT KILL.</li>
          <li><strong>Blocking ops</strong> &mdash; BLPOP/BRPOP/BZPOPMIN/BLMOVE/BRPOPLPUSH, condvar-woken.</li>
          <li><strong>Persistence</strong> &mdash; rebuild-on-boot from the SQL mirror (all five types, TTL-correct) and fast-mode snapshots.</li>
          <li><strong>Pub/sub</strong> &mdash; PSUBSCRIBE glob patterns, keyspace notifications, <code>expired</code> events.</li>
          <li>30+ new commands (set ops, GETDEL/COPY/GETEX, ZREMRANGEBY*, ZUNION/ZINTERSTORE, LMPOP/ZMPOP, bit ops, sub-scans with real cursors), Prometheus command counters + latency histogram.</li>
        </ul>
      </div>
      <div class="change-group">
        <h4 class="change-type fixed">Fixed</h4>
        <ul>
          <li><strong>ZREVRANGE rank order</strong> &mdash; <code>ZREVRANGE key 0 0</code> returned the lowest member; ranks now index the descending view.</li>
        </ul>
      </div>
    </div>

    <!-- v0.34.0 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.34.0</h3>
        <span class="version-date">2026-07-08</span>
      </div>
      <div class="change-group">
        <h4 class="change-type added">Added</h4>
        <ul>
          <li><strong>Group commit</strong> &mdash; concurrent transactions share fsyncs; hot-account workloads went from ~130 to 300+ tx/s on a laptop, 1.5&ndash;2.2k tx/s on a 4-core Linux VPS at full durability.</li>
          <li><strong>SELECT FOR UPDATE</strong> &mdash; <code>find_for_update</code> pessimistic document locks: contenders queue instead of conflict-storming.</li>
          <li><strong>Time-series aggregation</strong> &mdash; <code>$ohlcv</code> (tick&rarr;candle), range/time window frames, <code>$densify</code>, <code>$fill</code>.</li>
          <li><strong>Prometheus</strong> &mdash; <code>GET /metrics</code> on the REST listener; zero dependencies.</li>
          <li><strong>explain &amp; slow-query profiler</strong> &mdash; real planner output plus <code>OXIDB_SLOW_QUERY_MS</code> capture.</li>
          <li><strong>Isolation characterization</strong> &mdash; the OCC model documented and pinned by tests; Jepsen-style crash suite, fsync-failure injection, Elle-style serializability checker, Raft partition tests.</li>
        </ul>
      </div>
      <div class="change-group">
        <h4 class="change-type fixed">Fixed</h4>
        <ul>
          <li><strong>Torn transaction-log writes</strong> &mdash; commit log now replaced atomically (found by the Jepsen-style suite).</li>
          <li><strong>Same-document write composition</strong> &mdash; two updates to one document in a transaction no longer clobber each other.</li>
          <li><strong>fsync-failure durability hole</strong> &mdash; a rejected commit can no longer leak into a checkpoint.</li>
        </ul>
      </div>
    </div>

    <!-- v0.29-v0.33 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.29 &ndash; v0.33</h3>
        <span class="version-date">2026-06 &ndash; 2026-07</span>
      </div>
      <div class="change-group">
        <h4 class="change-type added">Highlights</h4>
        <ul>
          <li><strong>SQL engine</strong> (ADR-0010) &mdash; standalone relational engine beside the document engine: DDL, DML, joins, GROUP BY/HAVING, secondary indexes, parameterized queries, per-engine transactions; beats PostgreSQL&nbsp;15 on the reference workload.</li>
          <li><strong>Stored procedures</strong> &mdash; CREATE/DROP PROCEDURE, CALL, named params, atomic execution.</li>
          <li><strong>EF Core &amp; ADO.NET</strong> (ADR-0013) &mdash; OxiDb.Data (Dapper-ready) and an EF&nbsp;Core&nbsp;9 provider; interactive transactions with savepoints.</li>
          <li><strong>Multi-database</strong> (ADR-0012) &mdash; isolated databases with per-database SQL engines, RBAC, TTL/alert threads.</li>
          <li><strong>Licensing</strong> &mdash; v0.33.0+ is proprietary (commercial licensing); TCP client libraries remain MIT.</li>
        </ul>
      </div>
    </div>

    <!-- v0.28.18 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.28.18</h3>
        <span class="version-date">2026-05-25</span>
        
      </div>

      <div class="change-group">
        <h4 class="change-type added">Added</h4>
        <ul>
          <li>
            <strong>OxiWire HELLO handshake</strong> &mdash; new <code>cmd: "hello"</code> returns server version, supported wire versions, stable-surface feature set, experimental feature set, and auth methods. Pre-auth, idempotent, backward-compatible (clients without HELLO default to wire v1). See <code>oxidb-server/src/hello.rs</code> and ADR-0003 Phase 2.
          </li>
          <li>
            <strong>REST <code>/v1/</code> URL prefix</strong> &mdash; <code>GET /v1/hello</code> returns server info; <code>/v1/api/...</code> is the 1.0 stable surface entry point. Legacy bare <code>/api/...</code> still routes during the deprecation window.
          </li>
          <li>
            <strong>WebSocket subprotocol versioning</strong> &mdash; server advertises <code>oxidb.v1</code> via <code>Sec-WebSocket-Protocol</code>. Clients without the header still connect.
          </li>
          <li>
            <strong><code>oxidb migrate</code> CLI</strong> &mdash; new subcommand on <code>oxidb-cli</code>: <code>migrate inspect --data &lt;PATH&gt;</code> walks a data directory and reports each file&apos;s on-disk format version (OXWA / OXTX / OXBT / OXIX / blob <code>format_version</code>). <code>migrate run</code> validates versions and is the scaffold for future v2 migrations (ADR-0003 Phase 4).
          </li>
        </ul>
      </div>

      <div class="change-group">
        <h4 class="change-type changed">Performance</h4>
        <ul>
          <li>
            <strong>Bytes-first find path for OxiWire responses</strong> &mdash; new <code>jsonb_oxiwire</code> module converts JSONB to OxiWire bytes via a custom serde Visitor, skipping the <code>serde_json::Value</code> tree intermediate (~20 µs/doc saving on cache miss). A new <code>doc_bytes_cache</code> (env-tunable via <code>OXIDB_DOC_BYTES_CACHE_SIZE</code>, default 1M) keeps pre-encoded bytes around.
          </li>
          <li>
            <strong>Composite-index fast path</strong> &mdash; <code>find</code> queries that are exactly covered by a composite index&apos;s fields now route through <code>find_prefix</code> directly, skipping post-filter and Value materialisation.
          </li>
          <li>
            <strong>Partial-JSONB filter</strong> &mdash; new helper evaluates top-level <code>$eq</code>/<code>$ne</code>/<code>$gt</code>/<code>$gte</code>/<code>$lt</code>/<code>$lte</code>/<code>$in</code> conditions plus <code>$and</code> / <code>$or</code> / dot-paths directly against JSONB bytes using <code>codec::extract_field</code>. Wired into the aggregation pipeline&apos;s <code>$match</code> step AND the find full-scan rayon path; reserves the full JSONB&rarr;Value decode for queries with predicates the partial matcher can&apos;t evaluate.
          </li>
          <li>
            <strong>Doc cache capacity is env-tunable</strong> &mdash; <code>OXIDB_DOC_CACHE_SIZE</code> overrides the 100K default. Production hardware with more RAM can hold the full working set.
          </li>
        </ul>
      </div>

      <div class="change-group">
        <h4 class="change-type added">Benchmark</h4>
        <ul>
          <li>
            <strong>OxiDB sweeps MongoDB at 1M docs.</strong> The full <code>tests/comparison-mongodb</code> bench at 1M-document scale (in-network Docker harness, no port-forward artifact) goes <strong>OxiDB 24 &ndash; MongoDB 0</strong> across 24 measured workloads. Largest wins: count-all 2189&times;, Top-5 cities aggregation 1262&times;, composite-indexed compound 4.1&times;. Smallest wins: bulk insert 1.1&times;, range-10K-rows-each 1.2&times;. Resource footprint at peak: OxiDB 1.71 GiB RSS / 741 MB disk vs MongoDB 1.00 GiB / 626 MB. See <a href="/benchmarks/">Benchmarks</a>.
          </li>
        </ul>
      </div>

      <div class="change-group">
        <h4 class="change-type added">.NET clients (developer-friendly rework)</h4>
        <ul>
          <li>
            <strong>4 NuGet packages published at v0.28.18</strong> &mdash; <code>OxiDb.Client.Tcp</code>, <code>OxiDb.Client.Embedded</code>, <code>OxiDb.EntityFrameworkCore</code>, and <strong>NEW: <code>OxiDb.Linq</code></strong> (LINQ provider, previously source-only).
          </li>
          <li>
            <strong>Exception hierarchy</strong> &mdash; <code>OxiDbException</code> base + <code>OxiDbDuplicateKeyException</code>, <code>OxiDbTransactionConflictException</code>, <code>OxiDbAuthenticationException</code>, <code>OxiDbNotFoundException</code>, <code>OxiDbImmutableException</code> (WORM), <code>OxiDbConnectionException</code>, <code>OxiDbProtocolException</code>. Server error strings routed to the right subclass via <code>FromServerMessage</code>. Legacy <code>OxiDbTcpException</code> retained as <code>[Obsolete]</code> alias.
          </li>
          <li>
            <strong><code>HelloAsync</code> + <code>HelloResponse</code> record</strong> &mdash; wire-protocol handshake returning server version, supported wire versions, stable + experimental feature sets, auth methods.
          </li>
          <li>
            <strong>Typed CRUD overloads</strong> &mdash; <code>FindAsync&lt;T&gt;</code>, <code>FindOneAsync&lt;T&gt;</code>, <code>InsertReturningIdAsync</code> (returns <code>long</code>), <code>InsertManyReturningIdsAsync</code> (returns <code>long[]</code>). Eliminate the <code>JsonElement</code>&rarr;parse dance.
          </li>
          <li>
            <strong><code>StreamAsync&lt;T&gt;</code></strong> &mdash; <code>IAsyncEnumerable&lt;T&gt;</code> over paginated LIMIT/SKIP batches for million-row result sets.
          </li>
          <li>
            <strong>DI integration</strong> &mdash; <code>services.AddOxiDbTcp(opts =&gt; opts.Host(&hellip;))</code> registers <code>IOxiDbClient</code> as a singleton.
          </li>
          <li>
            <strong>Type-safe query builder</strong> &mdash; <code>Query.Eq</code>, <code>Query.Gte</code>, <code>Query.In</code>, <code>Query.And</code>, <code>Query.Or</code>, <code>Query.Range</code> &hellip; for runtime-constructed queries that don&apos;t fit LINQ.
          </li>
        </ul>
      </div>

      <div class="change-group">
        <h4 class="change-type added">1.0 prep docs</h4>
        <ul>
          <li>
            <code>docs/SEMVER.md</code>, <code>docs/STABILITY.md</code>, <code>docs/DEPRECATION.md</code>, <code>docs/SECURITY.md</code> &mdash; Phase 5 of ADR-0003. Translate the ADR-0004 release-policy decisions into operational docs (24-month LTS, additive-only minor releases, GitHub Security Advisories channel, etc.).
          </li>
          <li>
            <code>docs/PHASE3-SDK-FREEZE.md</code> + Python client <code>api/v1.json</code> snapshot + CI gate script (template for the other 9 Tier-A clients).
          </li>
          <li>
            <code>docs/format/compat-matrix.md</code> &mdash; Phase 2 cross-version compat matrix (OxiWire / REST / WebSocket).
          </li>
        </ul>
      </div>
    </div>

    <!-- v0.28.12 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.28.12</h3>
        <span class="version-date">2026-05-24</span>
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