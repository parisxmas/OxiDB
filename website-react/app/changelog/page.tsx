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

    <!-- v0.38.0 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.38.0</h3>
        <span class="version-date">2026-07-19</span>
        <span class="version-badge latest">latest</span>
      </div>
      <div class="change-group">
        <h4 class="change-type added">Changed &mdash; disk-first document storage is the default</h4>
        <ul>
          <li><strong>Resident memory no longer scales with your data.</strong> Document bodies now live in an mmap'd, zstd-compressed file with only a ~24&nbsp;B/doc offset index in RAM; the OS page cache holds the hot part and gives it back under pressure. Measured on a live 48-hour IoT workload: <strong>489&nbsp;MB resident &rarr; ~35&nbsp;MB</strong>, with identical indexed-read throughput and a ~17% write cost. <code>OXIDB_DISK_FIRST=0</code> restores the always-resident mode; existing collections keep the format they were created with, so upgrading never reinterprets data. The entire test fleet now runs against disk-first, including the SIGKILL crash suites, and encryption at rest was re-verified against every file the engine writes.</li>
        </ul>
      </div>
      <div class="change-group">
        <h4 class="change-type added">Added &mdash; SQL</h4>
        <ul>
          <li><strong><code>SELECT ... FOR UPDATE</code> takes real row locks.</strong> It used to parse and silently not lock. Matched rows are now pessimistically locked until commit/rollback; concurrent <code>UPDATE</code>/<code>DELETE</code>/<code>FOR UPDATE</code> on them block (up to <code>OXIDB_SQL_LOCK_TIMEOUT_MS</code>, default 5000 &mdash; also how a deadlock resolves). Plain <code>UPDATE</code>/<code>DELETE</code> lock their rows too, closing the engine's lost-update window: two concurrent read-modify-write transactions on one row now serialize. Shapes that cannot lock base rows (joins, aggregates, <code>DISTINCT</code>, set ops, views, derived tables, <code>FOR SHARE</code>) are refused with a clear error, never accepted without the lock &mdash; and <code>FOR UPDATE</code> classifies as a <em>write</em>, so oxipool never routes it to a replica. <a href="/sql/">Details</a>.</li>
          <li><strong><code>{"cmd": "disk_usage"}</code></strong> &mdash; per-engine on-disk footprint of the data directory in one call (documents incl. the mmap'd share, SQL, time-series, blobs, OxiMem, MQTT/AMQP substrates, full-text, PITR archive, system).</li>
        </ul>
      </div>
      <div class="change-group">
        <h4 class="change-type fixed">Fixed &mdash; a WAL durability bug</h4>
        <ul>
          <li><strong>Sealed WAL segments after the first were invisible.</strong> The segment scanner's fast path assumed <code>.0</code> always exists, but the first online checkpoint deleted it &mdash; after which later segments were never retired (unbounded disk growth) and, far worse, <strong>never replayed at recovery</strong>: a crash between a seal and its persist lost the acknowledged writes in that segment. <code>.0</code> is now a permanent empty sentinel, every checkpoint retires all covered segments (data dirs the old bug left behind self-heal), and three regression tests pin it &mdash; the crash-replay test was red before the fix.</li>
        </ul>
      </div>
    </div>

    <!-- v0.37.0 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.37.0</h3>
        <span class="version-date">2026-07-17</span>
      </div>
      <div class="change-group">
        <h4 class="change-type added">Added &mdash; AMQP: the RabbitMQ protocol</h4>
        <ul>
          <li><strong><a href="/amqp/">AMQP 0-9-1 listener</a></strong> (<code>OXIDB_AMQP_PORT</code>, off by default) &mdash; RabbitMQ client code works unmodified, verified end-to-end with <strong>pika</strong> (Python), <strong>RabbitMQ.Client</strong> (.NET) and <strong>amqp091-go</strong> (Go). Work queues with competing consumers (the semantic MQTT cannot express), default + <code>direct</code>/<code>fanout</code>/<code>topic</code> exchanges, <code>Basic.Qos</code> prefetch, publisher confirms, mandatory <code>Basic.Return</code>, nack/reject. Anything outside the subset is refused with a clear channel error, never silently accepted.</li>
          <li><strong>Durability follows the protocol</strong>: a durable queue holding <code>delivery_mode=2</code> messages is written through the document engine&apos;s WAL &mdash; the confirm is only sent after the fsync, and messages survive a <code>SIGKILL</code> (crash-tested; acknowledged messages stay consumed).</li>
          <li><strong>MQTT &harr; AMQP bridge</strong> via the pre-declared <code>amq.topic</code> exchange, the same mapping RabbitMQ&apos;s MQTT plugin uses (<code>/</code>&nbsp;&harr;&nbsp;<code>.</code>, QoS&nbsp;&ge;1 &harr; persistent): a sensor publishes MQTT, a worker pool consumes AMQP, one binary.</li>
          <li><strong>Faster than RabbitMQ on 5 of 6 benchmark scenarios</strong> (same Go client both sides): pipelined confirms 1.50&times;, confirm latency 1.74&times;, end-to-end throughput 1.29&times;, end-to-end latency 1.52&times;, 8-connection durable 1.11&times;. Behind it: per-burst fsync batching (one <code>insert_many</code> per pipeline burst; 264&nbsp;&rarr;&nbsp;53k&nbsp;msg/s), a cross-connection group committer (concurrent bursts share fsync rounds), and a cross-thread wake pipe in each connection&apos;s <code>poll(2)</code> set (delivery latency 51&nbsp;ms&nbsp;&rarr;&nbsp;0.02&nbsp;ms). The one loss &mdash; single-connection durable &mdash; is the price of a real <code>F_FULLFSYNC</code> behind every confirm, which RabbitMQ&apos;s lazy interval flush does not pay.</li>
        </ul>
      </div>
      <div class="change-group">
        <h4 class="change-type added">Added &mdash; engine &amp; clients</h4>
        <ul>
          <li><strong>Online WAL checkpointing</strong> for the document engine &mdash; the write-ahead log is sealed and folded into the snapshot <em>while writers run</em>, so it no longer grows unboundedly between restarts. Size-triggered via <code>OXIDB_WAL_CHECKPOINT_BYTES</code> (default 64&nbsp;MiB, <code>0</code> restores the old behaviour); crash-tested against <code>SIGKILL</code> mid-checkpoint.</li>
          <li><strong>Typed time-series surface in <code>OxiDb.Client.Tcp</code></strong> (.NET) &mdash; <code>TsdbWriteAsync</code>, <code>TsdbWriteLineProtocolAsync</code> and <code>TsdbQueryAsync</code> with typed points, aggregations (<code>TsdbAgg.Mean</code> &hellip; <code>Percentile(p)</code>) and epoch-ms helpers, replacing hand-rolled raw commands.</li>
        </ul>
      </div>
      <div class="change-group">
        <h4 class="change-type fixed">Fixed</h4>
        <ul>
          <li><strong>S3 ETags are now real MD5s</strong> &mdash; the previous ETag (truncated SHA-256) broke the AWS SDK for .NET, which re-computes the MD5 of what it uploaded and refuses a disagreeing ETag. <code>aws-cli</code>, <code>boto3</code>, MinIO SDKs and the AWS .NET SDK now all verify uploads cleanly, with no workaround flags.</li>
        </ul>
      </div>
    </div>

    <!-- v0.36.0 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.36.0</h3>
        <span class="version-date">2026-07-17</span>
      </div>
      <div class="change-group">
        <h4 class="change-type fixed">Fixed &mdash; WebAssembly is back</h4>
        <ul>
          <li><strong>The wasm32 build is repaired</strong> &mdash; v0.35.0 shipped binary-only because it had been broken for a while. <strong>2.2 MB raw, 0.76 MB gzipped</strong>, verified in a browser. <code>TransactionId</code> (a <code>u64</code>) lived in the native-only <code>tx_log</code> module, so five portable modules each duplicated it behind a <code>cfg</code> and the sixth broke the build; it now has one portable home. Also fixed: explain's <code>Instant</code> (wasm32 has no monotonic clock) and a <code>shutdown()</code> that assumed background threads.</li>
        </ul>
      </div>
      <div class="change-group">
        <h4 class="change-type fixed">Fixed &mdash; clustering</h4>
        <ul>
          <li><strong>The bootstrap node published an empty Raft address.</strong> <code>raft_init</code> registered the initial member with no address while every learner got a real one. Invisible for as long as that node leads &mdash; nobody dials the leader &mdash; but once it loses leadership no new leader can ever reach it, and it silently freezes at its old log while the cluster commits without it. Bootstrapping with no address is now refused outright.</li>
          <li><strong>oxipool sent every SQL read to the master.</strong> The read/write classifier looked for the statement in a field the SQL wire shape does not have, so everything read as a write and replicas never served a SQL query.</li>
        </ul>
      </div>
      <div class="change-group">
        <h4 class="change-type added">Performance &mdash; compound predicates &amp; string affixes</h4>
        <ul>
          <li><strong><code>AND</code> / <code>OR</code> now short-circuit.</strong> Both sides were always evaluated &mdash; <code>x &gt; 995 AND TRUE</code> cost 52% more than <code>x &gt; 995</code> alone. Every compound <code>WHERE</code> in the engine gains.</li>
          <li><strong><code>STARTS_WITH</code> / <code>ENDS_WITH</code></strong> &mdash; exact, literal, case-sensitive affix tests that compare borrowed bytes in place. Ordinal <code>StartsWith</code>/<code>EndsWith</code> previously rendered as per-row <code>SUBSTRING</code>+<code>LENGTH</code>, because <code>LIKE</code> is case-insensitive and a needle containing <code>%</code> would become a wildcard. Faster than <code>LIKE</code> without giving up the semantics that ruled it out.</li>
          <li>Against PostgreSQL over EF Core both shapes flipped from losses to wins: <code>any_compound</code> 0.79x&rarr;1.21x, <code>string_multi</code> 0.67x&rarr;2.07x.</li>
        </ul>
      </div>
      <div class="change-group">
        <h4 class="change-type added">Added</h4>
        <ul>
          <li><strong><code>oxidb-server --version</code> / <code>--help</code></strong> &mdash; probing the binary with <code>--version</code> used to <em>start a server</em> on the default port. <code>--help</code> is also the only in-binary documentation of the env-var configuration.</li>
          <li><strong>Engine-aware backup &amp; restore for the SQL and time-series engines</strong>, both <strong>low-lock</strong>: the engine lock is held only to pin a generation, and the archive compresses with it released, so queries and writes continue throughout.</li>
        </ul>
      </div>
      <div class="change-group">
        <h4 class="change-type changed">Tested</h4>
        <ul>
          <li><strong>p99 soak harness</strong> (plus a stdlib-only driver for hosts with no Rust toolchain). On Linux alongside live instances: 1.6M ops at 5,387 ops/s, read p99 3.9 ms, zero errors, no latency drift, RSS flat.</li>
          <li><strong>Partition tests</strong> for the sharded router &mdash; a missing shard must fail loudly, never return a plausible undercount &mdash; and for <strong>asymmetric</strong> network failures, where one direction dies and the two ends disagree about whether the peer is alive.</li>
        </ul>
      </div>
    </div>

    <!-- v0.35.0 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.35.0</h3>
        <span class="version-date">2026-07-16</span>
      </div>
      <div class="change-group">
        <h4 class="change-type added">Added &mdash; instant, online schema changes</h4>
        <ul>
          <li><strong><code>ALTER TABLE ADD COLUMN</code> / <code>DROP COLUMN</code> are O(1)</strong> &mdash; metadata-only, no row rewrite, no checkpoint. Add or drop a column on a 500M-row live table with zero downtime. ADD pads old rows with the default on read; DROP tombstones the column in place and projects it out.</li>
          <li><strong>Checkpoint compaction</strong> reclaims a dropped column's space, folded into a checkpoint that rewrites every row anyway.</li>
        </ul>
      </div>
      <div class="change-group">
        <h4 class="change-type changed">Changed &mdash; crash-atomic durability</h4>
        <ul>
          <li><strong>MANIFEST + generation checkpoints</strong> &mdash; each checkpoint writes a whole new <code>gen.&lt;N&gt;/</code> and promotes it with a single atomic MANIFEST rename. A crash before it leaves the previous generation whole; catalog and snapshot arities can never disagree after a crash. Recovery replays only WAL records past a watermark.</li>
        </ul>
      </div>
      <div class="change-group">
        <h4 class="change-type added">Added &mdash; EF Core provider &amp; SQL performance</h4>
        <ul>
          <li><strong>Official EF Core specification tests: 3832/3832 green</strong> across all 12 Northwind suites, with full migrations and design-time scaffolding.</li>
          <li><strong>OxiDB beats PostgreSQL on the EF Core benchmark</strong> &mdash; contiguous scan cache, correlated-subquery decorrelation, streamed scans with push-down, single-pass GROUP BY, index-nested-loop joins, a 48&rarr;24-byte <code>Value</code>, and an OxiWire binary wire format.</li>
          <li><strong>Analytics surface</strong> &mdash; <code>WITH</code> / <code>WITH RECURSIVE</code> CTEs, set operations (<code>UNION</code>/<code>EXCEPT</code>/<code>INTERSECT</code>), <code>LATERAL</code> joins, <code>DISTINCT ON</code>, <code>mode() WITHIN GROUP</code>, <code>CREATE SEQUENCE</code> / <code>NEXT VALUE FOR</code>, multi-level correlation, case-insensitive <code>LIKE</code> + <code>COLLATE</code>.</li>
        </ul>
      </div>
      <div class="change-group">
        <h4 class="change-type added">Added &mdash; TSDB time-series engine</h4>
        <ul>
          <li>A standalone <code>oxidb-tsdb</code> engine (mounted like SQL, <code>engine: "tsdb"</code>) &mdash; Gorilla-compressed columnar streams (~0.3 bytes/point), typed fields, InfluxDB line-protocol ingest, <code>rate()</code>/<code>percentile</code>, continuous-aggregate rollups, and MANIFEST-atomic persistence. Go client included.</li>
          <li><strong>OxiDB Studio</strong> desktop app &mdash; visual Query Designer, schema tree, editable result grids (macOS build on the <a href="/downloads/">downloads page</a>).</li>
        </ul>
      </div>
      <div class="change-group">
        <h4 class="change-type fixed">Fixed</h4>
        <ul>
          <li>Linux (musl) build: qualified <code>std::fs</code> in the <code>/proc</code> stats readers.</li>
          <li>Sequences persist in a separate <code>sequences.json</code> so a <code>NEXT VALUE FOR</code> can never desync a generation's catalog from its snapshots.</li>
        </ul>
      </div>
    </div>

    <!-- v0.34.7 -->
    <div class="version-block">
      <div class="version-header">
        <h3 class="version-tag">v0.34.7</h3>
        <span class="version-date">2026-07-09</span>
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
            <strong>OxiDB sweeps MongoDB at 1M docs.</strong> The full <code>tests/comparison-mongodb</code> bench at 1M-document scale (in-network Docker harness, no port-forward artifact) goes <strong>OxiDB 24 &ndash; MongoDB 0</strong> across 24 measured workloads. Largest wins: count-all 2189&times;, Top-5 cities aggregation 1262&times;, composite-indexed compound 4.1&times;. Smallest wins: bulk insert 1.1&times;, range-10K-rows-each 1.2&times;. Resource footprint at peak: OxiDB 1.71 GiB RSS / 741 MB disk vs MongoDB 1.00 GiB / 626 MB.
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