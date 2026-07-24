import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Fast Multi-Model Database — Document, SQL & Time-Series",
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<header class="hero">
  <div class="container">
    <p class="hero-kicker"><span class="hero-pulse"></span>v0.39 &middot; OxiBase multi-tenant backend &mdash; per-project auth, RLS &amp; quotas &middot; row-level security &middot; one shared cache (RSS bounded across tenants)</p>
    <div class="hero-lockup">
      <img src="/oxidb-logo.svg" class="hero-logo" alt="OxiDB logo" width="96" height="96" />
      <h1>OxiDB</h1>
    </div>
    <p class="tagline">A fast, multi-model database.</p>
    <p class="sub"><strong>OxiDB is a single, small binary</strong> that speaks every data model your application needs. Query documents with MongoDB-style JSON or a <strong>full SQL engine</strong>, record metrics in a built-in <strong>time-series</strong> store, cache in a <strong>Redis-compatible</strong> key-value layer, publish over <strong>MQTT</strong>, queue work over <strong>AMQP (RabbitMQ)</strong>, and keep files in <strong>S3-compatible</strong> object storage &mdash; with <strong>full-text &amp; vector search</strong> across all of it. ACID transactions, Raft replication, and encryption at rest are built in. Embed it in your process, or run it as a server.</p>
    <div class="hero-actions">
      <a href="/quickstart/" class="btn btn-primary">Get Started</a>
      <a href="/downloads/" class="btn btn-secondary">Downloads</a>
    </div>
    <div class="hero-install">
      <code>one 20 MB binary &middot; zero dependencies &middot; embed or run as a server</code>
    </div>
  </div>
</header>

<section class="section">
  <div class="container">
    <div class="termblock">
      <div class="termbar"><span></span><span></span><span></span></div>
      <pre>One 20 MB binary. Zero dependencies. Everything below:

OxiDB &mdash; a database engine in Rust:

 &#9642; Document DB (Mongo-style queries &amp; aggregation)
 &#9642; Full SQL engine (joins, DDL, stored procedures)
 &#9642; ACID transactions &mdash; OCC + SELECT FOR UPDATE + group commit
 &#9642; Raft clustering &amp; replication
 &#9642; Redis-compatible in-memory store (RESP) with MULTI/EXEC/WATCH + EVAL
 &#9642; MQTT + AMQP (RabbitMQ) brokers, bridged, cross-protocol pub/sub
 &#9642; Full-text search: HTML, PDF, DOCX, XLSX, OCR
 &#9642; S3-compatible blob storage (SigV4, multipart, lifecycle)
 &#9642; Time-series: OHLCV candles, window functions, gap filling
 &#9642; TTL indexes, point-in-time recovery
 &#9642; AES-GCM encryption at rest
 &#9642; REST + WebSocket + JWT auth + security rules
 &#9642; Prometheus metrics, explain &amp; slow-query profiler
 &#9642; Multi-database, RBAC, SCRAM auth, TLS, audit log
 &#9642; Runs embedded too &mdash; Python, Go, .NET (EF Core), JS, Julia, Swift, PHP
 &#9642; Compiles to WASM

Postgres + Mongo + Redis + Elastic + S3. 20 MB.</pre>
    </div>
  </div>
</section>

<section class="section section-alt">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg> Architecture</h2>
    <p class="section-desc">One binary, one server process. Every client speaks its own <strong>native wire protocol</strong>; the router authenticates and dispatches each request to the right engine; all engines share one <strong>durable storage foundation</strong>.</p>

    <div class="arch">
      <div class="arch-layer">
        <div class="arch-label">Clients &amp; tools</div>
        <div class="arch-row">
          <span class="arch-chip">Python</span>
          <span class="arch-chip">Go</span>
          <span class="arch-chip">.NET / EF&nbsp;Core</span>
          <span class="arch-chip">JS / TS</span>
          <span class="arch-chip">Julia</span>
          <span class="arch-chip">Swift</span>
          <span class="arch-chip">PHP</span>
          <span class="arch-chip alt">redis-cli</span>
          <span class="arch-chip alt">mosquitto</span>
          <span class="arch-chip alt">pika / RabbitMQ.Client</span>
          <span class="arch-chip alt">aws-cli / boto3</span>
          <span class="arch-chip alt">Browser</span>
        </div>
      </div>

      <div class="arch-flow"><span class="arch-flow-txt">speak native wire protocols</span></div>

      <div class="arch-layer">
        <div class="arch-label">Wire protocols</div>
        <div class="arch-row">
          <span class="arch-proto">TCP&nbsp;+&nbsp;JSON</span>
          <span class="arch-proto">RESP</span>
          <span class="arch-proto">MQTT&nbsp;3.1.1</span>
          <span class="arch-proto">AMQP&nbsp;0-9-1</span>
          <span class="arch-proto">S3&nbsp;HTTP&nbsp;+&nbsp;SigV4</span>
          <span class="arch-proto">REST</span>
          <span class="arch-proto">WebSocket</span>
        </div>
      </div>

      <div class="arch-flow"></div>

      <div class="arch-core">
        <strong>OxiDB Server</strong>
        <span>request router &middot; RBAC &middot; SCRAM-SHA-256 auth &middot; TLS &middot; audit log &middot; multi-database</span>
      </div>

      <div class="arch-flow"><span class="arch-flow-txt">routes each request to an engine</span></div>

      <div class="arch-engines">
        <div class="arch-engine e-doc">
          <div class="arch-engine-h">Document Engine</div>
          <div class="arch-engine-b">Mongo-style JSON queries, aggregation pipeline, full-text &amp; vector search</div>
          <div class="arch-engine-p">TCP &middot; REST &middot; WS</div>
        </div>
        <div class="arch-engine e-sql">
          <div class="arch-engine-h">SQL Engine</div>
          <div class="arch-engine-b">Joins, CTEs, window functions, stored procedures, EF&nbsp;Core provider</div>
          <div class="arch-engine-p">TCP</div>
        </div>
        <div class="arch-engine e-tsdb">
          <div class="arch-engine-h">Time-Series Engine</div>
          <div class="arch-engine-b">Gorilla-compressed columns, line-protocol ingest, continuous rollups</div>
          <div class="arch-engine-p">TCP</div>
        </div>
        <div class="arch-engine e-mem">
          <div class="arch-engine-h">OxiMem</div>
          <div class="arch-engine-b">Redis-compatible KV, MULTI/EXEC/WATCH, EVAL/Lua, pub/sub</div>
          <div class="arch-engine-p">RESP</div>
        </div>
        <div class="arch-engine e-mqtt">
          <div class="arch-engine-h">MQTT Broker</div>
          <div class="arch-engine-b">Topic wildcards, retained messages, QoS&nbsp;0/1/2, Last&nbsp;Will</div>
          <div class="arch-engine-p">MQTT</div>
        </div>
        <div class="arch-engine e-s3">
          <div class="arch-engine-h">S3 Blob Store</div>
          <div class="arch-engine-b">Buckets, multipart upload, lifecycle, presigned URLs</div>
          <div class="arch-engine-p">S3 HTTP</div>
        </div>
      </div>

      <div class="arch-flow"><span class="arch-flow-txt">shared durable foundation</span></div>

      <div class="arch-layer arch-storage">
        <div class="arch-label">Storage &amp; durability</div>
        <div class="arch-row">
          <span class="arch-chip">WAL &mdash; CRC32, 3-fsync</span>
          <span class="arch-chip">MANIFEST checkpoints</span>
          <span class="arch-chip">AES-GCM at rest</span>
          <span class="arch-chip">Raft replication</span>
          <span class="arch-chip">Point-in-time recovery</span>
          <span class="arch-chip">TTL indexes</span>
        </div>
      </div>
    </div>

    <p class="arch-note"><strong>Cross-protocol pub/sub:</strong> a message published over RESP (OxiMem) reaches MQTT subscribers, and vice-versa &mdash; one bus, two protocols. The three query engines are <strong>per-database</strong>; OxiMem's keyspace and S3 buckets are global. Everything above runs <strong>embedded in-process</strong> too, with no server or protocol at all.</p>
  </div>
</section>

<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="1" y="3" width="15" height="13"/><polygon points="16 8 20 8 23 11 23 16 16 16 16 8"/><circle cx="5.5" cy="18.5" r="2.5"/><circle cx="18.5" cy="18.5" r="2.5"/></svg> Showcase &mdash; ColdChain</h2>
    <p class="section-desc">A cold-chain compliance system running on <strong>every engine at once</strong>, live. Sensors in trucks report temperature over MQTT; two years later an auditor asks you to <em>prove</em> a shipment never left its contracted range.</p>

    <div class="showcase">
      <div class="showcase-main">
        <p>The point is not that OxiDB <em>can</em> do these things &mdash; the list above already says that. It is that ColdChain is an <strong>ordinary .NET application</strong>. It uses EF&nbsp;Core, MQTTnet, StackExchange.Redis and the AWS SDK: four libraries that have never heard of OxiDB and are not configured to know. They are pointed at localhost ports and behave exactly as they would against the systems they were written for.</p>

        <p>So the interesting number is not a benchmark. It is the count of things you did <strong>not</strong> deploy:</p>

        <div class="showcase-swap">
          <div class="swap-row"><span class="swap-was">Mosquitto</span><span class="swap-arrow">&rarr;</span><span class="swap-now">MQTT broker</span><span class="swap-for">sensors publishing readings</span></div>
          <div class="swap-row"><span class="swap-was">InfluxDB</span><span class="swap-arrow">&rarr;</span><span class="swap-now">time-series engine</span><span class="swap-for">the readings themselves</span></div>
          <div class="swap-row"><span class="swap-was">Redis</span><span class="swap-arrow">&rarr;</span><span class="swap-now">OxiMem</span><span class="swap-for">live state + pub/sub</span></div>
          <div class="swap-row"><span class="swap-was">PostgreSQL</span><span class="swap-arrow">&rarr;</span><span class="swap-now">SQL engine</span><span class="swap-for">shipments, customers, penalties</span></div>
          <div class="swap-row"><span class="swap-was">MongoDB</span><span class="swap-arrow">&rarr;</span><span class="swap-now">document engine</span><span class="swap-for">raw events, verbatim</span></div>
          <div class="swap-row"><span class="swap-was">MinIO</span><span class="swap-arrow">&rarr;</span><span class="swap-now">S3 API + full-text</span><span class="swap-for">certificates, searchable</span></div>
        </div>

        <p>Six systems to install, secure, monitor, back up, upgrade on six schedules and keep consistent with each other &mdash; against five ports on one binary, where the backup is one archive taken at one instant with one restore. The dashboard shows what the two processes actually cost while it does it.</p>

        <p class="showcase-cta"><a href="https://coldchain.baltavista.com" class="btn btn-primary">Open the live demo</a> <span class="showcase-cta-note">runs continuously &middot; six probes &middot; two of them broken on purpose</span></p>
      </div>

    </div>
  </div>
</section>

<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg> Install OxiDB in 60 seconds</h2>
    <p class="section-desc">From zero to a running server. Single static binary, no dependencies, no installer.</p>

    <h3>1. Download the server</h3>
    <pre><code class="lang-bash"><span class="co"># Linux (x86_64)</span>
curl -LO https://oxidb.baltavista.com/releases/v0.39.10/oxidb-server-v0.39.10-linux-amd64.tar.gz
tar xzf oxidb-server-v0.39.10-linux-amd64.tar.gz

<span class="co"># macOS (Apple Silicon)</span>
curl -LO https://oxidb.baltavista.com/releases/v0.39.10/oxidb-server-v0.39.10-darwin-arm64.tar.gz
tar xzf oxidb-server-v0.39.10-darwin-arm64.tar.gz

<span class="co"># Windows: download oxidb-server-v0.39.10-windows-amd64.zip from /downloads/</span></code></pre>

    <h3>2. Start the server</h3>
    <pre><code class="lang-bash"><span class="co"># Listens on 127.0.0.1:4444 by default; data goes to ./oxidb_data</span>
./oxidb-server

<span class="co"># Or override with env vars:</span>
OXIDB_ADDR=0.0.0.0:4444 OXIDB_DATA=/var/lib/oxidb ./oxidb-server</code></pre>

    <h3>3. Connect and run your first query</h3>
    <pre><code class="lang-bash"><span class="co"># Download the CLI</span>
curl -LO https://oxidb.baltavista.com/releases/v0.39.10/oxidb-v0.39.10-linux-amd64.tar.gz
tar xzf oxidb-v0.39.10-linux-amd64.tar.gz

<span class="co"># Open the REPL against the running server</span>
./oxidb --host 127.0.0.1 --port 4444

oxidb&gt; insert users {<span class="str">"name"</span>: <span class="str">"Alice"</span>, <span class="str">"age"</span>: <span class="num">30</span>}
oxidb&gt; find users {<span class="str">"age"</span>: {<span class="str">"$gte"</span>: <span class="num">18</span>}}
oxidb&gt; update users {<span class="str">"name"</span>: <span class="str">"Alice"</span>} {<span class="str">"$inc"</span>: {<span class="str">"age"</span>: <span class="num">1</span>}}</code></pre>

    <div class="install-hint">
      <h4>Prefer to embed instead of running a server?</h4>
      <pre><code class="lang-bash"><span class="co"># Rust</span>
cargo add oxidb

<span class="co"># Python (TCP client / embedded)</span>
pip install oxidb
pip install oxidb-embedded

<span class="co"># Go</span>
go get oxidb

<span class="co"># .NET</span>
dotnet add package OxiDb.Client.Tcp</code></pre>
    </div>

    <p style="margin-top: 24px;">Need a different platform, source build, or the WebAssembly bundle? See <a href="/downloads/">Downloads</a> &middot; <a href="/quickstart/">Full Quick Start</a> &middot; <a href="/clients/">All clients</a>.</p>
  </div>
</section>

<section class="section section-alt">
  <div class="container">
    <h2>Why OxiDB</h2>
    <div class="feature-grid">
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg></div>
        <h3>Three engines, one binary</h3>
        <p>A MongoDB-style document store, a full relational SQL engine, and an InfluxDB-style time-series engine &mdash; in one process, one file, no glue. Use one or all three; each owns separate storage.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg></div>
        <h3>Fast</h3>
        <p>2-3x faster than MongoDB on indexed queries at 1M docs, 2189x on indexed counts &mdash; and beats PostgreSQL across the EF Core benchmark. A bytes-first wire path with a direct JSONB&rarr;OxiWire converter.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg></div>
        <h3>MongoDB-style documents</h3>
        <p>JSON queries with $eq, $gt, $in, $regex, $or, $elemMatch, $expr, plus a 16-stage aggregation pipeline (JOINs via $lookup, GROUP BY via $group, window &amp; time-series stages).</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg></div>
        <h3>Full SQL engine</h3>
        <p>INNER/LEFT/RIGHT/FULL joins, GROUP BY, window functions, CTEs (incl. WITH RECURSIVE) and LATERAL. An EF Core provider passing all 3832 official spec tests, with ADO.NET, Dapper, and migrations. Instant, online ALTER TABLE.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg></div>
        <h3>Time-series engine</h3>
        <p>InfluxDB-style, with Gorilla-compressed columnar streams (~0.3 bytes/point), line-protocol ingest, continuous-aggregate rollups, and rate() / percentile aggregations.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><polyline points="9 12 11 14 15 10"/></svg></div>
        <h3>ACID + crash-safe backups</h3>
        <p>Multi-collection OCC transactions, 3-phase commit with WAL, CRC32 crash recovery. Crash-atomic MANIFEST checkpoints, low-lock online backup/restore, and point-in-time recovery.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg></div>
        <h3>Full-Text & Vector Search</h3>
        <p>TF-IDF ranked full-text search across 8 document formats (PDF, DOCX, XLSX, HTML, XML, JSON, images/OCR). HNSW vector index with cosine, euclidean, and dot product.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="2"/><path d="M4.93 19.07a10 10 0 010-14.14"/><path d="M7.76 16.24a6 6 0 010-8.49"/><path d="M16.24 7.76a6 6 0 010 8.49"/><path d="M19.07 4.93a10 10 0 010 14.14"/></svg></div>
        <h3>Redis-compatible + MQTT</h3>
        <p>OxiMem &mdash; a Redis-compatible in-memory store (RESP, MULTI/EXEC/WATCH, EVAL/Lua, persistence) &mdash; plus a full MQTT 3.1.1 broker (wildcards, retained, QoS, LWT) and an AMQP 0-9-1 (RabbitMQ-protocol) work-queue broker, bridged through amq.topic.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg></div>
        <h3>S3 Blob Storage</h3>
        <p>A full S3-compatible HTTP API (SigV4, multipart, Range, lifecycle) &mdash; aws-cli, boto3, and the MinIO SDKs work unmodified. Store files alongside documents.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><circle cx="5" cy="6" r="2"/><circle cx="19" cy="6" r="2"/><circle cx="5" cy="18" r="2"/><circle cx="19" cy="18" r="2"/><line x1="7" y1="7" x2="10" y2="10"/><line x1="14" y1="10" x2="17" y2="7"/><line x1="7" y1="17" x2="10" y2="14"/><line x1="14" y1="14" x2="17" y2="17"/></svg></div>
        <h3>Sharding + Raft Replication</h3>
        <p>Horizontal sharding via oxipool (CRC32 routing, scatter-gather) on per-shard Raft groups. Persistent log survives restarts. Verified at 1M records under mid-stream failover.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="2" width="20" height="8" rx="2"/><rect x="2" y="14" width="20" height="8" rx="2"/><circle cx="6" cy="6" r="1"/><circle cx="6" cy="18" r="1"/></svg></div>
        <h3>Flexible Deployment</h3>
        <p>Embed as a Rust library with zero network overhead, or run as a single static TCP server with SCRAM auth, TLS, and RBAC. One binary, no dependencies, no installer.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg></div>
        <h3>Multi-Language</h3>
        <p>Official clients for Rust, Python, Go, .NET (ADO.NET + EF Core + LINQ + Embedded), Java, Julia, Swift, PHP/WordPress, and JS/TS &mdash; plus a WebAssembly build.</p>
      </div>
    </div>
  </div>
</section>

<section class="section">
  <div class="container">
    <h2>At a Glance</h2>
    <div class="glance-grid">
      <a href="/features/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>
        <span>3 engines: document · SQL · time-series</span>
      </a>
      <a href="/features/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>
        <span>Full SQL engine (joins, CTEs, window fns)</span>
      </a>
      <a href="/sql/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="4" y1="9" x2="20" y2="9"/><line x1="4" y1="15" x2="20" y2="15"/><line x1="10" y1="3" x2="8" y2="21"/><line x1="16" y1="3" x2="14" y2="21"/></svg>
        <span>EF Core provider (3832/3832 spec tests)</span>
      </a>
      <a href="/features/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
        <span>Time-series engine (Gorilla-compressed)</span>
      </a>
      <a href="/queries/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
        <span>20 query operators</span>
      </a>
      <a href="/updates/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
        <span>12 update operators</span>
      </a>
      <a href="/aggregation/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>
        <span>16 aggregation stages</span>
      </a>
      <a href="/transactions/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><polyline points="9 12 11 14 15 10"/></svg>
        <span>ACID transactions + crash-safe backups</span>
      </a>
      <a href="/indexes/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></svg>
        <span>5 index types</span>
      </a>
      <a href="/vectors/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
        <span>Full-text + vector search (PDF/DOCX/OCR)</span>
      </a>
      <a href="/blobs/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>
        <span>S3 blob storage · Redis · MQTT</span>
      </a>
      <a href="/server/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><circle cx="5" cy="6" r="2"/><circle cx="19" cy="6" r="2"/><line x1="7" y1="7" x2="10" y2="10"/><line x1="14" y1="10" x2="17" y2="7"/></svg>
        <span>Sharding + Raft</span>
      </a>
      <a href="/clients/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>
        <span>10 client libraries</span>
      </a>
    </div>
  </div>
</section>` }} />
}