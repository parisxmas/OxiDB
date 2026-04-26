import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Fast Embeddable Document Database",
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<header class="hero">
  <div class="container">
    <p class="hero-kicker"><span class="hero-pulse"></span>v0.25.3 &middot; durable Raft state &middot; 1M-record cluster verified</p>
    <h1>OxiDB</h1>
    <p class="tagline">A fast, versatile document database.</p>
    <p class="sub">JSON &amp; SQL queries. ACID transactions. Full-text &amp; vector search. S3-compatible blob storage. <strong>Sharded routing via oxipool. Raft replication with persistent state.</strong> Encryption at rest.</p>
    <div class="hero-actions">
      <a href="/quickstart/" class="btn btn-primary">Get Started</a>
      <a href="/downloads/" class="btn btn-secondary">Downloads v0.25.3</a>
    </div>
    <div class="hero-install">
      <code>~5 MB binary &middot; zero dependencies &middot; embed or run as a server</code>
    </div>
  </div>
</header>

<section class="section section-alt">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg> Install OxiDB in 60 seconds</h2>
    <p class="section-desc">From zero to a running server. Single static binary, no dependencies, no installer.</p>

    <h3>1. Download the server</h3>
    <pre><code class="lang-bash"><span class="co"># Linux (x86_64)</span>
curl -LO https://github.com/parisxmas/OxiDB/releases/download/v0.25.3/oxidb-server-v0.25.3-linux-amd64.tar.gz
tar xzf oxidb-server-v0.25.3-linux-amd64.tar.gz

<span class="co"># macOS (Apple Silicon)</span>
curl -LO https://github.com/parisxmas/OxiDB/releases/download/v0.25.3/oxidb-server-v0.25.3-darwin-arm64.tar.gz
tar xzf oxidb-server-v0.25.3-darwin-arm64.tar.gz

<span class="co"># Windows: download oxidb-server-v0.25.3-windows-amd64.zip from /downloads/</span></code></pre>

    <h3>2. Start the server</h3>
    <pre><code class="lang-bash"><span class="co"># Listens on 127.0.0.1:4444 by default; data goes to ./oxidb_data</span>
./oxidb-server

<span class="co"># Or override with env vars:</span>
OXIDB_ADDR=0.0.0.0:4444 OXIDB_DATA=/var/lib/oxidb ./oxidb-server</code></pre>

    <h3>3. Connect and run your first query</h3>
    <pre><code class="lang-bash"><span class="co"># Download the CLI</span>
curl -LO https://github.com/parisxmas/OxiDB/releases/download/v0.25.3/oxidb-cli-v0.25.3-linux-amd64.tar.gz
tar xzf oxidb-cli-v0.25.3-linux-amd64.tar.gz

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

<section class="section">
  <div class="container">
    <h2>Why OxiDB</h2>
    <div class="feature-grid">
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg></div>
        <h3>Fast</h3>
        <p>3-6x faster than MongoDB on queries. 446x faster on indexed counts. OxiWire binary protocol for minimal overhead.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="2" width="20" height="8" rx="2"/><rect x="2" y="14" width="20" height="8" rx="2"/><circle cx="6" cy="6" r="1"/><circle cx="6" cy="18" r="1"/></svg></div>
        <h3>Flexible Deployment</h3>
        <p>Embed as a Rust library with zero network overhead, or run as a TCP server with auth, TLS, and RBAC.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg></div>
        <h3>JSON + SQL</h3>
        <p>MongoDB-style JSON queries with $eq, $gt, $in, $regex, $or. Or use standard SQL with JOINs and GROUP BY.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg></div>
        <h3>ACID Transactions</h3>
        <p>Multi-collection OCC transactions. 3-phase commit with WAL. Crash recovery with CRC32 checksums.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg></div>
        <h3>Full-Text & Vector Search</h3>
        <p>TF-IDF ranked search across 8 document formats. HNSW vector index with cosine, euclidean, and dot product.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><circle cx="5" cy="6" r="2"/><circle cx="19" cy="6" r="2"/><circle cx="5" cy="18" r="2"/><circle cx="19" cy="18" r="2"/><line x1="7" y1="7" x2="10" y2="10"/><line x1="14" y1="10" x2="17" y2="7"/><line x1="7" y1="17" x2="10" y2="14"/><line x1="14" y1="14" x2="17" y2="17"/></svg></div>
        <h3>Sharding + Raft Replication</h3>
        <p>Horizontal sharding via oxipool (CRC32 routing, scatter-gather) on top of per-shard Raft groups. Persistent log survives container restarts. Verified at 1M records under mid-stream failover.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 22h14a2 2 0 002-2V7.5L14.5 2H6a2 2 0 00-2 2v4"/><polyline points="14 2 14 8 20 8"/><path d="M2 15h10"/><path d="M9 18l3-3-3-3"/></svg></div>
        <h3>Document Indexing</h3>
        <p>Full-text index PDF, DOCX, XLSX, HTML, XML, JSON, and images (OCR). Search across all your documents with TF-IDF ranking.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg></div>
        <h3>S3 Blob Storage</h3>
        <p>S3-style bucket and object API for binary data. Store files alongside documents with CRC32 integrity checks.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg></div>
        <h3>Multi-Language</h3>
        <p>Official clients for Rust, Python, Go, .NET (TCP + Embedded + EF Core), Julia, and Swift.</p>
      </div>
    </div>
  </div>
</section>

<section class="section section-alt">
  <div class="container">
    <h2>At a Glance</h2>
    <div class="glance-grid">
      <a href="/features/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>
        <span>19 features</span>
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
        <span>10 pipeline stages</span>
      </a>
      <a href="/indexes/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></svg>
        <span>5 index types</span>
      </a>
      <a href="/clients/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>
        <span>10 client libraries</span>
      </a>
      <a href="/sql/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg>
        <span>Full SQL support</span>
      </a>
      <a href="/blobs/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>
        <span>S3 blob storage</span>
      </a>
      <a href="/search/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
        <span>PDF/DOCX indexing</span>
      </a>
      <a href="/server/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><circle cx="5" cy="6" r="2"/><circle cx="19" cy="6" r="2"/><line x1="7" y1="7" x2="10" y2="10"/><line x1="14" y1="10" x2="17" y2="7"/></svg>
        <span>Sharding + Raft</span>
      </a>
      <a href="/benchmarks/" class="glance-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>
        <span>19-1 vs MongoDB</span>
      </a>
    </div>
  </div>
</section>` }} />
}