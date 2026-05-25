import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Features",
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section id="features" class="section">
  <div class="container">
    <h2>Features</h2>
    <div class="feature-grid">
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="2" width="20" height="8" rx="2"/><rect x="2" y="14" width="20" height="8" rx="2"/><circle cx="6" cy="6" r="1"/><circle cx="6" cy="18" r="1"/></svg></div>
        <h3>Embeddable</h3>
        <p>Use as a library in your Rust app with zero network overhead. No separate process needed.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="2" width="20" height="20" rx="2"/><path d="M7 2v20"/><path d="M2 12h5"/><path d="M2 7h5"/><path d="M2 17h5"/></svg></div>
        <h3>Client/Server</h3>
        <p>Run as a TCP server with SCRAM-SHA-256 auth, TLS, RBAC, and audit logging. Connect from any language.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg></div>
        <h3>JSON + SQL</h3>
        <p>Query with MongoDB-style JSON operators or standard SQL. SELECT, INSERT, UPDATE, DELETE, JOINs, GROUP BY.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg></div>
        <h3>ACID Transactions</h3>
        <p>Multi-collection transactions with optimistic concurrency control. 3-phase commit with WAL durability.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg></div>
        <h3>Indexes</h3>
        <p>Single-field, composite, unique, text, and vector indexes. Index-only counts. Index-backed sort.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/></svg></div>
        <h3>Aggregation Pipeline</h3>
        <p>$match, $group, $sort, $project, $lookup, $unwind, $addFields, $skip, $limit, $count.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg></div>
        <h3>Full-Text Search</h3>
        <p>TF-IDF ranked search. Indexes HTML, XML, JSON, PDF, DOCX, XLSX, and images (OCR).</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M12 2a15 15 0 014 10 15 15 0 01-4 10 15 15 0 01-4-10 15 15 0 014-10z"/><line x1="2" y1="12" x2="22" y2="12"/></svg></div>
        <h3>Vector Search</h3>
        <p>HNSW index with cosine, euclidean, and dot product distance metrics.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg></div>
        <h3>Blob Storage</h3>
        <p>S3-style bucket/object API. Store files with metadata, content types, and ETags.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0110 0v4"/></svg></div>
        <h3>Encryption at Rest</h3>
        <p>AES-256-GCM authenticated encryption. Transparent at the storage layer.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg></div>
        <h3>Change Streams</h3>
        <p>Watch collections for insert/update/delete events in real-time. Resumable with replay buffer.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg></div>
        <h3>Stored Procedures</h3>
        <p>Define named multi-step operations with parameter substitution. Call them by name.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg></div>
        <h3>Compression</h3>
        <p>Zstd compression (level 3) at the storage layer. Transparent per-document compression.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21H5a2 2 0 01-2-2V5a2 2 0 012-2h11l5 5v11a2 2 0 01-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg></div>
        <h3>Backup & Restore</h3>
        <p>Compressed full backups. Restore to any directory. All data, indexes, and metadata included.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg></div>
        <h3>Multi-Database</h3>
        <p>Create isolated databases within a single server. Switch context at runtime.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg></div>
        <h3>Scheduled Tasks</h3>
        <p>Background job scheduling with enable/disable control and configurable intervals.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="14" rx="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/><path d="M7 10h2l1-3 2 6 1-3h2"/></svg></div>
        <h3>WebAssembly</h3>
        <p>Run OxiDB entirely in the browser via WASM. No server needed — in-memory mode with full query, SQL, and aggregation support. ~1.5 MB gzipped.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><circle cx="5" cy="6" r="2"/><circle cx="19" cy="6" r="2"/><circle cx="5" cy="18" r="2"/><circle cx="19" cy="18" r="2"/><line x1="7" y1="7" x2="10" y2="10"/><line x1="14" y1="10" x2="17" y2="7"/><line x1="7" y1="17" x2="10" y2="14"/><line x1="14" y1="14" x2="17" y2="17"/></svg></div>
        <h3>Raft Replication <span class="version-badge latest">v0.28.18</span></h3>
        <p>Multi-node replication via openraft consensus. Persistent state (<code>raft_meta.json</code> + append-only <code>raft_log.jsonl</code>) survives container restarts. Quorum-based commits, automatic catch-up on rejoin, verified at 1M records under mid-stream failover.</p>
      </div>
      <div class="feature-card">
        <div class="feature-icon"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg></div>
        <h3>Sharding (oxipool) <span class="version-badge latest">v0.25.x</span></h3>
        <p>Two-tier <code>oxipool</code> router: top-level CRC32 hash on configurable shard keys (e.g. <code>customer_id</code>) → 256 virtual chunks → N shards. Per-shard pool fronts master + replicas with read/write split + TX pinning. Scatter-gather for cross-shard queries. Reference deployment: <code>ShardReplicaRealWorldTest/</code>.</p>
      </div>
    </div>
  </div>
</section>` }} />
}