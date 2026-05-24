import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Storage Engine",
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section id="storage" class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z"/></svg> Storage Engine</h2>

    <h3>Architecture</h3>
    <ul>
      <li><strong>Append-only file</strong> -- Documents stored as <code>[status:u8][length:u32 LE][payload]</code>. Soft-delete flips the status byte in place.</li>
      <li><strong>Write-Ahead Log (WAL)</strong> -- CRC32 checksums per entry. Transaction ID tagging. 3-fsync protocol: WAL &rarr; data &rarr; checkpoint.</li>
      <li><strong>Zstd compression</strong> -- Level 3 by default. Transparent per-document. Thread-local compressor/decompressor reuse.</li>
      <li><strong>AES-256-GCM encryption</strong> -- Optional. Random 12-byte nonce per document. Applied after compression.</li>
      <li><strong>LRU document cache</strong> -- Per-collection in-memory cache. JSON deserialized once, then Arc-refcounted. Configurable capacity.</li>
      <li><strong>Lock-free reads</strong> -- Separate read-only file handle uses <code>pread</code>. Writes are serialized via Mutex.</li>
      <li><strong>Lazy sync mode</strong> -- Background thread batches fsyncs at a configurable interval. Reduces write latency at the cost of durability window.</li>
    </ul>

    <h3>Collection Isolation</h3>
    <p>Each collection has its own storage file, WAL, indexes, and cache. Per-collection <code>RwLock</code> enables concurrent reads across different collections and concurrent reads within the same collection.</p>

    <h3>Cluster mode persistence <span class="version-badge latest">v0.28.12</span></h3>
    <p>When <code>--features cluster</code> is enabled and <code>OXIDB_NODE_ID</code> is set, each node also writes its Raft state inside <code>OXIDB_DATA</code>:</p>
    <ul>
      <li><strong><code>raft_meta.json</code></strong> -- small file (~400 B): vote, last committed log id, last purged log id, last applied log id, current membership. Rewritten on metadata changes.</li>
      <li><strong><code>raft_log.jsonl</code></strong> -- append-only log: one openraft Entry per line. <code>append_to_log</code> is O(1) per entry; only conflict-resolution and snapshot purges rewrite the file.</li>
    </ul>
    <p>This is what allows a node to come back as a <code>Follower</code> after a restart instead of a fresh <code>Learner term=0</code>. Verified end-to-end at 1M records under mid-stream failover; see <code>oxidb-server/src/raft/log_store.rs</code> for the implementation.</p>
  </div>
</section>` }} />
}