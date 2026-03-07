import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Server",
  description: `Production-ready TCP server with security, clustering, and monitoring.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section id="server" class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="2" width="20" height="8" rx="2"/><rect x="2" y="14" width="20" height="8" rx="2"/><circle cx="6" cy="6" r="1"/><circle cx="6" cy="18" r="1"/></svg> Server</h2>
    <p class="section-desc">Production-ready TCP server with security, clustering, and monitoring.</p>

    <h3>Security</h3>
    <ul>
      <li><strong>Authentication</strong> -- SCRAM-SHA-256 (salted challenge-response). No plaintext passwords on the wire.</li>
      <li><strong>TLS/SSL</strong> -- Certificate-based encryption for all traffic.</li>
      <li><strong>RBAC</strong> -- Three roles:
        <ul>
          <li><code>Admin</code> -- Full access to all commands including user management</li>
          <li><code>ReadWrite</code> -- CRUD, indexes, transactions, blobs, search, procedures</li>
          <li><code>Read</code> -- Queries, counts, aggregations, list operations only</li>
        </ul>
      </li>
      <li><strong>Audit logging</strong> -- Every operation logged with timestamp and user context. GELF format for centralized logging.</li>
    </ul>

    <h3>Wire Protocol</h3>
    <p>OxiWire is OxiDB's custom binary protocol. Fixed-size encoding (1-byte type tags, 4-byte LE lengths, 8-byte LE numbers). Faster than JSON and MsgPack for both serialization and deserialization.</p>
    <p>Clients can also use plain JSON over the same TCP connection.</p>

    <h3>Clustering (Raft)</h3>
    <p>Multi-node replication via Raft consensus. State machine replication with persistent log. Enable with the <code>cluster</code> feature flag.</p>

    <h3>Configuration</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Environment Variable</th><th>Default</th><th>Description</th></tr></thead>
        <tbody>
          <tr><td><code>OXIDB_ADDR</code></td><td><code>127.0.0.1:4444</code></td><td>Bind address</td></tr>
          <tr><td><code>OXIDB_DATA</code></td><td><code>./oxidb_data</code></td><td>Data directory</td></tr>
          <tr><td><code>OXIDB_POOL_SIZE</code></td><td><code>4</code></td><td>Worker thread count</td></tr>
          <tr><td><code>OXIDB_IDLE_TIMEOUT</code></td><td><code>30</code></td><td>Connection timeout (seconds, 0 = never)</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</section>` }} />
}