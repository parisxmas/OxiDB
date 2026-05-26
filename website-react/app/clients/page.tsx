import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Client Libraries",
  description: `Official clients for multiple languages. All support the full OxiDB API.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section id="clients" class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg> Client Libraries</h2>
    <p class="section-desc">Official clients for multiple languages. All support the full OxiDB API.</p>

    <div class="client-grid">
      <div class="client-card">
        <div class="client-icon"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg></div>
        <h3>Rust (Embedded)</h3>
        <p>Use OxiDB as a library. Zero network overhead. Add to Cargo.toml:</p>
        <pre><code>[dependencies]
oxidb = "0.25"</code></pre>
      </div>

      <div class="client-card">
        <div class="client-icon"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 19l7-7 3 3-7 7-3-3z"/><path d="M18 13l-1.5-7.5L2 2l3.5 14.5L13 18l5-5z"/><path d="M2 2l7.586 7.586"/><circle cx="11" cy="11" r="2"/></svg></div>
        <h3>Python</h3>
        <p>Zero external dependencies. Uses only the standard library.</p>
        <pre><code>from oxidb import OxiDbClient
db = OxiDbClient("127.0.0.1", 4444)</code></pre>
      </div>

      <div class="client-card">
        <div class="client-icon"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg></div>
        <h3>Go <span class="version-badge latest">v0.25.1</span></h3>
        <p>Native Go client with OxiWire binary protocol. Stored procedures, TTL indexes, retention policies, alerting, blob text extraction, and backup/restore.</p>
        <pre><code>import "oxidb"
client, _ := oxidb.ConnectDefault()
client.UseOxiWire()

<span class="co">// Stored procedures</span>
client.CreateProcedure(<span class="str">"name"</span>, oxiScript)
client.CallProcedure(<span class="str">"name"</span>, args)

<span class="co">// TTL + retention</span>
client.CreateTTLIndex(<span class="str">"sessions"</span>, <span class="str">"created_at"</span>, <span class="num">3600</span>)
client.SetRetention(<span class="str">"events"</span>, <span class="num">30</span>)

<span class="co">// Alerting · Backup</span>
client.CreateAlert(rule)
client.Backup(path)</code></pre>
      </div>

      <div class="client-card">
        <div class="client-icon"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="14" rx="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></svg></div>
        <h3>.NET (TCP) <span class="version-badge latest">v0.28.18</span></h3>
        <p>Pure managed C# client. <code>HelloAsync</code> handshake, typed <code>FindAsync&lt;T&gt;</code> / <code>InsertReturningIdAsync</code>, <code>IAsyncEnumerable&lt;T&gt;</code> streaming, <code>services.AddOxiDbTcp(...)</code> DI extension, <code>Query.Eq/Gte/In/...</code> builder, domain exception hierarchy.</p>
        <pre><code>dotnet add package OxiDb.Client.Tcp</code></pre>
      </div>

      <div class="client-card">
        <div class="client-icon"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="4" width="16" height="16" rx="2"/><rect x="9" y="9" width="6" height="6"/><line x1="9" y1="1" x2="9" y2="4"/><line x1="15" y1="1" x2="15" y2="4"/><line x1="9" y1="20" x2="9" y2="23"/><line x1="15" y1="20" x2="15" y2="23"/><line x1="20" y1="9" x2="23" y2="9"/><line x1="20" y1="14" x2="23" y2="14"/><line x1="1" y1="9" x2="4" y2="9"/><line x1="1" y1="14" x2="4" y2="14"/></svg></div>
        <h3>.NET (Embedded) <span class="version-badge latest">v0.28.18</span></h3>
        <p>FFI bindings to the native Rust library. In-process, no server needed. Bundles native runtimes for darwin-arm64/amd64, linux-x64/arm64, windows-x64.</p>
        <pre><code>dotnet add package OxiDb.Client.Embedded</code></pre>
      </div>

      <div class="client-card">
        <div class="client-icon"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg></div>
        <h3>.NET (LINQ) <span class="version-badge latest">v0.28.18</span></h3>
        <p>Standalone LINQ provider over <code>IOxiDbClient</code>. <code>client.GetCollection&lt;T&gt;("users").Where(u =&gt; u.Age &gt;= 18).ToListAsync()</code>. Translates expression trees to OxiWire document queries — no SQL, no EF Core.</p>
        <pre><code>dotnet add package OxiDb.Linq</code></pre>
      </div>

      </div>
</section>` }} />
}