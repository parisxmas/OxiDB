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
oxidb = "0.18"</code></pre>
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
        <h3>Go</h3>
        <p>Native Go client with OxiWire binary protocol support.</p>
        <pre><code>import "oxidb"
client, _ := oxidb.ConnectDefault()
client.UseOxiWire()</code></pre>
      </div>

      <div class="client-card">
        <div class="client-icon"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="14" rx="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></svg></div>
        <h3>.NET (TCP)</h3>
        <p>Pure managed C# client with async/await.</p>
        <pre><code>dotnet add package OxiDb.Client.Tcp</code></pre>
      </div>

      <div class="client-card">
        <div class="client-icon"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="4" width="16" height="16" rx="2"/><rect x="9" y="9" width="6" height="6"/><line x1="9" y1="1" x2="9" y2="4"/><line x1="15" y1="1" x2="15" y2="4"/><line x1="9" y1="20" x2="9" y2="23"/><line x1="15" y1="20" x2="15" y2="23"/><line x1="20" y1="9" x2="23" y2="9"/><line x1="20" y1="14" x2="23" y2="14"/><line x1="1" y1="9" x2="4" y2="9"/><line x1="1" y1="14" x2="4" y2="14"/></svg></div>
        <h3>.NET (Embedded)</h3>
        <p>FFI bindings to the native Rust library. In-process, no server needed.</p>
        <pre><code>dotnet add package OxiDb.Client.Embedded</code></pre>
      </div>

      <div class="client-card">
        <div class="client-icon"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg></div>
        <h3>.NET (EF Core)</h3>
        <p>Full Entity Framework Core provider. LINQ, migrations, both TCP and embedded modes.</p>
        <pre><code>dotnet add package OxiDb.EntityFrameworkCore</code></pre>
      </div>

      <div class="client-card">
        <div class="client-icon"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 3a3 3 0 00-3 3v12a3 3 0 003 3 3 3 0 003-3 3 3 0 00-3-3H6a3 3 0 00-3 3 3 3 0 003 3 3 3 0 003-3V6a3 3 0 00-3-3 3 3 0 00-3 3 3 3 0 003 3h12a3 3 0 003-3 3 3 0 00-3-3z"/></svg></div>
        <h3>Java / Spring Boot</h3>
        <p>Spring Boot starter with auto-configuration.</p>
        <pre><code>&lt;dependency&gt;
  &lt;groupId&gt;com.oxidb&lt;/groupId&gt;
  &lt;artifactId&gt;oxidb-spring-boot-starter&lt;/artifactId&gt;
&lt;/dependency&gt;</code></pre>
      </div>

      <div class="client-card">
        <div class="client-icon"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="4"/><line x1="4.93" y1="4.93" x2="9.17" y2="9.17"/><line x1="14.83" y1="14.83" x2="19.07" y2="19.07"/><line x1="14.83" y1="9.17" x2="19.07" y2="4.93"/><line x1="4.93" y1="19.07" x2="9.17" y2="14.83"/></svg></div>
        <h3>Julia</h3>
        <p>Julia language bindings with full API coverage.</p>
        <pre><code>using OxiDB
client = connect("127.0.0.1", 4444)</code></pre>
      </div>

      <div class="client-card">
        <div class="client-icon"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="2" width="14" height="20" rx="2"/><line x1="12" y1="18" x2="12.01" y2="18"/></svg></div>
        <h3>Swift</h3>
        <p>iOS/macOS support via C FFI to the embedded Rust library.</p>
        <pre><code>let db = OxiDB(path: "./data")</code></pre>
      </div>
    </div>
  </div>
</section>` }} />
}