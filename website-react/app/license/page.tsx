import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "License",
  description: `OxiDB licensing — v0.40.0 and later is source-available: free to run in production for your own applications and business. A commercial license covers offering it as a service, or distributing it.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2l7 4v6c0 5-3.5 8.5-7 10-3.5-1.5-7-5-7-10V6l7-4z"/></svg> License</h2>
    <p class="section-desc">As of <strong>v0.40.0</strong>, OxiDB is <strong>source-available</strong>. Read it, modify it, and run it in production for your own applications and business — free, at any scale, with no registration.</p>

    <h3>What is free</h3>
    <p>Running the server for your own application, at any scale and on any number of instances. Reading and modifying the source. Building on it internally. Evaluating it without asking anyone.</p>
    <p>If your users reach <em>your</em> application and OxiDB is what stands behind it, that is free use. If your users reach <em>OxiDB</em> — you are selling access to the database, or a platform over it — that is the hosted-service case below.</p>

    <h3>What needs a commercial license</h3>
    <p>Two things, and only two:</p>
    <ul>
      <li><strong>Hosting</strong> — offering OxiDB, or a service built on it, to third parties over a network;</li>
      <li><strong>Distribution</strong> — shipping OxiDB to third parties, on its own or embedded in your product or device.</li>
    </ul>
    <p>A commercial license is negotiated directly and can also cover source access and modification rights, support, and update terms. To obtain one, email <a href="mailto:barisakin@gmail.com">barisakin@gmail.com</a>.</p>

    <h3>Client libraries</h3>
    <p>The thin <strong>TCP client libraries</strong> (Python <code>oxidb</code>, the JavaScript clients, <code>OxiDb.Client.Tcp</code> / <code>OxiDb.Linq</code> / <code>OxiDb.Data</code> on NuGet, Go, Julia, Dart, Swift) are <strong>MIT-licensed</strong>, redistribution included — shipping one inside your application needs no license.</p>
    <p>Packages that <strong>bundle the engine itself</strong> — <code>oxidb-embedded</code> on PyPI, <code>OxiDb.Client.Embedded</code> on NuGet, and the FFI/WASM artifacts — contain the engine and are covered by the source-available license.</p>

    <h3>Prior versions</h3>
    <p>Each version stays under the license it was published with, and those grants are irrevocable <em>for those specific versions</em>:</p>
    <div class="table-wrap">
      <table>
        <thead>
          <tr><th>Versions</th><th>License</th></tr>
        </thead>
        <tbody>
          <tr><td>Early releases (MIT era)</td><td><code>MIT OR Apache-2.0</code></td></tr>
          <tr><td>Up to and including <strong>v0.32.x</strong></td><td><code>AGPL-3.0-only</code> (dual: AGPL / commercial)</td></tr>
          <tr><td><strong>v0.33.0 – v0.39.x</strong></td><td>Proprietary — commercial license required for any use</td></tr>
          <tr><td><strong>v0.40.0 and later</strong></td><td><strong>Source-available — free for your own production use</strong></td></tr>
        </tbody>
      </table>
    </div>
    <p>v0.40.0 opens the v0.33–v0.39 line up rather than closing it further: those versions required a license for any use at all, including running it yourself. If you hold a commercial license covering them, nothing about it changes.</p>

    <h3>Why not open source</h3>
    <p>The engine is the product. An OSI license would let a cloud provider sell it as a managed service without contributing anything back — the asymmetry that pushed MongoDB, Elastic, Redis and HashiCorp off their original licenses. This license blocks that case and nothing else: your own production use, at any scale, is free.</p>
    <p>The text is modelled on the <strong>Elastic License 2.0</strong>, with one limitation added — ELv2 permits redistribution, and embedding OxiDB in a distributed product remains a commercial arrangement here. It carries no conversion date.</p>
  </div>
</section>` }} />
}
