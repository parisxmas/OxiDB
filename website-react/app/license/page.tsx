import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "License",
  description: `OxiDB licensing — v0.33.0 and later is proprietary, commercially licensed software. Earlier versions remain available under their original open-source licenses.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2l7 4v6c0 5-3.5 8.5-7 10-3.5-1.5-7-5-7-10V6l7-4z"/></svg> License</h2>
    <p class="section-desc">As of <strong>v0.33.0</strong>, OxiDB is <strong>proprietary, commercially licensed software</strong>. Earlier versions remain available under their original open-source licenses.</p>

    <h3>Commercial licensing</h3>
    <p>All use of OxiDB v0.33.0 or later — running the server, embedding the engine (the Rust crates, FFI libraries, WASM build, or embedded client packages), redistributing it, or offering it as a service — requires a commercial license from the copyright holder.</p>
    <p>A commercial license is negotiated directly and can cover:</p>
    <ul>
      <li><strong>Embedding</strong> — shipping OxiDB inside your application or device;</li>
      <li><strong>Redistribution</strong> — bundling OxiDB binaries with your product;</li>
      <li><strong>Hosting</strong> — offering OxiDB, or a service built on it, to third parties over a network;</li>
      <li><strong>Source access and modification rights</strong>, support, and update terms as agreed.</li>
    </ul>
    <p>To obtain a commercial license, contact the author via <a href="https://github.com/parisxmas">GitHub (@parisxmas)</a>.</p>

    <h3>Client libraries</h3>
    <p>The thin <strong>TCP client libraries</strong> (Python <code>oxidb</code>, npm <code>oxidb</code>, <code>OxiDb.Client.Tcp</code> / <code>OxiDb.Linq</code> / <code>OxiDb.Data</code> on NuGet, Go, Julia, PHP) are <strong>MIT-licensed</strong>. Talking to a licensed OxiDB server from your own application does not require a commercial license of its own.</p>
    <p>Packages that <strong>bundle the engine itself</strong> — <code>oxidb-embedded</code> on PyPI, <code>OxiDb.Client.Embedded</code> on NuGet, and the FFI/WASM artifacts — contain the proprietary engine and are covered by the commercial license.</p>

    <h3>Prior versions</h3>
    <p>Earlier versions of OxiDB were published under open-source licenses, and those grants are irrevocable <em>for those specific versions</em>:</p>
    <div class="table-wrap">
      <table>
        <thead>
          <tr><th>Versions</th><th>License</th></tr>
        </thead>
        <tbody>
          <tr><td>Early releases (MIT era)</td><td><code>MIT OR Apache-2.0</code></td></tr>
          <tr><td>Later releases up to and including <strong>v0.32.x</strong></td><td><code>AGPL-3.0-only</code> (dual: AGPL / commercial)</td></tr>
          <tr><td><strong>v0.33.0 and later</strong></td><td><strong>Proprietary — commercial license required</strong></td></tr>
        </tbody>
      </table>
    </div>
    <p>You may continue to use those past versions under their original terms. No new versions will be published under an open-source license.</p>
  </div>
</section>` }} />
}
