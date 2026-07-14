import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Downloads",
  description: `Pre-built binaries for oxidb-server and oxidb CLI. Statically linked on Linux (musl). No dependencies required.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg> Downloads</h2>
    <p class="section-desc">Pre-built binaries for <code>oxidb-server</code> and <code>oxidb</code> CLI. Statically linked on Linux (musl). No dependencies required.</p>

    <!-- v0.34.0 -->
    <div class="release-block">
      <div class="release-header">
        <h3 class="version-tag">v0.34.0</h3>
        <span class="version-date">2026-04-10</span>
        <span class="version-badge latest">latest</span>
      </div>
      <p class="release-notes">WebAssembly support — run OxiDB in the browser. JWT auth, WebSocket subscriptions, security rules, TTL indexes, JS/TS SDK. <a href="/changelog/">Full changelog</a></p>

      <div class="table-wrap">
        <table class="dl-table">
          <thead>
            <tr><th>Platform</th><th>Architecture</th><th>File</th><th>Size</th><th>Type</th><th></th></tr>
          </thead>
          <tbody>
            <tr>
              <td>Linux</td>
              <td>x86_64</td>
              <td><code>oxidb-server-v0.34.0-linux-amd64.tar.gz</code></td>
              <td>5.4 MB</td>
              <td>musl static</td>
              <td><a href="/releases/v0.34.0/oxidb-server-v0.34.0-linux-amd64.tar.gz" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>Linux</td>
              <td>ARM64</td>
              <td><code>oxidb-server-v0.34.0-linux-arm64.tar.gz</code></td>
              <td>4.8 MB</td>
              <td>musl static</td>
              <td><a href="/releases/v0.34.0/oxidb-server-v0.34.0-linux-arm64.tar.gz" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>macOS</td>
              <td>ARM64 (Apple Silicon)</td>
              <td><code>oxidb-server-v0.34.0-darwin-arm64.tar.gz</code></td>
              <td>4.6 MB</td>
              <td>tar.gz</td>
              <td><a href="/releases/v0.34.0/oxidb-server-v0.34.0-darwin-arm64.tar.gz" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>macOS</td>
              <td>x86_64 (Intel)</td>
              <td><code>oxidb-server-v0.34.0-darwin-amd64.tar.gz</code></td>
              <td>5.1 MB</td>
              <td>tar.gz</td>
              <td><a href="/releases/v0.34.0/oxidb-server-v0.34.0-darwin-amd64.tar.gz" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>Windows</td>
              <td>x86_64</td>
              <td><code>oxidb-server-v0.34.0-windows-amd64.zip</code></td>
              <td>5.0 MB</td>
              <td>zip</td>
              <td><a href="/releases/v0.34.0/oxidb-server-v0.34.0-windows-amd64.zip" class="dl-btn">Download</a></td>
            </tr>
            <tr class="checksum-row">
              <td colspan="3"><strong>SHA256 Checksums</strong></td>
              <td></td>
              <td>txt</td>
              <td><a href="/releases/v0.34.0/SHA256SUMS.txt" class="dl-btn dl-btn-secondary">Verify</a></td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="install-hint">
        <h4>Quick Install (Server)</h4>
        <pre><code><span class="co"># Linux / macOS</span>
curl -LO https://oxidb.baltavista.com/releases/v0.34.0/oxidb-server-v0.34.0-linux-amd64.tar.gz
tar xzf oxidb-server-v0.34.0-linux-amd64.tar.gz
./oxidb-server

<span class="co"># Or use as a Rust library</span>
cargo add oxidb</code></pre>
      </div>
    </div>

    <!-- CLI v0.34.0 -->
    <div class="release-block">
      <div class="release-header">
        <h3 class="version-tag">oxidb CLI v0.34.0</h3>
        <span class="version-date">2026-04-10</span>
        <span class="version-badge latest">latest</span>
      </div>
      <p class="release-notes">Interactive shell and REPL with MongoDB-style syntax, SQL support, vector search, and embedded mode.</p>

      <div class="table-wrap">
        <table class="dl-table">
          <thead>
            <tr><th>Platform</th><th>Architecture</th><th>File</th><th>Size</th><th>Type</th><th></th></tr>
          </thead>
          <tbody>
            <tr>
              <td>Linux</td>
              <td>x86_64</td>
              <td><code>oxidb-cli-v0.34.0-linux-amd64.tar.gz</code></td>
              <td>3.9 MB</td>
              <td>musl static</td>
              <td><a href="/releases/v0.34.0/oxidb-cli-v0.34.0-linux-amd64.tar.gz" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>Linux</td>
              <td>ARM64</td>
              <td><code>oxidb-cli-v0.34.0-linux-arm64.tar.gz</code></td>
              <td>3.6 MB</td>
              <td>musl static</td>
              <td><a href="/releases/v0.34.0/oxidb-cli-v0.34.0-linux-arm64.tar.gz" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>macOS</td>
              <td>ARM64 (Apple Silicon)</td>
              <td><code>oxidb-cli-v0.34.0-darwin-arm64.tar.gz</code></td>
              <td>3.3 MB</td>
              <td>tar.gz</td>
              <td><a href="/releases/v0.34.0/oxidb-cli-v0.34.0-darwin-arm64.tar.gz" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>macOS</td>
              <td>x86_64 (Intel)</td>
              <td><code>oxidb-cli-v0.34.0-darwin-amd64.tar.gz</code></td>
              <td>3.7 MB</td>
              <td>tar.gz</td>
              <td><a href="/releases/v0.34.0/oxidb-cli-v0.34.0-darwin-amd64.tar.gz" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>Windows</td>
              <td>x86_64</td>
              <td><code>oxidb-cli-v0.34.0-windows-amd64.zip</code></td>
              <td>3.8 MB</td>
              <td>zip</td>
              <td><a href="/releases/v0.34.0/oxidb-cli-v0.34.0-windows-amd64.zip" class="dl-btn">Download</a></td>
            </tr>
            <tr class="checksum-row">
              <td colspan="3"><strong>SHA256 Checksums</strong></td>
              <td></td>
              <td>txt</td>
              <td><a href="/releases/v0.34.0/SHA256SUMS.txt" class="dl-btn dl-btn-secondary">Verify</a></td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="install-hint">
        <h4>Quick Install (CLI)</h4>
        <pre><code><span class="co"># Linux / macOS</span>
curl -LO https://oxidb.baltavista.com/releases/v0.34.0/oxidb-cli-v0.34.0-linux-amd64.tar.gz
tar xzf oxidb-cli-v0.34.0-linux-amd64.tar.gz
./oxidb --data ./mydb              <span class="co"># embedded mode</span>
./oxidb --host 127.0.0.1           <span class="co"># client mode</span>

<span class="co"># Or build from source</span>
cargo build --release -p oxidb-cli</code></pre>
      </div>
    </div>

    <!-- WebAssembly v0.34.0 -->
    <div class="release-block">
      <div class="release-header">
        <h3 class="version-tag">WebAssembly v0.34.0</h3>
        <span class="version-date">2026-04-10</span>
      </div>
      <p class="release-notes">Run OxiDB directly in the browser via WebAssembly. Built with wasm-pack.</p>

      <div class="table-wrap">
        <table class="dl-table">
          <thead>
            <tr><th>Platform</th><th>Architecture</th><th>File</th><th>Size</th><th>Type</th><th></th></tr>
          </thead>
          <tbody>
            <tr>
              <td>Browser</td>
              <td>wasm32</td>
              <td><code>oxidb-wasm-v0.34.0.tar.gz</code></td>
              <td>1.5 MB</td>
              <td>wasm-pack</td>
              <td><a href="https://github.com/parisxmas/OxiDB/releases/download/v0.34.0/oxidb-wasm-v0.34.0.tar.gz" class="dl-btn">Download</a></td>
            </tr>
            <tr class="checksum-row">
              <td colspan="3"><strong>SHA256 Checksums</strong></td>
              <td></td>
              <td>txt</td>
              <td><a href="https://github.com/parisxmas/OxiDB/releases/download/v0.34.0/SHA256SUMS.txt" class="dl-btn dl-btn-secondary">Verify</a></td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="install-hint">
        <h4>Quick Install (WebAssembly)</h4>
        <pre><code>curl -L -o oxidb-wasm.tar.gz \\
  https://github.com/parisxmas/OxiDB/releases/download/v0.34.0/oxidb-wasm-v0.34.0.tar.gz
mkdir wasm && tar xzf oxidb-wasm.tar.gz -C wasm/</code></pre>
      </div>
    </div>

    <!-- v0.18.0 -->
    <div class="release-block">
      <div class="release-header">
        <h3 class="version-tag">v0.18.0</h3>
        <span class="version-date">2026-03-05</span>
      </div>
      <p class="release-notes">OxiWire binary protocol, .NET EF Core provider, parallel serialization, MsgPack removed. <a href="/changelog/">Full changelog</a></p>

      <div class="table-wrap">
        <table class="dl-table">
          <thead>
            <tr><th>Platform</th><th>Architecture</th><th>File</th><th>Size</th><th>Type</th><th></th></tr>
          </thead>
          <tbody>
            <tr>
              <td>Linux</td>
              <td>x86_64</td>
              <td><code>oxidb-server-v0.18.0-linux-amd64.tar.gz</code></td>
              <td>5.4 MB</td>
              <td>musl static</td>
              <td><a href="/releases/v0.18.0/oxidb-server-v0.18.0-linux-amd64.tar.gz" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>Linux</td>
              <td>ARM64</td>
              <td><code>oxidb-server-v0.18.0-linux-arm64.tar.gz</code></td>
              <td>4.8 MB</td>
              <td>musl static</td>
              <td><a href="/releases/v0.18.0/oxidb-server-v0.18.0-linux-arm64.tar.gz" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>macOS</td>
              <td>ARM64 (Apple Silicon)</td>
              <td><code>oxidb-server-v0.18.0-darwin-arm64.tar.gz</code></td>
              <td>4.6 MB</td>
              <td>tar.gz</td>
              <td><a href="/releases/v0.18.0/oxidb-server-v0.18.0-darwin-arm64.tar.gz" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>macOS</td>
              <td>x86_64 (Intel)</td>
              <td><code>oxidb-server-v0.18.0-darwin-amd64.tar.gz</code></td>
              <td>5.1 MB</td>
              <td>tar.gz</td>
              <td><a href="/releases/v0.18.0/oxidb-server-v0.18.0-darwin-amd64.tar.gz" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>Windows</td>
              <td>x86_64</td>
              <td><code>oxidb-server-v0.18.0-windows-amd64.zip</code></td>
              <td>5.0 MB</td>
              <td>zip</td>
              <td><a href="/releases/v0.18.0/oxidb-server-v0.18.0-windows-amd64.zip" class="dl-btn">Download</a></td>
            </tr>
            <tr class="checksum-row">
              <td colspan="3"><strong>SHA256 Checksums</strong></td>
              <td></td>
              <td>txt</td>
              <td><a href="/releases/v0.18.0/SHA256SUMS.txt" class="dl-btn dl-btn-secondary">Verify</a></td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- CLI v0.18.0 -->
    <div class="release-block">
      <div class="release-header">
        <h3 class="version-tag">oxidb CLI v0.18.0</h3>
        <span class="version-date">2026-03-05</span>
      </div>
      <p class="release-notes">Interactive shell and REPL with MongoDB-style syntax, SQL support, vector search, and embedded mode.</p>

      <div class="table-wrap">
        <table class="dl-table">
          <thead>
            <tr><th>Platform</th><th>Architecture</th><th>File</th><th>Size</th><th>Type</th><th></th></tr>
          </thead>
          <tbody>
            <tr>
              <td>Linux</td>
              <td>x86_64</td>
              <td><code>oxidb-cli-v0.18.0-linux-amd64.tar.gz</code></td>
              <td>3.9 MB</td>
              <td>musl static</td>
              <td><a href="/releases/v0.18.0/oxidb-cli-v0.18.0-linux-amd64.tar.gz" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>Linux</td>
              <td>ARM64</td>
              <td><code>oxidb-cli-v0.18.0-linux-arm64.tar.gz</code></td>
              <td>3.6 MB</td>
              <td>musl static</td>
              <td><a href="/releases/v0.18.0/oxidb-cli-v0.18.0-linux-arm64.tar.gz" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>macOS</td>
              <td>ARM64 (Apple Silicon)</td>
              <td><code>oxidb-cli-v0.18.0-darwin-arm64.tar.gz</code></td>
              <td>3.3 MB</td>
              <td>tar.gz</td>
              <td><a href="/releases/v0.18.0/oxidb-cli-v0.18.0-darwin-arm64.tar.gz" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>macOS</td>
              <td>x86_64 (Intel)</td>
              <td><code>oxidb-cli-v0.18.0-darwin-amd64.tar.gz</code></td>
              <td>3.7 MB</td>
              <td>tar.gz</td>
              <td><a href="/releases/v0.18.0/oxidb-cli-v0.18.0-darwin-amd64.tar.gz" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>Windows</td>
              <td>x86_64</td>
              <td><code>oxidb-cli-v0.18.0-windows-amd64.zip</code></td>
              <td>3.8 MB</td>
              <td>zip</td>
              <td><a href="/releases/v0.18.0/oxidb-cli-v0.18.0-windows-amd64.zip" class="dl-btn">Download</a></td>
            </tr>
            <tr class="checksum-row">
              <td colspan="3"><strong>SHA256 Checksums</strong></td>
              <td></td>
              <td>txt</td>
              <td><a href="/releases/v0.18.0/SHA256SUMS.txt" class="dl-btn dl-btn-secondary">Verify</a></td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="other-install">
      <h3>Other Installation Methods</h3>
      <div class="table-wrap">
        <table>
          <thead><tr><th>Method</th><th>Command</th></tr></thead>
          <tbody>
            <tr><td>Rust (embedded library)</td><td><code>cargo add oxidb</code></td></tr>
            <tr><td>Python client</td><td><code>pip install oxidb</code> or copy <code>oxidb.py</code></td></tr>
            <tr><td>Python Embedded (FFI)</td><td><code>pip install oxidb-embedded</code></td></tr>
            <tr><td>Go client</td><td><code>go get oxidb</code></td></tr>
            <tr><td>.NET TCP client</td><td><code>dotnet add package OxiDb.Client.Tcp</code></td></tr>
            <tr><td>.NET Embedded</td><td><code>dotnet add package OxiDb.Client.Embedded</code></td></tr>
            <tr><td>.NET LINQ provider</td><td><code>dotnet add package OxiDb.Linq</code></td></tr>
            <tr><td>WebAssembly (browser)</td><td><code>See /wasm for setup</code></td></tr>
            <tr><td>Build server from source</td><td><code>cargo build --release -p oxidb-server</code></td></tr>
            <tr><td>Build CLI from source</td><td><code>cargo build --release -p oxidb-cli</code></td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="release-block">
      <div class="release-header">
        <h3 class="version-tag">The Book</h3>
        <span class="version-date">2026</span>
        <span class="version-badge latest">free</span>
      </div>
      <p class="release-notes"><strong>Her Şeyin Veritabanı</strong> — a free 235-page book (in Turkish) on how a database works inside, from first principles to OxiDB's document, SQL and time-series engines. <a href="/book/">Details</a></p>

      <div class="table-wrap">
        <table class="dl-table">
          <thead>
            <tr><th>Format</th><th>File</th><th>Size</th><th>Language</th><th></th></tr>
          </thead>
          <tbody>
            <tr>
              <td>PDF</td>
              <td><code>her-seyin-veritabani.pdf</code></td>
              <td>6.3 MB</td>
              <td>Türkçe</td>
              <td><a href="/kitap/her-seyin-veritabani.pdf" class="dl-btn">Download</a></td>
            </tr>
            <tr>
              <td>EPUB</td>
              <td><code>her-seyin-veritabani.epub</code></td>
              <td>965 KB</td>
              <td>Türkçe</td>
              <td><a href="/kitap/her-seyin-veritabani.epub" class="dl-btn">Download</a></td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

  </div>
</section>` }} />
}
