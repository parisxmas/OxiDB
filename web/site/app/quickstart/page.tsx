import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Quick Start",
  description: `Install and run OxiDB from scratch — download the server binary, start it, connect with the CLI or any client library, and run your first query.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section id="quickstart" class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> Quick Start</h2>
    <p class="section-desc">Install OxiDB from scratch in under five minutes. Pick a path:</p>
    <ul>
      <li><strong>Server mode</strong> — one binary, listens on TCP, talk to it from any language. Start <a href="#server-install">here</a>.</li>
      <li><strong>Embedded mode</strong> — link OxiDB into your app, no separate process. Jump to <a href="#embedded">Embed in your app</a>.</li>
    </ul>

    <h3 id="server-install">1. Install the server</h3>
    <p>Pre-built static binaries — no runtime, no installer, no dependencies. Pick your platform:</p>

    <h4>Linux (x86_64)</h4>
    <pre><code class="lang-bash">curl -LO https://oxidb.baltavista.com/releases/v0.42.8/oxidb-server-v0.42.8-linux-amd64.tar.gz
tar xzf oxidb-server-v0.42.8-linux-amd64.tar.gz
sudo mv oxidb-server /usr/local/bin/
oxidb-server --version</code></pre>

    <h4>Linux (ARM64)</h4>
    <pre><code class="lang-bash">curl -LO https://oxidb.baltavista.com/releases/v0.42.8/oxidb-server-v0.42.8-linux-arm64.tar.gz
tar xzf oxidb-server-v0.42.8-linux-arm64.tar.gz
sudo mv oxidb-server /usr/local/bin/</code></pre>

    <h4>macOS (Apple Silicon / Intel)</h4>
    <pre><code class="lang-bash"><span class="co"># Apple Silicon</span>
curl -LO https://oxidb.baltavista.com/releases/v0.42.8/oxidb-server-v0.42.8-darwin-arm64.tar.gz
tar xzf oxidb-server-v0.42.8-darwin-arm64.tar.gz

<span class="co"># Intel</span>
curl -LO https://oxidb.baltavista.com/releases/v0.42.8/oxidb-server-v0.42.8-darwin-amd64.tar.gz
tar xzf oxidb-server-v0.42.8-darwin-amd64.tar.gz

sudo mv oxidb-server /usr/local/bin/</code></pre>

    <h4>Windows (x86_64)</h4>
    <pre><code class="lang-bash"><span class="co"># PowerShell</span>
Invoke-WebRequest -Uri https://oxidb.baltavista.com/releases/v0.42.8/oxidb-server-v0.42.8-windows-amd64.zip -OutFile oxidb-server.zip
Expand-Archive oxidb-server.zip -DestinationPath .
.\\oxidb-server.exe --version</code></pre>

    <h4>Build from source (any platform)</h4>
    <pre><code class="lang-bash">git clone https://github.com/parisxmas/OxiDB.git
cd OxiDB
cargo build --release -p oxidb-server
./target/release/oxidb-server --version</code></pre>

    <h4>Docker</h4>
    <pre><code class="lang-bash">docker run -d --name oxidb \\
  -p 4444:4444 \\
  -v $(pwd)/data:/data \\
  -e OXIDB_DATA=/data \\
  -e OXIDB_ADDR=0.0.0.0:4444 \\
  ghcr.io/parisxmas/oxidb:0.36.0</code></pre>

    <h3>2. Start the server</h3>
    <pre><code class="lang-bash"><span class="co"># Defaults: listens on 127.0.0.1:4444, data directory ./oxidb_data</span>
oxidb-server

<span class="co"># Or override with environment variables:</span>
OXIDB_ADDR=0.0.0.0:4444 \\
OXIDB_DATA=/var/lib/oxidb \\
OXIDB_POOL_SIZE=8 \\
oxidb-server</code></pre>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Variable</th><th>Default</th><th>Purpose</th></tr></thead>
        <tbody>
          <tr><td><code>OXIDB_ADDR</code></td><td><code>127.0.0.1:4444</code></td><td>Bind address</td></tr>
          <tr><td><code>OXIDB_DATA</code></td><td><code>./oxidb_data</code></td><td>Data directory</td></tr>
          <tr><td><code>OXIDB_POOL_SIZE</code></td><td><code>4</code></td><td>Worker threads</td></tr>
          <tr><td><code>OXIDB_IDLE_TIMEOUT</code></td><td><code>30</code></td><td>Idle connection timeout (s, 0 = never)</td></tr>
        </tbody>
      </table>
    </div>
    <p>For TLS, SCRAM-SHA-256 auth, RBAC, and audit logging see <a href="/server/">Server</a>.</p>

    <h3>3. Install the CLI &amp; run your first query</h3>
    <pre><code class="lang-bash"><span class="co"># Linux example — pick the matching CLI archive from /downloads/ for your OS</span>
curl -LO https://oxidb.baltavista.com/releases/v0.42.8/oxidb-v0.42.8-linux-amd64.tar.gz
tar xzf oxidb-v0.42.8-linux-amd64.tar.gz
sudo mv oxidb /usr/local/bin/

<span class="co"># Open the REPL against the running server</span>
oxidb --host 127.0.0.1 --port 4444

oxidb&gt; insert users {<span class="str">"name"</span>: <span class="str">"Alice"</span>, <span class="str">"age"</span>: <span class="num">30</span>, <span class="str">"department"</span>: <span class="str">"Engineering"</span>}
oxidb&gt; find   users {<span class="str">"age"</span>: {<span class="str">"$gte"</span>: <span class="num">18</span>}}
oxidb&gt; update users {<span class="str">"name"</span>: <span class="str">"Alice"</span>} {<span class="str">"$inc"</span>: {<span class="str">"age"</span>: <span class="num">1</span>}}
oxidb&gt; count  users
oxidb&gt; .exit</code></pre>

    <h3>4. Connect from your application</h3>

    <h4>Python</h4>
    <pre><code class="lang-bash">pip install oxidb</code></pre>
    <pre><code class="lang-python"><span class="kw">from</span> oxidb <span class="kw">import</span> OxiDbClient

<span class="kw">with</span> OxiDbClient(<span class="str">"127.0.0.1"</span>, <span class="num">4444</span>) <span class="kw">as</span> db:
    db.insert(<span class="str">"users"</span>, {<span class="str">"name"</span>: <span class="str">"Alice"</span>, <span class="str">"age"</span>: <span class="num">30</span>})
    docs = db.find(<span class="str">"users"</span>, {<span class="str">"age"</span>: {<span class="str">"$gte"</span>: <span class="num">25</span>}})
    db.update(<span class="str">"users"</span>, {<span class="str">"name"</span>: <span class="str">"Alice"</span>}, {<span class="str">"$inc"</span>: {<span class="str">"age"</span>: <span class="num">1</span>}})</code></pre>

    <h4>Go</h4>
    <pre><code class="lang-bash">go get github.com/parisxmas/oxiwire-go</code></pre>
    <pre><code class="lang-go">client, _ := oxidb.ConnectDefault()
<span class="kw">defer</span> client.Close()
client.UseOxiWire() <span class="co">// binary protocol — fastest</span>

client.Insert(<span class="str">"users"</span>, <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{
    <span class="str">"name"</span>: <span class="str">"Alice"</span>, <span class="str">"age"</span>: <span class="num">30</span>,
})
docs, _ := client.Find(<span class="str">"users"</span>, <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{
    <span class="str">"age"</span>: <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"$gte"</span>: <span class="num">25</span>},
}, <span class="kw">nil</span>)</code></pre>

    <h4>.NET (TCP + LINQ)</h4>
    <pre><code class="lang-bash">dotnet add package OxiDb.Client.Tcp
dotnet add package OxiDb.Linq  <span class="co"># optional typed LINQ surface</span></code></pre>
    <pre><code class="lang-csharp"><span class="co">// Program.cs</span>
builder.Services.AddOxiDb(options => {
    options.UseTcp(<span class="str">"127.0.0.1"</span>, <span class="num">4444</span>);
});

<span class="kw">var</span> users = <span class="kw">await</span> db.Users
    .Where(u => u.Age &gt;= <span class="num">25</span>)
    .OrderBy(u => u.Name)
    .ToListAsync();</code></pre>

    <p>Other languages: <a href="/clients/">Julia, Swift, JavaScript/TypeScript</a>.</p>

    <h3 id="engines">5. Use all three engines</h3>
    <p>OxiDB bundles three engines in one server. The <strong>document</strong> engine is always on; enable the <strong>SQL</strong> and <strong>time-series</strong> engines with env vars (zero cost when off). Each owns entirely separate files.</p>
    <pre><code class="lang-bash">OXIDB_SQL=1 OXIDB_TSDB=1 oxidb-server</code></pre>

    <h4>Document &mdash; MongoDB-style</h4>
    <pre><code class="lang-python">db.insert(<span class="str">"users"</span>, {<span class="str">"name"</span>: <span class="str">"Alice"</span>, <span class="str">"age"</span>: <span class="num">30</span>})
db.find(<span class="str">"users"</span>, {<span class="str">"age"</span>: {<span class="str">"$gte"</span>: <span class="num">18</span>}})
db.aggregate(<span class="str">"orders"</span>, [{<span class="str">"$group"</span>: {<span class="str">"_id"</span>: <span class="str">"$user"</span>, <span class="str">"total"</span>: {<span class="str">"$sum"</span>: <span class="str">"$amount"</span>}}}])</code></pre>

    <h4>SQL &mdash; relational</h4>
    <pre><code class="lang-python"><span class="co"># the Python client has a .sql() helper (server started with OXIDB_SQL=1)</span>
db.sql(<span class="str">"CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)"</span>)
db.sql(<span class="str">"INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)"</span>)
rows = db.sql(<span class="str">"SELECT name, age FROM users WHERE age &gt;= ? ORDER BY age"</span>, [<span class="num">18</span>])
db.sql(<span class="str">"ALTER TABLE users ADD COLUMN score INT DEFAULT 0"</span>)   <span class="co"># instant, online</span></code></pre>
    <p>Joins, CTEs, window functions, transactions, and the EF Core provider are on the <a href="/sql/">SQL Engine</a> page.</p>

    <h4>Time-series &mdash; InfluxDB-style</h4>
    <p>Send an <code>engine: "tsdb"</code> request (any client that speaks the JSON protocol; the Go client has native helpers):</p>
    <pre><code class="lang-json"><span class="co">// write a point</span>
{"engine":"tsdb","cmd":"tsdb","op":"write","measurement":"cpu","tags":{"host":"a"},"fields":{"usage":0.63},"ts":1700000000000}

<span class="co">// query — mean over 10-minute buckets</span>
{"engine":"tsdb","cmd":"tsdb","op":"query","measurement":"cpu","field":"usage","interval":600000,"agg":"mean"}</code></pre>
    <p>Line protocol, rollups, and retention are on the <a href="/tsdb/">Time-Series Engine</a> page.</p>

    <h3 id="embedded">Embed in your app (no server)</h3>
    <p>Skip the TCP layer entirely — link OxiDB into your process, talk to it via function calls, zero network overhead.</p>

    <h4>Rust</h4>
    <pre><code class="lang-bash">cargo add oxidb</code></pre>
    <pre><code class="lang-rust"><span class="kw">use</span> oxidb::OxiDb;
<span class="kw">use</span> serde_json::json;

<span class="kw">fn</span> <span class="fn">main</span>() {
    <span class="kw">let</span> db = OxiDb::open(<span class="str">"./my_data"</span>).unwrap();

    db.insert(<span class="str">"users"</span>, json!({
        <span class="str">"name"</span>: <span class="str">"Alice"</span>, <span class="str">"age"</span>: <span class="num">30</span>
    })).unwrap();

    <span class="kw">let</span> results = db.find(<span class="str">"users"</span>,
        json!({<span class="str">"age"</span>: {<span class="str">"$gte"</span>: <span class="num">25</span>}}), <span class="kw">None</span>).unwrap();
}</code></pre>

    <h4>Python (Embedded via FFI)</h4>
    <pre><code class="lang-bash">pip install oxidb-embedded</code></pre>
    <pre><code class="lang-python"><span class="kw">from</span> oxidb_embedded <span class="kw">import</span> OxiDb

db = OxiDb(<span class="str">"./my_data"</span>)
db.insert(<span class="str">"users"</span>, {<span class="str">"name"</span>: <span class="str">"Alice"</span>, <span class="str">"age"</span>: <span class="num">30</span>})</code></pre>

    <h4>.NET (Embedded via FFI)</h4>
    <pre><code class="lang-bash">dotnet add package OxiDb.Client.Embedded</code></pre>

    <h4>WebAssembly (browser)</h4>
    <p>Run the full engine in the browser — in-memory, no server. See <a href="/wasm/">WebAssembly</a>.</p>

    <h3>Production: 3-node Raft cluster <span class="version-badge latest">v0.42.8</span></h3>
    <p>Each node sets a unique <code>OXIDB_NODE_ID</code> and the same <code>OXIDB_RAFT_PEERS</code> list. After all three are up, bootstrap once via the leader candidate. Raft state is persisted to disk, so nodes survive container restarts.</p>
    <pre><code class="lang-bash"><span class="co"># node 1 — initial leader candidate</span>
OXIDB_NODE_ID=1 OXIDB_RAFT_ADDR=0.0.0.0:5000 \\
  OXIDB_RAFT_PEERS=<span class="str">"1=db-a0:5000,2=db-a1:5000,3=db-a2:5000"</span> \\
  oxidb-server &amp;

<span class="co"># node 2 + node 3 — same OXIDB_RAFT_PEERS, different NODE_ID and host</span>

<span class="co"># one-shot bootstrap on db-a0</span>
oxidb raft_init
oxidb raft_add_learner --id=2 --addr=db-a1:5000
oxidb raft_add_learner --id=3 --addr=db-a2:5000
oxidb raft_change_membership --members=1,2,3</code></pre>
    <p>For a full reference deployment (3 shards × 3 Raft nodes, oxipool routing, Go API, Python failover + 1M-record load tests) see <a href="https://github.com/parisxmas/OxiDB/tree/master/ShardReplicaRealWorldTest"><code>ShardReplicaRealWorldTest/</code></a> in the repo.</p>

    <h3>Next steps</h3>
    <ul>
      <li><a href="/queries/">Query operators</a> — JSON query language reference</li>
      <li><a href="/aggregation/">Aggregation</a> — pipeline stages, JOINs via $lookup, GROUP BY via $group</li>
      <li><a href="/sql/">SQL Engine</a> — joins, CTEs, window functions, EF Core, online ALTER</li>
      <li><a href="/tsdb/">Time-Series Engine</a> — line protocol, downsampling, rollups</li>
      <li><a href="/transactions/">Transactions</a> — multi-collection ACID with OCC</li>
      <li><a href="/server/">Server</a> — TLS, auth, RBAC, sharding, Raft</li>
      <li><a href="/clients/">Clients</a> — every language binding</li>
    </ul>
  </div>
</section>` }} />
}
