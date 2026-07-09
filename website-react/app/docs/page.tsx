'use client'

import { useEffect } from 'react'

import type { Metadata } from "next"

export default function Page() {
  useEffect(() => {
    // Language tab switching
    document.querySelectorAll('.lang-tab').forEach(tab => {
      tab.addEventListener('click', () => {
        const block = tab.closest('.doc-block')
        if (!block) return
        block.querySelectorAll('.lang-tab').forEach(t => t.classList.remove('active'))
        block.querySelectorAll('.lang-panel').forEach(p => p.classList.remove('active'))
        tab.classList.add('active')
        const lang = tab.getAttribute('data-lang')
        block.querySelectorAll(`.lang-panel[data-lang="${lang}"]`).forEach(p => p.classList.add('active'))
      })
    })
    // Double-click to switch all
    document.querySelectorAll('.lang-tab').forEach(tab => {
      tab.addEventListener('dblclick', () => {
        const lang = tab.getAttribute('data-lang')
        document.querySelectorAll('.doc-block').forEach(block => {
          const matchTab = block.querySelector(`.lang-tab[data-lang="${lang}"]`)
          if (matchTab) {
            block.querySelectorAll('.lang-tab').forEach(t => t.classList.remove('active'))
            block.querySelectorAll('.lang-panel').forEach(p => p.classList.remove('active'))
            matchTab.classList.add('active')
            block.querySelectorAll(`.lang-panel[data-lang="${lang}"]`).forEach(p => p.classList.add('active'))
          }
        })
      })
    })
  }, [])

  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2 3h6a4 4 0 014 4v14a3 3 0 00-3-3H2z"/><path d="M22 3h-6a4 4 0 00-4 4v14a3 3 0 013-3h7z"/></svg> API Documentation</h2>
    <p class="section-desc">Every OxiDB command with examples in Rust, Python, Go, and .NET. Click a language tab to switch.</p>

    <!-- Table of Contents -->
    <div class="toc">
      <div class="toc-group">
        <h4>Connection</h4>
        <a href="#connect">Connect / Open</a>
        <a href="#close">Close</a>
        <a href="#ping">Ping</a>
      </div>
      <div class="toc-group">
        <h4>Collections</h4>
        <a href="#create-collection">Create Collection</a>
        <a href="#list-collections">List Collections</a>
        <a href="#drop-collection">Drop Collection</a>
      </div>
      <div class="toc-group">
        <h4>CRUD</h4>
        <a href="#insert">Insert</a>
        <a href="#insert-many">Insert Many</a>
        <a href="#find">Find</a>
        <a href="#find-one">Find One</a>
        <a href="#count">Count</a>
        <a href="#update">Update</a>
        <a href="#update-one">Update One</a>
        <a href="#delete">Delete</a>
        <a href="#delete-one">Delete One</a>
      </div>
      <div class="toc-group">
        <h4>Indexes</h4>
        <a href="#create-index">Create Index</a>
        <a href="#create-unique-index">Create Unique Index</a>
        <a href="#create-composite-index">Create Composite Index</a>
        <a href="#create-text-index">Create Text Index</a>
        <a href="#list-indexes">List Indexes</a>
        <a href="#drop-index">Drop Index</a>
      </div>
      <div class="toc-group">
        <h4>Aggregation</h4>
        <a href="#aggregate">Aggregate</a>
      </div>
      <div class="toc-group">
        <h4>Transactions</h4>
        <a href="#begin-tx">Begin Transaction</a>
        <a href="#commit-tx">Commit Transaction</a>
        <a href="#rollback-tx">Rollback Transaction</a>
        <a href="#transaction-ctx">Transaction Context</a>
      </div>
      <div class="toc-group">
        <h4>Search</h4>
        <a href="#text-search">Text Search</a>
        <a href="#search">Search (Blobs)</a>
      </div>
      <div class="toc-group">
        <h4>Vectors</h4>
        <a href="#create-vector-index">Create Vector Index</a>
        <a href="#vector-search">Vector Search</a>
      </div>
      <div class="toc-group">
        <h4>Blob Storage</h4>
        <a href="#create-bucket">Create Bucket</a>
        <a href="#list-buckets">List Buckets</a>
        <a href="#delete-bucket">Delete Bucket</a>
        <a href="#put-object">Put Object</a>
        <a href="#get-object">Get Object</a>
        <a href="#head-object">Head Object</a>
        <a href="#delete-object">Delete Object</a>
        <a href="#list-objects">List Objects</a>
      </div>
      <div class="toc-group">
        <h4>Maintenance</h4>
        <a href="#compact">Compact</a>
      </div>
    </div>

    <!-- ============================================================ -->
    <!-- CONNECTION                                                    -->
    <!-- ============================================================ -->

    <h3 style="font-size:22px; margin-top:40px; padding-top:24px; border-top:2px solid var(--accent-light);">Connection</h3>

    <div class="doc-block" id="connect">
      <h3>Connect / Open</h3>
      <p>Open a database connection. TCP mode connects to a server; embedded mode opens files directly.</p>
      <div class="lang-tabs" data-group="connect">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="python-emb">Python Embedded</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">use</span> oxidb::OxiDb;

<span class="co">// Embedded — opens database files at the given path</span>
<span class="kw">let</span> db = OxiDb::open(<span class="str">"./my_data"</span>).unwrap();

<span class="co">// With encryption</span>
<span class="kw">let</span> db = OxiDb::open_encrypted(<span class="str">"./my_data"</span>, <span class="str">"./key.bin"</span>).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code><span class="kw">from</span> oxidb <span class="kw">import</span> OxiDbClient

<span class="co"># TCP client — connects to oxidb-server</span>
client = OxiDbClient(<span class="str">"127.0.0.1"</span>, <span class="num">4444</span>)

<span class="co"># With timeout</span>
client = OxiDbClient(<span class="str">"127.0.0.1"</span>, <span class="num">4444</span>, timeout=<span class="num">10.0</span>)

<span class="co"># Context manager</span>
<span class="kw">with</span> OxiDbClient() <span class="kw">as</span> db:
    db.ping()</code></pre></div>
      <div class="lang-panel" data-lang="python-emb"><pre><code><span class="kw">from</span> oxidb_embedded <span class="kw">import</span> OxiDbEmbedded

<span class="co"># Embedded — no server required</span>
db = OxiDbEmbedded(<span class="str">"./my_data"</span>)

<span class="co"># With encryption</span>
db = OxiDbEmbedded(<span class="str">"./my_data"</span>, encryption_key_path=<span class="str">"./key.bin"</span>)

<span class="co"># Context manager</span>
<span class="kw">with</span> OxiDbEmbedded(<span class="str">"./my_data"</span>) <span class="kw">as</span> db:
    db.ping()</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>client, err := oxidb.Connect(<span class="str">"127.0.0.1"</span>, <span class="num">4444</span>)
<span class="co">// or</span>
client, err := oxidb.ConnectDefault() <span class="co">// localhost:4444</span>

<span class="co">// Enable binary protocol (faster)</span>
client.UseOxiWire()

<span class="kw">defer</span> client.Close()</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="co">// TCP client</span>
<span class="kw">var</span> client = <span class="kw">new</span> OxiDbTcpClient(<span class="str">"127.0.0.1"</span>, <span class="num">4444</span>);

<span class="co">// Embedded client</span>
<span class="kw">var</span> client = <span class="kw">new</span> OxiDbEmbeddedClient(<span class="str">"./my_data"</span>);

<span class="co">// LINQ over either client</span>
<span class="kw">var</span> users = client.GetCollection&lt;User&gt;(<span class="str">"users"</span>);</code></pre></div>
    </div>

    <div class="doc-block" id="close">
      <h3>Close</h3>
      <p>Close the database connection and free resources.</p>
      <div class="lang-tabs" data-group="close">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="co">// Dropped automatically when out of scope</span>
drop(db);</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.close()
<span class="co"># or use context manager: with OxiDbClient() as db: ...</span></code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>client.Close()</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code>client.Dispose();
<span class="co">// or: using var client = new OxiDbTcpClient(...);</span></code></pre></div>
    </div>

    <div class="doc-block" id="ping">
      <h3>Ping</h3>
      <p>Check that the database is reachable. Returns <code>"pong"</code>.</p>
      <div class="lang-tabs" data-group="ping">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">let</span> pong = db.ping(); <span class="co">// "pong"</span></code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>result = client.ping()  <span class="co"># "pong"</span></code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>result, err := client.Ping() <span class="co">// "pong"</span></code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">var</span> result = <span class="kw">await</span> client.PingAsync(); <span class="co">// "pong"</span></code></pre></div>
    </div>

    <div class="doc-block" id="hello">
      <h3>Hello (handshake) <span class="version-badge latest">v0.28.13+</span></h3>
      <p>Pre-auth, idempotent server-info handshake. Returns the server&apos;s version, the wire-protocol versions it speaks, the 1.0 stable feature set, the experimental feature set, and the auth methods accepted. New clients send this as the first message to negotiate capabilities; old clients that skip it still work (the server defaults to wire v1). See <a href="/format/compat-matrix/" target="_blank" rel="noopener">compat-matrix.md</a> for the full negotiation rules.</p>
      <p><strong>Wire format</strong> (OxiWire over TCP):</p>
      <pre><code><span class="co">// Request</span>
{
  <span class="str">"cmd"</span>: <span class="str">"hello"</span>,
  <span class="str">"client"</span>: <span class="str">"oxidb-py/1.0"</span>,        <span class="co">// optional, free-form identification</span>
  <span class="str">"wire_versions"</span>: [<span class="num">1</span>]          <span class="co">// optional, defaults to [1]</span>
}

<span class="co">// Response</span>
{
  <span class="str">"ok"</span>: <span class="kw">true</span>,
  <span class="str">"server"</span>: {
    <span class="str">"name"</span>: <span class="str">"oxidb-server"</span>,
    <span class="str">"version"</span>: <span class="str">"0.34.0"</span>,
    <span class="str">"wire_version"</span>: <span class="num">1</span>,
    <span class="str">"supported_wire_versions"</span>: [<span class="num">1</span>],
    <span class="str">"stable_surface_version"</span>: <span class="str">"1.0"</span>,
    <span class="str">"features"</span>: [<span class="str">"fts"</span>, <span class="str">"blobs"</span>, <span class="str">"txn"</span>, <span class="str">"rbac"</span>, <span class="str">"tls"</span>, <span class="str">"encryption_at_rest"</span>, <span class="str">"audit"</span>, <span class="str">"scram_sha_256"</span>, <span class="str">"indexes"</span>, <span class="str">"aggregation"</span>],
    <span class="str">"experimental_features"</span>: [<span class="str">"raft"</span>, <span class="str">"pitr"</span>, <span class="str">"vector_search"</span>, <span class="str">"fdw"</span>, <span class="str">"stored_procedures"</span>, <span class="str">"ttl_indexes"</span>, <span class="str">"change_streams"</span>, <span class="str">"rest_http"</span>, <span class="str">"websocket"</span>, <span class="str">"oximem"</span>, <span class="str">"mqtt"</span>, <span class="str">"s3"</span>, <span class="str">"gelf"</span>],
    <span class="str">"auth_methods"</span>: [<span class="str">"scram-sha-256"</span>]    <span class="co">// or ["anonymous"] when auth is disabled</span>
  }
}</code></pre>
      <p>If the client&apos;s <code>wire_versions</code> array shares no element with the server&apos;s <code>supported_wire_versions</code>, the server returns <code>&#123;"ok": false, "error": "no compatible wire version …"&#125;</code> and the client should close the connection.</p>
      <p><strong>REST equivalent:</strong> <code>GET /v1/hello</code> returns the same server-info envelope (unauthenticated).</p>
      <p><strong>Note:</strong> Client SDK methods (<code>db.hello()</code>, <code>client.hello()</code>, etc.) are scheduled for Phase 3 of the 1.0 prep. For now, you can send the raw command via the low-level <code>send_raw</code> / <code>execute</code> escape hatch most clients expose.</p>
    </div>

    <!-- ============================================================ -->
    <!-- COLLECTIONS                                                   -->
    <!-- ============================================================ -->

    <h3 style="font-size:22px; margin-top:40px; padding-top:24px; border-top:2px solid var(--accent-light);">Collections</h3>

    <div class="doc-block" id="create-collection">
      <h3>Create Collection</h3>
      <p>Explicitly create a collection. Collections are also auto-created on first insert.</p>
      <div class="lang-tabs" data-group="create-collection">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.create_collection(<span class="str">"users"</span>).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.create_collection(<span class="str">"users"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>err := client.CreateCollection(<span class="str">"users"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.CreateCollectionAsync(<span class="str">"users"</span>);</code></pre></div>
    </div>

    <div class="doc-block" id="list-collections">
      <h3>List Collections</h3>
      <div class="lang-tabs" data-group="list-collections">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">let</span> names = db.list_collections(); <span class="co">// Vec&lt;String&gt;</span></code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>names = client.list_collections()  <span class="co"># ["users", "orders"]</span></code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>names, err := client.ListCollections() <span class="co">// []string</span></code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">var</span> names = <span class="kw">await</span> client.ListCollectionsAsync();</code></pre></div>
    </div>

    <div class="doc-block" id="drop-collection">
      <h3>Drop Collection</h3>
      <p>Delete a collection and all its data.</p>
      <div class="lang-tabs" data-group="drop-collection">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.drop_collection(<span class="str">"temp"</span>).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.drop_collection(<span class="str">"temp"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>err := client.DropCollection(<span class="str">"temp"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.DropCollectionAsync(<span class="str">"temp"</span>);</code></pre></div>
    </div>

    <!-- ============================================================ -->
    <!-- CRUD                                                          -->
    <!-- ============================================================ -->

    <h3 style="font-size:22px; margin-top:40px; padding-top:24px; border-top:2px solid var(--accent-light);">CRUD Operations</h3>

    <div class="doc-block" id="insert">
      <h3>Insert</h3>
      <p>Insert a single document. Returns the generated <code>_id</code>. Collection is auto-created if it doesn't exist.</p>
      <div class="lang-tabs" data-group="insert">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">let</span> id = db.insert(<span class="str">"users"</span>, json!({
    <span class="str">"name"</span>: <span class="str">"Alice"</span>,
    <span class="str">"age"</span>: <span class="num">30</span>,
    <span class="str">"email"</span>: <span class="str">"alice@example.com"</span>
})).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>result = client.insert(<span class="str">"users"</span>, {
    <span class="str">"name"</span>: <span class="str">"Alice"</span>,
    <span class="str">"age"</span>: <span class="num">30</span>,
    <span class="str">"email"</span>: <span class="str">"alice@example.com"</span>,
})
<span class="co"># result: {"id": 1}</span></code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>result, err := client.Insert(<span class="str">"users"</span>, <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{
    <span class="str">"name"</span>:  <span class="str">"Alice"</span>,
    <span class="str">"age"</span>:   <span class="num">30</span>,
    <span class="str">"email"</span>: <span class="str">"alice@example.com"</span>,
})</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">var</span> result = <span class="kw">await</span> client.InsertAsync(<span class="str">"users"</span>, <span class="kw">new</span> {
    name = <span class="str">"Alice"</span>,
    age = <span class="num">30</span>,
    email = <span class="str">"alice@example.com"</span>
});</code></pre></div>
    </div>

    <div class="doc-block" id="insert-many">
      <h3>Insert Many</h3>
      <p>Insert multiple documents in a single call.</p>
      <div class="lang-tabs" data-group="insert-many">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">let</span> docs = vec![
    json!({<span class="str">"name"</span>: <span class="str">"Bob"</span>, <span class="str">"age"</span>: <span class="num">25</span>}),
    json!({<span class="str">"name"</span>: <span class="str">"Charlie"</span>, <span class="str">"age"</span>: <span class="num">35</span>}),
];
db.insert_many(<span class="str">"users"</span>, docs).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.insert_many(<span class="str">"users"</span>, [
    {<span class="str">"name"</span>: <span class="str">"Bob"</span>, <span class="str">"age"</span>: <span class="num">25</span>},
    {<span class="str">"name"</span>: <span class="str">"Charlie"</span>, <span class="str">"age"</span>: <span class="num">35</span>},
])</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>err := client.InsertMany(<span class="str">"users"</span>, []<span class="kw">any</span>{
    <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"name"</span>: <span class="str">"Bob"</span>, <span class="str">"age"</span>: <span class="num">25</span>},
    <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"name"</span>: <span class="str">"Charlie"</span>, <span class="str">"age"</span>: <span class="num">35</span>},
})</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.InsertManyAsync(<span class="str">"users"</span>, <span class="kw">new</span>[] {
    <span class="kw">new</span> { name = <span class="str">"Bob"</span>, age = <span class="num">25</span> },
    <span class="kw">new</span> { name = <span class="str">"Charlie"</span>, age = <span class="num">35</span> },
});</code></pre></div>
    </div>

    <div class="doc-block" id="find">
      <h3>Find</h3>
      <p>Query documents with filters, sort, skip, and limit.</p>
      <div class="lang-tabs" data-group="find">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="co">// Simple query</span>
<span class="kw">let</span> users = db.find(<span class="str">"users"</span>, json!({<span class="str">"age"</span>: {<span class="str">"$gt"</span>: <span class="num">25</span>}}), <span class="kw">None</span>).unwrap();

<span class="co">// With sort, skip, limit</span>
<span class="kw">let</span> opts = json!({<span class="str">"sort"</span>: {<span class="str">"age"</span>: <span class="num">-1</span>}, <span class="str">"skip"</span>: <span class="num">0</span>, <span class="str">"limit"</span>: <span class="num">10</span>});
<span class="kw">let</span> users = db.find(<span class="str">"users"</span>, json!({}), Some(opts)).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code><span class="co"># Simple query</span>
users = client.find(<span class="str">"users"</span>, {<span class="str">"age"</span>: {<span class="str">"$gt"</span>: <span class="num">25</span>}})

<span class="co"># With sort, skip, limit</span>
users = client.find(<span class="str">"users"</span>, {},
    sort={<span class="str">"age"</span>: <span class="num">-1</span>},
    skip=<span class="num">0</span>,
    limit=<span class="num">10</span>,
)</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code><span class="co">// Simple query</span>
users, err := client.Find(<span class="str">"users"</span>, <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{
    <span class="str">"age"</span>: <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"$gt"</span>: <span class="num">25</span>},
}, <span class="kw">nil</span>)

<span class="co">// With options</span>
users, err := client.Find(<span class="str">"users"</span>, <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{}, &amp;oxidb.FindOptions{
    Sort:  <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"age"</span>: <span class="num">-1</span>},
    Skip:  <span class="num">0</span>,
    Limit: <span class="num">10</span>,
})</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="co">// Simple query</span>
<span class="kw">var</span> users = <span class="kw">await</span> client.FindAsync(<span class="str">"users"</span>, <span class="kw">new</span> { age = <span class="kw">new</span> { _gt = <span class="num">25</span> } });

<span class="co">// OxiDb.Linq</span>
<span class="kw">var</span> users = <span class="kw">await</span> db.GetCollection&lt;User&gt;(<span class="str">"users"</span>)
    .Where(u => u.Age > <span class="num">25</span>)
    .OrderByDescending(u => u.Age)
    .Skip(<span class="num">0</span>).Take(<span class="num">10</span>)
    .ToListAsync();</code></pre></div>
    </div>

    <div class="doc-block" id="find-one">
      <h3>Find One</h3>
      <p>Return the first matching document, or null/None if none match.</p>
      <div class="lang-tabs" data-group="find-one">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">let</span> user = db.find_one(<span class="str">"users"</span>, json!({<span class="str">"name"</span>: <span class="str">"Alice"</span>})).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>user = client.find_one(<span class="str">"users"</span>, {<span class="str">"name"</span>: <span class="str">"Alice"</span>})</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>user, err := client.FindOne(<span class="str">"users"</span>, <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"name"</span>: <span class="str">"Alice"</span>})</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">var</span> user = <span class="kw">await</span> client.FindOneAsync(<span class="str">"users"</span>, <span class="kw">new</span> { name = <span class="str">"Alice"</span> });</code></pre></div>
    </div>

    <div class="doc-block" id="count">
      <h3>Count</h3>
      <p>Count documents matching a query. Uses index-only path when possible.</p>
      <div class="lang-tabs" data-group="count">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">let</span> total = db.count(<span class="str">"users"</span>, json!({})).unwrap();
<span class="kw">let</span> adults = db.count(<span class="str">"users"</span>, json!({<span class="str">"age"</span>: {<span class="str">"$gte"</span>: <span class="num">18</span>}})).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>total = client.count(<span class="str">"users"</span>)
adults = client.count(<span class="str">"users"</span>, {<span class="str">"age"</span>: {<span class="str">"$gte"</span>: <span class="num">18</span>}})</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>total, err := client.Count(<span class="str">"users"</span>, <span class="kw">nil</span>)
adults, err := client.Count(<span class="str">"users"</span>, <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"age"</span>: <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"$gte"</span>: <span class="num">18</span>}})</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">var</span> total = <span class="kw">await</span> client.CountAsync(<span class="str">"users"</span>);
<span class="kw">var</span> adults = <span class="kw">await</span> client.CountAsync(<span class="str">"users"</span>, <span class="kw">new</span> { age = <span class="kw">new</span> { _gte = <span class="num">18</span> } });</code></pre></div>
    </div>

    <div class="doc-block" id="update">
      <h3>Update</h3>
      <p>Update all documents matching a query. Supports <code>$set</code>, <code>$unset</code>, <code>$inc</code>, <code>$mul</code>, <code>$min</code>, <code>$max</code>, <code>$rename</code>, <code>$currentDate</code>, <code>$push</code>, <code>$pull</code>, <code>$addToSet</code>, <code>$pop</code>.</p>
      <div class="lang-tabs" data-group="update">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.update(<span class="str">"users"</span>,
    json!({<span class="str">"status"</span>: <span class="str">"pending"</span>}),
    json!({<span class="str">"$set"</span>: {<span class="str">"status"</span>: <span class="str">"active"</span>}, <span class="str">"$inc"</span>: {<span class="str">"login_count"</span>: <span class="num">1</span>}}),
).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.update(<span class="str">"users"</span>,
    {<span class="str">"status"</span>: <span class="str">"pending"</span>},
    {<span class="str">"$set"</span>: {<span class="str">"status"</span>: <span class="str">"active"</span>}, <span class="str">"$inc"</span>: {<span class="str">"login_count"</span>: <span class="num">1</span>}},
)</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>result, err := client.Update(<span class="str">"users"</span>,
    <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"status"</span>: <span class="str">"pending"</span>},
    <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"$set"</span>: <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"status"</span>: <span class="str">"active"</span>}, <span class="str">"$inc"</span>: <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"login_count"</span>: <span class="num">1</span>}},
)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.UpdateAsync(<span class="str">"users"</span>,
    <span class="kw">new</span> { status = <span class="str">"pending"</span> },
    <span class="kw">new</span> { _set = <span class="kw">new</span> { status = <span class="str">"active"</span> }, _inc = <span class="kw">new</span> { login_count = <span class="num">1</span> } });</code></pre></div>
    </div>

    <div class="doc-block" id="update-one">
      <h3>Update One</h3>
      <p>Update the first matching document. Stops after first match (early termination).</p>
      <div class="lang-tabs" data-group="update-one">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.update_one(<span class="str">"users"</span>,
    json!({<span class="str">"name"</span>: <span class="str">"Alice"</span>}),
    json!({<span class="str">"$set"</span>: {<span class="str">"age"</span>: <span class="num">31</span>}}),
).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.update_one(<span class="str">"users"</span>, {<span class="str">"name"</span>: <span class="str">"Alice"</span>}, {<span class="str">"$set"</span>: {<span class="str">"age"</span>: <span class="num">31</span>}})</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>result, err := client.UpdateOne(<span class="str">"users"</span>,
    <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"name"</span>: <span class="str">"Alice"</span>},
    <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"$set"</span>: <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"age"</span>: <span class="num">31</span>}},
)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.UpdateOneAsync(<span class="str">"users"</span>,
    <span class="kw">new</span> { name = <span class="str">"Alice"</span> },
    <span class="kw">new</span> { _set = <span class="kw">new</span> { age = <span class="num">31</span> } });</code></pre></div>
    </div>

    <div class="doc-block" id="delete">
      <h3>Delete</h3>
      <p>Delete all documents matching a query.</p>
      <div class="lang-tabs" data-group="delete">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.delete(<span class="str">"users"</span>, json!({<span class="str">"age"</span>: {<span class="str">"$lt"</span>: <span class="num">18</span>}})).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.delete(<span class="str">"users"</span>, {<span class="str">"age"</span>: {<span class="str">"$lt"</span>: <span class="num">18</span>}})</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>result, err := client.Delete(<span class="str">"users"</span>, <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"age"</span>: <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"$lt"</span>: <span class="num">18</span>}})</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.DeleteAsync(<span class="str">"users"</span>, <span class="kw">new</span> { age = <span class="kw">new</span> { _lt = <span class="num">18</span> } });</code></pre></div>
    </div>

    <div class="doc-block" id="delete-one">
      <h3>Delete One</h3>
      <p>Delete the first matching document.</p>
      <div class="lang-tabs" data-group="delete-one">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.delete_one(<span class="str">"users"</span>, json!({<span class="str">"name"</span>: <span class="str">"Charlie"</span>})).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.delete_one(<span class="str">"users"</span>, {<span class="str">"name"</span>: <span class="str">"Charlie"</span>})</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>result, err := client.DeleteOne(<span class="str">"users"</span>, <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"name"</span>: <span class="str">"Charlie"</span>})</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.DeleteOneAsync(<span class="str">"users"</span>, <span class="kw">new</span> { name = <span class="str">"Charlie"</span> });</code></pre></div>
    </div>

    <!-- ============================================================ -->
    <!-- INDEXES                                                       -->
    <!-- ============================================================ -->

    <h3 style="font-size:22px; margin-top:40px; padding-top:24px; border-top:2px solid var(--accent-light);">Indexes</h3>

    <div class="doc-block" id="create-index">
      <h3>Create Index</h3>
      <p>Create a B-tree index on a single field. Speeds up equality, range, and sort queries.</p>
      <div class="lang-tabs" data-group="create-index">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.create_index(<span class="str">"users"</span>, <span class="str">"age"</span>).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.create_index(<span class="str">"users"</span>, <span class="str">"age"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>err := client.CreateIndex(<span class="str">"users"</span>, <span class="str">"age"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.CreateIndexAsync(<span class="str">"users"</span>, <span class="str">"age"</span>);</code></pre></div>
    </div>

    <div class="doc-block" id="create-unique-index">
      <h3>Create Unique Index</h3>
      <p>Enforces uniqueness on the field. Inserts with duplicate values will fail.</p>
      <div class="lang-tabs" data-group="create-unique-index">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.create_unique_index(<span class="str">"users"</span>, <span class="str">"email"</span>).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.create_unique_index(<span class="str">"users"</span>, <span class="str">"email"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>err := client.CreateUniqueIndex(<span class="str">"users"</span>, <span class="str">"email"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.CreateUniqueIndexAsync(<span class="str">"users"</span>, <span class="str">"email"</span>);</code></pre></div>
    </div>

    <div class="doc-block" id="create-composite-index">
      <h3>Create Composite Index</h3>
      <p>Multi-field B-tree index for prefix scans.</p>
      <div class="lang-tabs" data-group="create-composite-index">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.create_composite_index(<span class="str">"orders"</span>, &amp;[<span class="str">"user_id"</span>, <span class="str">"status"</span>]).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.create_composite_index(<span class="str">"orders"</span>, [<span class="str">"user_id"</span>, <span class="str">"status"</span>])</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>err := client.CreateCompositeIndex(<span class="str">"orders"</span>, []<span class="ty">string</span>{<span class="str">"user_id"</span>, <span class="str">"status"</span>})</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.CreateCompositeIndexAsync(<span class="str">"orders"</span>, <span class="kw">new</span>[] { <span class="str">"user_id"</span>, <span class="str">"status"</span> });</code></pre></div>
    </div>

    <div class="doc-block" id="create-text-index">
      <h3>Create Text Index</h3>
      <p>Full-text search index on one or more fields. Supports TF-IDF ranking.</p>
      <div class="lang-tabs" data-group="create-text-index">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.create_text_index(<span class="str">"articles"</span>, &amp;[<span class="str">"title"</span>, <span class="str">"body"</span>]).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.create_text_index(<span class="str">"articles"</span>, [<span class="str">"title"</span>, <span class="str">"body"</span>])</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>err := client.CreateTextIndex(<span class="str">"articles"</span>, []<span class="ty">string</span>{<span class="str">"title"</span>, <span class="str">"body"</span>})</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.CreateTextIndexAsync(<span class="str">"articles"</span>, <span class="kw">new</span>[] { <span class="str">"title"</span>, <span class="str">"body"</span> });</code></pre></div>
    </div>

    <div class="doc-block" id="list-indexes">
      <h3>List Indexes</h3>
      <div class="lang-tabs" data-group="list-indexes">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">let</span> indexes = db.list_indexes(<span class="str">"users"</span>).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>indexes = client.list_indexes(<span class="str">"users"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>indexes, err := client.ListIndexes(<span class="str">"users"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">var</span> indexes = <span class="kw">await</span> client.ListIndexesAsync(<span class="str">"users"</span>);</code></pre></div>
    </div>

    <div class="doc-block" id="drop-index">
      <h3>Drop Index</h3>
      <div class="lang-tabs" data-group="drop-index">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.drop_index(<span class="str">"users"</span>, <span class="str">"idx_age"</span>).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.drop_index(<span class="str">"users"</span>, <span class="str">"idx_age"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>err := client.DropIndex(<span class="str">"users"</span>, <span class="str">"idx_age"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.DropIndexAsync(<span class="str">"users"</span>, <span class="str">"idx_age"</span>);</code></pre></div>
    </div>

    <!-- ============================================================ -->
    <!-- AGGREGATION                                                   -->
    <!-- ============================================================ -->

    <h3 style="font-size:22px; margin-top:40px; padding-top:24px; border-top:2px solid var(--accent-light);">Aggregation</h3>

    <div class="doc-block" id="aggregate">
      <h3>Aggregate</h3>
      <p>Run an aggregation pipeline. Stages: <code>$match</code>, <code>$group</code>, <code>$sort</code>, <code>$project</code>, <code>$limit</code>, <code>$skip</code>, <code>$unwind</code>, <code>$addFields</code>, <code>$lookup</code>, <code>$count</code>.</p>
      <p>Group accumulators: <code>$sum</code>, <code>$avg</code>, <code>$min</code>, <code>$max</code>, <code>$first</code>, <code>$last</code>.</p>
      <div class="lang-tabs" data-group="aggregate">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">let</span> results = db.aggregate(<span class="str">"orders"</span>, vec![
    json!({<span class="str">"$match"</span>: {<span class="str">"status"</span>: <span class="str">"delivered"</span>}}),
    json!({<span class="str">"$group"</span>: {
        <span class="str">"_id"</span>: <span class="str">"$customer"</span>,
        <span class="str">"total"</span>: {<span class="str">"$sum"</span>: <span class="str">"$amount"</span>},
        <span class="str">"count"</span>: {<span class="str">"$sum"</span>: <span class="num">1</span>},
    }}),
    json!({<span class="str">"$sort"</span>: {<span class="str">"total"</span>: <span class="num">-1</span>}}),
    json!({<span class="str">"$limit"</span>: <span class="num">10</span>}),
]).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>results = client.aggregate(<span class="str">"orders"</span>, [
    {<span class="str">"$match"</span>: {<span class="str">"status"</span>: <span class="str">"delivered"</span>}},
    {<span class="str">"$group"</span>: {
        <span class="str">"_id"</span>: <span class="str">"$customer"</span>,
        <span class="str">"total"</span>: {<span class="str">"$sum"</span>: <span class="str">"$amount"</span>},
        <span class="str">"count"</span>: {<span class="str">"$sum"</span>: <span class="num">1</span>},
    }},
    {<span class="str">"$sort"</span>: {<span class="str">"total"</span>: <span class="num">-1</span>}},
    {<span class="str">"$limit"</span>: <span class="num">10</span>},
])</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>results, err := client.Aggregate(<span class="str">"orders"</span>, []<span class="kw">any</span>{
    <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"$match"</span>: <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"status"</span>: <span class="str">"delivered"</span>}},
    <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"$group"</span>: <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{
        <span class="str">"_id"</span>: <span class="str">"$customer"</span>,
        <span class="str">"total"</span>: <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"$sum"</span>: <span class="str">"$amount"</span>},
        <span class="str">"count"</span>: <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"$sum"</span>: <span class="num">1</span>},
    }},
    <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"$sort"</span>: <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"total"</span>: <span class="num">-1</span>}},
    <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"$limit"</span>: <span class="num">10</span>},
})</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">var</span> results = <span class="kw">await</span> client.AggregateAsync(<span class="str">"orders"</span>, <span class="kw">new</span> object[] {
    <span class="kw">new</span> { _match = <span class="kw">new</span> { status = <span class="str">"delivered"</span> } },
    <span class="kw">new</span> { _group = <span class="kw">new</span> { _id = <span class="str">"$customer"</span>, total = <span class="kw">new</span> { _sum = <span class="str">"$amount"</span> } } },
    <span class="kw">new</span> { _sort = <span class="kw">new</span> { total = <span class="num">-1</span> } },
    <span class="kw">new</span> { _limit = <span class="num">10</span> },
});</code></pre></div>
    </div>

    <!-- ============================================================ -->
    <!-- TRANSACTIONS                                                  -->
    <!-- ============================================================ -->

    <h3 style="font-size:22px; margin-top:40px; padding-top:24px; border-top:2px solid var(--accent-light);">Transactions</h3>
    <p>OCC (Optimistic Concurrency Control) with 3-phase commit. Writes are buffered until commit. Deadlock-free via sorted collection locking.</p>

    <div class="doc-block" id="begin-tx">
      <h3>Begin Transaction</h3>
      <div class="lang-tabs" data-group="begin-tx">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.begin_transaction().unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.begin_tx()</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>err := client.BeginTx()</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.BeginTxAsync();</code></pre></div>
    </div>

    <div class="doc-block" id="commit-tx">
      <h3>Commit Transaction</h3>
      <div class="lang-tabs" data-group="commit-tx">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.commit_transaction().unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.commit_tx()</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>err := client.CommitTx()</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.CommitTxAsync();</code></pre></div>
    </div>

    <div class="doc-block" id="rollback-tx">
      <h3>Rollback Transaction</h3>
      <div class="lang-tabs" data-group="rollback-tx">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.rollback_transaction().unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.rollback_tx()</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>err := client.RollbackTx()</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.RollbackTxAsync();</code></pre></div>
    </div>

    <div class="doc-block" id="transaction-ctx">
      <h3>Transaction Context Manager</h3>
      <p>Auto-commits on success, auto-rolls back on exception.</p>
      <div class="lang-tabs" data-group="transaction-ctx">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.transaction(|tx| {
    tx.insert(<span class="str">"accounts"</span>, json!({<span class="str">"owner"</span>: <span class="str">"Alice"</span>, <span class="str">"balance"</span>: <span class="num">1000</span>}))?;
    tx.update_one(<span class="str">"accounts"</span>,
        json!({<span class="str">"owner"</span>: <span class="str">"Bob"</span>}),
        json!({<span class="str">"$inc"</span>: {<span class="str">"balance"</span>: <span class="num">-500</span>}}),
    )?;
    Ok(())
}).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code><span class="kw">with</span> client.transaction():
    client.insert(<span class="str">"accounts"</span>, {<span class="str">"owner"</span>: <span class="str">"Alice"</span>, <span class="str">"balance"</span>: <span class="num">1000</span>})
    client.update_one(<span class="str">"accounts"</span>,
        {<span class="str">"owner"</span>: <span class="str">"Bob"</span>},
        {<span class="str">"$inc"</span>: {<span class="str">"balance"</span>: <span class="num">-500</span>}},
    )
<span class="co"># auto-committed here; rolls back on exception</span></code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>err := client.WithTransaction(<span class="kw">func</span>() error {
    client.Insert(<span class="str">"accounts"</span>, <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"owner"</span>: <span class="str">"Alice"</span>, <span class="str">"balance"</span>: <span class="num">1000</span>})
    client.UpdateOne(<span class="str">"accounts"</span>,
        <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"owner"</span>: <span class="str">"Bob"</span>},
        <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"$inc"</span>: <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"balance"</span>: <span class="num">-500</span>}},
    )
    <span class="kw">return</span> <span class="kw">nil</span>
})</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.TransactionAsync(<span class="kw">async</span> () => {
    <span class="kw">await</span> client.InsertAsync(<span class="str">"accounts"</span>, <span class="kw">new</span> { owner = <span class="str">"Alice"</span>, balance = <span class="num">1000</span> });
    <span class="kw">await</span> client.UpdateOneAsync(<span class="str">"accounts"</span>,
        <span class="kw">new</span> { owner = <span class="str">"Bob"</span> },
        <span class="kw">new</span> { _inc = <span class="kw">new</span> { balance = <span class="num">-500</span> } });
});</code></pre></div>
    </div>

    <!-- SQL section removed — OxiDB is a document database. -->

    <!-- ============================================================ -->
    <!-- SEARCH                                                        -->
    <!-- ============================================================ -->

    <h3 style="font-size:22px; margin-top:40px; padding-top:24px; border-top:2px solid var(--accent-light);">Search</h3>

    <div class="doc-block" id="text-search">
      <h3>Text Search</h3>
      <p>Full-text search within a collection's text index. TF-IDF ranked results.</p>
      <div class="lang-tabs" data-group="text-search">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">let</span> results = db.text_search(<span class="str">"articles"</span>, <span class="str">"rust database"</span>, <span class="num">10</span>).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>results = client.text_search(<span class="str">"articles"</span>, <span class="str">"rust database"</span>, limit=<span class="num">10</span>)</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>results, err := client.TextSearch(<span class="str">"articles"</span>, <span class="str">"rust database"</span>, <span class="num">10</span>)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">var</span> results = <span class="kw">await</span> client.TextSearchAsync(<span class="str">"articles"</span>, <span class="str">"rust database"</span>, <span class="num">10</span>);</code></pre></div>
    </div>

    <div class="doc-block" id="search">
      <h3>Search (Blobs)</h3>
      <p>Search across blob content (PDF, DOCX, HTML, images with OCR).</p>
      <div class="lang-tabs" data-group="search">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">let</span> results = db.search(<span class="str">"quarterly report"</span>, <span class="kw">None</span>, <span class="num">10</span>).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>results = client.search(<span class="str">"quarterly report"</span>, limit=<span class="num">10</span>)</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>results, err := client.Search(<span class="str">"quarterly report"</span>, <span class="str">""</span>, <span class="num">10</span>)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">var</span> results = <span class="kw">await</span> client.SearchAsync(<span class="str">"quarterly report"</span>, limit: <span class="num">10</span>);</code></pre></div>
    </div>

    <!-- ============================================================ -->
    <!-- VECTORS                                                       -->
    <!-- ============================================================ -->

    <h3 style="font-size:22px; margin-top:40px; padding-top:24px; border-top:2px solid var(--accent-light);">Vector Search</h3>

    <div class="doc-block" id="create-vector-index">
      <h3>Create Vector Index</h3>
      <p>Create an HNSW vector index. Distance metrics: <code>cosine</code>, <code>euclidean</code>, <code>dot_product</code>.</p>
      <div class="lang-tabs" data-group="create-vector-index">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">use</span> oxidb::vector::DistanceMetric;
db.create_vector_index(<span class="str">"products"</span>, <span class="str">"embedding"</span>, <span class="num">384</span>, DistanceMetric::Cosine).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.create_vector_index(<span class="str">"products"</span>, <span class="str">"embedding"</span>, dimension=<span class="num">384</span>, metric=<span class="str">"cosine"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>err := client.CreateVectorIndex(<span class="str">"products"</span>, <span class="str">"embedding"</span>, <span class="num">384</span>, <span class="str">"cosine"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.CreateVectorIndexAsync(<span class="str">"products"</span>, <span class="str">"embedding"</span>, <span class="num">384</span>, <span class="str">"cosine"</span>);</code></pre></div>
    </div>

    <div class="doc-block" id="vector-search">
      <h3>Vector Search</h3>
      <p>Find nearest neighbors by vector similarity.</p>
      <div class="lang-tabs" data-group="vector-search">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">let</span> results = db.vector_search(<span class="str">"products"</span>, <span class="str">"embedding"</span>, &amp;query_vec, <span class="num">5</span>, <span class="kw">None</span>).unwrap();
<span class="co">// Each result has _similarity and _distance fields</span></code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>results = client.vector_search(<span class="str">"products"</span>, <span class="str">"embedding"</span>, query_vec, limit=<span class="num">5</span>)
<span class="co"># Each result has _similarity and _distance fields</span></code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>results, err := client.VectorSearch(<span class="str">"products"</span>, <span class="str">"embedding"</span>, queryVec, <span class="num">5</span>)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">var</span> results = <span class="kw">await</span> client.VectorSearchAsync(<span class="str">"products"</span>, <span class="str">"embedding"</span>, queryVec, <span class="num">5</span>);</code></pre></div>
    </div>

    <!-- ============================================================ -->
    <!-- BLOB STORAGE                                                  -->
    <!-- ============================================================ -->

    <h3 style="font-size:22px; margin-top:40px; padding-top:24px; border-top:2px solid var(--accent-light);">Blob Storage</h3>
    <p>S3-style object storage. Store files, PDFs, images with metadata. CRC32 etags.</p>

    <div class="doc-block" id="create-bucket">
      <h3>Create Bucket</h3>
      <div class="lang-tabs" data-group="create-bucket">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.create_bucket(<span class="str">"documents"</span>).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.create_bucket(<span class="str">"documents"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>err := client.CreateBucket(<span class="str">"documents"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.CreateBucketAsync(<span class="str">"documents"</span>);</code></pre></div>
    </div>

    <div class="doc-block" id="list-buckets">
      <h3>List Buckets</h3>
      <div class="lang-tabs" data-group="list-buckets">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">let</span> buckets = db.list_buckets().unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>buckets = client.list_buckets()</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>buckets, err := client.ListBuckets()</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">var</span> buckets = <span class="kw">await</span> client.ListBucketsAsync();</code></pre></div>
    </div>

    <div class="doc-block" id="delete-bucket">
      <h3>Delete Bucket</h3>
      <div class="lang-tabs" data-group="delete-bucket">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.delete_bucket(<span class="str">"documents"</span>).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.delete_bucket(<span class="str">"documents"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>err := client.DeleteBucket(<span class="str">"documents"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.DeleteBucketAsync(<span class="str">"documents"</span>);</code></pre></div>
    </div>

    <div class="doc-block" id="put-object">
      <h3>Put Object</h3>
      <p>Upload a file/blob with content type and optional metadata.</p>
      <div class="lang-tabs" data-group="put-object">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">let</span> data = std::fs::read(<span class="str">"report.pdf"</span>).unwrap();
db.put_object(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>, &amp;data,
    <span class="str">"application/pdf"</span>, Some(json!({<span class="str">"author"</span>: <span class="str">"Alice"</span>}))).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code><span class="kw">with</span> open(<span class="str">"report.pdf"</span>, <span class="str">"rb"</span>) <span class="kw">as</span> f:
    client.put_object(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>, f.read(),
        content_type=<span class="str">"application/pdf"</span>,
        metadata={<span class="str">"author"</span>: <span class="str">"Alice"</span>})</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>data, _ := os.ReadFile(<span class="str">"report.pdf"</span>)
err := client.PutObject(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>, data,
    <span class="str">"application/pdf"</span>, <span class="kw">map</span>[<span class="ty">string</span>]<span class="ty">string</span>{<span class="str">"author"</span>: <span class="str">"Alice"</span>})</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">var</span> data = <span class="kw">await</span> File.ReadAllBytesAsync(<span class="str">"report.pdf"</span>);
<span class="kw">await</span> client.PutObjectAsync(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>, data,
    <span class="str">"application/pdf"</span>, <span class="kw">new</span> { author = <span class="str">"Alice"</span> });</code></pre></div>
    </div>

    <div class="doc-block" id="get-object">
      <h3>Get Object</h3>
      <p>Download a blob. Returns the data and metadata.</p>
      <div class="lang-tabs" data-group="get-object">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">let</span> (data, meta) = db.get_object(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>data, metadata = client.get_object(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>data, meta, err := client.GetObject(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">var</span> (data, meta) = <span class="kw">await</span> client.GetObjectAsync(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>);</code></pre></div>
    </div>

    <div class="doc-block" id="head-object">
      <h3>Head Object</h3>
      <p>Get metadata without downloading the data.</p>
      <div class="lang-tabs" data-group="head-object">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">let</span> meta = db.head_object(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>meta = client.head_object(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>meta, err := client.HeadObject(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">var</span> meta = <span class="kw">await</span> client.HeadObjectAsync(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>);</code></pre></div>
    </div>

    <div class="doc-block" id="delete-object">
      <h3>Delete Object</h3>
      <div class="lang-tabs" data-group="delete-object">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code>db.delete_object(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>client.delete_object(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>err := client.DeleteObject(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">await</span> client.DeleteObjectAsync(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>);</code></pre></div>
    </div>

    <div class="doc-block" id="list-objects">
      <h3>List Objects</h3>
      <p>List objects in a bucket with optional prefix filter.</p>
      <div class="lang-tabs" data-group="list-objects">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">let</span> objects = db.list_objects(<span class="str">"documents"</span>, Some(<span class="str">"reports/"</span>), <span class="kw">None</span>).unwrap();</code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>objects = client.list_objects(<span class="str">"documents"</span>, prefix=<span class="str">"reports/"</span>)</code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>objects, err := client.ListObjects(<span class="str">"documents"</span>, <span class="str">"reports/"</span>, <span class="num">0</span>)</code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">var</span> objects = <span class="kw">await</span> client.ListObjectsAsync(<span class="str">"documents"</span>, prefix: <span class="str">"reports/"</span>);</code></pre></div>
    </div>

    <!-- ============================================================ -->
    <!-- MAINTENANCE                                                   -->
    <!-- ============================================================ -->

    <h3 style="font-size:22px; margin-top:40px; padding-top:24px; border-top:2px solid var(--accent-light);">Maintenance</h3>

    <div class="doc-block" id="compact">
      <h3>Compact</h3>
      <p>Reclaim disk space by removing soft-deleted records. Returns old/new size and docs kept.</p>
      <div class="lang-tabs" data-group="compact">
        <button class="lang-tab active" data-lang="rust">Rust</button>
        <button class="lang-tab" data-lang="python">Python</button>
        <button class="lang-tab" data-lang="go">Go</button>
        <button class="lang-tab" data-lang="dotnet">C# / .NET</button>
      </div>
      <div class="lang-panel active" data-lang="rust"><pre><code><span class="kw">let</span> stats = db.compact(<span class="str">"users"</span>).unwrap();
<span class="co">// stats.old_size, stats.new_size, stats.docs_kept</span></code></pre></div>
      <div class="lang-panel" data-lang="python"><pre><code>stats = client.compact(<span class="str">"users"</span>)
<span class="co"># {"old_size": 102400, "new_size": 81920, "docs_kept": 500}</span></code></pre></div>
      <div class="lang-panel" data-lang="go"><pre><code>stats, err := client.Compact(<span class="str">"users"</span>)
<span class="co">// stats["old_size"], stats["new_size"], stats["docs_kept"]</span></code></pre></div>
      <div class="lang-panel" data-lang="dotnet"><pre><code><span class="kw">var</span> stats = <span class="kw">await</span> client.CompactAsync(<span class="str">"users"</span>);</code></pre></div>
    </div>

  </div>
</section>` }} />
}