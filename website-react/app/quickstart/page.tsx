import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Quick Start",
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section id="quickstart" class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg> Quick Start</h2>

    <h3>Embedded (Rust)</h3>
    <pre><code class="lang-rust"><span class="kw">use</span> oxidb::OxiDb;
<span class="kw">use</span> serde_json::json;

<span class="kw">fn</span> <span class="fn">main</span>() {
    <span class="kw">let</span> db = OxiDb::open(<span class="str">"./my_data"</span>).unwrap();

    <span class="co">// Insert a document</span>
    db.insert(<span class="str">"users"</span>, json!({
        <span class="str">"name"</span>: <span class="str">"Alice"</span>,
        <span class="str">"age"</span>: <span class="num">30</span>,
        <span class="str">"department"</span>: <span class="str">"Engineering"</span>
    })).unwrap();

    <span class="co">// Query</span>
    <span class="kw">let</span> results = db.find(<span class="str">"users"</span>, json!({<span class="str">"age"</span>: {<span class="str">"$gte"</span>: <span class="num">25</span>}}), <span class="kw">None</span>).unwrap();

    <span class="co">// Update</span>
    db.update(<span class="str">"users"</span>, json!({<span class="str">"name"</span>: <span class="str">"Alice"</span>}), json!({
        <span class="str">"$set"</span>: {<span class="str">"age"</span>: <span class="num">31</span>}
    })).unwrap();
}</code></pre>

    <h3>Server Mode</h3>
    <pre><code class="lang-bash"><span class="co"># Start the server</span>
OXIDB_ADDR=127.0.0.1:4444 OXIDB_DATA=./data oxidb-server

<span class="co"># Environment variables:</span>
<span class="co">#   OXIDB_ADDR       - Bind address (default: 127.0.0.1:4444)</span>
<span class="co">#   OXIDB_DATA       - Data directory (default: ./oxidb_data)</span>
<span class="co">#   OXIDB_POOL_SIZE  - Worker threads (default: 4)</span>
<span class="co">#   OXIDB_IDLE_TIMEOUT - Connection timeout in seconds (default: 30)</span></code></pre>

    <h3>Python</h3>
    <pre><code class="lang-python"><span class="kw">from</span> oxidb <span class="kw">import</span> OxiDbClient

<span class="kw">with</span> OxiDbClient(<span class="str">"127.0.0.1"</span>, <span class="num">4444</span>) <span class="kw">as</span> db:
    db.insert(<span class="str">"users"</span>, {<span class="str">"name"</span>: <span class="str">"Alice"</span>, <span class="str">"age"</span>: <span class="num">30</span>})
    docs = db.find(<span class="str">"users"</span>, {<span class="str">"age"</span>: {<span class="str">"$gte"</span>: <span class="num">25</span>}})
    db.update(<span class="str">"users"</span>, {<span class="str">"name"</span>: <span class="str">"Alice"</span>}, {<span class="str">"$inc"</span>: {<span class="str">"age"</span>: <span class="num">1</span>}})</code></pre>

    <h3>Go</h3>
    <pre><code class="lang-go">client, _ := oxidb.ConnectDefault()
<span class="kw">defer</span> client.Close()

client.UseOxiWire() <span class="co">// enable binary protocol (fastest)</span>

client.Insert(<span class="str">"users"</span>, <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{
    <span class="str">"name"</span>: <span class="str">"Alice"</span>, <span class="str">"age"</span>: <span class="num">30</span>,
})

docs, _ := client.Find(<span class="str">"users"</span>, <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{
    <span class="str">"age"</span>: <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{<span class="str">"$gte"</span>: <span class="num">25</span>},
}, <span class="kw">nil</span>)</code></pre>

    <h3>.NET (EF Core)</h3>
    <pre><code class="lang-csharp"><span class="co">// Configure in Program.cs</span>
builder.Services.AddOxiDb(options => {
    options.UseTcp(<span class="str">"127.0.0.1"</span>, <span class="num">4444</span>);
    <span class="co">// or: options.UseEmbedded("./data");</span>
});

<span class="co">// Use with EF Core</span>
<span class="kw">var</span> users = <span class="kw">await</span> db.Users
    .Where(u => u.Age >= <span class="num">25</span>)
    .OrderBy(u => u.Name)
    .ToListAsync();</code></pre>
  </div>
</section>` }} />
}