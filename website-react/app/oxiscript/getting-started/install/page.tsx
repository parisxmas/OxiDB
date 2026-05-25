import type { Metadata } from "next"
export const metadata: Metadata = { title: "Install & Enable OxiScript" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">Getting Started · 1 of 3</p>
<h2>Install &amp; enable OxiScript</h2>
<p>OxiScript ships built into <code>oxidb-server</code>. There is no separate install — if your server is v0.20 or newer, you already have it. This page just makes sure your environment is ready to run procedures.</p>

<h3>1. Get a server running</h3>
<pre><code class="lang-bash"><span class="co"># macOS (Apple Silicon)</span>
curl -LO https://github.com/parisxmas/OxiDB/releases/download/v0.28.18/oxidb-server-v0.28.18-darwin-arm64.tar.gz
tar xzf oxidb-server-v0.28.18-darwin-arm64.tar.gz
./oxidb-server &amp;</code></pre>
<p>For Linux, Windows, Docker, or building from source, see the <a href="/quickstart/">Quick Start</a>.</p>

<h3>2. Get the CLI</h3>
<pre><code class="lang-bash">curl -LO https://github.com/parisxmas/OxiDB/releases/download/v0.28.18/oxidb-cli-v0.28.18-darwin-arm64.tar.gz
tar xzf oxidb-cli-v0.28.18-darwin-arm64.tar.gz
./oxidb --host 127.0.0.1 --port 4444</code></pre>

<h3>3. Confirm OxiScript is on</h3>
<p>From a JSON client (Python, Go, raw netcat) send a <code>compile_oxiscript</code> command. If the server returns a step list, you're good.</p>
<pre><code class="lang-bash">{<span class="str">"cmd"</span>: <span class="str">"compile_oxiscript"</span>, <span class="str">"script"</span>: <span class="str">"proc ping() { return {ok: true} }"</span>}</code></pre>
<p>Response:</p>
<pre><code class="lang-json">{<span class="str">"ok"</span>: <span class="kw">true</span>, <span class="str">"data"</span>: {
  <span class="str">"name"</span>: <span class="str">"ping"</span>,
  <span class="str">"params"</span>: [],
  <span class="str">"steps"</span>: [{<span class="str">"step"</span>: <span class="str">"return"</span>, <span class="str">"value"</span>: {<span class="str">"ok"</span>: <span class="kw">true</span>}}]
}}</code></pre>

<h3>4. Enable the REST API (optional)</h3>
<p>If you want to call procedures over plain HTTP, set <code>OXIDB_HTTP_PORT</code> when starting the server:</p>
<pre><code class="lang-bash">OXIDB_HTTP_PORT=8080 OXIDB_DATA=./data ./oxidb-server</code></pre>
<p>Now <code>POST /api/procedures</code> and <code>POST /api/procedures/&lt;name&gt;/call</code> are live.</p>

<h3>5. Quick smoke test (Python)</h3>
<pre><code class="lang-python"><span class="kw">import</span> socket, struct, json

s = socket.create_connection((<span class="str">"127.0.0.1"</span>, <span class="num">4444</span>))
<span class="kw">def</span> <span class="fn">send</span>(payload):
    data = json.dumps(payload).encode()
    s.sendall(struct.pack(<span class="str">"&lt;I"</span>, len(data)) + data)
    n = struct.unpack(<span class="str">"&lt;I"</span>, s.recv(<span class="num">4</span>))[<span class="num">0</span>]
    <span class="kw">return</span> json.loads(s.recv(n))

<span class="kw">print</span>(send({<span class="str">"cmd"</span>: <span class="str">"create_procedure"</span>, <span class="str">"script"</span>: <span class="str">"proc hi(name) { return {hi: name} }"</span>}))
<span class="kw">print</span>(send({<span class="str">"cmd"</span>: <span class="str">"call_procedure"</span>, <span class="str">"name"</span>: <span class="str">"hi"</span>, <span class="str">"params"</span>: {<span class="str">"name"</span>: <span class="str">"world"</span>}}))</code></pre>

<div class="docs-callout"><strong>Already familiar with OxiDB?</strong> Procedures are stored in the special <code>_procedures</code> collection and survive restarts. The OxiScript compiler is exposed via the same TCP port as everything else.</div>

<div class="docs-prevnext">
  <a href="/oxiscript/tutorial/" class="prev"><div class="label">Back to</div><div class="title">← Tutorial overview</div></a>
  <a href="/oxiscript/getting-started/hello-world/" class="next"><div class="label">Next</div><div class="title">2. Hello, OxiScript →</div></a>
</div>` }} />
}
