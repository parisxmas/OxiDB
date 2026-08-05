import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "MCP Server",
  description:
    "oxidb-mcp — a Model Context Protocol server for OxiDB. Let Claude Code, Claude Desktop, Cursor and other AI hosts query documents, SQL, time-series and full-text search directly, read-only by default.",
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M9 9h6v6H9z"/><path d="M9 1v4M15 1v4M9 19v4M15 19v4M1 9h4M1 15h4M19 9h4M19 15h4"/></svg> MCP Server</h2>
    <p class="section-desc"><code>oxidb-mcp</code> speaks the <strong>Model Context Protocol</strong> &mdash; the standard Claude Code, Claude Desktop, Cursor and other agentic editors use to reach external tools. Point a host at it and an AI assistant can orient itself in an OxiDB instance and query it directly: documents, SQL, time-series and full-text search, with nothing copy-pasted into a prompt.</p>

    <h3>Set it up</h3>
    <p>The server is a standalone binary that the AI host launches for you. With Claude Code:</p>
    <pre><code class="lang-bash">claude mcp add oxidb \\
  -e OXIDB_ADDR=127.0.0.1:4444 \\
  -e OXIDB_USER=assistant -e OXIDB_PASSWORD=… \\
  -- oxidb-mcp</code></pre>

    <p>Any other MCP host takes the same thing as JSON &mdash; Claude Desktop, Cursor, Windsurf, Zed:</p>
    <pre><code class="lang-json">{
  "mcpServers": {
    "oxidb": {
      "command": "oxidb-mcp",
      "env": {
        "OXIDB_ADDR": "127.0.0.1:4444",
        "OXIDB_USER": "assistant",
        "OXIDB_PASSWORD": "…"
      }
    }
  }
}</code></pre>

    <h3>Configuration</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Variable</th><th>Default</th><th>What it does</th></tr></thead>
        <tbody>
          <tr><td><code>OXIDB_ADDR</code></td><td><code>127.0.0.1:4444</code></td><td>The OxiDB server to connect to</td></tr>
          <tr><td><code>OXIDB_USER</code> / <code>OXIDB_PASSWORD</code></td><td>&mdash;</td><td>SCRAM credentials. Omit both only against a server with auth disabled</td></tr>
          <tr><td><code>OXIDB_MCP_DB</code></td><td>&mdash;</td><td>Pin every call to one database. A request naming a different one is refused</td></tr>
          <tr><td><code>OXIDB_MCP_WRITES</code></td><td><code>0</code></td><td>Register the write tools. Off means they are not offered at all</td></tr>
        </tbody>
      </table>
    </div>

    <h3>What the assistant can do</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Area</th><th>Tools</th></tr></thead>
        <tbody>
          <tr><td>Orientation</td><td><code>list_databases</code>, <code>list_collections</code>, <code>list_tables</code>, <code>describe_table</code>, <code>list_indexes</code></td></tr>
          <tr><td>Documents</td><td><code>find</code>, <code>count</code>, <code>aggregate</code></td></tr>
          <tr><td>SQL</td><td><code>sql_query</code> &mdash; parameterized, single statement, read-only</td></tr>
          <tr><td>Time-series</td><td><code>tsdb_query</code> &mdash; tag filters, time ranges, downsampling, group-by</td></tr>
          <tr><td>Search</td><td><code>text_search</code> &mdash; BM25, with optional highlights</td></tr>
          <tr><td>Diagnostics</td><td><code>explain</code></td></tr>
          <tr><td>Writes <span class="version-badge">opt-in</span></td><td><code>insert</code>, <code>update</code>, <code>delete</code>, <code>sql_execute</code></td></tr>
        </tbody>
      </table>
    </div>

    <p><code>explain</code> is a tool of its own on purpose. It returns the plan &mdash; strategy, index used, documents examined versus returned &mdash; plus real timing, so an assistant can work out <em>why</em> a query is slow and propose the index that fixes it, instead of guessing.</p>

    <h3>Security: read-only by default</h3>
    <p>There are two gates, and the one that matters is the server's.</p>
    <ul>
      <li><strong>The write tools are not registered</strong> unless <code>OXIDB_MCP_WRITES=1</code>. A model cannot call a tool it was never offered &mdash; asking for one is a protocol error, and nothing reaches the database.</li>
      <li><strong>Give the assistant a Read-role account.</strong> Then writes are refused by OxiDB's own RBAC, whatever the tool layer asks for. This is the gate to rely on: it holds even if the MCP process is misconfigured or compromised.</li>
    </ul>
    <pre><code class="lang-json"><span class="co">// Create a read-only account for the assistant (admin connection)</span>
{ "cmd": "create_user", "username": "assistant",
  "password": "…", "role": "read" }</code></pre>
    <p>This matters more than it first looks. Everything a model reads out of a database enters its context, and a hostile document that talks a model into writing is a known attack shape. Read-only by default doesn't make that impossible &mdash; it means there is nothing to write with until an operator widens it, deliberately, twice.</p>

    <h3>Results are budgeted for a context window</h3>
    <p>A read returns 50 rows by default and 500 at most. When a result is trimmed it <strong>says so</strong> and reports the true total from an index-only count, so an assistant knows to narrow the query rather than believe it has seen everything. A silent cap would read as &ldquo;that was all of it&rdquo; &mdash; a different, and wrong, answer.</p>

    <h3>Hosted: one endpoint per project</h3>
    <p>The setup above spawns <code>oxidb-mcp</code> on the machine running the host. For a hosted project there is a second mode: run it once as a server, and an assistant reaches a project over plain HTTP with nothing installed.</p>
    <pre><code class="lang-bash">OXIDB_MCP_HTTP_PORT=8090 \\
OXIDB_MCP_UPSTREAM=http://127.0.0.1:8080 \\
  oxidb-mcp

<span class="co"># the endpoint, carrying the project's own key</span>
curl -X POST https://your-host/mcp/&lt;project-ref&gt; \\
  -H "Authorization: Bearer $ANON_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'</code></pre>
    <p><strong>The key is forwarded, not interpreted.</strong> The request is served by passing your project key to the REST surface, which verifies it against that project&rsquo;s own secret and applies the project&rsquo;s security rules, role and rate limit &mdash; the same gate every other request to the project passes. The MCP layer decides nothing about access; if a rule refuses a collection to your anon key, the tool call is refused with it.</p>
    <p>Each request is independent: the project comes from the path and the key from the header, and nothing is cached between requests. One process can serve many projects with no shared state. Set <code>OXIDB_MCP_DB</code> to fix it to a single project, and a ref in the path is then ignored rather than honoured. <code>GET /mcp/health</code> answers without a key.</p>
    <p>Two differences from the local mode: <code>explain</code> is not available (it is a wire-protocol diagnostic with no HTTP equivalent), and the time-series tool goes through the PostgREST time-series route, so its rows come back flattened.</p>

    <h3>How it fits</h3>
    <p><code>oxidb-mcp</code> is a <strong>client</strong>, not a listener: it talks to any OxiDB server over the native protocol, changes nothing on the server side, and works against deployments you already have running. It is a separate process the AI host starts and stops, so an instance with no assistant attached pays nothing for it.</p>
    <p>The design decisions &mdash; and what was deliberately left out &mdash; are recorded in ADR-0024 in the repository. Not in this version: MCP resources and prompts (tools only), realtime subscriptions, and the remote HTTP transport; each is refused by name rather than silently ignored.</p>

    <div class="docs-callout"><strong>Download:</strong> <code>oxidb-mcp</code> ships for all five platforms on the <a href="/downloads/">downloads page</a>, or build it with <code>cargo build --release -p oxidb-mcp</code>.</div>
  </div>
</section>` }} />
}
