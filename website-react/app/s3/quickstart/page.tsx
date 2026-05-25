import type { Metadata } from "next"
export const metadata: Metadata = { title: "S3 Quick Start" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">S3 · Get Going</p>
<h2>S3 Quick Start</h2>
<p>From zero to a working S3 endpoint in three steps.</p>

<h3>1. Build / get a server with S3 support</h3>
<p>The S3 module is feature-gated. Pre-built v0.28.18 binaries already include it. If you build from source:</p>
<pre><code class="lang-bash">cargo build --release -p oxidb-server --features s3</code></pre>

<h3>2. Start the server</h3>
<pre><code class="lang-bash">OXIDB_S3_PORT=9000 \\
OXIDB_S3_ACCESS_KEY=minioadmin \\
OXIDB_S3_SECRET_KEY=minioadmin \\
OXIDB_DATA=./data \\
oxidb-server</code></pre>
<p>You should see:</p>
<pre><code class="lang-bash">[oxidb-server] listening on 127.0.0.1:4444
[s3] S3-compatible API listening on 0.0.0.0:9000</code></pre>

<h3>3. Configure aws-cli</h3>
<pre><code class="lang-bash">aws configure set aws_access_key_id minioadmin
aws configure set aws_secret_access_key minioadmin
aws configure set default.region us-east-1</code></pre>
<p>Then either pass <code>--endpoint-url</code> on every call, or alias the command:</p>
<pre><code class="lang-bash">alias os3=<span class="str">"aws --endpoint-url http://localhost:9000"</span></code></pre>

<h3>4. Hello, S3</h3>
<pre><code class="lang-bash">os3 s3 mb s3://test-bucket
os3 s3 cp /etc/hosts s3://test-bucket/hosts.txt
os3 s3 ls s3://test-bucket/
os3 s3 cp s3://test-bucket/hosts.txt /tmp/recovered
os3 s3 rm s3://test-bucket/hosts.txt
os3 s3 rb s3://test-bucket</code></pre>

<h3>5. Or with curl + a presigned URL</h3>
<p>Generate a presigned URL on the server, then use plain curl:</p>
<pre><code class="lang-bash">URL=$(os3 s3 presign s3://test-bucket/hosts.txt --expires-in 600)
curl -O <span class="str">"$URL"</span></code></pre>

<h3>Common env vars</h3>
<div class="table-wrap"><table>
<thead><tr><th>Variable</th><th>Default</th><th>Purpose</th></tr></thead>
<tbody>
<tr><td><code>OXIDB_S3_PORT</code></td><td><em>(off)</em></td><td>Port to bind. Setting this enables the S3 server.</td></tr>
<tr><td><code>OXIDB_S3_ACCESS_KEY</code></td><td><em>(off)</em></td><td>Single-tenant access key.</td></tr>
<tr><td><code>OXIDB_S3_SECRET_KEY</code></td><td><em>(off)</em></td><td>Single-tenant secret key.</td></tr>
<tr><td><code>OXIDB_S3_CREDENTIALS</code></td><td><em>(off)</em></td><td>Multi-tenant: <code>"ak1:sk1,ak2:sk2,..."</code></td></tr>
<tr><td><code>OXIDB_S3_ENCRYPTION_KEY</code></td><td><em>(off)</em></td><td>32-byte hex key for SSE-S3.</td></tr>
<tr><td><code>OXIDB_S3_DEFAULT_ENCRYPTION</code></td><td><code>false</code></td><td>If <code>true</code>, every PUT is auto-encrypted with SSE-S3.</td></tr>
<tr><td><code>OXIDB_S3_CORS_ORIGIN</code></td><td><code>*</code></td><td><code>Access-Control-Allow-Origin</code> header value.</td></tr>
<tr><td><code>OXIDB_DATA</code></td><td><code>./oxidb_data</code></td><td>Where bucket/object files live (shared with native API).</td></tr>
</tbody></table></div>

<h3>Where data lives on disk</h3>
<pre><code class="lang-bash">$OXIDB_DATA/
├── _blobs/                  # S3 buckets &amp; objects (same store as embedded blob API)
│   └── photos/
│       ├── &lt;object-id&gt;.data
│       └── &lt;object-id&gt;.meta
└── ... (the rest of the database)</code></pre>

<div class="docs-callout"><strong>Heads-up:</strong> the S3 API and OxiDB's <a href="/blobs/">embedded blob API</a> read/write the same store. You can <code>put_object</code> from a Rust embedded program and download it via <code>aws s3 cp</code>.</div>

<div class="docs-prevnext">
  <a href="/s3/" class="prev"><div class="label">Back to</div><div class="title">← S3 overview</div></a>
  <a href="/s3/auth/" class="next"><div class="label">Next</div><div class="title">Authentication →</div></a>
</div>` }} />
}
