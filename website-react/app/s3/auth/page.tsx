import type { Metadata } from "next"
export const metadata: Metadata = { title: "S3 Authentication" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">S3 · Get Going</p>
<h2>Authentication</h2>
<p>OxiDB implements <strong>AWS Signature V4</strong> — the same scheme S3 itself uses. Every authenticated request carries an <code>Authorization</code> header (or signed query string for presigned URLs) that the server verifies against the configured access keys.</p>

<h3>Single-tenant credentials</h3>
<pre><code class="lang-bash">OXIDB_S3_ACCESS_KEY=minioadmin
OXIDB_S3_SECRET_KEY=minioadmin</code></pre>
<p>One pair, one identity. Good for dev or single-app deployments.</p>

<h3>Multi-tenant credentials</h3>
<pre><code class="lang-bash">OXIDB_S3_CREDENTIALS=<span class="str">"alice-key:alice-secret,bob-key:bob-secret,svc-backups:abc...xyz"</span></code></pre>
<p>Comma-separated <code>access:secret</code> pairs. The server tries each entry on every request. Good for sharing one bucket store across multiple apps with different keys.</p>

<h3>Anonymous mode (auth disabled)</h3>
<p>If neither <code>OXIDB_S3_ACCESS_KEY</code>/<code>OXIDB_S3_SECRET_KEY</code> nor <code>OXIDB_S3_CREDENTIALS</code> is set, the server runs <strong>open</strong> — every request is accepted. The startup banner warns about this:</p>
<pre><code class="lang-bash">[s3] Set OXIDB_S3_ACCESS_KEY/OXIDB_S3_SECRET_KEY or OXIDB_S3_CREDENTIALS to enable auth.</code></pre>
<p>Only use anonymous mode behind a trusted reverse proxy or in test environments.</p>

<h3>Presigned URLs</h3>
<p>Sign a short-lived URL on the server side, hand it to a browser or a curl client. No SDK needed for the consumer.</p>

<h4>aws-cli</h4>
<pre><code class="lang-bash"><span class="co"># 10-minute upload URL</span>
URL=$(aws --endpoint-url http://localhost:9000 \\
  s3 presign s3://photos/upload.png --expires-in 600)
echo <span class="str">"$URL"</span></code></pre>

<h4>boto3</h4>
<pre><code class="lang-python"><span class="kw">import</span> boto3

s3 = boto3.client(<span class="str">"s3"</span>,
    endpoint_url=<span class="str">"http://localhost:9000"</span>,
    aws_access_key_id=<span class="str">"minioadmin"</span>,
    aws_secret_access_key=<span class="str">"minioadmin"</span>
)

<span class="co"># 1-hour download URL</span>
url = s3.generate_presigned_url(
    <span class="str">"get_object"</span>,
    Params={<span class="str">"Bucket"</span>: <span class="str">"photos"</span>, <span class="str">"Key"</span>: <span class="str">"hero.jpg"</span>},
    ExpiresIn=<span class="num">3600</span>
)
<span class="kw">print</span>(url)</code></pre>

<h4>Use with curl</h4>
<pre><code class="lang-bash">curl -O <span class="str">"$URL"</span></code></pre>

<h3>Per-request signature anatomy</h3>
<pre><code class="lang-bash">PUT /photos/hero.jpg HTTP/1.1
Host: localhost:9000
x-amz-content-sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
x-amz-date: 20260426T120000Z
Authorization: AWS4-HMAC-SHA256
  Credential=minioadmin/20260426/us-east-1/s3/aws4_request,
  SignedHeaders=host;x-amz-content-sha256;x-amz-date,
  Signature=&lt;hex sha256 hmac&gt;
Content-Length: 12345

&lt;binary body&gt;</code></pre>

<h3>What gets verified</h3>
<ul>
  <li>Access key resolves to a known secret.</li>
  <li>Date is recent (5-minute clock skew window).</li>
  <li>Canonical request hash matches.</li>
  <li>Signature recomputed with the secret matches the one on the wire.</li>
  <li>Body SHA-256 matches <code>x-amz-content-sha256</code>.</li>
</ul>

<h3>CORS</h3>
<pre><code class="lang-bash">OXIDB_S3_CORS_ORIGIN=https://app.example.com</code></pre>
<p>Default is <code>*</code>. The server responds to OPTIONS preflights and adds <code>Access-Control-Allow-Origin</code> to every response.</p>

<div class="docs-callout"><strong>Tip:</strong> for browser uploads, combine presigned URLs with a strict <code>OXIDB_S3_CORS_ORIGIN</code>. The browser hits the server directly with no token in your frontend code.</div>

<div class="docs-prevnext">
  <a href="/s3/quickstart/" class="prev"><div class="label">Previous</div><div class="title">← Quick Start</div></a>
  <a href="/s3/buckets/" class="next"><div class="label">Next</div><div class="title">Buckets →</div></a>
</div>` }} />
}
