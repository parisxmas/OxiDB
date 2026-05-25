import type { Metadata } from "next"
export const metadata: Metadata = { title: "S3 Buckets" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">S3 · API</p>
<h2>Buckets</h2>
<p>Buckets are flat namespaces for objects. Names follow standard S3 rules: lowercase, hyphens allowed, 3–63 characters.</p>

<h3>Endpoint summary</h3>
<div class="table-wrap"><table>
<thead><tr><th>Method</th><th>Path</th><th>Action</th></tr></thead>
<tbody>
<tr><td>GET</td><td><code>/</code></td><td>List buckets</td></tr>
<tr><td>PUT</td><td><code>/&lt;bucket&gt;</code></td><td>Create bucket</td></tr>
<tr><td>HEAD</td><td><code>/&lt;bucket&gt;</code></td><td>Check if a bucket exists</td></tr>
<tr><td>DELETE</td><td><code>/&lt;bucket&gt;</code></td><td>Delete bucket (must be empty)</td></tr>
<tr><td>GET</td><td><code>/&lt;bucket&gt;</code></td><td>List objects (with <code>prefix</code>, <code>delimiter</code>, <code>marker</code>, <code>max-keys</code>)</td></tr>
</tbody></table></div>

<h3>Create</h3>
<pre><code class="lang-bash">aws --endpoint-url http://localhost:9000 s3 mb s3://photos
aws --endpoint-url http://localhost:9000 s3 mb s3://logs-2026
aws --endpoint-url http://localhost:9000 s3 mb s3://backups</code></pre>

<h4>boto3</h4>
<pre><code class="lang-python">s3.create_bucket(Bucket=<span class="str">"photos"</span>)</code></pre>

<h4>Raw curl</h4>
<pre><code class="lang-bash">curl -X PUT <span class="str">"http://localhost:9000/photos"</span> \\
     --aws-sigv4 <span class="str">"aws:amz:us-east-1:s3"</span> \\
     --user <span class="str">"minioadmin:minioadmin"</span></code></pre>

<h3>List buckets</h3>
<pre><code class="lang-bash">aws --endpoint-url http://localhost:9000 s3 ls</code></pre>
<p>Response (XML):</p>
<pre><code class="lang-xml"><span class="kw">&lt;?xml</span> version=<span class="str">"1.0"</span> encoding=<span class="str">"UTF-8"</span><span class="kw">?&gt;</span>
<span class="kw">&lt;ListAllMyBucketsResult&gt;</span>
  <span class="kw">&lt;Buckets&gt;</span>
    <span class="kw">&lt;Bucket&gt;</span>
      <span class="kw">&lt;Name&gt;</span>backups<span class="kw">&lt;/Name&gt;</span>
      <span class="kw">&lt;CreationDate&gt;</span>2026-04-26T12:00:00Z<span class="kw">&lt;/CreationDate&gt;</span>
    <span class="kw">&lt;/Bucket&gt;</span>
    <span class="kw">&lt;Bucket&gt;</span>
      <span class="kw">&lt;Name&gt;</span>photos<span class="kw">&lt;/Name&gt;</span>
      <span class="kw">&lt;CreationDate&gt;</span>2026-04-26T12:01:00Z<span class="kw">&lt;/CreationDate&gt;</span>
    <span class="kw">&lt;/Bucket&gt;</span>
  <span class="kw">&lt;/Buckets&gt;</span>
<span class="kw">&lt;/ListAllMyBucketsResult&gt;</span></code></pre>

<h3>List objects (with prefix &amp; pagination)</h3>
<pre><code class="lang-bash"><span class="co"># All objects, default page size</span>
aws --endpoint-url http://localhost:9000 s3 ls s3://photos/

<span class="co"># Recursive</span>
aws --endpoint-url http://localhost:9000 s3 ls s3://photos/ --recursive

<span class="co"># Filtered by prefix</span>
aws --endpoint-url http://localhost:9000 s3 ls s3://photos/2026/

<span class="co"># Raw API call with query params</span>
aws --endpoint-url http://localhost:9000 s3api list-objects-v2 \\
    --bucket photos --prefix users/ --max-keys 100</code></pre>

<h3>HEAD bucket</h3>
<pre><code class="lang-bash">aws --endpoint-url http://localhost:9000 s3api head-bucket --bucket photos
<span class="co"># exit code 0  → exists, you have access</span>
<span class="co"># exit code 254 → 404 not found</span></code></pre>

<h3>Delete</h3>
<pre><code class="lang-bash"><span class="co"># Empty bucket first (or use --force)</span>
aws --endpoint-url http://localhost:9000 s3 rm s3://photos --recursive
aws --endpoint-url http://localhost:9000 s3 rb s3://photos

<span class="co"># Or in one shot</span>
aws --endpoint-url http://localhost:9000 s3 rb s3://photos --force</code></pre>

<h3>Programmatic listing in boto3</h3>
<pre><code class="lang-python">paginator = s3.get_paginator(<span class="str">"list_objects_v2"</span>)
<span class="kw">for</span> page <span class="kw">in</span> paginator.paginate(Bucket=<span class="str">"photos"</span>, Prefix=<span class="str">"users/"</span>):
    <span class="kw">for</span> obj <span class="kw">in</span> page.get(<span class="str">"Contents"</span>, []):
        <span class="kw">print</span>(obj[<span class="str">"Key"</span>], obj[<span class="str">"Size"</span>], obj[<span class="str">"ETag"</span>])</code></pre>

<h3>Naming rules</h3>
<ul>
  <li>3–63 characters, lowercase letters, digits, hyphens.</li>
  <li>Must start and end with a letter or digit.</li>
  <li>No dots — keep DNS-safe.</li>
  <li>No uppercase, no underscores.</li>
</ul>

<div class="docs-prevnext">
  <a href="/s3/auth/" class="prev"><div class="label">Previous</div><div class="title">← Authentication</div></a>
  <a href="/s3/objects/" class="next"><div class="label">Next</div><div class="title">Objects →</div></a>
</div>` }} />
}
