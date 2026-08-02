import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Python Examples",
  description: `Complete examples for the OxiDB Python client. TCP mode: pip install oxidb &middot; Embedded mode: pip install oxidb-embedded`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg> Python Examples</h2>
    <p class="section-desc">Complete examples for the OxiDB Python client. TCP mode: <code>pip install oxidb</code> &middot; Embedded mode: <code>pip install oxidb-embedded</code></p>

    <!-- Connect -->
    <div class="doc-block">
      <h3>Connect</h3>
      <pre><code><span class="kw">from</span> oxidb <span class="kw">import</span> OxiDbClient

<span class="co"># Connect to localhost:4444 (default)</span>
client = OxiDbClient()

<span class="co"># Custom host/port/timeout</span>
client = OxiDbClient(<span class="str">"192.0.2.100"</span>, <span class="num">4444</span>, timeout=<span class="num">10.0</span>)

<span class="co"># As a context manager (auto-closes)</span>
<span class="kw">with</span> OxiDbClient() <span class="kw">as</span> db:
    db.ping()  <span class="co"># "pong"</span></code></pre>
    </div>

    <!-- Insert -->
    <div class="doc-block">
      <h3>Insert Documents</h3>
      <pre><code><span class="co"># Insert a single document</span>
result = client.insert(<span class="str">"users"</span>, {
    <span class="str">"name"</span>: <span class="str">"Alice"</span>,
    <span class="str">"email"</span>: <span class="str">"alice@example.com"</span>,
    <span class="str">"age"</span>: <span class="num">30</span>,
})
<span class="co"># result: {"id": "..."}</span>

<span class="co"># Insert many documents</span>
result = client.insert_many(<span class="str">"users"</span>, [
    {<span class="str">"name"</span>: <span class="str">"Bob"</span>, <span class="str">"age"</span>: <span class="num">25</span>},
    {<span class="str">"name"</span>: <span class="str">"Charlie"</span>, <span class="str">"age"</span>: <span class="num">35</span>},
    {<span class="str">"name"</span>: <span class="str">"Diana"</span>, <span class="str">"age"</span>: <span class="num">28</span>},
])</code></pre>
    </div>

    <!-- Find -->
    <div class="doc-block">
      <h3>Find Documents</h3>
      <pre><code><span class="co"># Find all users over 25</span>
users = client.find(<span class="str">"users"</span>, {<span class="str">"age"</span>: {<span class="str">"$gt"</span>: <span class="num">25</span>}})

<span class="co"># Find with sort, skip, limit</span>
users = client.find(<span class="str">"users"</span>, {},
    sort={<span class="str">"age"</span>: <span class="num">-1</span>},
    skip=<span class="num">0</span>,
    limit=<span class="num">10</span>,
)

<span class="co"># Find one document</span>
user = client.find_one(<span class="str">"users"</span>, {<span class="str">"name"</span>: <span class="str">"Alice"</span>})

<span class="co"># Count documents</span>
count = client.count(<span class="str">"users"</span>, {<span class="str">"age"</span>: {<span class="str">"$gte"</span>: <span class="num">18</span>}})</code></pre>
    </div>

    <!-- Query Operators -->
    <div class="doc-block">
      <h3>Query Operators</h3>
      <pre><code><span class="co"># Comparison: $eq, $ne, $gt, $gte, $lt, $lte</span>
query = {<span class="str">"age"</span>: {<span class="str">"$gte"</span>: <span class="num">18</span>, <span class="str">"$lt"</span>: <span class="num">65</span>}}

<span class="co"># $in — match any value in list</span>
query = {<span class="str">"status"</span>: {<span class="str">"$in"</span>: [<span class="str">"active"</span>, <span class="str">"pending"</span>]}}

<span class="co"># $exists — check field presence</span>
query = {<span class="str">"email"</span>: {<span class="str">"$exists"</span>: <span class="kw">True</span>}}

<span class="co"># $regex — pattern matching</span>
query = {<span class="str">"name"</span>: {<span class="str">"$regex"</span>: <span class="str">"^A"</span>}}

<span class="co"># $and / $or — logical operators</span>
query = {<span class="str">"$or"</span>: [
    {<span class="str">"age"</span>: {<span class="str">"$lt"</span>: <span class="num">18</span>}},
    {<span class="str">"age"</span>: {<span class="str">"$gte"</span>: <span class="num">65</span>}},
]}

<span class="co"># Nested fields with dot notation</span>
query = {<span class="str">"address.city"</span>: <span class="str">"Berlin"</span>}</code></pre>
    </div>

    <!-- Update -->
    <div class="doc-block">
      <h3>Update Documents</h3>
      <pre><code><span class="co"># Update all matching documents</span>
result = client.update(<span class="str">"users"</span>,
    {<span class="str">"name"</span>: <span class="str">"Alice"</span>},
    {<span class="str">"$set"</span>: {<span class="str">"age"</span>: <span class="num">31</span>}},
)

<span class="co"># Update one document</span>
result = client.update_one(<span class="str">"users"</span>,
    {<span class="str">"name"</span>: <span class="str">"Bob"</span>},
    {
        <span class="str">"$inc"</span>: {<span class="str">"login_count"</span>: <span class="num">1</span>},
        <span class="str">"$set"</span>: {<span class="str">"last_login"</span>: <span class="str">"2026-03-05T12:00:00Z"</span>},
    },
)

<span class="co"># Update operators: $set, $unset, $inc, $mul, $min, $max,</span>
<span class="co"># $rename, $currentDate, $push, $pull, $addToSet, $pop</span></code></pre>
    </div>

    <!-- Delete -->
    <div class="doc-block">
      <h3>Delete Documents</h3>
      <pre><code><span class="co"># Delete all matching documents</span>
result = client.delete(<span class="str">"users"</span>, {<span class="str">"age"</span>: {<span class="str">"$lt"</span>: <span class="num">18</span>}})

<span class="co"># Delete one document</span>
result = client.delete_one(<span class="str">"users"</span>, {<span class="str">"name"</span>: <span class="str">"Charlie"</span>})</code></pre>
    </div>

    <!-- Indexes -->
    <div class="doc-block">
      <h3>Indexes</h3>
      <pre><code><span class="co"># Create a regular index</span>
client.create_index(<span class="str">"users"</span>, <span class="str">"age"</span>)

<span class="co"># Create a unique index</span>
client.create_unique_index(<span class="str">"users"</span>, <span class="str">"email"</span>)

<span class="co"># Create a composite index</span>
client.create_composite_index(<span class="str">"orders"</span>, [<span class="str">"customer_id"</span>, <span class="str">"date"</span>])

<span class="co"># List indexes on a collection</span>
indexes = client.list_indexes(<span class="str">"users"</span>)

<span class="co"># Drop an index</span>
client.drop_index(<span class="str">"users"</span>, <span class="str">"idx_age"</span>)</code></pre>
    </div>

    <!-- Aggregation -->
    <div class="doc-block">
      <h3>Aggregation Pipeline</h3>
      <pre><code><span class="co"># Group users by city, count and average age</span>
results = client.aggregate(<span class="str">"users"</span>, [
    {<span class="str">"$match"</span>: {<span class="str">"age"</span>: {<span class="str">"$gte"</span>: <span class="num">18</span>}}},
    {<span class="str">"$group"</span>: {
        <span class="str">"_id"</span>: <span class="str">"$city"</span>,
        <span class="str">"count"</span>: {<span class="str">"$sum"</span>: <span class="num">1</span>},
        <span class="str">"avg_age"</span>: {<span class="str">"$avg"</span>: <span class="str">"$age"</span>},
    }},
    {<span class="str">"$sort"</span>: {<span class="str">"count"</span>: <span class="num">-1</span>}},
    {<span class="str">"$limit"</span>: <span class="num">10</span>},
])

<span class="co"># Pipeline stages: $match, $group, $sort, $project,</span>
<span class="co"># $limit, $skip, $unwind, $addFields, $lookup, $count</span></code></pre>
    </div>

    <!-- Transactions -->
    <div class="doc-block">
      <h3>Transactions</h3>
      <pre><code><span class="co"># Context manager (auto-commit / auto-rollback)</span>
<span class="kw">with</span> client.transaction():
    client.insert(<span class="str">"accounts"</span>, {<span class="str">"owner"</span>: <span class="str">"Alice"</span>, <span class="str">"balance"</span>: <span class="num">1000</span>})
    client.update_one(<span class="str">"accounts"</span>,
        {<span class="str">"owner"</span>: <span class="str">"Bob"</span>},
        {<span class="str">"$inc"</span>: {<span class="str">"balance"</span>: <span class="num">-500</span>}},
    )
<span class="co"># auto-committed here</span>

<span class="co"># Manual transaction control</span>
client.begin_tx()
<span class="co"># ... operations ...</span>
client.commit_tx()   <span class="co"># or client.rollback_tx()</span></code></pre>
    </div>


    <!-- Full-Text Search -->
    <div class="doc-block">
      <h3>Full-Text Search</h3>
      <pre><code><span class="co"># Create a text index on fields</span>
client.create_text_index(<span class="str">"articles"</span>, [<span class="str">"title"</span>, <span class="str">"body"</span>])

<span class="co"># Search within a collection's text index</span>
results = client.text_search(<span class="str">"articles"</span>, <span class="str">"rust database"</span>, limit=<span class="num">20</span>)

<span class="co"># Search across blobs (PDF, DOCX, etc.)</span>
results = client.search(<span class="str">"quarterly report"</span>, limit=<span class="num">10</span>)</code></pre>
    </div>

    <!-- Vector Search -->
    <div class="doc-block">
      <h3>Vector Search</h3>
      <pre><code><span class="co"># Create a vector index</span>
client.create_vector_index(<span class="str">"products"</span>, <span class="str">"embedding"</span>, dimension=<span class="num">384</span>, metric=<span class="str">"cosine"</span>)

<span class="co"># Insert documents with vector embeddings</span>
client.insert(<span class="str">"products"</span>, {
    <span class="str">"name"</span>: <span class="str">"Wireless Mouse"</span>,
    <span class="str">"embedding"</span>: query_vector,  <span class="co"># list of 384 floats</span>
})

<span class="co"># Find 5 nearest neighbors</span>
results = client.vector_search(<span class="str">"products"</span>, <span class="str">"embedding"</span>, query_vector, limit=<span class="num">5</span>)
<span class="co"># Each result has _similarity and _distance fields</span></code></pre>
    </div>

    <!-- Blob Storage -->
    <div class="doc-block">
      <h3>Blob Storage</h3>
      <pre><code><span class="co"># Create a bucket</span>
client.create_bucket(<span class="str">"documents"</span>)

<span class="co"># Upload a file</span>
<span class="kw">with</span> open(<span class="str">"report.pdf"</span>, <span class="str">"rb"</span>) <span class="kw">as</span> f:
    client.put_object(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>, f.read(),
        content_type=<span class="str">"application/pdf"</span>,
        metadata={<span class="str">"author"</span>: <span class="str">"Alice"</span>})

<span class="co"># Download a file</span>
data, metadata = client.get_object(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>)

<span class="co"># Extract text from PDF/DOCX/HTML</span>
text = client.extract_text(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>)

<span class="co"># List objects with prefix filter</span>
objects = client.list_objects(<span class="str">"documents"</span>, prefix=<span class="str">"reports/"</span>)

<span class="co"># Delete</span>
client.delete_object(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>)</code></pre>
    </div>

    <!-- Schedules -->
    <div class="doc-block">
      <h3>Scheduled Tasks</h3>
      <pre><code><span class="co"># Create a schedule</span>
client.create_schedule(<span class="str">"cleanup"</span>, <span class="str">"delete_old_logs"</span>,
    cron=<span class="str">"0 3 * * *"</span>,  <span class="co"># every day at 3 AM</span>
    params={<span class="str">"days"</span>: <span class="num">30</span>},
)

<span class="co"># List, enable, disable, delete</span>
schedules = client.list_schedules()
client.disable_schedule(<span class="str">"cleanup"</span>)
client.enable_schedule(<span class="str">"cleanup"</span>)
client.delete_schedule(<span class="str">"cleanup"</span>)</code></pre>
    </div>

    <!-- Collections & Databases -->
    <div class="doc-block">
      <h3>Collections &amp; Databases</h3>
      <pre><code><span class="co"># Collection management</span>
client.create_collection(<span class="str">"users"</span>)
collections = client.list_collections()
client.drop_collection(<span class="str">"temp"</span>)

<span class="co"># Multi-database support</span>
client.create_database(<span class="str">"analytics"</span>)
client.use_database(<span class="str">"analytics"</span>)
databases = client.list_databases()
client.drop_database(<span class="str">"analytics"</span>)

<span class="co"># Compaction (reclaim disk space)</span>
stats = client.compact(<span class="str">"users"</span>)
<span class="co"># stats: {"old_size": ..., "new_size": ..., "docs_kept": ...}</span></code></pre>
    </div>

    <!-- Auth -->
    <div class="doc-block">
      <h3>Authentication &amp; User Roles</h3>
      <pre><code><span class="co"># Per-database role grants (requires Admin)</span>
client.grant_db_role(<span class="str">"alice"</span>, <span class="str">"analytics"</span>, <span class="str">"Admin"</span>)
client.revoke_db_role(<span class="str">"alice"</span>, <span class="str">"analytics"</span>)</code></pre>
    </div>

    <!-- Error Handling -->
    <div class="doc-block">
      <h3>Error Handling</h3>
      <pre><code><span class="kw">from</span> oxidb <span class="kw">import</span> OxiDbClient, OxiDbError, TransactionConflictError

<span class="kw">try</span>:
    client.insert(<span class="str">"users"</span>, {<span class="str">"_id"</span>: <span class="str">"duplicate"</span>})
<span class="kw">except</span> OxiDbError <span class="kw">as</span> e:
    print(f<span class="str">"Error: {e}"</span>)

<span class="co"># Transaction conflict (OCC)</span>
<span class="kw">try</span>:
    <span class="kw">with</span> client.transaction():
        client.update(<span class="str">"accounts"</span>, {}, {<span class="str">"$inc"</span>: {<span class="str">"balance"</span>: <span class="num">1</span>}})
<span class="kw">except</span> TransactionConflictError:
    print(<span class="str">"Conflict — retry the transaction"</span>)</code></pre>
    </div>

    <!-- Install -->
    <div class="doc-block">
      <h3>Installation</h3>
      <pre><code><span class="co"># TCP client — connects to oxidb-server</span>
pip install oxidb

<span class="co"># Embedded mode — runs in-process, no server needed</span>
pip install oxidb-embedded

<span class="co"># Or just copy the single TCP client file</span>
curl -O https://oxidb.baltavista.com/oxidb.py</code></pre>
    </div>

    <!-- Embedded Mode -->
    <div class="doc-block">
      <h3>Embedded Mode (No Server)</h3>
      <pre><code><span class="kw">from</span> oxidb_embedded <span class="kw">import</span> OxiDbEmbedded

<span class="co"># Run the database engine directly in your Python process</span>
<span class="co"># No server required — zero network overhead</span>
<span class="kw">with</span> OxiDbEmbedded(<span class="str">"./mydata"</span>) <span class="kw">as</span> db:
    db.insert(<span class="str">"users"</span>, {<span class="str">"name"</span>: <span class="str">"Alice"</span>, <span class="str">"age"</span>: <span class="num">30</span>})
    users = db.find(<span class="str">"users"</span>, {<span class="str">"age"</span>: {<span class="str">"$gt"</span>: <span class="num">25</span>}})
    print(users)

<span class="co"># Encryption at rest</span>
db = OxiDbEmbedded(<span class="str">"./mydata"</span>, encryption_key_path=<span class="str">"./key.bin"</span>)

<span class="co"># Same API as TCP client — insert, find, update, delete,</span>
<span class="co"># indexes, aggregation, transactions, search, blobs</span>
db.create_index(<span class="str">"users"</span>, <span class="str">"email"</span>)
db.aggregate(<span class="str">"users"</span>, [
    {<span class="str">"$group"</span>: {<span class="str">"_id"</span>: <span class="str">"$city"</span>, <span class="str">"count"</span>: {<span class="str">"$sum"</span>: <span class="num">1</span>}}},
])

<span class="kw">with</span> db.transaction():
    db.insert(<span class="str">"accounts"</span>, {<span class="str">"owner"</span>: <span class="str">"Bob"</span>, <span class="str">"balance"</span>: <span class="num">500</span>})
    db.update_one(<span class="str">"accounts"</span>, {<span class="str">"owner"</span>: <span class="str">"Alice"</span>}, {<span class="str">"$inc"</span>: {<span class="str">"balance"</span>: <span class="num">-500</span>}})

db.close()

<span class="co"># Requires the native library (liboxidb_embedded_ffi.dylib/.so/.dll)</span>
<span class="co"># Set OXIDB_LIB_PATH or place it next to the installed package</span></code></pre>
      <p style="margin-top:0.75rem"><a href="https://github.com/OxiDB-Pub/examples/tree/main/python" target="_blank">Full e-commerce analytics example on GitHub &rarr;</a></p>
    </div>

  </div>
</section>` }} />
}