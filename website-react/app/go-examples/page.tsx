import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Go Examples",
  description: `Complete examples for the OxiDB Go client. Install with go get github.com/parisxmas/oxiwire-go`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg> Go Examples</h2>
    <p class="section-desc">Complete examples for the OxiDB Go client. Install with <code>go get github.com/parisxmas/oxiwire-go</code></p>

    <!-- Connect -->
    <div class="doc-block">
      <h3>Connect</h3>
      <pre><code><span class="kw">import</span> <span class="str">"github.com/parisxmas/OxiDB/go/oxidb"</span>

<span class="co">// Connect to localhost:4444 (default)</span>
client, err := oxidb.ConnectDefault()
<span class="kw">if</span> err != <span class="kw">nil</span> {
    log.Fatal(err)
}
<span class="kw">defer</span> client.Close()

<span class="co">// Enable OxiWire binary protocol (fastest)</span>
client.UseOxiWire()

<span class="co">// Or connect with custom host/port/timeout</span>
client, err = oxidb.Connect(<span class="str">"192.0.2.100"</span>, <span class="num">4444</span>, <span class="num">10</span>*time.Second)</code></pre>
    </div>

    <!-- Insert -->
    <div class="doc-block">
      <h3>Insert Documents</h3>
      <pre><code><span class="co">// Insert a single document</span>
result, err := client.Insert(<span class="str">"users"</span>, map[<span class="kw">string</span>]<span class="kw">any</span>{
    <span class="str">"name"</span>:  <span class="str">"Alice"</span>,
    <span class="str">"email"</span>: <span class="str">"alice@example.com"</span>,
    <span class="str">"age"</span>:   <span class="num">30</span>,
})

<span class="co">// Insert many documents</span>
docs := []map[<span class="kw">string</span>]<span class="kw">any</span>{
    {<span class="str">"name"</span>: <span class="str">"Bob"</span>, <span class="str">"age"</span>: <span class="num">25</span>},
    {<span class="str">"name"</span>: <span class="str">"Charlie"</span>, <span class="str">"age"</span>: <span class="num">35</span>},
    {<span class="str">"name"</span>: <span class="str">"Diana"</span>, <span class="str">"age"</span>: <span class="num">28</span>},
}
result, err := client.InsertMany(<span class="str">"users"</span>, docs)</code></pre>
    </div>

    <!-- Find -->
    <div class="doc-block">
      <h3>Find Documents</h3>
      <pre><code><span class="co">// Find all users over 25</span>
users, err := client.Find(<span class="str">"users"</span>, map[<span class="kw">string</span>]<span class="kw">any</span>{
    <span class="str">"age"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"$gt"</span>: <span class="num">25</span>},
}, <span class="kw">nil</span>)

<span class="co">// Find with sort, skip, limit</span>
limit := <span class="num">10</span>
skip := <span class="num">0</span>
users, err := client.Find(<span class="str">"users"</span>, map[<span class="kw">string</span>]<span class="kw">any</span>{}, &oxidb.FindOptions{
    Sort:  map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"age"</span>: <span class="num">-1</span>},
    Skip:  &skip,
    Limit: &limit,
})

<span class="co">// Find one document</span>
user, err := client.FindOne(<span class="str">"users"</span>, map[<span class="kw">string</span>]<span class="kw">any</span>{
    <span class="str">"name"</span>: <span class="str">"Alice"</span>,
})

<span class="co">// Count documents</span>
count, err := client.Count(<span class="str">"users"</span>, map[<span class="kw">string</span>]<span class="kw">any</span>{
    <span class="str">"age"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"$gte"</span>: <span class="num">18</span>},
})</code></pre>
    </div>

    <!-- Query Operators -->
    <div class="doc-block">
      <h3>Query Operators</h3>
      <pre><code><span class="co">// Comparison: $eq, $ne, $gt, $gte, $lt, $lte</span>
query := map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"age"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"$gte"</span>: <span class="num">18</span>, <span class="str">"$lt"</span>: <span class="num">65</span>}}

<span class="co">// $in — match any value in list</span>
query = map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"status"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"$in"</span>: []<span class="kw">any</span>{<span class="str">"active"</span>, <span class="str">"pending"</span>}}}

<span class="co">// $exists — check field presence</span>
query = map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"email"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"$exists"</span>: <span class="kw">true</span>}}

<span class="co">// $regex — pattern matching</span>
query = map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"name"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"$regex"</span>: <span class="str">"^A"</span>}}

<span class="co">// $and / $or — logical operators</span>
query = map[<span class="kw">string</span>]<span class="kw">any</span>{
    <span class="str">"$or"</span>: []<span class="kw">any</span>{
        map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"age"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"$lt"</span>: <span class="num">18</span>}},
        map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"age"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"$gte"</span>: <span class="num">65</span>}},
    },
}

<span class="co">// Nested fields with dot notation</span>
query = map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"address.city"</span>: <span class="str">"Berlin"</span>}</code></pre>
    </div>

    <!-- Update -->
    <div class="doc-block">
      <h3>Update Documents</h3>
      <pre><code><span class="co">// Update all matching documents</span>
result, err := client.Update(<span class="str">"users"</span>,
    map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"name"</span>: <span class="str">"Alice"</span>},
    map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"$set"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"age"</span>: <span class="num">31</span>}},
)

<span class="co">// Update one document</span>
result, err = client.UpdateOne(<span class="str">"users"</span>,
    map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"name"</span>: <span class="str">"Bob"</span>},
    map[<span class="kw">string</span>]<span class="kw">any</span>{
        <span class="str">"$inc"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"login_count"</span>: <span class="num">1</span>},
        <span class="str">"$set"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"last_login"</span>: time.Now().Format(time.RFC3339)},
    },
)

<span class="co">// Update operators: $set, $unset, $inc, $mul, $min, $max,</span>
<span class="co">// $rename, $currentDate, $push, $pull, $addToSet, $pop</span></code></pre>
    </div>

    <!-- Delete -->
    <div class="doc-block">
      <h3>Delete Documents</h3>
      <pre><code><span class="co">// Delete all matching documents</span>
result, err := client.Delete(<span class="str">"users"</span>, map[<span class="kw">string</span>]<span class="kw">any</span>{
    <span class="str">"age"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"$lt"</span>: <span class="num">18</span>},
})

<span class="co">// Delete one document</span>
result, err = client.DeleteOne(<span class="str">"users"</span>, map[<span class="kw">string</span>]<span class="kw">any</span>{
    <span class="str">"name"</span>: <span class="str">"Charlie"</span>,
})</code></pre>
    </div>

    <!-- Indexes -->
    <div class="doc-block">
      <h3>Indexes</h3>
      <pre><code><span class="co">// Create a regular index</span>
err := client.CreateIndex(<span class="str">"users"</span>, <span class="str">"age"</span>)

<span class="co">// Create a unique index</span>
err = client.CreateUniqueIndex(<span class="str">"users"</span>, <span class="str">"email"</span>)

<span class="co">// Create a composite index</span>
err = client.CreateCompositeIndex(<span class="str">"orders"</span>, []<span class="kw">string</span>{<span class="str">"customer_id"</span>, <span class="str">"date"</span>})

<span class="co">// List indexes on a collection</span>
indexes, err := client.ListIndexes(<span class="str">"users"</span>)

<span class="co">// Drop an index</span>
err = client.DropIndex(<span class="str">"users"</span>, <span class="str">"idx_age"</span>)</code></pre>
    </div>

    <!-- Aggregation -->
    <div class="doc-block">
      <h3>Aggregation Pipeline</h3>
      <pre><code><span class="co">// Group users by city, count and average age</span>
results, err := client.Aggregate(<span class="str">"users"</span>, []map[<span class="kw">string</span>]<span class="kw">any</span>{
    {<span class="str">"$match"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"age"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"$gte"</span>: <span class="num">18</span>}}},
    {<span class="str">"$group"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{
        <span class="str">"_id"</span>:       <span class="str">"$city"</span>,
        <span class="str">"count"</span>:    map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"$sum"</span>: <span class="num">1</span>},
        <span class="str">"avg_age"</span>:  map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"$avg"</span>: <span class="str">"$age"</span>},
    }},
    {<span class="str">"$sort"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"count"</span>: <span class="num">-1</span>}},
    {<span class="str">"$limit"</span>: <span class="num">10</span>},
})

<span class="co">// Pipeline stages: $match, $group, $sort, $project,</span>
<span class="co">// $limit, $skip, $unwind, $addFields, $lookup, $count</span></code></pre>
    </div>

    <!-- Transactions -->
    <div class="doc-block">
      <h3>Transactions</h3>
      <pre><code><span class="co">// Simple transaction helper</span>
err := client.WithTransaction(<span class="kw">func</span>() error {
    _, err := client.Insert(<span class="str">"accounts"</span>, map[<span class="kw">string</span>]<span class="kw">any</span>{
        <span class="str">"owner"</span>: <span class="str">"Alice"</span>, <span class="str">"balance"</span>: <span class="num">1000</span>,
    })
    <span class="kw">if</span> err != <span class="kw">nil</span> {
        <span class="kw">return</span> err
    }
    _, err = client.UpdateOne(<span class="str">"accounts"</span>,
        map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"owner"</span>: <span class="str">"Bob"</span>},
        map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"$inc"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{<span class="str">"balance"</span>: <span class="num">-500</span>}},
    )
    <span class="kw">return</span> err
})

<span class="co">// Manual transaction control</span>
client.BeginTx()
<span class="co">// ... operations ...</span>
client.CommitTx()   <span class="co">// or client.RollbackTx()</span></code></pre>
    </div>

    <!-- Full-Text Search -->
    <div class="doc-block">
      <h3>Full-Text Search</h3>
      <pre><code><span class="co">// Create a text index on fields</span>
err := client.CreateTextIndex(<span class="str">"articles"</span>, []<span class="kw">string</span>{<span class="str">"title"</span>, <span class="str">"body"</span>})

<span class="co">// Search within a collection's text index</span>
results, err := client.TextSearch(<span class="str">"articles"</span>, <span class="str">"rust database"</span>, <span class="num">20</span>)

<span class="co">// Search across blobs (PDF, DOCX, etc.)</span>
results, err = client.Search(<span class="str">"quarterly report"</span>, <span class="kw">nil</span>, <span class="num">10</span>)</code></pre>
    </div>

    <!-- Vector Search -->
    <div class="doc-block">
      <h3>Vector Search</h3>
      <pre><code><span class="co">// Create a vector index</span>
err := client.CreateVectorIndex(<span class="str">"products"</span>, <span class="str">"embedding"</span>, <span class="num">384</span>, <span class="str">"cosine"</span>)

<span class="co">// Insert documents with vector embeddings</span>
client.Insert(<span class="str">"products"</span>, map[<span class="kw">string</span>]<span class="kw">any</span>{
    <span class="str">"name"</span>:      <span class="str">"Wireless Mouse"</span>,
    <span class="str">"embedding"</span>: queryVector, <span class="co">// []float64 of dimension 384</span>
})

<span class="co">// Find 5 nearest neighbors</span>
results, err := client.VectorSearch(<span class="str">"products"</span>, <span class="str">"embedding"</span>, queryVector, <span class="num">5</span>)
<span class="co">// Each result has _similarity and _distance fields</span></code></pre>
    </div>

    <!-- Blob Storage -->
    <div class="doc-block">
      <h3>Blob Storage</h3>
      <pre><code><span class="co">// Create a bucket</span>
err := client.CreateBucket(<span class="str">"documents"</span>)

<span class="co">// Upload a file</span>
data, _ := os.ReadFile(<span class="str">"report.pdf"</span>)
meta, err := client.PutObject(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>, data,
    <span class="str">"application/pdf"</span>, map[<span class="kw">string</span>]<span class="kw">string</span>{<span class="str">"author"</span>: <span class="str">"Alice"</span>})

<span class="co">// Download a file</span>
content, metadata, err := client.GetObject(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>)

<span class="co">// List objects</span>
prefix := <span class="str">"reports/"</span>
objects, err := client.ListObjects(<span class="str">"documents"</span>, &prefix, <span class="kw">nil</span>)

<span class="co">// Delete</span>
err = client.DeleteObject(<span class="str">"documents"</span>, <span class="str">"report.pdf"</span>)</code></pre>
    </div>

    <!-- Pipeline -->
    <div class="doc-block">
      <h3>Pipeline (Batch Commands)</h3>
      <pre><code><span class="co">// Send multiple commands in a single roundtrip</span>
results, err := client.Pipeline([]map[<span class="kw">string</span>]<span class="kw">any</span>{
    {<span class="str">"cmd"</span>: <span class="str">"find"</span>, <span class="str">"collection"</span>: <span class="str">"users"</span>, <span class="str">"query"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{}},
    {<span class="str">"cmd"</span>: <span class="str">"count"</span>, <span class="str">"collection"</span>: <span class="str">"orders"</span>, <span class="str">"query"</span>: map[<span class="kw">string</span>]<span class="kw">any</span>{}},
    {<span class="str">"cmd"</span>: <span class="str">"list_collections"</span>},
})

<span class="co">// Bulk insert with pipeline (multiple batches in one roundtrip)</span>
total, err := client.PipelineInsertMany(<span class="str">"logs"</span>, batches)</code></pre>
    </div>

    <!-- Collections & Databases -->
    <div class="doc-block">
      <h3>Collections &amp; Databases</h3>
      <pre><code><span class="co">// Collection management</span>
err := client.CreateCollection(<span class="str">"users"</span>)
collections, err := client.ListCollections()
err = client.DropCollection(<span class="str">"temp"</span>)

<span class="co">// Multi-database support</span>
err = client.CreateDatabase(<span class="str">"analytics"</span>)
err = client.UseDatabase(<span class="str">"analytics"</span>)
databases, err := client.ListDatabases()
err = client.DropDatabase(<span class="str">"analytics"</span>)

<span class="co">// Compaction (reclaim disk space)</span>
stats, err := client.Compact(<span class="str">"users"</span>)</code></pre>
    </div>

    <!-- Auth -->
    <div class="doc-block">
      <h3>Authentication &amp; Users</h3>
      <pre><code><span class="co">// Authenticate</span>
role, err := client.AuthSimple(<span class="str">"admin"</span>, <span class="str">"secretpassword"</span>)

<span class="co">// User management (requires Admin role)</span>
err = client.CreateUser(<span class="str">"alice"</span>, <span class="str">"password123"</span>, <span class="str">"ReadWrite"</span>)
users, err := client.ListUsers()

<span class="co">// Per-database roles</span>
err = client.GrantDbRole(<span class="str">"alice"</span>, <span class="str">"analytics"</span>, <span class="str">"Admin"</span>)
err = client.RevokeDbRole(<span class="str">"alice"</span>, <span class="str">"analytics"</span>)</code></pre>
    </div>

    <!-- Install -->
    <div class="doc-block">
      <h3>Installation</h3>
      <pre><code><span class="co"># OxiDB Go client</span>
go get github.com/parisxmas/OxiDB/go/oxidb

<span class="co"># OxiWire protocol library (standalone)</span>
go get github.com/parisxmas/oxiwire-go@v0.1.0</code></pre>
    </div>

  </div>
</section>` }} />
}