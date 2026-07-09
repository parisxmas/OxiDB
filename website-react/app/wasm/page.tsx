import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "WebAssembly",
  description: `Run OxiDB entirely in the browser via WebAssembly. No server needed — JSON queries and aggregation all work in-memory.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="14" rx="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></svg> WebAssembly</h2>
    <p class="section-desc">Run OxiDB entirely in the browser. No server needed &mdash; all data lives in memory within the WASM module. Supports MongoDB-style JSON queries, aggregation, and indexes.</p>

    <!-- Install -->
    <div class="doc-block">
      <h3>Installation</h3>
      <p>Download the pre-built WASM package from GitHub releases and extract into your project:</p>
      <pre><code>curl -L -o oxidb-wasm.tar.gz \\
  https://github.com/parisxmas/OxiDB/releases/download/v0.34.0/oxidb-wasm-v0.34.0.tar.gz

mkdir wasm &amp;&amp; tar xzf oxidb-wasm.tar.gz -C wasm/</code></pre>
      <p>Your project structure:</p>
      <pre><code>your-project/
  wasm/
    oxidb_wasm.js          # ES module entry point
    oxidb_wasm.d.ts        # TypeScript types
    oxidb_wasm_bg.wasm     # WASM binary (~1.5 MB gzipped)
    oxidb_wasm_bg.wasm.d.ts
  index.html</code></pre>
      <p class="tip"><strong>Note:</strong> WASM files must be served from your own web server (same origin). Direct import from GitHub URLs will not work due to CORS restrictions.</p>
    </div>

    <!-- Initialize -->
    <div class="doc-block">
      <h3>Initialize</h3>
      <p>Load the WASM module and create an in-memory database:</p>
      <pre><code>&lt;script type="module"&gt;
  import init, * as oxidb from './wasm/oxidb_wasm.js';

  await init();     // load WASM binary
  oxidb.init();     // create in-memory database
  // ready to use
&lt;/script&gt;</code></pre>
    </div>

    <!-- Insert -->
    <div class="doc-block">
      <h3>Insert Documents</h3>
      <pre><code>// Single document &mdash; returns document ID
const id = oxidb.insert('users', JSON.stringify({
  name: 'Alice', age: 30, city: 'Berlin'
}));

// Multiple documents &mdash; returns JSON array of IDs
const ids = JSON.parse(oxidb.insert_many('users', JSON.stringify([
  { name: 'Bob', age: 25, city: 'Tokyo' },
  { name: 'Charlie', age: 35, city: 'Berlin' }
])));</code></pre>
    </div>

    <!-- Find -->
    <div class="doc-block">
      <h3>Query Documents</h3>
      <pre><code>// All documents
const all = JSON.parse(oxidb.find('users', '{}'));

// Filter
const berliners = JSON.parse(oxidb.find('users',
  JSON.stringify({ city: 'Berlin' })
));

// Operators: $gt, $gte, $lt, $lte, $ne, $in, $nin, $exists, $regex
const older = JSON.parse(oxidb.find('users',
  JSON.stringify({ age: { $gt: 25 } })
));

// Single document
const alice = JSON.parse(oxidb.find_one('users',
  JSON.stringify({ name: 'Alice' })
));</code></pre>
    </div>

    <!-- Update -->
    <div class="doc-block">
      <h3>Update Documents</h3>
      <pre><code>// Returns number of modified documents
const n = oxidb.update('users',
  JSON.stringify({ name: 'Alice' }),          // filter
  JSON.stringify({ $set: { age: 31 } })       // update
);

// Operators: $set, $unset, $inc, $mul, $min, $max, $push, $pull, $addToSet
oxidb.update('users',
  JSON.stringify({ city: 'Berlin' }),
  JSON.stringify({ $inc: { age: 1 } })
);</code></pre>
    </div>

    <!-- Delete -->
    <div class="doc-block">
      <h3>Delete Documents</h3>
      <pre><code>// Returns number of deleted documents
const n = oxidb.delete('users',
  JSON.stringify({ age: { $lt: 20 } })
);</code></pre>
    </div>

    <!-- Count -->
    <div class="doc-block">
      <h3>Count</h3>
      <pre><code>const total = oxidb.count('users', '{}');
const berliners = oxidb.count('users',
  JSON.stringify({ city: 'Berlin' })
);</code></pre>
    </div>

    <!-- Indexes -->
    <div class="doc-block">
      <h3>Indexes</h3>
      <pre><code>oxidb.create_index('users', 'age');
oxidb.create_index('users', 'city');</code></pre>
    </div>

    <!-- Aggregation -->
    <div class="doc-block">
      <h3>Aggregation Pipeline</h3>
      <pre><code>const pipeline = JSON.stringify([
  { $match: { age: { $gte: 20 } } },
  { $group: {
      _id: '$city',
      count: { $sum: 1 },
      avg_age: { $avg: '$age' }
  }},
  { $sort: { count: -1 } }
]);
const stats = JSON.parse(oxidb.aggregate('users', pipeline));</code></pre>
    </div>

    <!-- Collections -->
    <div class="doc-block">
      <h3>Collections</h3>
      <pre><code>const names = JSON.parse(oxidb.list_collections());
oxidb.drop_collection('old_data');</code></pre>
    </div>

    <!-- Complete Example -->
    <div class="doc-block">
      <h3>Complete Example</h3>
      <pre><code>&lt;!DOCTYPE html&gt;
&lt;html&gt;
&lt;head&gt;&lt;title&gt;OxiDB WASM&lt;/title&gt;&lt;/head&gt;
&lt;body&gt;
  &lt;pre id="out"&gt;&lt;/pre&gt;
  &lt;script type="module"&gt;
    import init, * as oxidb from './wasm/oxidb_wasm.js';

    await init();
    oxidb.init();

    // Insert
    oxidb.insert('tasks', JSON.stringify(
      { title: 'Buy groceries', done: false, priority: 'high' }
    ));
    oxidb.insert('tasks', JSON.stringify(
      { title: 'Write docs', done: true, priority: 'medium' }
    ));

    // Index
    oxidb.create_index('tasks', 'priority');

    // JSON query
    const urgent = JSON.parse(oxidb.find('tasks',
      JSON.stringify({ priority: 'high', done: false })
    ));

    // Aggregation
    const stats = JSON.parse(oxidb.aggregate('tasks', JSON.stringify([
      { $group: { _id: '$done', count: { $sum: 1 } } }
    ])));

    document.getElementById('out').textContent =
      JSON.stringify({ urgent, stats }, null, 2);
  &lt;/script&gt;
&lt;/body&gt;
&lt;/html&gt;</code></pre>
    </div>

    <!-- API Reference -->
    <div class="doc-block">
      <h3>API Reference</h3>
      <div class="table-wrap">
        <table>
          <thead><tr><th>Function</th><th>Arguments</th><th>Returns</th></tr></thead>
          <tbody>
            <tr><td><code>init()</code></td><td>&mdash;</td><td>Creates in-memory database</td></tr>
            <tr><td><code>insert(collection, jsonStr)</code></td><td>collection name, JSON document string</td><td>Document ID (string)</td></tr>
            <tr><td><code>insert_many(collection, jsonStr)</code></td><td>collection name, JSON array string</td><td>JSON array of IDs</td></tr>
            <tr><td><code>find(collection, queryStr)</code></td><td>collection name, JSON query string</td><td>JSON array string</td></tr>
            <tr><td><code>find_one(collection, queryStr)</code></td><td>collection name, JSON query string</td><td>JSON string or "null"</td></tr>
            <tr><td><code>update(collection, queryStr, updateStr)</code></td><td>filter, update operations</td><td>Modified count (number)</td></tr>
            <tr><td><code>delete(collection, queryStr)</code></td><td>collection name, JSON query string</td><td>Deleted count (number)</td></tr>
            <tr><td><code>count(collection, queryStr)</code></td><td>collection name, JSON query string</td><td>Count (number)</td></tr>
            <tr><td><code>aggregate(collection, pipelineStr)</code></td><td>collection name, JSON pipeline string</td><td>JSON array string</td></tr>
            <tr><td><code>create_index(collection, field)</code></td><td>collection name, field name</td><td>&mdash;</td></tr>
            <tr><td><code>list_collections()</code></td><td>&mdash;</td><td>JSON array of names</td></tr>
            <tr><td><code>drop_collection(name)</code></td><td>collection name</td><td>&mdash;</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Build from Source -->
    <div class="doc-block">
      <h3>Build from Source</h3>
      <pre><code># Prerequisites: Rust, wasm-pack
cargo install wasm-pack

# Clone and build
git clone https://github.com/parisxmas/OxiDB.git
cd OxiDB/oxidb-wasm
./build.sh

# Output in pkg/</code></pre>
    </div>

    <!-- Notes -->
    <div class="doc-block">
      <h3>Notes</h3>
      <ul>
        <li>All data is <strong>in-memory only</strong> &mdash; does not persist across page reloads</li>
        <li>WASM binary is ~1.5 MB gzipped (~4.7 MB uncompressed)</li>
        <li>All queries run synchronously on the main thread</li>
        <li>Supports the same query operators and aggregation pipeline as native OxiDB</li>
        <li>TypeScript types included (<code>oxidb_wasm.d.ts</code>)</li>
      </ul>
    </div>

  </div>
</section>` }} />
}
