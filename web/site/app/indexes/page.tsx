import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Indexes",
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section id="indexes" class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></svg> Indexes</h2>

    <h3>Index Types</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Type</th><th>Command</th><th>Use Case</th></tr></thead>
        <tbody>
          <tr><td>Single-field</td><td><code>create_index("users", "email")</code></td><td>Fast equality and range lookups</td></tr>
          <tr><td>Unique</td><td><code>create_unique_index("users", "email")</code></td><td>Enforce uniqueness</td></tr>
          <tr><td>Composite</td><td><code>create_composite_index("users", ["dept", "age"])</code></td><td>Multi-field queries, compound sort</td></tr>
          <tr><td>Text</td><td><code>create_text_index("articles", ["title", "body"])</code></td><td>Full-text search</td></tr>
          <tr><td>Vector</td><td><code>create_vector_index("items", "embedding", 384, "cosine")</code></td><td>Similarity search</td></tr>
        </tbody>
      </table>
    </div>

    <h3>Index Optimizations</h3>
    <ul>
      <li><strong>Index-only count</strong> -- <code>count()</code> returns the index set size without touching documents. Up to 446x faster than scanning.</li>
      <li><strong>Index-backed sort</strong> -- When the sort field is indexed, retrieval is O(limit) instead of O(n log n).</li>
      <li><strong>Composite prefix scan</strong> -- Index on [A, B, C] can answer queries on A, or A+B, or A+B+C.</li>
      <li><strong>Early termination</strong> -- <code>find_one</code> and <code>delete_one</code> stop after the first match.</li>
      <li><strong>Zero-decode</strong> -- Fully indexed queries skip document deserialization entirely.</li>
    </ul>
  </div>
</section>` }} />
}