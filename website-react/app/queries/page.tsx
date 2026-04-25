import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Query Operators",
  description: `MongoDB-compatible query language with JSON syntax. Dot notation for nested fields.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section id="queries" class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg> Query Operators</h2>
    <p class="section-desc">MongoDB-compatible query language with JSON syntax. Dot notation for nested fields.</p>

    <h3>Comparison</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Operator</th><th>Description</th><th>Example</th></tr></thead>
        <tbody>
          <tr><td><code>$eq</code></td><td>Equals</td><td><code>{"age": {"$eq": 30}}</code> or just <code>{"age": 30}</code></td></tr>
          <tr><td><code>$ne</code></td><td>Not equal</td><td><code>{"status": {"$ne": "inactive"}}</code></td></tr>
          <tr><td><code>$gt</code></td><td>Greater than</td><td><code>{"age": {"$gt": 25}}</code></td></tr>
          <tr><td><code>$gte</code></td><td>Greater than or equal</td><td><code>{"age": {"$gte": 25}}</code></td></tr>
          <tr><td><code>$lt</code></td><td>Less than</td><td><code>{"salary": {"$lt": 100000}}</code></td></tr>
          <tr><td><code>$lte</code></td><td>Less than or equal</td><td><code>{"salary": {"$lte": 100000}}</code></td></tr>
          <tr><td><code>$in</code></td><td>Match any in array</td><td><code>{"country": {"$in": ["US", "UK", "JP"]}}</code></td></tr>
          <tr><td><code>$exists</code></td><td>Field exists</td><td><code>{"email": {"$exists": true}}</code></td></tr>
          <tr><td><code>$regex</code></td><td>Regular expression</td><td><code>{"name": {"$regex": "^A", "$options": "i"}}</code></td></tr>
        </tbody>
      </table>
    </div>

    <h3>Logical</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Operator</th><th>Description</th><th>Example</th></tr></thead>
        <tbody>
          <tr><td><code>$and</code></td><td>All conditions match</td><td><code>{"$and": [{"age": {"$gte": 25}}, {"status": "active"}]}</code></td></tr>
          <tr><td><code>$or</code></td><td>Any condition matches</td><td><code>{"$or": [{"city": "Tokyo"}, {"city": "Paris"}]}</code></td></tr>
          <tr><td><code>$nor</code></td><td>None of the conditions match <span class="version-badge latest">v0.25.1</span></td><td><code>{"$nor": [{"status": "banned"}, {"deleted": true}]}</code></td></tr>
          <tr><td><code>$not</code></td><td>Negate a field condition (missing fields → true) <span class="version-badge latest">v0.25.1</span></td><td><code>{"age": {"$not": {"$lt": 18}}}</code></td></tr>
        </tbody>
      </table>
    </div>

    <h3>Array <span class="version-badge latest">v0.25.1</span></h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Operator</th><th>Description</th><th>Example</th></tr></thead>
        <tbody>
          <tr><td><code>$elemMatch</code></td><td>Match an array element against a sub-query (AND across conditions)</td><td><code>{"items": {"$elemMatch": {"qty": {"$gte": 3}, "in_stock": true}}}</code></td></tr>
          <tr><td><code>$all</code></td><td>Array contains all listed values</td><td><code>{"tags": {"$all": ["sale", "new"]}}</code></td></tr>
          <tr><td><code>$size</code></td><td>Array has exactly this length</td><td><code>{"tags": {"$size": 3}}</code></td></tr>
        </tbody>
      </table>
    </div>

    <h3>Element &amp; evaluation <span class="version-badge latest">v0.25.1</span></h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Operator</th><th>Description</th><th>Example</th></tr></thead>
        <tbody>
          <tr><td><code>$type</code></td><td>JSON type match: <code>string</code>, <code>number</code>, <code>int</code>, <code>bool</code>, <code>array</code>, <code>object</code>, <code>null</code></td><td><code>{"phone": {"$type": "string"}}</code></td></tr>
          <tr><td><code>$mod</code></td><td>Modulo arithmetic <code>[divisor, remainder]</code></td><td><code>{"age": {"$mod": [10, 0]}}</code> (multiples of 10)</td></tr>
          <tr><td><code>$expr</code></td><td>Top-level cross-field comparisons</td><td><code>{"$expr": {"$gt": ["$sold", "$stock"]}}</code></td></tr>
        </tbody>
      </table>
    </div>

    <h3>Find Options</h3>
    <pre><code class="lang-json">{
  <span class="str">"sort"</span>: {<span class="str">"age"</span>: <span class="num">-1</span>, <span class="str">"name"</span>: <span class="num">1</span>},
  <span class="str">"skip"</span>: <span class="num">20</span>,
  <span class="str">"limit"</span>: <span class="num">10</span>
}</code></pre>
    <p>Sort values: <code>1</code> ascending, <code>-1</code> descending. When an index covers the sort field, sort is O(limit) instead of O(n log n).</p>

    <h3>Nested Fields</h3>
    <pre><code class="lang-json"><span class="co">// Dot notation for nested access</span>
{<span class="str">"address.city"</span>: <span class="str">"Tokyo"</span>}
{<span class="str">"address.zip"</span>: {<span class="str">"$regex"</span>: <span class="str">"^0"</span>}}</code></pre>

    <h3>Type Ordering</h3>
    <p>Cross-type comparisons follow a consistent ordering:</p>
    <p><code>Null &lt; Bool &lt; Number &lt; DateTime &lt; String</code></p>
    <p>Date strings (ISO 8601, RFC 3339, YYYY-MM-DD) are auto-detected and stored as epoch milliseconds.</p>
  </div>
</section>` }} />
}