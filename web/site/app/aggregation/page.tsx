import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Aggregation Pipeline",
  description: `Process data through a sequence of stages. Each stage transforms the document stream.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section id="aggregation" class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg> Aggregation Pipeline</h2>
    <p class="section-desc">Process data through a sequence of stages. Each stage transforms the document stream.</p>

    <h3>Stages</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Stage</th><th>Description</th></tr></thead>
        <tbody>
          <tr><td><code>$match</code></td><td>Filter documents using query operators</td></tr>
          <tr><td><code>$group</code></td><td>Group by key with accumulators ($sum, $avg, $min, $max, $count, $first, $last, $push)</td></tr>
          <tr><td><code>$sort</code></td><td>Sort documents (1 asc, -1 desc)</td></tr>
          <tr><td><code>$project</code></td><td>Include/exclude fields, compute new fields</td></tr>
          <tr><td><code>$limit</code></td><td>Limit output count</td></tr>
          <tr><td><code>$skip</code></td><td>Skip N documents</td></tr>
          <tr><td><code>$unwind</code></td><td>Deconstruct array field into separate documents</td></tr>
          <tr><td><code>$addFields</code></td><td>Add computed fields</td></tr>
          <tr><td><code>$lookup</code></td><td>Join with another collection</td></tr>
          <tr><td><code>$count</code></td><td>Count documents and output as named field</td></tr>
        </tbody>
      </table>
    </div>

    <h3>Group Accumulators</h3>
    <pre><code class="lang-json">[
  {<span class="str">"$group"</span>: {
    <span class="str">"_id"</span>: <span class="str">"$department"</span>,
    <span class="str">"avg_salary"</span>: {<span class="str">"$avg"</span>: <span class="str">"$salary"</span>},
    <span class="str">"total"</span>: {<span class="str">"$sum"</span>: <span class="num">1</span>},
    <span class="str">"max_age"</span>: {<span class="str">"$max"</span>: <span class="str">"$age"</span>},
    <span class="str">"names"</span>: {<span class="str">"$push"</span>: <span class="str">"$name"</span>}
  }},
  {<span class="str">"$sort"</span>: {<span class="str">"avg_salary"</span>: <span class="num">-1</span>}},
  {<span class="str">"$limit"</span>: <span class="num">5</span>}
]</code></pre>

    <h3>$lookup (Join)</h3>
    <pre><code class="lang-json">{
  <span class="str">"$lookup"</span>: {
    <span class="str">"from"</span>: <span class="str">"orders"</span>,
    <span class="str">"local_field"</span>: <span class="str">"_id"</span>,
    <span class="str">"foreign_field"</span>: <span class="str">"user_id"</span>,
    <span class="str">"as"</span>: <span class="str">"user_orders"</span>
  }
}</code></pre>

    <h3>Expression Operators</h3>
    <p>Use inside <code>$project</code> and <code>$addFields</code>:</p>
    <pre><code class="lang-json">{<span class="str">"$addFields"</span>: {
  <span class="str">"total"</span>: {<span class="str">"$add"</span>: [<span class="str">"$price"</span>, <span class="str">"$tax"</span>]},
  <span class="str">"discount_price"</span>: {<span class="str">"$multiply"</span>: [<span class="str">"$price"</span>, <span class="num">0.9</span>]}
}}</code></pre>
    <p>Available: <code>$add</code>, <code>$subtract</code>, <code>$multiply</code>, <code>$divide</code></p>
  </div>
</section>` }} />
}