import type { Metadata } from "next"
export const metadata: Metadata = { title: "aggregate" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">Database Ops · 6 of 6</p>
<h2><code>aggregate</code></h2>
<p>Run a full aggregation pipeline inside a procedure. Returns an array of documents.</p>

<h3>Signature</h3>
<pre><code class="lang-rust">aggregate(collection, [stage1, stage2, ...])</code></pre>

<h3>Sum</h3>
<pre><code class="lang-rust">proc total_sales() {
    <span class="kw">return</span> aggregate(<span class="str">"orders"</span>, [
        {$group: {_id: <span class="kw">null</span>, total: {$sum: <span class="str">"$amount"</span>}}}
    ])
}</code></pre>

<h3>Group by category</h3>
<pre><code class="lang-rust">proc sales_by_category() {
    <span class="kw">return</span> aggregate(<span class="str">"orders"</span>, [
        {$group: {_id: <span class="str">"$category"</span>, total: {$sum: <span class="str">"$amount"</span>}, n: {$sum: <span class="num">1</span>}}},
        {$sort: {total: -<span class="num">1</span>}}
    ])
}</code></pre>

<h3>Filter then group</h3>
<pre><code class="lang-rust">proc monthly_revenue(year, month) {
    <span class="kw">return</span> aggregate(<span class="str">"orders"</span>, [
        {$match: {year: year, month: month, status: <span class="str">"paid"</span>}},
        {$group: {_id: <span class="str">"$category"</span>, total: {$sum: <span class="str">"$amount"</span>}}},
        {$sort: {total: -<span class="num">1</span>}}
    ])
}</code></pre>

<h3>Top-N</h3>
<pre><code class="lang-rust">proc top_customers(n) {
    <span class="kw">return</span> aggregate(<span class="str">"orders"</span>, [
        {$group: {_id: <span class="str">"$customer_id"</span>, spend: {$sum: <span class="str">"$amount"</span>}, orders: {$sum: <span class="num">1</span>}}},
        {$sort: {spend: -<span class="num">1</span>}},
        {$limit: n}
    ])
}</code></pre>

<h3>Avg / min / max</h3>
<pre><code class="lang-rust">proc product_stats(sku) {
    <span class="kw">return</span> aggregate(<span class="str">"reviews"</span>, [
        {$match: {sku: sku}},
        {$group: {
            _id: <span class="str">"$sku"</span>,
            avg_rating: {$avg: <span class="str">"$rating"</span>},
            min_rating: {$min: <span class="str">"$rating"</span>},
            max_rating: {$max: <span class="str">"$rating"</span>},
            count: {$sum: <span class="num">1</span>}
        }}
    ])
}</code></pre>

<h3>$lookup (join)</h3>
<pre><code class="lang-rust">proc orders_with_customer() {
    <span class="kw">return</span> aggregate(<span class="str">"orders"</span>, [
        {$lookup: {
            from: <span class="str">"customers"</span>, localField: <span class="str">"customer_id"</span>,
            foreignField: <span class="str">"_id"</span>, as: <span class="str">"customer"</span>
        }},
        {$limit: <span class="num">100</span>}
    ])
}</code></pre>

<h3>$unwind + $group</h3>
<pre><code class="lang-rust">proc tag_popularity() {
    <span class="kw">return</span> aggregate(<span class="str">"posts"</span>, [
        {$unwind: <span class="str">"$tags"</span>},
        {$group: {_id: <span class="str">"$tags"</span>, n: {$sum: <span class="num">1</span>}}},
        {$sort: {n: -<span class="num">1</span>}},
        {$limit: <span class="num">20</span>}
    ])
}</code></pre>

<h3>Use the result</h3>
<pre><code class="lang-rust">proc check_quota(user_id, monthly_limit) {
    let result = aggregate(<span class="str">"messages"</span>, [
        {$match: {user_id: user_id, this_month: <span class="kw">true</span>}},
        {$group: {_id: <span class="kw">null</span>, total: {$sum: <span class="num">1</span>}}}
    ])
    let used = <span class="num">0</span>
    <span class="kw">if</span> result[<span class="num">0</span>] != <span class="kw">null</span> {
        used = result[<span class="num">0</span>].total
    }
    <span class="kw">return</span> {used: used, limit: monthly_limit, ok: used &lt; monthly_limit}
}</code></pre>

<p>For the full stage and operator reference, see <a href="/aggregation/">Aggregation Pipeline</a>.</p>

<div class="docs-prevnext">
  <a href="/oxiscript/db/count/" class="prev"><div class="label">Previous</div><div class="title">← count</div></a>
  <a href="/oxiscript/patterns/validation/" class="next"><div class="label">Next</div><div class="title">Input validation →</div></a>
</div>` }} />
}
