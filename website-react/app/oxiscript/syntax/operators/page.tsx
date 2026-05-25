import type { Metadata } from "next"
export const metadata: Metadata = { title: "Operators" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">Syntax · 3 of 6</p>
<h2>Operators</h2>
<p>OxiScript has the operators you'd expect from a JavaScript-shaped language. They behave on JSON values.</p>

<h3>Arithmetic</h3>
<div class="table-wrap"><table>
<thead><tr><th>Op</th><th>Use</th><th>Example</th></tr></thead>
<tbody>
<tr><td><code>+</code></td><td>add</td><td><code>a + b</code></td></tr>
<tr><td><code>-</code></td><td>subtract / unary negate</td><td><code>a - b</code>, <code>-x</code></td></tr>
<tr><td><code>*</code></td><td>multiply</td><td><code>price * qty</code></td></tr>
<tr><td><code>/</code></td><td>divide</td><td><code>total / count</code></td></tr>
<tr><td><code>%</code></td><td>modulo</td><td><code>i % 2</code></td></tr>
</tbody></table></div>

<pre><code class="lang-rust">proc invoice(qty, unit_price, tax_rate) {
    let subtotal = qty * unit_price
    let tax      = subtotal * tax_rate
    let total    = subtotal + tax
    <span class="kw">return</span> {subtotal: subtotal, tax: tax, total: total}
}</code></pre>

<h3>Comparison</h3>
<div class="table-wrap"><table>
<thead><tr><th>Op</th><th>Use</th></tr></thead>
<tbody>
<tr><td><code>==</code></td><td>equal</td></tr>
<tr><td><code>!=</code></td><td>not equal</td></tr>
<tr><td><code>&lt;</code> <code>&gt;</code> <code>&lt;=</code> <code>&gt;=</code></td><td>numeric &amp; string ordering</td></tr>
</tbody></table></div>

<pre><code class="lang-rust">proc tier(spend) {
    <span class="kw">if</span> spend &gt;= <span class="num">10000</span> { <span class="kw">return</span> <span class="str">"platinum"</span> }
    <span class="kw">if</span> spend &gt;= <span class="num">1000</span>  { <span class="kw">return</span> <span class="str">"gold"</span> }
    <span class="kw">if</span> spend &gt;= <span class="num">100</span>   { <span class="kw">return</span> <span class="str">"silver"</span> }
    <span class="kw">return</span> <span class="str">"bronze"</span>
}</code></pre>

<h3>Logical</h3>
<div class="table-wrap"><table>
<thead><tr><th>Op</th><th>Use</th></tr></thead>
<tbody>
<tr><td><code>&amp;&amp;</code></td><td>and (short-circuits)</td></tr>
<tr><td><code>||</code></td><td>or (short-circuits)</td></tr>
<tr><td><code>!</code></td><td>not</td></tr>
</tbody></table></div>

<pre><code class="lang-rust">proc can_purchase(user, item) {
    <span class="kw">if</span> user.is_adult &amp;&amp; user.balance &gt;= item.price {
        <span class="kw">return</span> {ok: <span class="kw">true</span>}
    }
    <span class="kw">return</span> {ok: <span class="kw">false</span>}
}</code></pre>

<pre><code class="lang-rust">proc validate(input) {
    <span class="kw">if</span> input == <span class="kw">null</span> || input.email == <span class="kw">null</span> {
        <span class="kw">abort</span> <span class="str">"email required"</span>
    }
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3>Field access</h3>
<pre><code class="lang-rust">proc deep(doc) {
    let city = doc.user.address.city
    let first_tag = doc.tags[<span class="num">0</span>]
    let by_key = doc[<span class="str">"first name"</span>]
    <span class="kw">return</span> {city: city, first_tag: first_tag, by_key: by_key}
}</code></pre>

<h3>Operator precedence (high → low)</h3>
<ol>
  <li>Field access (<code>.</code>), index (<code>[]</code>), call (<code>f(...)</code>), unary (<code>-</code>, <code>!</code>)</li>
  <li><code>*</code>, <code>/</code>, <code>%</code></li>
  <li><code>+</code>, <code>-</code></li>
  <li><code>&lt;</code>, <code>&gt;</code>, <code>&lt;=</code>, <code>&gt;=</code></li>
  <li><code>==</code>, <code>!=</code></li>
  <li><code>&amp;&amp;</code></li>
  <li><code>||</code></li>
</ol>
<p>Use parens to make intent explicit:</p>
<pre><code class="lang-rust">let ok = (a + b) * c &gt; threshold &amp;&amp; user.active</code></pre>

<h3>Combining everything</h3>
<pre><code class="lang-rust">proc score(user, opts) {
    let base = user.purchases * <span class="num">10</span>
    let bonus = <span class="num">0</span>
    <span class="kw">if</span> user.is_premium {
        bonus = <span class="num">500</span>
    }
    let final = base + bonus - opts.penalty
    <span class="kw">if</span> final &lt; <span class="num">0</span> {
        final = <span class="num">0</span>
    }
    <span class="kw">return</span> final
}</code></pre>
<div class="docs-callout"><strong>Note:</strong> reassignment (<code>x = expr</code> without <code>let</code>) parses, but it's the <em>same statement form</em> as <code>let x</code> — variables shadow the previous binding. Practically: just always write <code>let</code>.</div>

<div class="docs-prevnext">
  <a href="/oxiscript/syntax/variables/" class="prev"><div class="label">Previous</div><div class="title">← Variables</div></a>
  <a href="/oxiscript/syntax/control-flow/" class="next"><div class="label">Next</div><div class="title">if / else →</div></a>
</div>` }} />
}
