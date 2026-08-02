import type { Metadata } from "next"
export const metadata: Metadata = { title: "if / else" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">Syntax · 4 of 6</p>
<h2><code>if</code> / <code>else</code></h2>
<p>The only branching construct. There is no <code>switch</code> — chain <code>if</code>/<code>else if</code>.</p>

<h3>Plain <code>if</code></h3>
<pre><code class="lang-rust">proc check(x) {
    <span class="kw">if</span> x &gt; <span class="num">100</span> {
        <span class="kw">return</span> <span class="str">"big"</span>
    }
    <span class="kw">return</span> <span class="str">"small"</span>
}</code></pre>

<h3><code>if</code> / <code>else</code></h3>
<pre><code class="lang-rust">proc check(x) {
    <span class="kw">if</span> x &gt; <span class="num">100</span> {
        <span class="kw">return</span> <span class="str">"big"</span>
    } <span class="kw">else</span> {
        <span class="kw">return</span> <span class="str">"small"</span>
    }
}</code></pre>

<h3><code>if</code> / <code>else if</code> / <code>else</code></h3>
<pre><code class="lang-rust">proc tier(spend) {
    <span class="kw">if</span> spend &gt;= <span class="num">10000</span> {
        <span class="kw">return</span> <span class="str">"platinum"</span>
    } <span class="kw">else</span> <span class="kw">if</span> spend &gt;= <span class="num">1000</span> {
        <span class="kw">return</span> <span class="str">"gold"</span>
    } <span class="kw">else</span> <span class="kw">if</span> spend &gt;= <span class="num">100</span> {
        <span class="kw">return</span> <span class="str">"silver"</span>
    } <span class="kw">else</span> {
        <span class="kw">return</span> <span class="str">"bronze"</span>
    }
}</code></pre>

<h3>Guard-clause style (recommended)</h3>
<p>Validate inputs at the top, fail fast, then proceed with the happy path.</p>
<pre><code class="lang-rust">proc transfer(from, to, amount) {
    <span class="kw">if</span> amount &lt;= <span class="num">0</span>     { <span class="kw">abort</span> <span class="str">"amount must be positive"</span> }
    <span class="kw">if</span> from == to       { <span class="kw">abort</span> <span class="str">"cannot transfer to self"</span> }

    let sender = find_one(<span class="str">"accounts"</span>, {account_id: from})
    <span class="kw">if</span> sender == <span class="kw">null</span>          { <span class="kw">abort</span> <span class="str">"sender not found"</span> }
    <span class="kw">if</span> sender.balance &lt; amount { <span class="kw">abort</span> <span class="str">"insufficient funds"</span> }

    <span class="co">// happy path here</span>
    update(<span class="str">"accounts"</span>, {account_id: from}, {$inc: {balance: -amount}})
    update(<span class="str">"accounts"</span>, {account_id: to},   {$inc: {balance:  amount}})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3>Nested <code>if</code></h3>
<pre><code class="lang-rust">proc classify(user) {
    <span class="kw">if</span> user.country == <span class="str">"TR"</span> {
        <span class="kw">if</span> user.age &gt;= <span class="num">18</span> {
            <span class="kw">return</span> <span class="str">"TR-adult"</span>
        }
        <span class="kw">return</span> <span class="str">"TR-minor"</span>
    }
    <span class="kw">return</span> <span class="str">"non-TR"</span>
}</code></pre>

<h3>Combined conditions</h3>
<pre><code class="lang-rust">proc can_view(user, post) {
    <span class="kw">if</span> post.is_public || user._id == post.author_id {
        <span class="kw">return</span> <span class="kw">true</span>
    }
    <span class="kw">return</span> <span class="kw">false</span>
}</code></pre>

<h3>Branching on database state</h3>
<pre><code class="lang-rust">proc upsert(email, name) {
    let existing = find_one(<span class="str">"users"</span>, {email: email})
    <span class="kw">if</span> existing == <span class="kw">null</span> {
        insert(<span class="str">"users"</span>, {email: email, name: name, signups: <span class="num">1</span>})
        <span class="kw">return</span> {created: <span class="kw">true</span>}
    } <span class="kw">else</span> {
        update(<span class="str">"users"</span>, {email: email}, {$inc: {signups: <span class="num">1</span>}})
        <span class="kw">return</span> {created: <span class="kw">false</span>, signups: existing.signups + <span class="num">1</span>}
    }
}</code></pre>

<h3>Returning from inside an <code>if</code></h3>
<p>You can <code>return</code> or <code>abort</code> from any branch — execution stops immediately.</p>
<pre><code class="lang-rust">proc handle_role(user) {
    <span class="kw">if</span> user.role == <span class="str">"admin"</span>  { <span class="kw">return</span> {can_edit: <span class="kw">true</span>,  can_delete: <span class="kw">true</span>} }
    <span class="kw">if</span> user.role == <span class="str">"editor"</span> { <span class="kw">return</span> {can_edit: <span class="kw">true</span>,  can_delete: <span class="kw">false</span>} }
    <span class="kw">if</span> user.role == <span class="str">"viewer"</span> { <span class="kw">return</span> {can_edit: <span class="kw">false</span>, can_delete: <span class="kw">false</span>} }
    <span class="kw">abort</span> <span class="str">"unknown role"</span>
}</code></pre>

<h3>Conditional updates</h3>
<pre><code class="lang-rust">proc adjust_inventory(sku, delta) {
    let p = find_one(<span class="str">"products"</span>, {sku: sku})
    <span class="kw">if</span> p == <span class="kw">null</span> { <span class="kw">abort</span> <span class="str">"product not found"</span> }

    <span class="kw">if</span> delta &gt; <span class="num">0</span> {
        update(<span class="str">"products"</span>, {sku: sku}, {$inc: {stock: delta, restocks: <span class="num">1</span>}})
    } <span class="kw">else</span> {
        <span class="kw">if</span> p.stock + delta &lt; <span class="num">0</span> { <span class="kw">abort</span> <span class="str">"would go negative"</span> }
        update(<span class="str">"products"</span>, {sku: sku}, {$inc: {stock: delta}})
    }
    <span class="kw">return</span> {sku: sku, new_stock: p.stock + delta}
}</code></pre>

<div class="docs-prevnext">
  <a href="/oxiscript/syntax/operators/" class="prev"><div class="label">Previous</div><div class="title">← Operators</div></a>
  <a href="/oxiscript/syntax/loops/" class="next"><div class="label">Next</div><div class="title">for / in loops →</div></a>
</div>` }} />
}
