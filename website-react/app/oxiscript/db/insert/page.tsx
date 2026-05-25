import type { Metadata } from "next"
export const metadata: Metadata = { title: "insert" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">Database Ops · 2 of 6</p>
<h2><code>insert</code></h2>
<p>Append a document to a collection. Returns the inserted id.</p>

<h3>Signature</h3>
<pre><code class="lang-rust">insert(collection, document)</code></pre>

<h3>Simple insert</h3>
<pre><code class="lang-rust">proc add_log(level, msg) {
    insert(<span class="str">"logs"</span>, {level: level, msg: msg, ts: <span class="str">"now"</span>})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3>Capture the inserted id</h3>
<pre><code class="lang-rust">proc create_post(author_id, title, body) {
    let id = insert(<span class="str">"posts"</span>, {
        author_id: author_id, title: title, body: body, likes: <span class="num">0</span>
    })
    <span class="kw">return</span> {created: <span class="kw">true</span>, post_id: id}
}</code></pre>

<h3>Insert with computed fields</h3>
<pre><code class="lang-rust">proc charge(account_id, amount) {
    let acc = find_one(<span class="str">"accounts"</span>, {_id: account_id})
    insert(<span class="str">"ledger"</span>, {
        account_id: account_id,
        amount: amount,
        balance_before: acc.balance,
        balance_after: acc.balance - amount,
        type: <span class="str">"charge"</span>
    })
    update(<span class="str">"accounts"</span>, {_id: account_id}, {$inc: {balance: -amount}})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3>Insert into multiple collections</h3>
<pre><code class="lang-rust">proc signup(email, name) {
    insert(<span class="str">"users"</span>, {email: email, name: name})
    insert(<span class="str">"audit_log"</span>, {action: <span class="str">"signup"</span>, target: email})
    insert(<span class="str">"notifications"</span>, {to: email, kind: <span class="str">"welcome"</span>, status: <span class="str">"queued"</span>})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3>Insert in a loop (batch)</h3>
<pre><code class="lang-rust">proc bulk_seed(items) {
    <span class="kw">for</span> i <span class="kw">in</span> items {
        insert(<span class="str">"products"</span>, {sku: i.sku, name: i.name, price: i.price, stock: i.stock})
    }
    <span class="kw">return</span> {inserted: count(<span class="str">"products"</span>)}
}</code></pre>
<p>Pass an array as the <code>items</code> parameter:</p>
<pre><code class="lang-bash">{<span class="str">"cmd"</span>: <span class="str">"call_procedure"</span>, <span class="str">"name"</span>: <span class="str">"bulk_seed"</span>,
 <span class="str">"params"</span>: {<span class="str">"items"</span>: [
    {<span class="str">"sku"</span>: <span class="str">"A"</span>, <span class="str">"name"</span>: <span class="str">"Apple"</span>, <span class="str">"price"</span>: <span class="num">1</span>, <span class="str">"stock"</span>: <span class="num">100</span>},
    {<span class="str">"sku"</span>: <span class="str">"B"</span>, <span class="str">"name"</span>: <span class="str">"Banana"</span>, <span class="str">"price"</span>: <span class="num">2</span>, <span class="str">"stock"</span>: <span class="num">50</span>}
]}}</code></pre>

<h3>Conditional insert (idempotent create)</h3>
<pre><code class="lang-rust">proc ensure_user(email) {
    let existing = find_one(<span class="str">"users"</span>, {email: email})
    <span class="kw">if</span> existing != <span class="kw">null</span> {
        <span class="kw">return</span> {created: <span class="kw">false</span>, user: existing}
    }
    let id = insert(<span class="str">"users"</span>, {email: email, signups: <span class="num">1</span>})
    <span class="kw">return</span> {created: <span class="kw">true</span>, user_id: id}
}</code></pre>

<h3>Insert with nested document</h3>
<pre><code class="lang-rust">proc record_event(user_id, kind, payload) {
    insert(<span class="str">"events"</span>, {
        user_id: user_id,
        kind: kind,
        payload: payload,
        meta: {source: <span class="str">"oxiscript"</span>, version: <span class="str">"1"</span>}
    })
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<div class="docs-callout"><strong>Tip:</strong> If you insert the same document many times, create a unique index on the dedupe field — let the engine reject duplicates instead of doing a <code>find_one</code>+<code>insert</code> dance.</div>

<div class="docs-prevnext">
  <a href="/oxiscript/db/find/" class="prev"><div class="label">Previous</div><div class="title">← find</div></a>
  <a href="/oxiscript/db/update/" class="next"><div class="label">Next</div><div class="title">update →</div></a>
</div>` }} />
}
