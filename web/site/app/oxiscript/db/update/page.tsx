import type { Metadata } from "next"
export const metadata: Metadata = { title: "update / update_one" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">Database Ops · 3 of 6</p>
<h2><code>update</code> / <code>update_one</code></h2>
<p>Modify documents in place. <code>update</code> changes every match; <code>update_one</code> stops after the first.</p>

<h3>Signatures</h3>
<pre><code class="lang-rust">update(collection, query, modification)
update_one(collection, query, modification)</code></pre>

<h3><code>$set</code> — overwrite or add fields</h3>
<pre><code class="lang-rust">proc rename(id, name) {
    update(<span class="str">"users"</span>, {_id: id}, {$set: {name: name}})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3><code>$inc</code> — atomic increment / decrement</h3>
<pre><code class="lang-rust">proc credit(account_id, amount) {
    update(<span class="str">"accounts"</span>, {_id: account_id}, {$inc: {balance: amount}})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}

proc debit(account_id, amount) {
    update(<span class="str">"accounts"</span>, {_id: account_id}, {$inc: {balance: -amount}})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3><code>$unset</code> — remove a field</h3>
<pre><code class="lang-rust">proc clear_phone(id) {
    update(<span class="str">"users"</span>, {_id: id}, {$unset: {phone: <span class="str">""</span>}})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3><code>$push</code> — append to an array</h3>
<pre><code class="lang-rust">proc add_tag(id, tag) {
    update(<span class="str">"posts"</span>, {_id: id}, {$push: {tags: tag}})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3><code>$pull</code> — remove array elements matching query</h3>
<pre><code class="lang-rust">proc remove_tag(id, tag) {
    update(<span class="str">"posts"</span>, {_id: id}, {$pull: {tags: tag}})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3><code>$addToSet</code> — push only if not present</h3>
<pre><code class="lang-rust">proc follow(user_id, followee) {
    update(<span class="str">"users"</span>, {_id: user_id}, {$addToSet: {following: followee}})
    update(<span class="str">"users"</span>, {_id: followee}, {$addToSet: {followers: user_id}})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3>Combined operators (recommended)</h3>
<p>Combine all field changes for the same document into a single call. The OCC validator checks document versions, not field versions.</p>
<pre><code class="lang-rust">proc place_order(account_id, sku, qty, price) {
    update(<span class="str">"accounts"</span>, {_id: account_id}, {
        $inc: {balance: -(qty * price), order_count: <span class="num">1</span>, total_spent: qty * price},
        $set: {last_order_at: <span class="str">"now"</span>},
        $push: {recent_orders: sku}
    })
    update(<span class="str">"products"</span>, {sku: sku}, {$inc: {stock: -qty, sold: qty}})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3>Dot-notation for nested fields</h3>
<pre><code class="lang-rust">proc move_user(id, new_city) {
    update(<span class="str">"users"</span>, {_id: id}, {$set: {<span class="str">"address.city"</span>: new_city}})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3><code>update</code> across many docs</h3>
<pre><code class="lang-rust">proc archive_inactive(days_old) {
    update(<span class="str">"users"</span>, {last_seen_days: {$gte: days_old}}, {$set: {archived: <span class="kw">true</span>}})
    <span class="kw">return</span> {archived: count(<span class="str">"users"</span>, {archived: <span class="kw">true</span>})}
}</code></pre>

<h3><code>update_one</code> for "first match wins"</h3>
<pre><code class="lang-rust">proc consume_token(user_id) {
    update_one(<span class="str">"tokens"</span>, {user_id: user_id, used: <span class="kw">false</span>}, {
        $set: {used: <span class="kw">true</span>, used_at: <span class="str">"now"</span>}
    })
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<div class="docs-callout"><strong>OCC rule:</strong> Multiple <code>update</code> calls on the <em>same document</em> in one procedure will conflict on commit. Always merge them into a single <code>update</code> with combined operators.</div>

<div class="docs-prevnext">
  <a href="/oxiscript/db/insert/" class="prev"><div class="label">Previous</div><div class="title">← insert</div></a>
  <a href="/oxiscript/db/delete/" class="next"><div class="label">Next</div><div class="title">delete →</div></a>
</div>` }} />
}
