import type { Metadata } from "next"
export const metadata: Metadata = { title: "Procedure composition" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">Patterns · 3 of 4</p>
<h2>Procedure composition</h2>
<p>Procedures can call other procedures. Pass parameters as a single object literal. The whole chain runs in one transaction.</p>

<h3>Calling another procedure</h3>
<pre><code class="lang-rust">proc get_balance(account_id) {
    let acc = find_one(<span class="str">"accounts"</span>, {_id: account_id})
    <span class="kw">if</span> acc == <span class="kw">null</span> { <span class="kw">abort</span> <span class="str">"not found"</span> }
    <span class="kw">return</span> acc.balance
}

proc safe_withdraw(account_id, amount) {
    let bal = get_balance({account_id: account_id})
    <span class="kw">if</span> bal &lt; amount { <span class="kw">abort</span> <span class="str">"insufficient funds"</span> }
    update(<span class="str">"accounts"</span>, {_id: account_id}, {$inc: {balance: -amount}})
    <span class="kw">return</span> {ok: <span class="kw">true</span>, withdrawn: amount, remaining: bal - amount}
}</code></pre>

<h3>Composing reads</h3>
<pre><code class="lang-rust">proc get_user(id)     { <span class="kw">return</span> find_one(<span class="str">"users"</span>, {_id: id}) }
proc get_orders(uid)  { <span class="kw">return</span> find(<span class="str">"orders"</span>, {user_id: uid}) }
proc get_addresses(uid) { <span class="kw">return</span> find(<span class="str">"addresses"</span>, {user_id: uid}) }

proc user_profile(id) {
    let user      = get_user({id: id})
    let orders    = get_orders({uid: id})
    let addresses = get_addresses({uid: id})
    <span class="kw">return</span> {user: user, orders: orders, addresses: addresses}
}</code></pre>

<h3>Composing writes</h3>
<pre><code class="lang-rust">proc charge(account_id, amount) {
    update(<span class="str">"accounts"</span>, {_id: account_id}, {$inc: {balance: -amount}})
    insert(<span class="str">"ledger"</span>, {account_id: account_id, type: <span class="str">"charge"</span>, amount: amount})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}

proc credit(account_id, amount) {
    update(<span class="str">"accounts"</span>, {_id: account_id}, {$inc: {balance: amount}})
    insert(<span class="str">"ledger"</span>, {account_id: account_id, type: <span class="str">"credit"</span>, amount: amount})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}

proc transfer(from, to, amount) {
    charge({account_id: from, amount: amount})
    credit({account_id: to,   amount: amount})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3>Composing validation</h3>
<pre><code class="lang-rust">proc require_active(user_id) {
    let u = find_one(<span class="str">"users"</span>, {_id: user_id})
    <span class="kw">if</span> u == <span class="kw">null</span>     { <span class="kw">abort</span> <span class="str">"user not found"</span> }
    <span class="kw">if</span> !u.active     { <span class="kw">abort</span> <span class="str">"user not active"</span> }
    <span class="kw">return</span> u
}

proc post_message(user_id, text) {
    require_active({user_id: user_id})
    insert(<span class="str">"messages"</span>, {user_id: user_id, text: text})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}

proc upload_file(user_id, size) {
    require_active({user_id: user_id})
    insert(<span class="str">"files"</span>, {user_id: user_id, size: size})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3>Building a service surface</h3>
<pre><code class="lang-rust">proc account_actions(action, params) {
    <span class="kw">if</span> action == <span class="str">"create"</span>   { <span class="kw">return</span> create_account(params) }
    <span class="kw">if</span> action == <span class="str">"freeze"</span>   { <span class="kw">return</span> freeze_account(params) }
    <span class="kw">if</span> action == <span class="str">"unfreeze"</span> { <span class="kw">return</span> unfreeze_account(params) }
    <span class="kw">if</span> action == <span class="str">"close"</span>    { <span class="kw">return</span> close_account(params) }
    <span class="kw">abort</span> <span class="str">"unknown action"</span>
}</code></pre>

<div class="docs-callout"><strong>Note:</strong> A called procedure is just inlined — its steps run in the same transaction. There's no separate retry, no commit between calls.</div>

<div class="docs-prevnext">
  <a href="/oxiscript/patterns/transactions/" class="prev"><div class="label">Previous</div><div class="title">← Atomic transactions</div></a>
  <a href="/oxiscript/patterns/upsert-soft-delete/" class="next"><div class="label">Next</div><div class="title">Upsert &amp; soft-delete →</div></a>
</div>` }} />
}
