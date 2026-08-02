import type { Metadata } from "next"
export const metadata: Metadata = { title: "Atomic transactions" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">Patterns · 2 of 4</p>
<h2>Atomic transactions</h2>
<p>Every procedure call is one OCC transaction. All steps either commit together or roll back together. Two rules to remember.</p>

<h3>Rule 1: <code>abort</code> rolls back everything</h3>
<pre><code class="lang-rust">proc atomic_demo(account_id, amount) {
    update(<span class="str">"accounts"</span>, {_id: account_id}, {$inc: {balance: -amount}})
    insert(<span class="str">"audit"</span>, {action: <span class="str">"debit"</span>, amount: amount})
    <span class="kw">if</span> amount &gt; <span class="num">10000</span> {
        <span class="kw">abort</span> <span class="str">"amount over daily limit"</span>
    }
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>
<p>If the abort fires, the <code>update</code> AND the <code>insert</code> are both rolled back.</p>

<h3>Rule 2: merge per-document updates</h3>
<p>OCC validates a version per document, not per field. Two updates on the same document inside one proc will conflict.</p>

<h4>WRONG — will conflict</h4>
<pre><code class="lang-rust">update(<span class="str">"accounts"</span>, {_id: id}, {$inc: {balance: -amount}})
update(<span class="str">"accounts"</span>, {_id: id}, {$inc: {tx_count: <span class="num">1</span>}})
update(<span class="str">"accounts"</span>, {_id: id}, {$set: {last_tx: <span class="str">"now"</span>}})</code></pre>

<h4>RIGHT — single update with combined operators</h4>
<pre><code class="lang-rust">update(<span class="str">"accounts"</span>, {_id: id}, {
    $inc: {balance: -amount, tx_count: <span class="num">1</span>},
    $set: {last_tx: <span class="str">"now"</span>}
})</code></pre>

<h3>Atomic transfer</h3>
<pre><code class="lang-rust">proc transfer(from, to, amount) {
    let sender = find_one(<span class="str">"accounts"</span>, {_id: from})
    <span class="kw">if</span> sender == <span class="kw">null</span>           { <span class="kw">abort</span> <span class="str">"sender not found"</span> }
    <span class="kw">if</span> sender.balance &lt; amount { <span class="kw">abort</span> <span class="str">"insufficient funds"</span> }

    update(<span class="str">"accounts"</span>, {_id: from}, {$inc: {balance: -amount, tx_count: <span class="num">1</span>}})
    update(<span class="str">"accounts"</span>, {_id: to},   {$inc: {balance:  amount, tx_count: <span class="num">1</span>}})
    insert(<span class="str">"ledger"</span>, {from: from, to: to, amount: amount})

    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>
<p>Three writes across two collections, one transaction. Either all happen or none.</p>

<h3>Reads inside a transaction</h3>
<p>Reads inside a procedure see a consistent snapshot. The version captured at <code>find_one</code> is what OCC validates against on commit.</p>
<pre><code class="lang-rust">proc reserve_seat(event_id, user_id) {
    let event = find_one(<span class="str">"events"</span>, {_id: event_id})
    <span class="kw">if</span> event.seats_left &lt;= <span class="num">0</span> { <span class="kw">abort</span> <span class="str">"sold out"</span> }
    update(<span class="str">"events"</span>, {_id: event_id}, {$inc: {seats_left: -<span class="num">1</span>}})
    insert(<span class="str">"reservations"</span>, {event_id: event_id, user_id: user_id})
    <span class="kw">return</span> {ok: <span class="kw">true</span>, seats_left: event.seats_left - <span class="num">1</span>}
}</code></pre>
<p>If two clients hit this at the same time, only one wins — the other gets an OCC retry-able error.</p>

<h3>Multi-document atomic update</h3>
<p>Different documents can be touched freely; only same-doc multiple updates are the problem.</p>
<pre><code class="lang-rust">proc batch_credit(account_ids, amount) {
    <span class="kw">for</span> id <span class="kw">in</span> account_ids {
        update(<span class="str">"accounts"</span>, {_id: id}, {$inc: {balance: amount}})
    }
    <span class="kw">return</span> {ok: <span class="kw">true</span>, credited: amount}
}</code></pre>

<h3>When to NOT use OxiScript</h3>
<ul>
  <li>Long-running batch jobs that touch millions of docs — break into smaller calls or use the pipeline.</li>
  <li>Operations that need to commit partial results — OxiScript is all-or-nothing.</li>
  <li>Workflows that wait on external systems mid-procedure — don't pause inside a transaction.</li>
</ul>

<div class="docs-prevnext">
  <a href="/oxiscript/patterns/validation/" class="prev"><div class="label">Previous</div><div class="title">← Input validation</div></a>
  <a href="/oxiscript/patterns/composition/" class="next"><div class="label">Next</div><div class="title">Procedure composition →</div></a>
</div>` }} />
}
