import type { Metadata } from "next"
export const metadata: Metadata = { title: "Upsert & soft-delete" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">Patterns · 4 of 4</p>
<h2>Upsert &amp; soft-delete</h2>
<p>Two idempotent patterns you'll reach for again and again.</p>

<h3>Upsert by key</h3>
<pre><code class="lang-rust">proc upsert_user(email, name) {
    let existing = find_one(<span class="str">"users"</span>, {email: email})
    <span class="kw">if</span> existing == <span class="kw">null</span> {
        insert(<span class="str">"users"</span>, {email: email, name: name, signups: <span class="num">1</span>})
        <span class="kw">return</span> {created: <span class="kw">true</span>, email: email}
    }
    update(<span class="str">"users"</span>, {email: email}, {
        $set: {name: name},
        $inc: {signups: <span class="num">1</span>}
    })
    <span class="kw">return</span> {created: <span class="kw">false</span>, signups: existing.signups + <span class="num">1</span>}
}</code></pre>

<h3>Upsert with merge</h3>
<pre><code class="lang-rust">proc upsert_setting(user_id, key, value) {
    let s = find_one(<span class="str">"settings"</span>, {user_id: user_id})
    <span class="kw">if</span> s == <span class="kw">null</span> {
        insert(<span class="str">"settings"</span>, {user_id: user_id, prefs: {key: value}})
    } <span class="kw">else</span> {
        update(<span class="str">"settings"</span>, {user_id: user_id}, {$set: {<span class="str">"prefs.key"</span>: value}})
    }
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3>Counter upsert</h3>
<pre><code class="lang-rust">proc bump_counter(name) {
    let c = find_one(<span class="str">"counters"</span>, {name: name})
    <span class="kw">if</span> c == <span class="kw">null</span> {
        insert(<span class="str">"counters"</span>, {name: name, value: <span class="num">1</span>})
        <span class="kw">return</span> {value: <span class="num">1</span>}
    }
    update(<span class="str">"counters"</span>, {name: name}, {$inc: {value: <span class="num">1</span>}})
    <span class="kw">return</span> {value: c.value + <span class="num">1</span>}
}</code></pre>

<h3>Soft-delete</h3>
<pre><code class="lang-rust">proc soft_delete(collection, id, actor) {
    let doc = find_one(collection, {_id: id})
    <span class="kw">if</span> doc == <span class="kw">null</span> { <span class="kw">abort</span> <span class="str">"not found"</span> }
    update(collection, {_id: id}, {
        $set: {deleted: <span class="kw">true</span>, deleted_at: <span class="str">"now"</span>, deleted_by: actor}
    })
    insert(<span class="str">"audit_log"</span>, {
        action: <span class="str">"delete"</span>, collection: collection,
        doc_id: id, actor: actor, original: doc
    })
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3>Soft-undelete</h3>
<pre><code class="lang-rust">proc undelete(collection, id, actor) {
    update(collection, {_id: id, deleted: <span class="kw">true</span>}, {
        $unset: {deleted: <span class="str">""</span>, deleted_at: <span class="str">""</span>, deleted_by: <span class="str">""</span>}
    })
    insert(<span class="str">"audit_log"</span>, {action: <span class="str">"undelete"</span>, doc_id: id, actor: actor})
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3>Filter out soft-deletes in reads</h3>
<pre><code class="lang-rust">proc list_active_posts() {
    <span class="kw">return</span> find(<span class="str">"posts"</span>, {deleted: {$exists: <span class="kw">false</span>}})
}</code></pre>
<p>Or just <code>{deleted: {$ne: true}}</code> if you sometimes set <code>deleted: false</code>.</p>

<h3>Hard-purge after grace period</h3>
<pre><code class="lang-rust">proc purge_old_soft_deletes(days) {
    let candidates = find(<span class="str">"posts"</span>, {deleted: <span class="kw">true</span>, deleted_age_days: {$gte: days}})
    <span class="kw">for</span> p <span class="kw">in</span> candidates {
        delete_one(<span class="str">"posts"</span>, {_id: p._id})
        insert(<span class="str">"audit_log"</span>, {action: <span class="str">"purge"</span>, doc_id: p._id})
    }
    <span class="kw">return</span> {purged: count(<span class="str">"posts"</span>, {deleted: <span class="kw">true</span>})}
}</code></pre>

<h3>Idempotent insert with dedupe key</h3>
<pre><code class="lang-rust">proc record_payment(idempotency_key, account_id, amount) {
    <span class="kw">if</span> count(<span class="str">"payments"</span>, {idempotency_key: idempotency_key}) &gt; <span class="num">0</span> {
        <span class="kw">return</span> {duplicate: <span class="kw">true</span>}
    }
    insert(<span class="str">"payments"</span>, {
        idempotency_key: idempotency_key,
        account_id: account_id, amount: amount, status: <span class="str">"ok"</span>
    })
    update(<span class="str">"accounts"</span>, {_id: account_id}, {$inc: {balance: amount}})
    <span class="kw">return</span> {duplicate: <span class="kw">false</span>}
}</code></pre>

<div class="docs-prevnext">
  <a href="/oxiscript/patterns/composition/" class="prev"><div class="label">Previous</div><div class="title">← Composition</div></a>
  <a href="/oxiscript/recipes/banking/" class="next"><div class="label">Next</div><div class="title">Banking →</div></a>
</div>` }} />
}
