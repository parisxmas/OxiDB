import type { Metadata } from "next"
export const metadata: Metadata = { title: "Audit log recipe" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">Recipes · 4 of 6</p>
<h2>Audit log — actor + action + diff</h2>
<p>A reusable audit pattern: every state-changing procedure also writes an <code>audit_log</code> entry in the same transaction.</p>

<h3>Schema</h3>
<pre><code class="lang-bash">audit_log: {actor, action, collection, doc_id, before, after, ts, ip}</code></pre>

<h3>Wrapper procedure</h3>
<pre><code class="lang-rust">proc audited_update(collection, query, modification, actor, action) {
    let before = find_one(collection, query)
    <span class="kw">if</span> before == <span class="kw">null</span> { <span class="kw">abort</span> <span class="str">"target not found"</span> }
    update(collection, query, modification)
    let after = find_one(collection, query)
    insert(<span class="str">"audit_log"</span>, {
        actor: actor, action: action,
        collection: collection, doc_id: before._id,
        before: before, after: after
    })
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3>Domain procs use it</h3>
<pre><code class="lang-rust">proc change_email(user_id, new_email, actor) {
    audited_update({
        collection: <span class="str">"users"</span>, query: {_id: user_id},
        modification: {$set: {email: new_email}},
        actor: actor, action: <span class="str">"email_change"</span>
    })
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}

proc change_role(user_id, new_role, actor) {
    audited_update({
        collection: <span class="str">"users"</span>, query: {_id: user_id},
        modification: {$set: {role: new_role}},
        actor: actor, action: <span class="str">"role_change"</span>
    })
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3>Audited delete</h3>
<pre><code class="lang-rust">proc audited_delete(collection, id, actor) {
    let original = find_one(collection, {_id: id})
    <span class="kw">if</span> original == <span class="kw">null</span> { <span class="kw">abort</span> <span class="str">"not found"</span> }
    delete_one(collection, {_id: id})
    insert(<span class="str">"audit_log"</span>, {
        actor: actor, action: <span class="str">"delete"</span>,
        collection: collection, doc_id: id, before: original
    })
    <span class="kw">return</span> {ok: <span class="kw">true</span>}
}</code></pre>

<h3>Query the log</h3>
<pre><code class="lang-rust">proc actor_history(actor) {
    <span class="kw">return</span> find(<span class="str">"audit_log"</span>, {actor: actor})
}

proc doc_history(collection, doc_id) {
    <span class="kw">return</span> find(<span class="str">"audit_log"</span>, {collection: collection, doc_id: doc_id})
}

proc recent_actions(action, n) {
    <span class="kw">return</span> aggregate(<span class="str">"audit_log"</span>, [
        {$match: {action: action}},
        {$sort: {ts: -<span class="num">1</span>}},
        {$limit: n}
    ])
}</code></pre>

<h3>Compliance summary</h3>
<pre><code class="lang-rust">proc compliance_summary(year, month) {
    <span class="kw">return</span> aggregate(<span class="str">"audit_log"</span>, [
        {$match: {year: year, month: month}},
        {$group: {_id: <span class="str">"$action"</span>, n: {$sum: <span class="num">1</span>}, actors: {$addToSet: <span class="str">"$actor"</span>}}},
        {$sort: {n: -<span class="num">1</span>}}
    ])
}</code></pre>

<div class="docs-callout"><strong>Why this works:</strong> the audit insert is in the same transaction as the change. There is no path where the data changes but the audit entry is missing.</div>

<div class="docs-prevnext">
  <a href="/oxiscript/recipes/inventory/" class="prev"><div class="label">Previous</div><div class="title">← Inventory</div></a>
  <a href="/oxiscript/recipes/rate-limiting/" class="next"><div class="label">Next</div><div class="title">Rate limiting →</div></a>
</div>` }} />
}
