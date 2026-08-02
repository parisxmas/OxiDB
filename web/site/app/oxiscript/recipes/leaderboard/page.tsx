import type { Metadata } from "next"
export const metadata: Metadata = { title: "Leaderboard recipe" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">Recipes · 6 of 6</p>
<h2>Leaderboards — top-N with score updates</h2>

<h3>Schema</h3>
<pre><code class="lang-bash">scores: {user_id, name, score, rank, updated_at}
        # index on score (desc) for top-N queries</code></pre>

<h3>award_points</h3>
<pre><code class="lang-rust">proc award_points(user_id, delta) {
    let s = find_one(<span class="str">"scores"</span>, {user_id: user_id})
    <span class="kw">if</span> s == <span class="kw">null</span> {
        insert(<span class="str">"scores"</span>, {user_id: user_id, score: delta})
        <span class="kw">return</span> {created: <span class="kw">true</span>, score: delta}
    }
    update(<span class="str">"scores"</span>, {user_id: user_id}, {$inc: {score: delta}})
    <span class="kw">return</span> {created: <span class="kw">false</span>, score: s.score + delta}
}</code></pre>

<h3>top_n</h3>
<pre><code class="lang-rust">proc top_n(n) {
    <span class="kw">return</span> aggregate(<span class="str">"scores"</span>, [
        {$sort: {score: -<span class="num">1</span>}},
        {$limit: n}
    ])
}</code></pre>

<h3>my_rank</h3>
<pre><code class="lang-rust">proc my_rank(user_id) {
    let me = find_one(<span class="str">"scores"</span>, {user_id: user_id})
    <span class="kw">if</span> me == <span class="kw">null</span> { <span class="kw">return</span> {rank: <span class="kw">null</span>} }
    let above = count(<span class="str">"scores"</span>, {score: {$gt: me.score}})
    <span class="kw">return</span> {user_id: user_id, score: me.score, rank: above + <span class="num">1</span>}
}</code></pre>

<h3>around_me</h3>
<pre><code class="lang-rust">proc around_me(user_id, window) {
    let me = find_one(<span class="str">"scores"</span>, {user_id: user_id})
    <span class="kw">if</span> me == <span class="kw">null</span> { <span class="kw">abort</span> <span class="str">"no score"</span> }
    <span class="kw">return</span> aggregate(<span class="str">"scores"</span>, [
        {$match: {score: {$gte: me.score - window, $lte: me.score + window}}},
        {$sort: {score: -<span class="num">1</span>}},
        {$limit: <span class="num">25</span>}
    ])
}</code></pre>

<h3>weekly_reset</h3>
<pre><code class="lang-rust">proc weekly_reset() {
    let snapshot = aggregate(<span class="str">"scores"</span>, [
        {$sort: {score: -<span class="num">1</span>}},
        {$limit: <span class="num">100</span>}
    ])
    <span class="kw">for</span> entry <span class="kw">in</span> snapshot {
        insert(<span class="str">"weekly_history"</span>, {
            user_id: entry.user_id, score: entry.score, week: <span class="str">"current"</span>
        })
    }
    update(<span class="str">"scores"</span>, {}, {$set: {score: <span class="num">0</span>}})
    <span class="kw">return</span> {ok: <span class="kw">true</span>, archived: <span class="num">100</span>}
}</code></pre>

<h3>award_with_event</h3>
<pre><code class="lang-rust">proc award_with_event(user_id, points, source) {
    let r = award_points({user_id: user_id, delta: points})
    insert(<span class="str">"score_events"</span>, {
        user_id: user_id, points: points, source: source, total_after: r.score
    })
    <span class="kw">return</span> r
}</code></pre>

<h3>category_top</h3>
<pre><code class="lang-rust">proc category_top(category, n) {
    <span class="kw">return</span> aggregate(<span class="str">"scores"</span>, [
        {$match: {category: category}},
        {$sort: {score: -<span class="num">1</span>}},
        {$limit: n}
    ])
}</code></pre>

<div class="docs-prevnext">
  <a href="/oxiscript/recipes/rate-limiting/" class="prev"><div class="label">Previous</div><div class="title">← Rate limiting</div></a>
  <a href="/oxiscript/api/tcp/" class="next"><div class="label">Next</div><div class="title">TCP / OxiWire →</div></a>
</div>` }} />
}
