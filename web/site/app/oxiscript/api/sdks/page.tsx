import type { Metadata } from "next"
export const metadata: Metadata = { title: "SDKs" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">API · 3 of 3</p>
<h2>SDKs — Go, Python, .NET</h2>
<p>Each official client wraps the wire commands.</p>

<h3>Go</h3>
<pre><code class="lang-go">script := <span class="str">\`proc transfer(from, to, amount) {
    let s = find_one("accounts", {_id: from})
    if s == null { abort "sender not found" }
    if s.balance &lt; amount { abort "insufficient funds" }
    update("accounts", {_id: from}, {$inc: {balance: -amount}})
    update("accounts", {_id: to},   {$inc: {balance:  amount}})
    return {ok: true}
}\`</span>

client.CreateProcedure(<span class="str">"transfer"</span>, script)

resp, err := client.CallProcedure(<span class="str">"transfer"</span>, <span class="kw">map</span>[<span class="ty">string</span>]<span class="kw">any</span>{
    <span class="str">"from"</span>: <span class="str">"alice"</span>, <span class="str">"to"</span>: <span class="str">"bob"</span>, <span class="str">"amount"</span>: <span class="num">1500</span>,
})
<span class="kw">if</span> err != <span class="kw">nil</span> {
    log.Fatal(err) <span class="co">// abort comes back as an error</span>
}

names, _ := client.ListProcedures()
def, _ := client.GetProcedure(<span class="str">"transfer"</span>)
client.DeleteProcedure(<span class="str">"transfer"</span>)</code></pre>

<h3>Python</h3>
<pre><code class="lang-python"><span class="kw">from</span> oxidb <span class="kw">import</span> OxiDbClient

<span class="kw">with</span> OxiDbClient(<span class="str">"127.0.0.1"</span>, <span class="num">4444</span>) <span class="kw">as</span> db:
    db.create_procedure(<span class="str">"award_points"</span>, <span class="str">"""
        proc award_points(email, amount) {
            let user = find_one("users", {email: email})
            if user == null { abort "user not found" }
            update("users", {email: email}, {$inc: {points: amount}})
            return {ok: true, new_total: user.points + amount}
        }
    """</span>)

    r = db.call_procedure(<span class="str">"award_points"</span>, {
        <span class="str">"email"</span>: <span class="str">"alice@example.com"</span>,
        <span class="str">"amount"</span>: <span class="num">100</span>
    })
    <span class="kw">print</span>(r)

    db.list_procedures()
    db.get_procedure(<span class="str">"award_points"</span>)
    db.delete_procedure(<span class="str">"award_points"</span>)</code></pre>

<h3>.NET (TCP client)</h3>
<pre><code class="lang-csharp"><span class="kw">using</span> OxiDb.Client.Tcp;

<span class="kw">var</span> client = <span class="kw">new</span> OxiDbClient(<span class="str">"127.0.0.1"</span>, <span class="num">4444</span>);

<span class="kw">var</span> script = @<span class="str">"
    proc transfer(from, to, amount) {
        let s = find_one(""accounts"", {_id: from})
        if s.balance &lt; amount { abort ""insufficient funds"" }
        update(""accounts"", {_id: from}, {$inc: {balance: -amount}})
        update(""accounts"", {_id: to},   {$inc: {balance:  amount}})
        return {ok: true}
    }"</span>;

<span class="kw">await</span> client.CreateProcedureAsync(<span class="str">"transfer"</span>, script);

<span class="kw">var</span> result = <span class="kw">await</span> client.CallProcedureAsync(<span class="str">"transfer"</span>, <span class="kw">new</span> {
    from = <span class="str">"alice"</span>, to = <span class="str">"bob"</span>, amount = <span class="num">1500</span>
});</code></pre>

<h3>JS / TypeScript (oxidb-js)</h3>
<pre><code class="lang-javascript"><span class="kw">import</span> { OxiDb } <span class="kw">from</span> <span class="str">"oxidb"</span>

<span class="kw">const</span> db = <span class="kw">new</span> OxiDb({rest: <span class="str">"http://localhost:8080"</span>})

<span class="kw">await</span> db.createProcedure(<span class="str">"transfer"</span>, <span class="str">\`
  proc transfer(from, to, amount) {
    let s = find_one("accounts", {_id: from})
    if s.balance &lt; amount { abort "insufficient funds" }
    update("accounts", {_id: from}, {$inc: {balance: -amount}})
    update("accounts", {_id: to},   {$inc: {balance:  amount}})
    return {ok: true}
  }
\`</span>)

<span class="kw">const</span> r = <span class="kw">await</span> db.callProcedure(<span class="str">"transfer"</span>, {
  from: <span class="str">"alice"</span>, to: <span class="str">"bob"</span>, amount: <span class="num">1500</span>
})</code></pre>

<h3>Common pattern: load procs from disk on boot</h3>
<pre><code class="lang-python"><span class="kw">import</span> os, glob

<span class="kw">for</span> path <span class="kw">in</span> glob.glob(<span class="str">"procs/*.oxs"</span>):
    name = os.path.splitext(os.path.basename(path))[<span class="num">0</span>]
    <span class="kw">with</span> open(path) <span class="kw">as</span> f:
        db.create_procedure(name, f.read())</code></pre>

<div class="docs-callout"><strong>Versioning tip:</strong> keep your procedures in source control as <code>.oxs</code> files and re-create them on deploy. There is no migration concept — <code>create_procedure</code> overwrites by name.</div>

<div class="docs-prevnext">
  <a href="/oxiscript/api/rest/" class="prev"><div class="label">Previous</div><div class="title">← REST endpoints</div></a>
  <a href="/oxiscript/" class="next"><div class="label">Back to</div><div class="title">OxiScript overview →</div></a>
</div>` }} />
}
