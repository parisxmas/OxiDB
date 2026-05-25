import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "OxiScript",
  description: `OxiScript — purpose-built scripting language for OxiDB stored procedures. Multi-step ACID logic with let, if, for, abort, return; calls find/insert/update/delete/count/aggregate; procedures can call other procedures.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">Stored Procedures</p>
<h2>OxiScript</h2>
<p>A purpose-built scripting language for OxiDB stored procedures — full lexer, recursive-descent parser, and compiler that lower into OxiDB's JSON step format. Procedures run server-side inside one ACID boundary; <code>abort</code> rolls everything back.</p>

<div class="docs-callout"><strong>New here?</strong> Start with the <a href="/oxiscript/tutorial/">tutorial</a> — install, hello-world, syntax, database operations, real-world recipes, and API reference. Or jump straight to <a href="/oxiscript/getting-started/install/">Install &amp; enable</a>.</div>

<h3>What you can do</h3>
<ul>
  <li><strong>One round-trip multi-step logic</strong> — no chatty client/server back-and-forth.</li>
  <li><strong>ACID by default</strong> — every procedure is a single OCC transaction. <code>abort</code> rolls back.</li>
  <li><strong>Composable</strong> — procedures can call other procedures.</li>
  <li><strong>Same query language</strong> — Mongo-style <code>$inc</code>/<code>$set</code>/<code>$push</code>/<code>$gte</code> work as-is.</li>
  <li><strong>Familiar syntax</strong> — JS/Rust-shaped: <code>proc</code>, <code>let</code>, <code>if/else</code>, <code>for/in</code>, <code>return</code>, <code>abort</code>.</li>
</ul>

<h3>The 30-second pitch</h3>
<pre><code class="lang-rust">proc transfer(from, to, amount) {
    let sender = find_one(<span class="str">"accounts"</span>, {account_id: from})
    <span class="kw">if</span> sender == <span class="kw">null</span>             { <span class="kw">abort</span> <span class="str">"sender not found"</span> }
    <span class="kw">if</span> sender.balance &lt; amount  { <span class="kw">abort</span> <span class="str">"insufficient funds"</span> }

    update(<span class="str">"accounts"</span>, {account_id: from}, {$inc: {balance: -amount}})
    update(<span class="str">"accounts"</span>, {account_id: to},   {$inc: {balance:  amount}})
    insert(<span class="str">"transactions"</span>, {from: from, to: to, amount: amount})

    <span class="kw">return</span> {ok: <span class="kw">true</span>, transferred: amount}
}</code></pre>
<p>One TCP round-trip. One transaction. Validates, debits, credits, audits — atomically.</p>

<h3>Where to go next</h3>
<div class="docs-grid-2">
  <a href="/oxiscript/tutorial/" class="feature-card"><h3>Tutorial</h3><p>Step-by-step from install to recipes. 28 lessons, 200+ examples.</p></a>
  <a href="/oxiscript/api/tcp/" class="feature-card"><h3>API Reference</h3><p>TCP/OxiWire commands, REST endpoints, SDK signatures.</p></a>
</div>` }} />
}
