import type { Metadata } from "next"
export const metadata: Metadata = { title: "Variables (let)" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">Syntax · 2 of 6</p>
<h2>Variables (<code>let</code>)</h2>
<p>OxiScript has a single binding form: <code>let name = expr</code>. Every variable lives until the end of the procedure.</p>

<h3>Basic binding</h3>
<pre><code class="lang-rust">proc demo() {
    let x = <span class="num">42</span>
    let name = <span class="str">"Alice"</span>
    let active = <span class="kw">true</span>
    <span class="kw">return</span> {x: x, name: name, active: active}
}</code></pre>

<h3>Binding to an expression</h3>
<pre><code class="lang-rust">proc area(w, h) {
    let perimeter = <span class="num">2</span> * (w + h)
    let surface   = w * h
    <span class="kw">return</span> {perimeter: perimeter, surface: surface}
}</code></pre>

<h3>Binding to a database call</h3>
<pre><code class="lang-rust">proc balance(account_id) {
    let acc = find_one(<span class="str">"accounts"</span>, {account_id: account_id})
    <span class="kw">return</span> acc.balance
}</code></pre>

<h3>Binding to another procedure's result</h3>
<pre><code class="lang-rust">proc double_balance(account_id) {
    let bal = balance({account_id: account_id})
    <span class="kw">return</span> bal * <span class="num">2</span>
}</code></pre>

<h3>Reusing a binding</h3>
<p>Re-binding with <code>let</code> works — the new binding shadows the old one for the rest of the procedure.</p>
<pre><code class="lang-rust">proc step() {
    let x = <span class="num">1</span>
    let x = x + <span class="num">10</span>     <span class="co">// now 11</span>
    let x = x * <span class="num">2</span>      <span class="co">// now 22</span>
    <span class="kw">return</span> x
}</code></pre>

<h3>Naming rules</h3>
<ul>
  <li>Letters, digits, underscore. Must not start with a digit.</li>
  <li>Case-sensitive: <code>Name</code> ≠ <code>name</code>.</li>
  <li>Reserved words: <code>proc</code>, <code>let</code>, <code>if</code>, <code>else</code>, <code>for</code>, <code>in</code>, <code>return</code>, <code>abort</code>, <code>true</code>, <code>false</code>, <code>null</code>.</li>
  <li>Convention: <code>snake_case</code> for variables and procedures.</li>
</ul>

<h3>Parameters are bindings too</h3>
<p>Procedure parameters behave exactly like <code>let</code> bindings; you can read but not reassign them.</p>
<pre><code class="lang-rust">proc greet(name) {
    let message = <span class="str">"Hello, "</span>
    <span class="kw">return</span> {greeting: message, who: name}
}</code></pre>

<h3>Order matters</h3>
<p>You can only refer to a name after it's been bound.</p>
<pre><code class="lang-rust">proc bad() {
    <span class="kw">return</span> y     <span class="co">// ERROR — y is not bound yet</span>
    let y = <span class="num">1</span>
}</code></pre>
<pre><code class="lang-rust">proc good() {
    let y = <span class="num">1</span>
    <span class="kw">return</span> y
}</code></pre>

<h3>Bindings inside <code>if</code> blocks</h3>
<p>Bindings inside an <code>if</code> are scoped to the procedure (no block-level scope), so they're visible after the block too.</p>
<pre><code class="lang-rust">proc fee(total) {
    <span class="kw">if</span> total &gt; <span class="num">100</span> {
        let discount = total * <span class="num">0.1</span>
        <span class="kw">return</span> {total: total, discount: discount}
    }
    <span class="kw">return</span> {total: total, discount: <span class="num">0</span>}
}</code></pre>

<div class="docs-callout"><strong>Tip:</strong> Bind anything you'll use more than once. <code>let user = find_one(...)</code> then refer to <code>user.name</code>, <code>user.age</code>, etc. — the database call only happens once.</div>

<div class="docs-prevnext">
  <a href="/oxiscript/syntax/types/" class="prev"><div class="label">Previous</div><div class="title">← Types &amp; literals</div></a>
  <a href="/oxiscript/syntax/operators/" class="next"><div class="label">Next</div><div class="title">Operators →</div></a>
</div>` }} />
}
