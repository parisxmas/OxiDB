import type { Metadata } from "next"
export const metadata: Metadata = { title: "Hello, OxiScript" }
export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">Getting Started · 2 of 3</p>
<h2>Hello, OxiScript</h2>
<p>The smallest possible OxiScript procedure: takes a parameter, returns an object.</p>

<h3>The procedure</h3>
<pre><code class="lang-rust">proc hello(name) {
    return {greeting: <span class="str">"Hi, "</span>, who: name}
}</code></pre>

<h3>Create it</h3>
<pre><code class="lang-bash">{<span class="str">"cmd"</span>: <span class="str">"create_procedure"</span>,
 <span class="str">"script"</span>: <span class="str">"proc hello(name) { return {greeting: 'Hi, ', who: name} }"</span>}</code></pre>
<p>Response:</p>
<pre><code class="lang-json">{<span class="str">"ok"</span>: <span class="kw">true</span>, <span class="str">"data"</span>: {<span class="str">"name"</span>: <span class="str">"hello"</span>}}</code></pre>

<h3>Call it</h3>
<pre><code class="lang-bash">{<span class="str">"cmd"</span>: <span class="str">"call_procedure"</span>, <span class="str">"name"</span>: <span class="str">"hello"</span>,
 <span class="str">"params"</span>: {<span class="str">"name"</span>: <span class="str">"Alice"</span>}}</code></pre>
<p>Response:</p>
<pre><code class="lang-json">{<span class="str">"ok"</span>: <span class="kw">true</span>, <span class="str">"data"</span>: {<span class="str">"greeting"</span>: <span class="str">"Hi, "</span>, <span class="str">"who"</span>: <span class="str">"Alice"</span>}}</code></pre>

<h3>Variations</h3>

<h4>No parameters</h4>
<pre><code class="lang-rust">proc heartbeat() {
    return {ok: <span class="kw">true</span>, time: <span class="str">"now"</span>}
}</code></pre>

<h4>Multiple parameters</h4>
<pre><code class="lang-rust">proc add_user(name, email, age) {
    return {created: <span class="kw">true</span>, name: name, email: email, age: age}
}</code></pre>

<h4>Returning a number</h4>
<pre><code class="lang-rust">proc square(n) {
    return n * n
}</code></pre>

<h4>Returning an array</h4>
<pre><code class="lang-rust">proc range3(start) {
    return [start, start + <span class="num">1</span>, start + <span class="num">2</span>]
}</code></pre>

<h4>Returning a boolean</h4>
<pre><code class="lang-rust">proc is_adult(age) {
    return age &gt;= <span class="num">18</span>
}</code></pre>

<h4>Returning <code>null</code></h4>
<pre><code class="lang-rust">proc do_nothing() {
    return <span class="kw">null</span>
}</code></pre>

<h4>Aborting instead of returning</h4>
<pre><code class="lang-rust">proc reject() {
    abort <span class="str">"not implemented"</span>
}</code></pre>
<p>Calling <code>reject</code> returns <code>{"ok": false, "error": "not implemented"}</code>.</p>

<h3>What just happened</h3>
<ol>
  <li>Your script was sent to the server as plain text.</li>
  <li>The lexer tokenized it; the parser built an AST; the compiler lowered it to JSON steps.</li>
  <li>Those steps were stored in the <code>_procedures</code> collection.</li>
  <li>On <code>call_procedure</code>, the procedure engine executed each step inside one OCC transaction.</li>
</ol>

<div class="docs-prevnext">
  <a href="/oxiscript/getting-started/install/" class="prev"><div class="label">Previous</div><div class="title">← 1. Install &amp; enable</div></a>
  <a href="/oxiscript/getting-started/first-procedure/" class="next"><div class="label">Next</div><div class="title">3. Your first real procedure →</div></a>
</div>` }} />
}
