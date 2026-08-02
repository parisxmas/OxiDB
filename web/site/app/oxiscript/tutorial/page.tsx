import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "OxiScript Tutorial",
  description: `Complete OxiScript tutorial — install, syntax, database operations, patterns, recipes, API reference.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<p class="docs-eyebrow">Tutorial</p>
<h2>OxiScript — Complete Tutorial</h2>
<p>Everything you need to build production-grade stored procedures. Each chapter is self-contained and packed with runnable examples.</p>

<div class="docs-callout"><strong>Path:</strong> If you're brand new, follow it top-to-bottom. If you already know the syntax, jump straight to <a href="#recipes">Recipes</a>.</div>

<h3>1. Getting Started</h3>
<ul>
  <li><a href="/oxiscript/getting-started/install/">Install &amp; enable</a> — get OxiDB running and confirm OxiScript is on</li>
  <li><a href="/oxiscript/getting-started/hello-world/">Hello, OxiScript</a> — your first procedure in three lines</li>
  <li><a href="/oxiscript/getting-started/first-procedure/">Your first real procedure</a> — read, validate, update, return</li>
</ul>

<h3>2. Language Syntax</h3>
<ul>
  <li><a href="/oxiscript/syntax/types/">Types &amp; literals</a> — numbers, strings, bools, null, arrays, objects</li>
  <li><a href="/oxiscript/syntax/variables/">Variables (<code>let</code>)</a> — bindings and scope</li>
  <li><a href="/oxiscript/syntax/operators/">Operators</a> — arithmetic, comparison, logical, field access</li>
  <li><a href="/oxiscript/syntax/control-flow/"><code>if</code>/<code>else</code></a> — conditional branches</li>
  <li><a href="/oxiscript/syntax/loops/"><code>for</code>/<code>in</code> loops</a> — iterating over result sets</li>
  <li><a href="/oxiscript/syntax/comments/">Comments</a> — line and block comments</li>
</ul>

<h3>3. Database Operations</h3>
<ul>
  <li><a href="/oxiscript/db/find/"><code>find</code> / <code>find_one</code></a> — querying documents</li>
  <li><a href="/oxiscript/db/insert/"><code>insert</code></a> — creating documents</li>
  <li><a href="/oxiscript/db/update/"><code>update</code> / <code>update_one</code></a> — modifying documents</li>
  <li><a href="/oxiscript/db/delete/"><code>delete</code> / <code>delete_one</code></a> — removing documents</li>
  <li><a href="/oxiscript/db/count/"><code>count</code></a> — counting matches</li>
  <li><a href="/oxiscript/db/aggregate/"><code>aggregate</code></a> — pipeline inside a procedure</li>
</ul>

<h3>4. Patterns</h3>
<ul>
  <li><a href="/oxiscript/patterns/validation/">Input validation</a> — guard clauses and friendly errors</li>
  <li><a href="/oxiscript/patterns/transactions/">Atomic transactions</a> — the OCC merge rule and how to avoid conflicts</li>
  <li><a href="/oxiscript/patterns/composition/">Procedure composition</a> — calling procs from procs</li>
  <li><a href="/oxiscript/patterns/upsert-soft-delete/">Upsert &amp; soft-delete</a> — common idempotency patterns</li>
</ul>

<h3 id="recipes">5. Real-world Recipes</h3>
<ul>
  <li><a href="/oxiscript/recipes/banking/">Banking</a> — transfers, withdrawals, statements</li>
  <li><a href="/oxiscript/recipes/ecommerce/">E-commerce</a> — orders, carts, refunds</li>
  <li><a href="/oxiscript/recipes/inventory/">Inventory</a> — stock check, restock, reservations</li>
  <li><a href="/oxiscript/recipes/audit-log/">Audit log</a> — actor + action + diff</li>
  <li><a href="/oxiscript/recipes/rate-limiting/">Rate limiting</a> — atomic counters with TTL</li>
  <li><a href="/oxiscript/recipes/leaderboard/">Leaderboards</a> — top-N with score updates</li>
</ul>

<h3>6. API Reference</h3>
<ul>
  <li><a href="/oxiscript/api/tcp/">TCP / OxiWire</a> — wire-protocol command list</li>
  <li><a href="/oxiscript/api/rest/">REST endpoints</a> — HTTP API</li>
  <li><a href="/oxiscript/api/sdks/">SDKs</a> — Go, Python, .NET</li>
</ul>

<div class="docs-prevnext">
  <a href="/oxiscript/" class="prev"><div class="label">Back to</div><div class="title">OxiScript overview</div></a>
  <a href="/oxiscript/getting-started/install/" class="next"><div class="label">Start with</div><div class="title">1. Install &amp; enable →</div></a>
</div>` }} />
}
