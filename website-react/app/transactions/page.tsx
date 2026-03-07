import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "ACID Transactions",
  description: `Multi-collection transactions with optimistic concurrency control.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section id="transactions" class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg> ACID Transactions</h2>
    <p class="section-desc">Multi-collection transactions with optimistic concurrency control.</p>

    <pre><code class="lang-python"><span class="co"># Python example</span>
tx = db.begin_transaction()

db.tx_insert(tx, <span class="str">"accounts"</span>, {<span class="str">"id"</span>: <span class="str">"A"</span>, <span class="str">"balance"</span>: <span class="num">1000</span>})
db.tx_update(tx, <span class="str">"accounts"</span>, {<span class="str">"id"</span>: <span class="str">"B"</span>}, {<span class="str">"$inc"</span>: {<span class="str">"balance"</span>: <span class="num">-500</span>}})
db.tx_update(tx, <span class="str">"accounts"</span>, {<span class="str">"id"</span>: <span class="str">"A"</span>}, {<span class="str">"$inc"</span>: {<span class="str">"balance"</span>: <span class="num">500</span>}})

db.commit_transaction(tx)  <span class="co"># atomic commit or rollback on conflict</span></code></pre>

    <h3>How It Works</h3>
    <ul>
      <li><strong>Optimistic Concurrency Control (OCC)</strong> -- Writes are buffered until commit. No locks held during the transaction.</li>
      <li><strong>Per-document versioning</strong> -- Each document has a version counter. Commit validates that no versions changed since read.</li>
      <li><strong>3-phase commit</strong> -- Prepare, validate versions, commit. WAL entries tagged with transaction IDs.</li>
      <li><strong>Deadlock-free</strong> -- Collections are locked in sorted order (BTreeSet) during commit.</li>
      <li><strong>Crash recovery</strong> -- Transaction log with CRC32 checksums. WAL replay on startup.</li>
    </ul>

    <h3>Transaction API</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Method</th><th>Description</th></tr></thead>
        <tbody>
          <tr><td><code>begin_transaction()</code></td><td>Start a new transaction, returns transaction ID</td></tr>
          <tr><td><code>tx_insert(tx, col, doc)</code></td><td>Buffered insert</td></tr>
          <tr><td><code>tx_find(tx, col, query)</code></td><td>Read with snapshot isolation</td></tr>
          <tr><td><code>tx_update(tx, col, query, update)</code></td><td>Buffered update</td></tr>
          <tr><td><code>tx_delete(tx, col, query)</code></td><td>Buffered delete</td></tr>
          <tr><td><code>commit_transaction(tx)</code></td><td>Validate and commit atomically</td></tr>
          <tr><td><code>rollback_transaction(tx)</code></td><td>Discard all buffered changes</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</section>` }} />
}