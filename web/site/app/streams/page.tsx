import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Change Streams",
  description: `Subscribe to real-time insert, update, and delete events.`,
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section id="streams" class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg> Change Streams</h2>
    <p class="section-desc">Subscribe to real-time insert, update, and delete events.</p>

    <pre><code class="lang-python"><span class="co"># Watch all changes</span>
sub_id = db.watch()

<span class="co"># Watch a specific collection</span>
sub_id = db.watch(collection=<span class="str">"orders"</span>)

<span class="co"># Each event contains:</span>
<span class="co"># - operation: "insert" | "update" | "delete"</span>
<span class="co"># - collection name</span>
<span class="co"># - document ID</span>
<span class="co"># - full document (on insert)</span>
<span class="co"># - timestamp</span>
<span class="co"># - resume token (for reconnection)</span>

<span class="co"># Unsubscribe</span>
db.unwatch(sub_id)</code></pre>

    <p>Replay buffer holds 4096 events for resumption. If a client disconnects and reconnects, it can resume from the last token.</p>
  </div>
</section>` }} />
}