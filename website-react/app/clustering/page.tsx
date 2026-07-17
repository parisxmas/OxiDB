import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Clustering, Raft Replication & Sharding",
  description:
    "How OxiDB scales out: Raft replication for durability (copies of the same data) and oxipool sharding for capacity (a split of the data) — two independent axes that compose. With diagrams, the routing math, and what actually happens during a network partition.",
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><circle cx="5" cy="6" r="2"/><circle cx="19" cy="6" r="2"/><line x1="7" y1="7" x2="10" y2="10"/><line x1="14" y1="10" x2="17" y2="7"/></svg> Clustering, Replication &amp; Sharding</h2>
    <p class="section-desc">Scaling out is two different problems, and OxiDB keeps them apart. <strong>Replication</strong> makes copies of the same data, so losing a machine doesn't lose the data. <strong>Sharding</strong> splits the data, so no machine has to hold all of it. They solve different things, they are configured separately, and they compose.</p>

    <div class="arch-note" style="margin-bottom:26px">
      <strong>The one-line version:</strong> <strong>Raft</strong> replicates writes across nodes that all hold the same data. <strong>oxipool</strong> is a router that splits data across independent shards by a key you choose. You can run either alone, or put oxipool in front of shards that are each internally Raft-replicated.
    </div>

    <h3>Two axes, not one</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th></th><th>Raft replication</th><th>oxipool sharding</th></tr></thead>
        <tbody>
          <tr><td><strong>Buys you</strong></td><td>Survival — a node can die</td><td>Capacity — data won't fit on one node</td></tr>
          <tr><td><strong>Each node holds</strong></td><td>The <em>same</em> data</td><td>A <em>slice</em> of the data</td></tr>
          <tr><td><strong>Costs you</strong></td><td>Write latency (a quorum must agree)</td><td>Cross-shard queries fan out</td></tr>
          <tr><td><strong>Doesn't help with</strong></td><td>Data bigger than one node</td><td>A shard dying</td></tr>
          <tr><td><strong>Turned on by</strong></td><td><code>OXIDB_NODE_ID</code> on the server</td><td>Running <code>oxipool</code> in front</td></tr>
        </tbody>
      </table>
    </div>
    <p>Pick replication if you fear machines failing. Pick sharding if you fear the data outgrowing a machine. Most people who need one eventually need both — which is why they are separate layers rather than one setting.</p>

    <h2 style="margin-top:44px"><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg> Raft replication</h2>
    <p class="section-desc">Every node holds a full copy. One node is the <strong>leader</strong>; it is the only one that accepts writes. A write is not acknowledged until a <strong>majority</strong> of nodes has durably stored it — which is what lets any minority of them fail without losing an acknowledged write.</p>

    <h3>How a write travels</h3>
    <pre><code class="lang-text">        client
          │  insert / update / delete / SQL write
          ▼
    ┌───────────┐        the follower does not apply it;
    │  node 2   │──┐     it forwards to the leader
    │ (follower)│  │
    └───────────┘  │
                   ▼
             ┌───────────┐
             │  node 1   │   1. append to own log
             │ (LEADER)  │   2. replicate to followers
             └─────┬─────┘   3. wait for a MAJORITY (here: 2 of 3)
          ┌────────┴────────┐ 4. commit + apply + ACK the client
          ▼                 ▼
    ┌───────────┐     ┌───────────┐
    │  node 2   │     │  node 3   │
    │ (follower)│     │ (follower)│
    └───────────┘     └───────────┘

  ACK only after a majority has it → any single node may now die
  without the write ever being lost.</code></pre>
    <p>Reads are served locally by whichever node you ask, which is why reads scale with the cluster and writes do not: a write costs a round trip to a quorum, every time.</p>

    <h3>Why the majority rule is the whole point</h3>
    <p>A 3-node cluster survives 1 failure; a 5-node cluster survives 2. The rule is <code>floor(N/2)</code>, and it is why cluster sizes are odd — a 4-node cluster also survives only 1 failure, so the 4th node buys nothing but another machine to pay for.</p>
    <p>The majority rule is also what makes <strong>split-brain impossible</strong>. Cut a 5-node cluster into 3 and 2: only the side of 3 can form a majority. The side of 2 keeps running and keeps refusing writes — it <em>cannot</em> accept them, because it can never reach a quorum. Two halves can never both commit, because two majorities of the same cluster must overlap.</p>

    <h3>Set one up</h3>
    <pre><code class="lang-bash"><span class="co"># three machines, each with its own id and Raft address</span>
OXIDB_NODE_ID=1 OXIDB_RAFT_ADDR=10.0.0.1:4445 OXIDB_ADDR=10.0.0.1:4444 ./oxidb-server
OXIDB_NODE_ID=2 OXIDB_RAFT_ADDR=10.0.0.2:4445 OXIDB_ADDR=10.0.0.2:4444 ./oxidb-server
OXIDB_NODE_ID=3 OXIDB_RAFT_ADDR=10.0.0.3:4445 OXIDB_ADDR=10.0.0.3:4444 ./oxidb-server</code></pre>
    <p>Then bootstrap once, from node 1 — it starts as a one-node cluster, the others join as learners (they catch up without voting), and a membership change promotes them to voters:</p>
    <pre><code class="lang-bash">oxidb --host 10.0.0.1 --port 4444
&gt; raft_init
&gt; raft_add_learner  node_id=2 addr=10.0.0.2:4445
&gt; raft_add_learner  node_id=3 addr=10.0.0.3:4445
&gt; raft_change_membership  members=[1,2,3]
&gt; raft_metrics          <span class="co"># state, term, leader, per-follower progress</span></code></pre>
    <p>The learner step matters: a fresh node joining a cluster with a large log has to copy it, and until it has, counting it toward a quorum would stall every write. As a learner it catches up silently and only then starts voting.</p>

    <h3>What is replicated — and what isn't</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Engine</th><th>Replicated?</th><th>How</th></tr></thead>
        <tbody>
          <tr><td>Document</td><td><strong>Yes</strong></td><td>Every mutating command goes through the log.</td></tr>
          <tr><td>SQL</td><td><strong>Yes — writes</strong></td><td>Each statement is <em>parsed</em> to decide. A statement that only reads runs locally; anything that can mutate replicates. Stored-procedure <code>CALL</code>s count as writes, which is why Cobra procedures are validated as deterministic at <code>CREATE</code>: a procedure that could read the clock or the network would apply differently on each node.</td></tr>
          <tr><td>Time-series</td><td>No — node-local</td><td>Replicate by ingesting into more than one node.</td></tr>
          <tr><td>OxiMem / MQTT</td><td>No — by design</td><td>An in-memory cache and a message bus; neither wants a consensus round trip.</td></tr>
        </tbody>
      </table>
    </div>
    <p>The SQL classification is a parse, not a prefix match, which is the only way to get it right: <code>WITH t AS (DELETE … RETURNING *) SELECT * FROM t</code> starts with <code>WITH</code> and is unmistakably a write.</p>

    <h2 style="margin-top:44px"><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg> Sharding with oxipool</h2>
    <p class="section-desc">Replication cannot help with data that does not fit. Sharding splits a collection across independent OxiDB servers by a <strong>shard key</strong> you choose per collection. <code>oxipool</code> sits in front and makes the split invisible: clients speak the ordinary wire protocol and never learn a shard exists.</p>

    <h3>The routing math</h3>
    <p>Keys are not hashed straight onto shards. They land on one of 256 <strong>virtual chunks</strong>, and a chunk map assigns chunks to shards:</p>
    <pre><code class="lang-text">  doc = { "region": "eu-west", "bal": 120 }
                │
                │  shard key for this collection is "region"
                ▼
        crc32("eu-west")
                │
                │  % 256
                ▼
           chunk 173
                │
                │  chunk_map[173]
                ▼
            shard 1
   ┌──────────┬──────────┬──────────┐
   │ shard 0  │ shard 1  │ shard 2  │
   │ chunks   │ chunks   │ chunks   │
   │ 0..85    │ 86..170  │ 171..255 │
   └──────────┴──────────┴──────────┘</code></pre>
    <p>The indirection is the point. Hash straight to <code>shard = hash % 3</code> and adding a fourth shard re-maps almost every key — a total reshuffle. With a chunk map you move <em>chunks</em>: to add a shard, hand it a quarter of the chunks and only that data moves. The map is data, not arithmetic, so it can also be lopsided on purpose — a bigger machine can simply own more chunks.</p>

    <h3>Two kinds of query</h3>
    <pre><code class="lang-text">  ROUTED — the query carries the shard key: one shard answers.

     find({region: "eu-west", bal: {$gt: 100}})
              │
           oxipool ──────────────► shard 1        (shards 0 and 2 idle)

  SCATTER-GATHER — no shard key: ask everyone, merge.

     count({bal: {$gt: 100}})
              │
           oxipool ──┬──► shard 0 ──┐
                     ├──► shard 1 ──┤  merge
                     └──► shard 2 ──┘    │
                                         ▼
                                   sum of the three</code></pre>
    <p>A routed query costs the same as a single-server query no matter how many shards there are — so choose the shard key to match how you actually query, not to look evenly distributed. A key that never appears in your queries turns every one of them into a fan-out.</p>

    <h3>Merging is per-command, and it is not obvious</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Command</th><th>Merge</th></tr></thead>
        <tbody>
          <tr><td><code>find</code>, <code>aggregate</code>, <code>search</code></td><td>Concatenate the documents</td></tr>
          <tr><td><code>count</code></td><td>Sum the counts</td></tr>
          <tr><td><code>update</code>, <code>delete</code></td><td>Sum the modified/deleted counts</td></tr>
          <tr><td><code>find_one</code></td><td>First shard with a match</td></tr>
          <tr><td>DDL</td><td>Broadcast to every shard</td></tr>
        </tbody>
      </table>
    </div>
    <p>Two of those are subtler than they look.</p>
    <p><strong><code>find</code> with <code>skip</code>/<code>limit</code></strong> cannot be sent to the shards as written. A per-shard <code>skip</code> would drop up to <code>skip × (shards−1)</code> documents that belong in the answer — each shard would skip its own rows independently. So <code>skip</code> is removed on the way out and <code>limit</code> becomes <code>skip + limit</code>; the global window is a subset of the union of the per-shard windows, and the merge re-sorts and slices it globally.</p>
    <p><strong><code>update_one</code> / <code>delete_one</code> without a shard key</strong> must not fan out at all. Sent to every shard, each one dutifully modifies <em>one</em> local document — up to N changes for a command that promised one, with the merge merely choosing which reply to show. Instead the shards are probed <strong>serially</strong> and it stops at the first that actually changed something.</p>

    <h3>Set one up</h3>
    <pre><code class="lang-bash"><span class="co"># three plain OxiDB servers — they don't know they're shards</span>
OXIDB_ADDR=10.0.0.1:4444 OXIDB_DATA=/data ./oxidb-server   <span class="co"># … and .2, .3</span>

<span class="co"># the router in front</span>
OXIPOOL_LISTEN=0.0.0.0:4445 \\
OXIPOOL_SHARDS=10.0.0.1:4444,10.0.0.2:4444,10.0.0.3:4444 \\
OXIPOOL_SHARD_KEYS="accounts:region,events:tenant_id" \\
OXIPOOL_NUM_CHUNKS=256 \\
OXIPOOL_REQUEST_TIMEOUT=30 \\
  ./oxipool</code></pre>
    <p>Clients now connect to <code>:4445</code> and use OxiDB exactly as before. Collections without a shard key live on shard 0 — sharding is opt-in per collection.</p>

    <h3>Read/write splitting (a different job)</h3>
    <p>oxipool also runs in a plain master/replica mode, where it classifies each request and sends reads to replicas and writes to the master:</p>
    <pre><code class="lang-bash">OXIPOOL_MASTER=10.0.0.1:4444 \\
OXIPOOL_REPLICAS=10.0.0.2:4444,10.0.0.3:4444 \\
  ./oxipool</code></pre>
    <p>Classification reads the request's <code>cmd</code> field — never the payload. Scanning the raw bytes for words like <code>insert</code> is the obvious shortcut and a real bug: a document that merely <em>contains</em> the word would reroute a read to the master, and one containing <code>begin_tx</code> would pin a pooled connection per occurrence. What a user stores must never steer routing.</p>

    <h2 style="margin-top:44px"><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/></svg> Putting them together</h2>
    <p class="section-desc">The layers compose: shard for size, replicate each shard for survival.</p>
    <pre><code class="lang-text">                         clients
                            │
                       ┌─────────┐
                       │ oxipool │   splits by shard key
                       └────┬────┘
             ┌──────────────┼──────────────┐
             ▼              ▼              ▼
       ┌───────────┐  ┌───────────┐  ┌───────────┐
       │  shard 0  │  │  shard 1  │  │  shard 2  │   ← a third of the data each
       │  ┌─────┐  │  │  ┌─────┐  │  │  ┌─────┐  │
       │  │ ldr │  │  │  │ ldr │  │  │  │ ldr │  │
       │  └──┬──┘  │  │  └──┬──┘  │  │  └──┬──┘  │   ← each shard is its own
       │  ┌──┴──┐  │  │  ┌──┴──┐  │  │  ┌──┴──┐  │     Raft group: 3 copies,
       │  │f   f│  │  │  │f   f│  │  │  │f   f│  │     its own leader, its own
       │  └─────┘  │  │  └─────┘  │  │  └─────┘  │     majority
       └───────────┘  └───────────┘  └───────────┘
        9 machines · 3× the capacity of one · any one machine may die</code></pre>
    <p>Each shard elects its own leader and reaches its own quorum; a shard losing a node is that shard's problem and nobody else's. What the layers do <em>not</em> give you is a transaction across shards — each shard commits independently, so cross-shard atomicity is not on offer. Pick a shard key that keeps things which must change together on the same shard (a tenant, an account, a region), and cross-shard writes stop being something you need.</p>
    <p>The SQL and time-series engines are <strong>not sharded</strong> — oxipool splits on a collection's shard key, and a SQL statement has no collection to split on. SQL scales by replication (reads across replicas, writes through the leader), not by sharding.</p>

    <h2 style="margin-top:44px"><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg> What a partition actually does</h2>
    <p class="section-desc">The interesting question isn't whether a cluster survives a clean crash — it's what it does when the network lies. These are OxiDB's tested behaviours, each pinned by a test that injects the fault deterministically at the transport.</p>

    <h3>The network splits the cluster</h3>
    <pre><code class="lang-text">   ╔═══════════════════╗  ╳  ╔═══════════════╗
   ║  n1    n2    n3   ║  ╳  ║   n4    n5    ║
   ║   majority (3)    ║  ╳  ║  minority (2) ║
   ║   elects a leader ║  ╳  ║  cannot elect ║
   ║   keeps committing║  ╳  ║  refuses writes║
   ╚═══════════════════╝  ╳  ╚═══════════════╝
                    heal ──►  minority discards nothing it
                              committed (it committed nothing)
                              and catches up. All 5 converge.</code></pre>
    <ul>
      <li><strong>No lost writes</strong> — everything the majority acknowledged survives on every node afterwards.</li>
      <li><strong>No split-brain</strong> — the minority cannot commit, even holding the old leader. Its writes fail; none of them appear after healing.</li>
      <li><strong>Available where it can be</strong> — the majority keeps serving throughout.</li>
      <li><strong>Convergence</strong> — after healing, all nodes hold an identical set.</li>
    </ul>

    <h3>The nastier ones: when one direction fails</h3>
    <p>A clean split is polite — both ends agree the other is gone. Real networks drop one direction and leave the two ends <em>disagreeing</em> about whether the peer is alive.</p>
    <p>A node that <strong>hears nothing but can still speak</strong> gets no heartbeats, so it calls an election — and since its own messages get through, it simply wins and leads. No harm done.</p>
    <p>The dangerous one is a node whose <strong>requests land but whose replies never come back</strong>. It asks for votes, they are granted, it never learns, so it times out and asks again at a higher term — forever. This is Raft's classic "disruptive server": each campaign could force the real leader to step down, and the cluster would lose its leader over and over to a node that can never win. It doesn't happen here, because a follower that has heard from a live leader recently <strong>refuses a vote request without adopting its term</strong>. Under test, the disruptor climbed to term 16 while the healthy cluster sat at term 5 and committed every single write.</p>

    <h3>A shard disappears</h3>
    <p>For a sharded cluster the failure is different in kind — and the danger is not an error, it's an answer. A fan-out has every shard's reply in hand and must fold them into one. Fold only the ones that came back and the client gets <code>ok: true</code> and a perfectly plausible number that is quietly missing a third of the data — a <code>count</code> of 40 where the truth is 60. That is worse than an outage, because an outage is visible.</p>
    <p>So every merge <strong>fails loudly</strong> if any shard did not answer. Even <code>find_one</code>: "not found" is only true if every shard was asked, and the document may be living on precisely the shard that is down. Meanwhile a query that <em>carries</em> the shard key still routes to a live shard and answers normally — one dead shard must not take down the healthy ones.</p>
    <pre><code class="lang-text">  shard 1 down:

    count({})                    → ok:false  "…failed on one or more shards"
                                   (NOT 40 — the plausible wrong answer)
    find({region:"eu"})          → ok:true   (routed to shard 0, which is fine)</code></pre>
    <p>A shard that has <em>crashed</em> is the easy case — the connection breaks and everything errors at once. A shard that is <em>partitioned</em> is worse: it accepts the request, reads it, and never answers. Nothing fails; the call simply never returns, and the pooled connection it borrowed is never given back — repeat that and the pool drains until the whole router hangs. <code>OXIPOOL_REQUEST_TIMEOUT</code> (seconds, 30 by default) is what turns that silence into an error.</p>

    <h3>The honest limits</h3>
    <ul>
      <li><strong>No cross-shard transactions.</strong> Each shard commits on its own.</li>
      <li><strong>The SQL and time-series engines don't shard.</strong> They replicate (SQL) or stay node-local (TSDB).</li>
      <li><strong>Rebalancing is manual.</strong> The chunk map makes moving data cheap; nothing moves it for you yet.</li>
      <li><strong>A write costs a quorum round trip.</strong> Replication buys survival with latency — that is the trade, not a bug to tune away.</li>
    </ul>

    <p style="margin-top:26px">More: <a href="/server/">Server &amp; configuration</a> · <a href="/transactions/">Transactions &amp; consistency</a> · <a href="/sql/">SQL engine</a></p>
  </div>
</section>` }} />
}
