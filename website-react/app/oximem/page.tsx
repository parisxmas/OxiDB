import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "OxiMem — Redis-compatible in-memory store",
  description:
    "OxiMem is OxiDB's Redis-compatible in-memory store — the RESP protocol, strings/hashes/lists/sets/sorted-sets, MULTI/EXEC/WATCH transactions, EVAL/Lua scripting, pub/sub, and optional persistence. redis-cli and Redis clients work unmodified.",
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg> OxiMem &mdash; in-memory store</h2>
    <p class="section-desc">A Redis-compatible in-memory store built into the same server. It speaks the <strong>RESP</strong> wire protocol, so <code>redis-cli</code> and existing Redis client libraries work unmodified. On the single-command benchmark it runs at 93&ndash;101% of Redis, and beats Redis on pipelined writes. Off by default.</p>

    <h3>Enable it</h3>
    <pre><code class="lang-bash"><span class="co"># start the RESP listener on a port (Redis' default is 6379)</span>
OXIDB_OXIMEM_PORT=6379 ./oxidb-server

<span class="co"># talk to it with the ordinary redis-cli</span>
redis-cli -p 6379 PING            <span class="co"># PONG</span></code></pre>

    <h3>Data types &amp; commands</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Type</th><th>Commands</th></tr></thead>
        <tbody>
          <tr><td>Strings</td><td><code>GET SET SETNX SETEX GETSET APPEND INCR DECR INCRBY MGET MSET</code></td></tr>
          <tr><td>Keys</td><td><code>DEL EXISTS TYPE KEYS SCAN EXPIRE TTL</code></td></tr>
          <tr><td>Hashes</td><td><code>HSET HGET HGETALL HDEL</code></td></tr>
          <tr><td>Lists</td><td><code>LPUSH RPUSH LPOP RPOP LRANGE</code></td></tr>
          <tr><td>Sets</td><td><code>SADD SMEMBERS SCARD</code></td></tr>
          <tr><td>Sorted sets</td><td><code>ZADD ZRANGE ZSCORE</code></td></tr>
        </tbody>
      </table>
    </div>
    <pre><code class="lang-bash">redis-cli -p 6379 SET user:1 Alice
redis-cli -p 6379 INCR visits
redis-cli -p 6379 HSET user:1 age 30 country TR
redis-cli -p 6379 RPUSH queue job1 job2 job3
redis-cli -p 6379 ZADD leaderboard 100 alice 90 bob
redis-cli -p 6379 EXPIRE user:1 3600</code></pre>

    <h3>Transactions &mdash; MULTI / EXEC / WATCH</h3>
    <p>Optimistic transactions with <code>WATCH</code> (check-and-set): queue commands with <code>MULTI</code>, run them atomically with <code>EXEC</code>; if a <code>WATCH</code>ed key changed, <code>EXEC</code> aborts.</p>
    <pre><code class="lang-bash">WATCH balance
MULTI
DECRBY balance 100
INCRBY spent 100
EXEC          <span class="co"># nil if 'balance' was modified by another client</span>
<span class="co"># DISCARD / UNWATCH also supported</span></code></pre>

    <h3>Lua scripting &mdash; EVAL</h3>
    <p>Run atomic server-side Lua (mlua). Scripts hold striped key locks so independent scripts run concurrently.</p>
    <pre><code class="lang-bash">redis-cli -p 6379 EVAL "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])" 1 mykey hello</code></pre>

    <h3>Pub / Sub</h3>
    <p>Channel and pattern subscriptions, cross-protocol: a message <code>PUBLISH</code>ed here can also reach MQTT subscribers.</p>
    <pre><code class="lang-bash"><span class="co"># terminal 1</span>
redis-cli -p 6379 SUBSCRIBE news          <span class="co"># or PSUBSCRIBE news.*</span>

<span class="co"># terminal 2</span>
redis-cli -p 6379 PUBLISH news "hello"</code></pre>

    <h3>Persistence (optional)</h3>
    <p>OxiMem is in-memory by default. Two independent durability options:</p>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Env var</th><th>Effect</th></tr></thead>
        <tbody>
          <tr><td><code>OXIDB_OXIMEM_SNAPSHOT_SECS</code></td><td>Periodic snapshot of the keyspace to disk (RDB-style), reloaded on restart.</td></tr>
          <tr><td><code>OXIDB_OXIMEM_SQL</code></td><td>Mirror writes into a SQL table, so the keyspace survives restarts and is queryable from the SQL engine.</td></tr>
        </tbody>
      </table>
    </div>

    <h3>Use it from any Redis client</h3>
    <pre><code class="lang-python"><span class="kw">import</span> redis
r = redis.Redis(host=<span class="str">"127.0.0.1"</span>, port=<span class="num">6379</span>)
r.set(<span class="str">"user:1"</span>, <span class="str">"Alice"</span>)
r.incr(<span class="str">"visits"</span>)
<span class="kw">with</span> r.pipeline() <span class="kw">as</span> p:          <span class="co"># pipelined writes beat Redis</span>
    <span class="kw">for</span> i <span class="kw">in</span> range(<span class="num">1000</span>): p.set(<span class="str">f"k{i}"</span>, i)
    p.execute()</code></pre>
    <p>The keyspace is global (shared across databases). See also the built-in <strong>MQTT broker</strong> for cross-protocol pub/sub on the <a href="/streams/">Streams</a> page.</p>
  </div>
</section>` }} />
}
