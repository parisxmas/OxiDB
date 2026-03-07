"""Add a blog post about OxiMem native store and MQTT support."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
from oxidb import OxiDbClient

HOST = os.getenv("OXIDB_HOST", "127.0.0.1")
PORT = int(os.getenv("OXIDB_PORT", "4444"))


def seed():
    db = OxiDbClient(HOST, PORT, timeout=10.0)

    slug = "oximem-native-store-mqtt"

    # Remove if already exists
    db.delete("posts", {"slug": slug})
    db.delete("comments", {"post_slug": slug})

    # Ensure category exists
    if not db.find_one("categories", {"slug": "deep-dive"}):
        db.insert("categories", {"name": "Deep Dive", "slug": "deep-dive"})

    post = {
        "title": "OxiMem: 3M+ ops/s In-Memory Store with MQTT",
        "slug": slug,
        "category": "deep-dive",
        "tags": ["oxidb", "oximem", "mqtt", "pub-sub", "sorted-sets", "in-memory", "redis"],
        "author": "admin",
        "published": True,
        "created_at": "2026-03-08T12:00:00Z",
        "updated_at": "2026-03-08T12:00:00Z",
        "cover_image": "https://images.pexels.com/photos/1148820/pexels-photo-1148820.jpeg?auto=compress&cs=tinysrgb&w=1260&h=750&dpr=1",
        "content": """
<p><strong>OxiMem</strong> is OxiDB's built-in in-memory data store &mdash; a Redis-compatible layer that speaks the RESP protocol and now also MQTT v3.1.1. In v0.19.3, OxiMem got a complete rewrite: native data structures replace the SQL-backed approach, pipeline throughput exceeds <strong>3 million ops/s</strong>, and the server can double as an MQTT message broker with zero additional infrastructure.</p>

<h2>Why OxiMem?</h2>
<p>Many applications need both a document database and a cache/message layer. Traditionally, that means running OxiDB plus Redis plus an MQTT broker &mdash; three services to deploy, monitor, and maintain. OxiMem collapses all three into a single binary:</p>

<ul>
<li><strong>Cache</strong> &mdash; GET/SET/EXPIRE with sub-millisecond latency</li>
<li><strong>Data structures</strong> &mdash; hashes, lists, sets, sorted sets</li>
<li><strong>Pub/Sub</strong> &mdash; RESP SUBSCRIBE/PUBLISH for real-time messaging</li>
<li><strong>MQTT broker</strong> &mdash; IoT devices publish on MQTT, apps subscribe via RESP (or vice versa)</li>
<li><strong>Document database</strong> &mdash; the full OxiDB query engine on the same port</li>
</ul>

<h2>Native Data Structures</h2>
<p>Previous versions stored OxiMem data as OxiDB documents (JSON in the <code>_kv</code>, <code>_hash</code>, <code>_list</code>, <code>_set</code> collections). This was clever &mdash; you could query cache data with SQL &mdash; but added serialization overhead on every operation.</p>

<p>v0.19.3 introduces a <strong>native in-memory store</strong> using Rust's standard library data structures directly:</p>

<pre><code>struct OxiMemStore {
    strings:     RwLock&lt;HashMap&lt;String, KvEntry&gt;&gt;,
    hashes:      RwLock&lt;HashMap&lt;String, HashMap&lt;String, String&gt;&gt;&gt;,
    lists:       RwLock&lt;HashMap&lt;String, VecDeque&lt;String&gt;&gt;&gt;,
    sets:        RwLock&lt;HashMap&lt;String, HashSet&lt;String&gt;&gt;&gt;,
    sorted_sets: RwLock&lt;HashMap&lt;String, SortedSet&gt;&gt;,
    pubsub:      Mutex&lt;HashMap&lt;String, Vec&lt;Sender&gt;&gt;&gt;,
}</code></pre>

<p>Each data type has its own <code>RwLock</code>, so a SET operation doesn't block LPUSH or ZADD. The optional SQL mirroring mode (<code>OXIDB_OXIMEM_SQL=true</code>) is still available for applications that need queryable cache data.</p>

<h2>Pipeline Optimization: 3M+ ops/s</h2>
<p>OxiMem achieves over 3 million operations per second on pipelined workloads through two key optimizations:</p>

<h3>1. Lock Coalescing</h3>
<p>When a pipeline of 16 SET commands arrives, instead of acquiring and releasing the strings lock 16 times, OxiMem acquires it <strong>once</strong> and executes all 16 in a single critical section:</p>

<pre><code>// Before: 16 lock acquisitions
for cmd in pipeline {
    let mut map = store.strings.write();  // lock
    map.insert(key, value);
}   // unlock x16

// After: 1 lock acquisition
let mut map = store.strings.write();  // lock once
for cmd in pipeline {
    map.insert(key, value);
}   // unlock once</code></pre>

<h3>2. Deferred Flush</h3>
<p>Instead of flushing the TCP write buffer after every response, OxiMem only flushes when the read buffer is empty &mdash; meaning all available commands have been processed. This batches write syscalls, reducing kernel transitions by up to 16x for pipelined workloads.</p>

<h3>3. Single-Command Fast Path</h3>
<p>For non-pipelined commands (the common case), OxiMem skips the Vec allocation and pipeline machinery entirely, dispatching directly to the command handler.</p>

<h3>Benchmark Results</h3>
<table>
<tr><th>Operation</th><th>OxiMem ops/s</th><th>Mode</th></tr>
<tr><td>SET (single)</td><td>240,000</td><td>Single command</td></tr>
<tr><td>GET (single)</td><td>250,000</td><td>Single command</td></tr>
<tr><td>SET (pipeline P=16)</td><td>3,200,000</td><td>Pipeline</td></tr>
<tr><td>HSET (pipeline P=16)</td><td>3,100,000</td><td>Pipeline</td></tr>
<tr><td>LPUSH (pipeline P=16)</td><td>3,100,000</td><td>Pipeline</td></tr>
</table>

<h2>Sorted Sets</h2>
<p>Sorted sets are one of Redis's most powerful data structures, and OxiMem now supports them fully. Each sorted set uses a <strong>dual data structure</strong>:</p>

<ul>
<li><code>HashMap&lt;String, f64&gt;</code> &mdash; O(1) score lookups by member</li>
<li><code>BTreeSet&lt;(Score, String)&gt;</code> &mdash; O(log n) sorted iteration and range queries</li>
</ul>

<p>Supported commands:</p>
<pre><code># Add members with scores
ZADD leaderboard 100 "alice" 85 "bob" 92 "carol"

# Get rank (0-based, lowest score first)
ZRANK leaderboard "alice"    # 2

# Get top players (highest scores)
ZREVRANGE leaderboard 0 2 WITHSCORES
# 1) "alice"  2) "100"  3) "carol"  4) "92"  5) "bob"  6) "85"

# Range by score
ZRANGEBYSCORE leaderboard 90 100 WITHSCORES
# 1) "carol"  2) "92"  3) "alice"  4) "100"

# Atomic increment
ZINCRBY leaderboard 15 "bob"   # "100"

# Pop minimum/maximum
ZPOPMIN leaderboard 1
ZPOPMAX leaderboard 1</code></pre>

<h2>Pub/Sub: RESP + MQTT Unified</h2>
<p>OxiMem's pub/sub system uses <code>mpsc</code> channels internally. When a client subscribes to a topic, a new channel receiver is created. When anyone publishes to that topic &mdash; whether via RESP or MQTT &mdash; the message is broadcast to all receivers:</p>

<pre><code># Terminal 1: RESP subscriber
redis-cli -p 6380
> SUBSCRIBE sensors/temperature
Reading messages...

# Terminal 2: MQTT publisher (mosquitto_pub)
mosquitto_pub -h 127.0.0.1 -p 1883 -t sensors/temperature -m "22.5"

# Terminal 1 receives:
1) "message"
2) "sensors/temperature"
3) "22.5"</code></pre>

<p>This cross-protocol interop means IoT devices can publish via MQTT while your application backend subscribes via redis-cli, a Redis client library, or another MQTT client. The topics share a single namespace.</p>

<h2>MQTT v3.1.1 Protocol</h2>
<p>OxiDB now includes a full MQTT v3.1.1 broker. Enable it with <code>OXIDB_MQTT_PORT=1883</code> (or any port). The broker supports:</p>

<ul>
<li><strong>CONNECT/CONNACK</strong> &mdash; client handshake with client ID</li>
<li><strong>PUBLISH</strong> &mdash; QoS 0 (fire-and-forget) and QoS 1 (acknowledged)</li>
<li><strong>SUBSCRIBE/SUBACK</strong> &mdash; topic subscriptions with granted QoS</li>
<li><strong>UNSUBSCRIBE/UNSUBACK</strong> &mdash; clean topic removal</li>
<li><strong>PINGREQ/PINGRESP</strong> &mdash; keep-alive heartbeats</li>
<li><strong>DISCONNECT</strong> &mdash; clean session termination</li>
</ul>

<h3>MQTT-Only Mode</h3>
<p>For pure messaging workloads, set <code>OXIDB_MODE=mqtt</code> to run OxiDB exclusively as an MQTT broker. The main TCP listener is skipped entirely:</p>

<pre><code>OXIDB_MODE=mqtt OXIDB_MQTT_PORT=1883 oxidb-server</code></pre>

<p>This gives you a lightweight, single-binary MQTT broker with no external dependencies.</p>

<h2>Command Logging</h2>
<p>Set <code>OXIDB_LOG_COMMANDS=true</code> to log every OxiMem and MQTT command with its response to stderr:</p>

<pre><code>[oximem] &lt;&lt; SET mykey hello
[oximem] &gt;&gt; +OK
[oximem] &lt;&lt; GET mykey
[oximem] &gt;&gt; $5 hello
[mqtt] &lt;&lt; PUBLISH topic="sensors/temp" msg="22.5"
[mqtt] &gt;&gt; PUBLISH topic="sensors/temp" len=4</code></pre>

<p>Useful for debugging, auditing, and understanding traffic patterns.</p>

<h2>Configuration</h2>
<table>
<tr><th>Variable</th><th>Default</th><th>Description</th></tr>
<tr><td><code>OXIDB_OXIMEM_PORT</code></td><td><code>6380</code></td><td>RESP protocol port (redis-cli compatible)</td></tr>
<tr><td><code>OXIDB_MQTT_PORT</code></td><td><code>1883</code></td><td>MQTT v3.1.1 broker port</td></tr>
<tr><td><code>OXIDB_OXIMEM_SQL</code></td><td><code>false</code></td><td>Mirror data to OxiDB collections (queryable via SQL)</td></tr>
<tr><td><code>OXIDB_LOG_COMMANDS</code></td><td><code>false</code></td><td>Log all OxiMem/MQTT commands to stderr</td></tr>
<tr><td><code>OXIDB_MODE</code></td><td><em>(normal)</em></td><td>Set to <code>mqtt</code> for MQTT-only broker mode</td></tr>
</table>

<h2>Testing MQTT</h2>
<p>You can test MQTT with any MQTT client. Using <strong>mosquitto</strong> command-line tools:</p>

<pre><code># Start OxiDB with MQTT enabled
OXIDB_MQTT_PORT=1883 OXIDB_LOG_COMMANDS=true oxidb-server

# Subscribe (in one terminal)
mosquitto_sub -h 127.0.0.1 -p 1883 -t "test/topic"

# Publish (in another terminal)
mosquitto_pub -h 127.0.0.1 -p 1883 -t "test/topic" -m "Hello MQTT"</code></pre>

<p>Or with Python's <code>paho-mqtt</code>:</p>

<pre><code>import paho.mqtt.client as mqtt

client = mqtt.Client()
client.connect("127.0.0.1", 1883)
client.subscribe("sensors/#")
client.on_message = lambda c, u, msg: print(f"{msg.topic}: {msg.payload}")
client.loop_forever()</code></pre>

<blockquote>OxiMem turns OxiDB into a Swiss Army knife: document database, key-value cache, sorted set engine, pub/sub hub, and MQTT broker &mdash; all in a single binary, all sharing the same data, all at millions of operations per second.</blockquote>
""",
    }

    db.insert("posts", post)

    # Add comments
    comments = [
        {
            "post_slug": slug,
            "author": "IoTDeveloper",
            "content": "Cross-protocol pub/sub is huge for us. Our sensors publish via MQTT but our dashboard subscribes via WebSocket backed by a Redis client. Having both in one server eliminates Mosquitto from our stack entirely.",
            "created_at": "2026-03-08T13:00:00Z",
        },
        {
            "post_slug": slug,
            "author": "CacheArchitect",
            "content": "3.2M SET ops/s on pipeline is impressive. The lock coalescing approach makes total sense - you're amortizing the lock overhead across the batch. How does it handle mixed-type pipelines (SET + LPUSH + ZADD)?",
            "created_at": "2026-03-08T14:30:00Z",
        },
        {
            "post_slug": slug,
            "author": "RustEnthusiast",
            "content": "The Score wrapper with total_cmp for BTreeSet is a clean solution. NaN ordering in f64 is such a common footgun. Glad to see it handled properly.",
            "created_at": "2026-03-08T15:15:00Z",
        },
    ]

    for comment in comments:
        db.insert("comments", comment)

    print(f"Created post: {post['title']}")
    print(f"Added {len(comments)} comments")

    db.close()


if __name__ == "__main__":
    seed()
