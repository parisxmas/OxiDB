import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "AMQP (RabbitMQ protocol)",
  description:
    "OxiDB speaks AMQP 0-9-1 — the RabbitMQ protocol. pika, amqplib, RabbitMQ.Client and amqp091-go work unmodified: work queues with competing consumers, direct/fanout/topic exchanges, publisher confirms, prefetch, durable queues that survive SIGKILL, and an MQTT bridge.",
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 11a9 9 0 019 9"/><path d="M4 4a16 16 0 0116 16"/><circle cx="5" cy="19" r="1"/></svg> AMQP &mdash; the RabbitMQ protocol</h2>
    <p class="section-desc">OxiDB speaks <strong>AMQP&nbsp;0-9-1</strong>, the protocol RabbitMQ clients use. Code written for RabbitMQ &mdash; <code>pika</code> (Python), <code>amqplib</code> (Node), <code>RabbitMQ.Client</code> (.NET), <code>amqp091-go</code> (Go), the official tutorials included &mdash; points at OxiDB and works unmodified. It adds the one semantic MQTT cannot express: a <strong>work queue</strong>, where each message goes to exactly <em>one</em> of a pool of competing consumers. Off by default.</p>

    <h3>Enable it</h3>
    <pre><code class="lang-bash"><span class="co"># start the AMQP listener (5672 is the AMQP default port)</span>
OXIDB_AMQP_PORT=5672 ./oxidb-server</code></pre>

    <h3>Hello world &mdash; RabbitMQ&apos;s own tutorial code, unmodified</h3>
    <pre><code class="lang-python">import pika

conn = pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1', 5672))
ch = conn.channel()
ch.queue_declare(queue='hello')
ch.basic_publish(exchange='', routing_key='hello', body=b'Hello OxiDB!')

method, props, body = ch.basic_get('hello', auto_ack=True)
print(body)          <span class="co"># b'Hello OxiDB!'</span>
conn.close()</code></pre>

    <h3>Work queues &mdash; competing consumers</h3>
    <p>An MQTT subscription copies every message to every subscriber. An AMQP queue hands each message to exactly <strong>one</strong> of its consumers, round-robin &mdash; work distribution across a worker pool. Unacked messages requeue (flagged <code>redelivered</code>) if a worker dies.</p>
    <pre><code class="lang-python"><span class="co"># worker.py — run as many of these as you like; they split the work</span>
def on_message(ch, method, props, body):
    do_work(body)
    ch.basic_ack(method.delivery_tag)   <span class="co"># ack AFTER the work: crash = requeue</span>

ch.basic_qos(prefetch_count=1)          <span class="co"># a busy worker's turn passes to an idle one</span>
ch.basic_consume('tasks', on_message)
ch.start_consuming()</code></pre>

    <h3>Exchanges</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Type</th><th>Routing</th><th>Example</th></tr></thead>
        <tbody>
          <tr><td><code>""</code> (default)</td><td>Routing key <em>is</em> the queue name</td><td><code>basic_publish('', 'tasks', ...)</code></td></tr>
          <tr><td><code>direct</code></td><td>Exact routing-key match</td><td><code>error</code> &rarr; the queue bound with <code>error</code></td></tr>
          <tr><td><code>fanout</code></td><td>Every bound queue gets a copy</td><td>broadcast</td></tr>
          <tr><td><code>topic</code></td><td><code>.</code>-separated words; <code>*</code> = one word, <code>#</code> = zero or more</td><td><code>kern.*</code> matches <code>kern.crit</code>; <code>#</code> matches everything</td></tr>
        </tbody>
      </table>
    </div>
    <pre><code class="lang-python">ch.exchange_declare(exchange='logs', exchange_type='topic')
ch.queue_bind('kernel-q', 'logs', 'kern.*')
ch.queue_bind('audit-q',  'logs', '#')
ch.basic_publish('logs', 'kern.crit', b'disk on fire')   <span class="co"># reaches both</span></code></pre>

    <h3>Publisher confirms &amp; mandatory</h3>
    <p>In confirm mode the broker acks each publish &mdash; and for a persistent message the ack is only sent <strong>after the fsync</strong> (write-before-confirm). An unroutable <code>mandatory</code> publish comes back as <code>Basic.Return</code> (312 <code>NO_ROUTE</code>) instead of vanishing; pika surfaces it as <code>UnroutableError</code>.</p>
    <pre><code class="lang-csharp"><span class="co">// .NET — RabbitMQ.Client 7.x, unmodified</span>
var factory = new ConnectionFactory { HostName = "127.0.0.1", Port = 5672 };
await using var conn = await factory.CreateConnectionAsync();
await using var ch = await conn.CreateChannelAsync(new CreateChannelOptions(
    publisherConfirmationsEnabled: true,
    publisherConfirmationTrackingEnabled: true));

await ch.QueueDeclareAsync("orders", durable: true, exclusive: false, autoDelete: false);
await ch.BasicPublishAsync("", "orders", mandatory: false,
    basicProperties: new BasicProperties { Persistent = true },
    body: JsonSerializer.SerializeToUtf8Bytes(order));
<span class="co">// awaited = the broker has fsync'd it. A crash after this point cannot lose it.</span></code></pre>

    <h3>Durability follows the protocol, not a config flag</h3>
    <p>AMQP already says what should survive: a queue declared <code>durable</code> holding messages published with <code>delivery_mode=2</code> (persistent) is written through the document engine&apos;s WAL and <strong>survives a <code>SIGKILL</code></strong> &mdash; recovered messages arrive flagged <code>redelivered</code>, acknowledged ones stay consumed. Everything else lives in memory, which is what the client asked for by not saying <code>durable</code>. There is no <code>OXIDB_AMQP_PERSIST</code> because the protocol makes the choice per queue and per message.</p>
    <p>Under the hood, pipelined persistent publishes are batched into one fsync per burst, and bursts from <em>different connections</em> share fsync rounds through a group committer &mdash; see the numbers below.</p>

    <h3>MQTT &harr; AMQP bridge</h3>
    <p>The pre-declared <code>amq.topic</code> exchange bridges the two brokers the same way RabbitMQ&apos;s own MQTT plugin does: MQTT topic slashes become AMQP routing-key dots and back, MQTT QoS&nbsp;&ge;&nbsp;1 maps to persistent. A sensor publishes MQTT; a worker pool consumes AMQP &mdash; one binary.</p>
    <pre><code class="lang-bash"><span class="co"># both listeners on</span>
OXIDB_MQTT_PORT=1883 OXIDB_AMQP_PORT=5672 ./oxidb-server</code></pre>
    <pre><code class="lang-python"><span class="co"># AMQP side: bind a queue to amq.topic (pre-declared, no declare needed)</span>
ch.queue_declare(queue='readings', durable=True)
ch.queue_bind('readings', 'amq.topic', 'sensors.#')</code></pre>
    <pre><code class="lang-bash"><span class="co"># MQTT side: publish normally…</span>
mosquitto_pub -p 1883 -t sensors/floor1/temp -m "21.5"
<span class="co"># …the AMQP consumer receives it with routing key sensors.floor1.temp</span></code></pre>
    <p>The reverse works too: an AMQP publish to <code>amq.topic</code> with routing key <code>alerts.fire</code> reaches MQTT subscribers of <code>alerts/+</code> (and OxiMem RESP subscribers on the same bus).</p>

    <h3>Authentication</h3>
    <pre><code class="lang-bash">OXIDB_AMQP_PORT=5672 OXIDB_AMQP_USER=app OXIDB_AMQP_PASSWORD=secret ./oxidb-server
<span class="co"># clients use PLAIN auth exactly as against RabbitMQ: amqp://app:secret@host:5672/</span></code></pre>

    <h3>Performance vs RabbitMQ</h3>
    <p>Measured with the same Go client (<code>amqp091-go</code>), the same code path, against RabbitMQ&nbsp;4.x on the same machine (100-byte bodies; harness in <code>tests/rabbitmq-benchmark-go</code>):</p>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Scenario</th><th>OxiDB</th><th>RabbitMQ</th><th>Ratio</th></tr></thead>
        <tbody>
          <tr><td>Publish confirms, pipelined</td><td>246k msg/s</td><td>164k msg/s</td><td><strong>1.50&times;</strong></td></tr>
          <tr><td>Publish confirm latency (p50)</td><td>0.02 ms</td><td>0.03 ms</td><td><strong>1.74&times;</strong></td></tr>
          <tr><td>Durable publish, 1 connection</td><td>54k msg/s</td><td>160k msg/s</td><td>0.34&times;</td></tr>
          <tr><td>Durable publish, 8 connections</td><td>103k msg/s</td><td>93k msg/s</td><td><strong>1.11&times;</strong></td></tr>
          <tr><td>End-to-end throughput</td><td>318k msg/s</td><td>246k msg/s</td><td><strong>1.29&times;</strong></td></tr>
          <tr><td>End-to-end latency (p50)</td><td>0.02 ms</td><td>0.04 ms</td><td><strong>1.52&times;</strong></td></tr>
        </tbody>
      </table>
    </div>
    <p>The one loss is deliberate: every OxiDB durable confirm sits behind a real <code>F_FULLFSYNC</code> of its batch, while RabbitMQ classic queues flush lazily on an interval &mdash; its persistent confirm does not, by itself, prove the message is on stable storage. Add concurrency and the honest fsync wins anyway.</p>

    <h3>The subset, stated honestly</h3>
    <p>Implemented: connections/channels/heartbeats, PLAIN auth, default + <code>direct</code>/<code>fanout</code>/<code>topic</code> exchanges, queue declare/bind (durable, exclusive, auto-delete, server-named, passive), publish with multi-frame bodies, consume/get/ack/nack/reject, <code>Basic.Qos</code> prefetch, publisher confirms, mandatory <code>Basic.Return</code>, and the MQTT bridge.</p>
    <p>Not implemented &mdash; and <em>refused with a clear channel error</em>, never silently accepted: the <code>tx</code> class, <code>headers</code> exchanges, exchange-to-exchange bindings, per-message TTL/priority, dead-letter arguments, alternate exchanges, streams, and AMQP&nbsp;1.0 (a different protocol entirely).</p>

    <p>AMQP queues are node-local, like the <a href="/mqtt/">MQTT broker</a>: consumers are connections and connections live on one node; the durable data behind a queue is what replicates in <a href="/clustering/">cluster mode</a>.</p>
  </div>
</section>` }} />
}
