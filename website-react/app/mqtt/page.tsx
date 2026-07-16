import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "MQTT Broker",
  description:
    "OxiDB ships a full MQTT 3.1.1 broker — topic wildcards, retained messages, QoS 0/1/2, Last Will & Testament, and auth. mosquitto and any MQTT client work unmodified, with cross-protocol pub/sub shared with OxiMem.",
}

export default function Page() {
  return <div dangerouslySetInnerHTML={{ __html: `<section class="section">
  <div class="container">
    <h2><svg class="section-icon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="2"/><path d="M4.93 19.07a10 10 0 010-14.14"/><path d="M7.76 16.24a6 6 0 010-8.49"/><path d="M16.24 7.76a6 6 0 010 8.49"/><path d="M19.07 4.93a10 10 0 010 14.14"/></svg> MQTT Broker</h2>
    <p class="section-desc">A full <strong>MQTT 3.1.1</strong> broker built into the same server &mdash; publish/subscribe messaging for IoT and event streams. <code>mosquitto_pub</code>/<code>mosquitto_sub</code> and any MQTT client library work unmodified. Off by default.</p>

    <h3>Enable it</h3>
    <pre><code class="lang-bash"><span class="co"># start the MQTT listener (1883 is the MQTT default port)</span>
OXIDB_MQTT_PORT=1883 ./oxidb-server</code></pre>

    <h3>Publish &amp; subscribe</h3>
    <pre><code class="lang-bash"><span class="co"># terminal 1 — subscribe</span>
mosquitto_sub -h 127.0.0.1 -p 1883 -t sensors/temp

<span class="co"># terminal 2 — publish</span>
mosquitto_pub -h 127.0.0.1 -p 1883 -t sensors/temp -m "22.4"</code></pre>

    <h3>Topic wildcards</h3>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Wildcard</th><th>Matches</th><th>Example</th></tr></thead>
        <tbody>
          <tr><td><code>+</code></td><td>Exactly one level</td><td><code>sensors/+/temp</code> &rarr; <code>sensors/a/temp</code>, <code>sensors/b/temp</code></td></tr>
          <tr><td><code>#</code></td><td>The whole subtree</td><td><code>sensors/#</code> &rarr; everything under <code>sensors/</code></td></tr>
        </tbody>
      </table>
    </div>

    <h3>Retained messages</h3>
    <p>Publish with the retain flag to deliver the last-known value to any new subscriber immediately. An empty retained payload clears it.</p>
    <pre><code class="lang-bash">mosquitto_pub -h 127.0.0.1 -p 1883 -t status/device1 -m online -r
mosquitto_sub -h 127.0.0.1 -p 1883 -t status/device1   <span class="co"># gets "online" at once</span></code></pre>

    <h3>QoS &amp; Last Will</h3>
    <ul>
      <li><strong>QoS 0/1/2</strong> &mdash; QoS&nbsp;1 delivery with packet ids; inbound QoS&nbsp;2 completes the PUBREC / PUBREL / PUBCOMP handshake.</li>
      <li><strong>Last Will &amp; Testament</strong> &mdash; a client's will message is published automatically on an abnormal disconnect or keepalive expiry (enforced at 1.5&times; the keepalive).</li>
    </ul>
    <pre><code class="lang-bash"><span class="co"># will published if this client drops without a clean DISCONNECT</span>
mosquitto_sub -h 127.0.0.1 -p 1883 -t data \\
  --will-topic status/me --will-payload offline --will-qos 1</code></pre>

    <h3>Authentication</h3>
    <p>Require matching CONNECT credentials with two env vars:</p>
    <pre><code class="lang-bash">OXIDB_MQTT_PORT=1883 OXIDB_MQTT_USER=iot OXIDB_MQTT_PASSWORD=secret ./oxidb-server

mosquitto_pub -h 127.0.0.1 -p 1883 -u iot -P secret -t t -m hi</code></pre>

    <h3>Cross-protocol pub/sub</h3>
    <p>The broker shares its subscriber layer with <a href="/oximem/">OxiMem</a>: a message <code>PUBLISH</code>ed over the Redis (RESP) protocol can reach MQTT subscribers, and vice-versa &mdash; one pub/sub bus, two wire protocols.</p>
    <pre><code class="lang-bash"><span class="co"># publish from the Redis side...</span>
redis-cli -p 6379 PUBLISH sensors/temp "22.4"
<span class="co"># ...an MQTT client subscribed to sensors/temp receives it</span></code></pre>

    <p>For real-time document subscriptions over WebSocket (Firebase-style <code>onSnapshot</code>), see <a href="/streams/">Streams</a>.</p>
  </div>
</section>` }} />
}
