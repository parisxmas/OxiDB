# ADR-0016: AMQP 0-9-1 — the RabbitMQ protocol, on the broker we already have

**Status:** Accepted — 2026-07-17 (Phase 1 shipped in 0.36.7, Phases 2–3 in 0.36.8 — complete)

## Context

ADR-0015 rebuilt OxiDB's MQTT broker into an honest one: persistent sessions,
write-before-ack QoS 1, exactly-once QoS 2, all crash-tested against SIGKILL.
Its Alternatives section considered a RabbitMQ-style durable queue and deferred
it: *"once persistent sessions, an inflight store and WAL-backed redelivery
exist, a durable-queue surface is an increment on top, not a separate engine —
revisit if a queue API is actually asked for."*

It has now been asked for, in protocol form: **can existing RabbitMQ clients
talk to OxiDB?** That means AMQP 0-9-1 — the protocol pika (Python), amqplib
(Node) and RabbitMQ.Client (.NET) speak. It is not an MQTT extension; it is a
second wire protocol with one genuinely new semantic.

**What MQTT cannot express and AMQP exists for: competing consumers.** An MQTT
subscription copies every message to every matching subscriber. An AMQP queue
gives each message to exactly ONE of its consumers — work distribution, the
reason RabbitMQ sits between producers and worker pools. OxiDB has pub/sub
(MQTT, OxiMem) but no work queue; this adds one, reachable by clients that
already exist.

Everything else the queue needs — durable storage through the doc engine's
WAL, write-before-ack, ack-exact deletion, bounded queues, crash recovery, the
SIGKILL test discipline — was built and tested in ADR-0015 Phases 1–3. The new
work is the wire codec and the queue object; the durability substrate is
shared.

## Decision

Add an **AMQP 0-9-1 listener** (`OXIDB_AMQP_PORT`, off by default, zero cost
when off), implementing a deliberately scoped subset that makes the mainstream
clients work unmodified, backed by the same document-engine persistence the
MQTT broker uses.

### Persistence follows the protocol, not an env var

MQTT needed `OXIDB_MQTT_PERSIST` because MQTT 3.1.1 has no wire-level concept
of storage. **AMQP does**: a queue is declared `durable` and a message is
published with `delivery_mode=2` (persistent) — the client states its intent in
the protocol. So there is no `OXIDB_AMQP_PERSIST`: a durable queue with
persistent messages is durable, period, mirrored to an `_amqp` collection; a
transient queue lives in memory. Publisher confirms (`Confirm.Select`) follow
the write-before-ack rule: the confirm is sent only after the fsync'd insert,
exactly as MQTT's PUBACK is.

### Scope

**Phase 1 (this ADR's commitment):**
- Connection handshake (Start/StartOk with PLAIN, Tune/TuneOk, Open/OpenOk),
  channels, heartbeats (honouring the client's TuneOk value), clean close.
- Default exchange (`""` → route by queue name) and client-declared **direct**
  exchanges with `Queue.Bind`.
- `Queue.Declare` (durable, exclusive, auto-delete, server-named queues,
  passive), `Basic.Publish` (multi-frame bodies), `Basic.Consume`/`Deliver`
  with per-channel delivery tags, `Basic.Ack` (incl. `multiple`), `Basic.Get`,
  `Basic.Cancel`, publisher confirms.
- Competing consumers: round-robin across a queue's consumers; unacked
  messages requeue (redelivered=true) when their channel or connection dies.
- Durable queues + persistent messages survive SIGKILL; recovered messages are
  flagged `redelivered=true` — conservatively, since "may have been delivered
  before the crash" is the promise the flag exists to keep, and tracking exact
  delivery state would cost a disk write per delivery.
- Auth: `OXIDB_AMQP_USER`/`OXIDB_AMQP_PASSWORD` (PLAIN); open when unset —
  the same posture as the MQTT listener.

**Phase 2:** topic + fanout exchanges, `Basic.Qos` prefetch, `Basic.Nack`/
`Reject`, mandatory-flag `Basic.Return`.

**Phase 3:** the MQTT ↔ AMQP bridge — MQTT topics into a topic exchange and
back (`/` ↔ `.`), the way RabbitMQ's own MQTT plugin does it, so a sensor
publishes MQTT and a worker pool consumes AMQP from one binary.

**Explicitly out, stated rather than implied:** the `tx` class, headers
exchanges, exchange-to-exchange bindings, per-message TTL/priority,
dead-letter arguments, alternate exchanges, AMQP 1.0 (a different protocol
entirely), streams, and any management UI. Declaring an exchange type Phase 1
does not support is answered with a channel error naming this ADR, not with
silent acceptance.

### Cluster

Node-local, same reasoning as ADR-0015's Cluster section verbatim: queue
consumers are connections, connections live on one node, and a cross-node
queue is the Kafka-shaped design both ADRs decline. The durable data behind
the queue is what replicates.

## Consequences

**Positive.** Existing RabbitMQ client code — tutorials included — points at
OxiDB and works; OxiDB gains a work-queue semantic it had no surface for; the
durability substrate, crash-test discipline and bounded-queue rules are shared
with MQTT rather than duplicated.

**Negative / cost.** AMQP 0-9-1 framing (channels, field tables, content
headers, bit-packed flags) is substantially heavier than MQTT's — roughly
double the wire code. The subset boundary must be policed honestly: a client
using an unimplemented feature must get a protocol error, not silence. Two
brokers now share the doc engine; their collections (`_mqtt`, `_amqp`) stay
separate so neither can corrupt the other's recovery.

## Verification

The same discipline as ADR-0015: end-to-end against a real client (pika,
vendored into `target/` for tests, skipping loudly when absent — with the
`have_mosquitto` exit-code lesson applied), SIGKILL crash tests for the
durable path, and mutation checks on the two load-bearing behaviours
(write-before-confirm; ack-exact deletion) using the rebuild-the-bin-first
method.
