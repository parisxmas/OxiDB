# ADR-0015: Durable MQTT — persistent sessions and honest QoS 1

**Status:** Accepted — 2026-07-17 (Phase 1 in progress)

## Context

OxiDB ships an MQTT 3.1.1 broker (`oxidb-server/src/mqtt.rs`, ~469 lines). It
rides on `OxiMemStore`'s pub/sub: a `SUBSCRIBE` hands the client an
`mpsc::channel`, and `PUBLISH` fans a message out to whatever channels are
**live at that instant** (`oximem.rs::publish`). Retained messages sit in an
in-memory `HashMap` (`oximem.rs:171`). This is a fire-and-forget broker, and
its CONNACK says so in its own words: `session_present=0 (stateless broker by
design)`.

**The broker over-promises QoS.** The delivery loop (`mqtt.rs` ~258) sets the
QoS-1 wire flag `0x02` and allocates a packet id, and the site
(`website-react/app/mqtt/page.tsx`) advertises *"QoS 1 delivery with packet
ids"*. But nothing backs the promise:

- a delivered QoS-1 message is **not stored pending its PUBACK**, so it can
  never be redelivered;
- an inbound PUBACK from a subscriber is **read and discarded** — there is no
  inflight record for it to acknowledge;
- `publish()` only reaches subscribers with a **live channel**, so a subscriber
  that is offline for even a moment loses the message entirely;
- `clean_session=false` does nothing — CONNACK always reports
  `session_present=0`, so there is no session to resume.

The wire says at-least-once; the behaviour is at-most-once. That is a
correctness defect of the same shape the TLP oracle (ADR none; `tlp_oracle.rs`)
and the S3 ETag fix (ADR-none; v0.36.2) addressed elsewhere: *the system claims
a guarantee it does not keep.*

The question that prompted this was whether to add a **fourth engine** — a
Kafka- or RabbitMQ-style message broker. We decided against it (see
Alternatives). The messaging surface OxiDB already has is the one to make
honest first.

### What MQTT 3.1.1 actually requires

QoS 1 ("at least once") obliges the broker to:

1. store an outbound message with its packet id until the subscriber PUBACKs it;
2. retransmit it (with the DUP flag set) if the connection breaks before the ack;
3. keep, for a session with `clean_session=false`, the subscription set **and**
   any messages that arrived while the client was disconnected, and replay them
   on reconnect (`session_present=1`).

QoS 2 ("exactly once") adds the PUBREC/PUBREL/PUBCOMP two-phase handshake and a
dedup set keyed by packet id. The broker already completes the *inbound* QoS-2
handshake; it does not persist the *outbound* side.

## Decision

**Do not add a broker engine. Make the existing MQTT broker keep its word,**
backed by durable state, and keep it **node-local** (see Cluster).

Concretely:

1. **A session store.** Keyed by client id, holding the subscription set, the
   inflight outbound messages (packet id → message, awaiting PUBACK), and a
   queue of messages that arrived while the client was offline. A session with
   `clean_session=true` is discarded on disconnect; one with `false` survives.

2. **Real QoS 1 delivery.** On `PUBLISH` to a subscriber, record the message as
   inflight before writing it. On PUBACK, clear it. On reconnect (or a periodic
   sweep), retransmit anything still inflight with DUP set. This is the whole of
   at-least-once, and it is small.

3. **Offline queueing.** `publish()` currently drops a message with no live
   subscriber. Instead, for every persistent-session subscriber whose filter
   matches, enqueue it; deliver on reconnect. This is what makes
   `session_present=1` mean anything.

4. **Durability tier, opt-in.** The session store is in-memory by default (a
   restart is a clean slate, which is the current behaviour and honest about
   it). With `OXIDB_MQTT_PERSIST=1`, sessions, inflight messages, the offline
   queue **and retained messages** are written through the document engine's
   WAL — the same seal-not-truncate substrate the doc engine already uses
   (`OxiDb::online_checkpoint`, ADR-none; v0.36.1) — so QoS-1 messages and
   retained state survive a crash. Off = zero cost, exactly as SQL/TSDB are off
   by default.

5. **Honest defaults and honest docs.** Whatever is not yet implemented is
   described accurately. Until persistence lands, the site says "QoS 1 in-memory
   (no persistent session across restart)". A broker that under-promises and
   over-delivers is the only acceptable direction; the reverse is the bug we are
   fixing.

### Storage shape

The session store is a `HashMap<ClientId, Session>` behind an `RwLock`, mirrored
to the document engine when persistence is on. It is **not** a new on-disk
format: a session is a document, an inflight message is a document, the offline
queue is an ordered list of documents. Reusing the doc engine's collection +
WAL machinery means crash-recovery, checkpointing and PITR come for free and
there is no second durability path to test.

Retained messages move from the bare in-memory `HashMap` into the same store, so
`retain_set` is durable under `OXIDB_MQTT_PERSIST` and a broker restart no longer
forgets the last retained value on every topic.

## Cluster

**Node-local, deliberately — the same reason SQL/TSDB messaging is node-local.**
MQTT sessions are per-connection and a connection lives on one node; there is no
correct way to make an `mpsc`-style delivery loop span nodes without a shared
log, and a shared log is exactly the Kafka-shaped design we are declining. A
client reconnecting to a *different* node after a failover does not resume its
session — the same limitation every non-clustered MQTT broker has, and
acceptable because MQTT here is an ingestion edge, not the system of record. The
durable data (readings, rows, blobs) is already replicated by the engines behind
the broker.

If cluster-wide sessions are ever wanted, that is a separate ADR and it is the
Kafka-log discussion resurfacing — which is the argument for keeping this one
node-local and small.

## Alternatives considered

**Add a Kafka-style log engine (a real 4th engine).** The most architecturally
tempting: the WAL, sealed segments and the GSN-stamped PITR archive are already
an append-only, offset-addressable, retention-bounded segment log — Kafka's
partition log in miniature. **Rejected for cluster reasons.** Kafka's value is
per-partition leadership with ISR replication; layering that on single-leader
openraft 0.9 either routes every message through one Raft leader (throwing away
the partition parallelism that is the whole point) or fights the consensus
model. A "Kafka engine" would be either fake-Kafka or a cluster liability. MQTT
being node-local is not an accident to fix; it is the honest shape.

**Add a RabbitMQ-style durable queue engine.** A queue with ack, redelivery and
dead-lettering is essentially an extended MQTT QoS 1 — which means this ADR
*builds its foundation anyway*. Once persistent sessions, an inflight store and
WAL-backed redelivery exist, a durable-queue surface is an increment on top, not
a separate engine. Doing MQTT-honest first is strictly the cheaper path to both.
Deferred, not rejected: revisit after Phase 3 if a queue API is actually asked
for.

**Adopt AMQP or the Kafka wire protocol.** Either would let existing clients
connect unmodified, and either is a very large amount of work for a wire format
we have no user asking for. MQTT is the protocol OxiDB already speaks and the one
the ColdChain demo uses. No.

**Just correct the docs and leave the broker fire-and-forget.** The one-line
option: change the site to say "QoS 0 only; QoS 1/2 accepted but delivered
at-most-once". Honest, and it is the immediate stopgap (Phase 1). But QoS 1 is
what makes an ingestion broker trustworthy under a flaky network — precisely the
ColdChain scenario, where a probe's reading must not vanish because the link
blinked — so correcting the docs is the floor, not the ceiling.

## Consequences

**Positive.**
- The broker keeps its advertised guarantee; the wire and the behaviour agree.
- ColdChain becomes genuinely reliable: a reading survives a subscriber blip
  instead of being silently dropped.
- Retained state survives a restart under persistence.
- No new engine, no new wire protocol, no cluster liability, no second
  durability path — it reuses the doc engine's WAL.
- A future durable-queue / Kafka-log surface, if ever wanted, has its
  foundation already built and tested.

**Negative / cost.**
- Real state in a broker that was pleasantly stateless: memory grows with
  offline queues, so a `max-queued` bound per session is required (a slow
  consumer must not OOM the broker — the same discipline as a bounded WAL).
- QoS 2 outbound is more work than QoS 1 (the two-phase handshake plus a dedup
  set); Phase 2, and only if asked for.
- The persistence path needs its own crash test — a QoS-1 message must survive
  a SIGKILL between "PUBLISH accepted" and "PUBACK received", the messaging
  analogue of the doc-engine online-checkpoint crash test (v0.36.1). A test that
  passes without a SIGKILL'd subprocess proves nothing, exactly as there.

## Rollout

- **Phase 1 — honesty (now).** Correct the public claim to match today's
  behaviour, and land the in-memory session store: persistent sessions,
  inflight tracking, real QoS-1 redelivery on reconnect, offline queueing with a
  bound. No disk yet. Restart is still a clean slate, and now the docs say so.
- **Phase 2 — durability.** `OXIDB_MQTT_PERSIST=1`: sessions, inflight, offline
  queue and retained messages through the doc-engine WAL. Crash test with a
  SIGKILL'd subscriber.
- **Phase 3 — QoS 2 outbound, and (only if asked) a durable-queue API** on top
  of the now-existing machinery.

Off by default at every phase where it costs anything; a broker with no
persistent-session clients behaves exactly as today.
