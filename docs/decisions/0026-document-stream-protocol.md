# ADR-0026: OxiStream — streaming result frames for the document engine

**Status:** Proposed
**Supersedes:** —
**Related:** [ADR-0023](0023-postgres-wire-protocol.md) (the protocol this
borrows its shape from, and the benchmark that motivates it),
[ADR-0017](0017-mvcc-lite-read-snapshots.md) (the read snapshot a stream
inherits),
[`oxidb-server/src/oxiwire.rs`](../../oxidb-server/src/oxiwire.rs) (the value
encoding, reused unchanged),
[`src/jsonb_oxiwire.rs`](../../src/jsonb_oxiwire.rs) (JSONB→wire without a
`Value` tree),
[`docs/wire-benchmark.md`](../wire-benchmark.md) (the measurement),
[`docs/protocol-reference.md`](../protocol-reference.md) (the reference this
must finally extend).

## Context

The native protocol frames every message as `[u32 LE length][payload]` and
picks the payload encoding from the first byte —`{`/`[` for JSON, `0xDB` for
OxiWire, anything else MsgPack (`protocol.rs:56`). The binary encoding is not
missing: it exists, it is fuzzed, and the Go, Java and .NET clients speak it.
What is missing is the *shape* of a reply.

Today a request produces exactly one response frame, and for `find` that frame
holds the entire result:

```
[u32 length][0xDB][status][TAG_ARRAY][count][doc][doc][doc]…
```

Three things follow from that, and all three are the reason this ADR exists.

**A result set cannot exceed 16 MiB.** The cap is enforced on the read side by
the server (`protocol.rs:18`), the async listener (`async_protocol.rs:11`) and
every client (`oxidb-client/src/lib.rs:30`). A query whose matches exceed it
produces a frame no client will read. There is no cursor, no batch, no
continuation command — those words do not appear in `handler.rs`.

**The client cannot touch the first document until the last one has arrived.**
It must read the whole frame, then decode an array of `count` values.

**Keys are re-sent for every document.** `TAG_MAP` encodes each pair as
`[u32 len][key bytes][value]`, so a five-field document with eight-character
keys spends `5 × (4 + 8) = 60` bytes per document on keys alone, however many
documents share that key set. That is arithmetic from the format, not a
measurement — Phase 2 below turns it into one before acting on it.

[`docs/wire-benchmark.md`](../wire-benchmark.md) measured what this costs
against a protocol that made the other choice. Same SQL engine, same process,
same data, 1000-row read: the native wire sends 29.8 KB and finishes 1.19k
requests/sec; the PostgreSQL wire sends 42.8 KB — **44% more bytes** — and
finishes 1.80k/sec, **1.51× faster**. The gap is absent at one row, 1.15× at a
hundred, 1.51× at a thousand: it tracks result size, not per-request cost. So
the deficit is not encoding density, and switching the remaining clients from
JSON to OxiWire will not close it. PostgreSQL streams a row at a time behind a
row description sent once; we build one object for the whole result.

Two pieces of existing machinery decide most of this design.

**The scan is already an iterator, and it holds no lock.**
`find_oxiwire_postfilter` (`btree_collection.rs:815`) walks matches through
`scan_bytes_chunked_while` and transcodes each one JSONB→OxiWire with no
intermediate `Value`. Its callback runs *outside* the data lock —
`scan_chunked_while` takes the lock only around each `get`, and
`btree_storage.rs:1524` says so explicitly ("the callback must never run under
it"). Writing to a socket from inside that callback is therefore safe, which is
the single fact that makes streaming a small change rather than an engine
rewrite. The only thing standing between today and a stream is
`buf.extend_from_slice(&oxi)` — a buffer that is never flushed until the end.

**Capability negotiation already exists.** `hello.rs` picks the highest
mutually supported entry of `SUPPORTED_WIRE_VERSIONS` (currently `&[1]`) and
parks it in `session.wire_version`, defaulting to 1 for clients that never say
hello. It already refuses cleanly when there is no overlap.

And there is precedent for multi-frame replies on this connection: `watch`
writes a frame per change event, plus an `{"event":"overflow"}` frame when
backpressure drops events (`main.rs:505`).

## Decision

Add **OxiStream**, wire version 2 of the native protocol: a reply to a
result-producing command may be a *sequence* of frames instead of one. The
value encoding is OxiWire, unchanged and byte-for-byte reused. No new port, no
new listener, no new encoder.

This deliberately differs from ADR-0016 and ADR-0023, which each added an
independent listener. Those adopted somebody else's protocol; this evolves our
own, and evolving it in place keeps SCRAM, RBAC, database routing, the session
transaction state and every existing command exactly where they are.

### Negotiation is HELLO, and nothing else

`SUPPORTED_WIRE_VERSIONS` becomes `&[1, 2]`. A client that offers 2 and gets 2
may receive stream replies; a client that offers only 1, or never calls
`hello`, gets today's single-frame reply forever. That is the entire
compatibility story: the existing Go, Java and .NET clients are untouched
because they never ask for 2.

Requests do not change at all — still one `0xDB` OxiWire frame, still the same
command objects. Only the reply may take a new shape.

### The first byte of each reply frame says which shape it is

```
0xDB  → today's envelope: [0xDB][status][value]        (unchanged)
0xDC  → a stream frame:   [0xDC][frame type][body]     (new)
```

Clients already dispatch on this byte — the Java client sniffs it today
because `hello` answers in JSON regardless of the request encoding
(`OxiDbClient.java:127`). So a v2 client reads one byte and knows whether it
holds a whole answer or the first piece of one.

Consequently the server is free to answer any single request either way, and
that freedom is load-bearing: see "queries that cannot stream" below.

### Frame types

| Type | Body | Meaning |
|---|---|---|
| `0x01` Shape | `[u32 shape id][u32 key count][(u32 len + bytes)…]` | Names a key set. Sent before the first batch that uses it. |
| `0x02` Batch | `[u32 shape id][u32 doc count][values…]` | Documents sharing one shape; each document is its field *values* in shape order, each a normal OxiWire value. No keys. |
| `0x03` Docs | `[u32 doc count][TAG_MAP document…]` | Documents carrying their own keys — byte-identical to what today's array holds. |
| `0x04` End | `[u64 total count][u32 flags]` | The stream completed. Always last on success. |
| `0x05` Error | `[u32 len][message bytes]` | The stream failed. Always last on failure. |

`Batch` and `Docs` both exist because the shape dictionary only pays when key
sets repeat. The server keeps a per-stream shape cache of at most 64 entries: a
document whose key set is cached goes into a `Batch`; a new key set emits a
`Shape` and then a `Batch`; once the cache is full, everything remaining goes
into `Docs` frames. The rule is mechanical and server-side — a client
implements both frame types and needs no heuristics of its own. Worst case (all
documents structurally distinct) costs the `Docs` frame header and nothing
else, so the format never loses to today's encoding.

### Batches, not one frame per document

PostgreSQL sends one `DataRow` message per row, and
[`wire_bench.rs:184`](../../oxidb-server/examples/wire_bench.rs) notes what
that costs a client that does not buffer: two syscalls per row. We control both
ends, so we take the streaming without the per-row framing — the server fills a
frame up to a flush threshold (64 KiB, one tunable) and writes it. Fewer
syscalls than PostgreSQL, and the client still starts work on batch one while
batch two is being scanned.

The 16 MiB cap then applies per frame instead of per result, and stops being a
ceiling on how much a query may return. It stays enforced, unchanged, as the
sanity bound it was meant to be.

### What a stream means, exactly

`scan_chunked_while` fixes its upper bound on the first pass, so a stream
reports the documents present when the scan began and skips ones deleted while
it ran — the same snapshot a `find` has today under ADR-0017, now merely
observed over a longer interval. This is a contract, and it belongs in
`protocol-reference.md` next to the frames.

### Queries that cannot stream still answer in stream frames

`find_oxiwire_postfilter` and `find_oxiwire_bytes` both decline `sort`, `skip`
and `limit`, because ordering is owned by the `Value` path. A sort is blocking
by nature: no ordered row can be emitted before the last candidate is seen.

Those queries still reply with `Shape`/`Batch`/`End` — the server simply
buffers internally and emits the frames at the end. The client sees one shape
of reply for `find` and cannot tell which path ran. Streaming stays what it
should be: a server-side optimisation, not a second client-visible mode.

### Failure after the first byte is a new state, and it is explicit

Today a `find` either wholly succeeds or wholly fails. Under OxiStream a scan
can fail after batches have already been delivered, and the client will hold
documents *and* an error. The `Error` frame makes that explicit, and clients
must surface both rather than discarding the partial result silently. An error
raised *before* any batch is written is sent as an ordinary `0xDB` error
envelope, so the common case — bad query, missing collection, permission denied
— looks exactly as it does today.

### No mid-stream cancellation in v1

The connection is busy for the duration of a stream, as it is for any request
today. A client that wants out closes the socket: the next frame write fails,
the scan callback returns `Ok(false)`, and the scan stops at the next document
rather than running to completion. A slow client is handled by the same
mechanism with a write timeout. PostgreSQL's answer — a second connection
carrying a cancel request — is a larger commitment than this needs; bounded
queries should use `limit`, which takes the buffering path anyway.

### Scope, in phases

1. **Frames, and `find` on the streaming path.** `Docs` + `End` + `Error`
   only, no shape dictionary. Server side is the flush inside the existing
   callback; client side is a reference implementation in `oxidb-client`,
   which is the crate `oxibase` and `oxidb-mcp` already depend on and is
   JSON-only today despite calling itself an OxiWire client.
2. **The shape dictionary.** Ship `Shape`/`Batch` only after Phase 1 measures
   what fraction of a real result is key bytes. The arithmetic above predicts a
   large saving for homogeneous collections; if the measurement disagrees,
   Phase 2 is dropped and `Docs` remains the only document frame.
3. **SQL `SELECT` over the same frames.** Rows are homogeneous by construction,
   so a result needs exactly one `Shape` — this is where the wire-benchmark gap
   was measured, and the direct comparison against the PostgreSQL listener
   re-runs unchanged.
4. **The other clients.** Go (which uses OxiWire for `find` and JSON for the
   other hundred methods), Java, .NET, then Python.

`watch` is left alone. It already streams, in JSON, with its own event
envelope; moving it onto these frames is a later and separate question.

## Consequences

**The protocol reference has to grow a binary section.** OxiWire is absent from
`protocol-reference.md` entirely, and `client-libraries.md` still says every
client speaks "length-prefixed JSON". Both are wrong today and would be more
wrong after this. A cross-client conformance corpus — one file of frames, every
client decoding it and agreeing — is the cheap defence against a fourth
hand-written codec drifting from the other three.

**Two reply shapes for one command.** Justified by first-byte dispatch, which
clients already do, but it is real complexity and it is the price of not
breaking the three shipped binary clients.

**Partial results become observable.** Handled by the `Error` frame, but it is
a genuine behaviour change for anyone writing a v2 client, and the reason
Phase 1 carries a test that kills a scan halfway.

**A long stream occupies its connection.** True of every request today; streams
just make the interval longer, and pooled clients should size for it.

**What this does not fix.** Round-trip latency for small results is bounded by
the syscall pair on both wires — the benchmark shows the two protocols level at
one row and level again on `INSERT`, where the WAL fsync dominates. This ADR
targets the part that grows with result size, and claims nothing about the
part that does not.
