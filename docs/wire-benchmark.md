# OxiWire vs the PostgreSQL wire

Both protocols reach the **same SQL engine**, in the same process, over the
same data. What is left is the wire and the per-request work each listener does
around it.

```bash
OXIDB_SQL=1 OXIDB_ADDR=127.0.0.1:4444 OXIDB_PG_PORT=5432 oxidb-server &
cargo run --release -p oxidb-server --example wire_bench
```

## Method

Both clients are hand-rolled inside
[`oxidb-server/examples/wire_bench.rs`](../oxidb-server/examples/wire_bench.rs)
and kept deliberately thin. Comparing `oxidb-client` against `psycopg` would
measure two libraries and two languages; two minimal clients in one binary
measure the wires. Each connects once, sends, reads the whole reply, and
**decodes every cell** — so neither side is credited for skipping work the
other has to do.

Two shapes: **sequential** (one connection, one request in flight) for latency,
and **concurrent** (`--mode conc`, N connections each on its own thread) for
throughput. Not measured: TLS, and auth (a per-connection cost on both).

Numbers below are from an M-series macOS laptop, 10,000-row table, release
build. Treat the *ratios* as the result; absolute throughput is hardware.

## Results

| Workload | OxiWire | PG simple | PG extended | PG ext ÷ OxiWire |
|---|---:|---:|---:|---:|
| `SELECT 1` (round trip) | 69.6k/s | 85.3k/s | 75.2k/s | **1.08×** |
| Point `SELECT` by primary key | 59.2k/s | 64.6k/s | 59.8k/s | **1.01×** |
| `SELECT` 100 rows | 5.1k/s | 6.0k/s | 5.9k/s | **1.15×** |
| `SELECT` 1000 rows | 1.19k/s | 1.99k/s | 1.80k/s | **1.51×** |
| Aggregate over 10k rows | 5.44k/s | 5.46k/s | 5.49k/s | **1.01×** |
| Point `SELECT`, parameterized | 58.0k/s | 64.8k/s | 58.9k/s | **1.02×** |
| Single-row `INSERT` | 263/s | — | 267/s | **1.02×** |

Bytes on the wire, per operation, for the 1000-row read: OxiWire 29.8 KB in,
PostgreSQL 42.8 KB. PostgreSQL sends **44% more bytes and still finishes
faster**.

## What the numbers say

**Round trips are a wash.** For small results both protocols are bounded by the
same thing — one syscall out, one back, and the engine in between. The
PostgreSQL *simple* query is slightly ahead because its request is smaller (14
bytes for `SELECT 1` against OxiWire's 49-byte JSON envelope); the extended
protocol gives that back by sending Parse/Bind/Execute/Sync where the simple
one sends a single message.

**Large result sets favour PostgreSQL, despite more bytes.** OxiWire has to
build one JSON document for the whole result — allocate, escape, and then have
the client parse it back into a tree. PostgreSQL streams a row at a time as
length-prefixed text cells, which is cheaper to produce and cheaper to consume
even though it is bulkier. That gap grows with result size: level at one row,
1.15× at a hundred, 1.5× at a thousand.

**Writes are identical, because neither protocol is the cost.** A single-row
`INSERT` lands at ~265/s and ~4 ms on both wires — that is the WAL's fsync, not
the encoding. It is also the best evidence that this benchmark is measuring
what it claims to: when the engine dominates, the two wires converge exactly.

## Concurrency

`--mode conc` runs the same workloads from N connections at once, each with its
own thread, closed-loop. Both listeners are thread-per-connection in standalone
mode (`OXIDB_POOL_SIZE` applies only to cluster mode), so neither side is
throttled by a pool the other does not have. 10 cores, 2 s per cell.

**Point `SELECT` — ops/sec, and scaling against one connection:**

| Connections | 1 | 2 | 4 | 8 | 16 | 32 |
|---|---:|---:|---:|---:|---:|---:|
| OxiWire | 43.5k | 82.5k | 109k | 124k | 155k | 155k |
| PG extended | 44.3k | 85.4k | 113k | 126k | 160k | 158k |
| scaling | 1.00× | 1.9× | 2.5× | 2.8× | **3.6×** | 3.6× |

**`SELECT` 100 rows:**

| Connections | 1 | 2 | 4 | 8 | 16 | 32 |
|---|---:|---:|---:|---:|---:|---:|
| OxiWire | 5.1k | 6.4k | 8.3k | 7.9k | 7.9k | 7.9k |
| PG extended | 5.9k | 7.1k | 8.3k | 8.2k | 7.9k | 8.0k |

**Single-row `INSERT`** (after group commit — see below for the before):

| Connections | 1 | 2 | 4 | 8 | 16 | 32 |
|---|---:|---:|---:|---:|---:|---:|
| OxiWire ops/sec | 260 | 262 | 522 | 1005 | 1257 | 1156 |
| PG extended ops/sec | 261 | 263 | 517 | 1000 | 1236 | 1345 |
| scaling | 1.00× | 1.01× | 2.0× | 3.9× | **4.8×** | 4.4× |
| p50 latency | 4.0 ms | 7.9 ms | 8.0 ms | 7.9 ms | 12 ms | 25 ms |

### What concurrency shows

**The wire stops mattering under load.** At one connection PostgreSQL leads on
the 100-row read (5.9k vs 5.1k); by four connections both sit at ~8.3k and stay
there. Whatever each protocol costs per request, it is not the constraint once
the engine is busy — the two curves converge because they are queueing for the
same lock.

**Reads scale to about 3.6×, not 10×,** on a 10-core machine, and stop at 16
connections. The SQL engine serializes on one mutex (`SqlEngine.inner`), so
past that point extra connections buy latency, not throughput: p99 on the point
read goes from 22 µs at one connection to ~800 µs at 32.

**Writes scale to ~4.5×, and both wires scale identically** — because what they
are scaling against is the WAL flush, not the protocol. Past 8 connections p50
stops tracking `N × 4 ms` and settles near two flush times: writers that arrive
while one flush is in progress are made durable by the next one, together.

## The gap this found: no group commit

The first version of this benchmark showed writes **flat at ~266/s from 1 to 32
connections**, with p50 growing almost exactly linearly (4 ms → 91 ms ≈ 32 ×
2.8 ms) — textbook pure serialization. `Wal::append` fsynced per record and was
called with the engine mutex held, so concurrent commits queued rather than
batched. The document engine had group commit; the SQL engine did not, and
nothing had made that measurable before.

The fix splits the write in two: append and apply under the engine lock, then
**flush after releasing it**, through a duplicated WAL descriptor held by a
`CommitGate`. One writer is elected to fsync; everyone whose record was already
on disk when that fsync began is covered by it and returns without flushing
again. Throughput went from flat to 4-5× (~1.2k/s at 16 connections).

Two things the table above shows honestly:

- **One and two connections gain nothing.** A flush may only claim what was
  written *before it started* — a record appended mid-fsync may or may not be
  included, so it is not credited. With two closed-loop writers the second one
  always appends after the first one's flush began, so it flushes on its own.
  Batching needs a group to accumulate during the previous flush, which is why
  the curve turns on at four.
- **The durability contract is unchanged.** A write still returns only after a
  flush covering its own sequence. What is newly observable is that another
  connection can read a write between its apply and its flush — the same window
  PostgreSQL has, and the one the document engine already accepts.

The measurement also caught a bug in the fix. The watermark published under the
lock was the sequence the *next* append would use rather than the last one
written, so every second write found itself already "durable" and skipped its
fsync — which showed up as single-connection throughput nearly doubling, to
495/s, where batching cannot possibly help. No functional test could have caught
it (a clean shutdown flushes the OS cache anyway; only power loss would tell),
so the arithmetic is pinned directly by an invariant test instead: the gate may
never claim a sequence the WAL has not written.

## The bug this found

The first run had PostgreSQL **slower** on every multi-row workload — 0.43× on
1000 rows. That was not the protocol; it was this server writing each `DataRow`
straight to the socket. A result set is one message *per row*, so a
thousand-row answer went out as a thousand `write` syscalls, and with
`TCP_NODELAY` set, close to a thousand packets.

Buffering the connection's output and flushing where the protocol says to
(after `ReadyForQuery`, on `Flush`) took the 1000-row read from 526 to 1796
ops/sec — **3.4×** — and turned a loss into a win. Real PostgreSQL has always
done this; the listener now does too.

Worth noting how it was found: the first suspicion was the *client* reading
unbuffered, which would have been a measurement artifact rather than a real
bug. Buffering the client changed nothing, which is what pointed at the server.
