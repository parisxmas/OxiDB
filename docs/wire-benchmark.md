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

One connection, one request in flight, sequential: the honest shape for a
latency comparison. Not measured: concurrency, TLS, and auth (a per-connection
cost on both).

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
