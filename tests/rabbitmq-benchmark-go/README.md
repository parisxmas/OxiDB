# OxiDB AMQP vs RabbitMQ — Go client benchmark

Same client (`rabbitmq/amqp091-go`), same code path, same scenarios; the only
variable is the broker behind the socket. OxiDB is spawned by the benchmark
itself (prefers `target/release/oxidb-server`; `cargo build --release -p
oxidb-server` first). RabbitMQ must already be running (`brew install
rabbitmq && rabbitmq-server`); if unreachable its column reads `n/a`.

```
go build . && ./rabbitmq-benchmark-go [-msgs N] [-durable-msgs N] [-lat-iters N] [-size BYTES] [-rabbit URL]
```

## Results — 2026-07-17, MacBook (arm64), OxiDB 0.36.12 vs RabbitMQ 4.x (brew)

body 100 B · throughput over 20 000 msgs (durable 5 000) · latency over 300 iters

| scenario                       |         OxiDB |      RabbitMQ | ratio |
|--------------------------------|--------------:|--------------:|------:|
| publish confirms, pipelined    | 246 295 msg/s | 164 224 msg/s | 1.50x |
| publish confirm, sequential    | 0.02/0.03 ms  | 0.03/0.06 ms  | 1.74x |
| durable publish, pipelined     |  53 724 msg/s | 159 614 msg/s | 0.34x |
| durable publish, 8 connections | 103 307 msg/s |  93 172 msg/s | 1.11x |
| end-to-end throughput          | 318 330 msg/s | 246 076 msg/s | 1.29x |
| end-to-end latency             | 0.02/0.05 ms  | 0.04/0.06 ms  | 1.52x |

ratio > 1.00x = OxiDB faster (throughput: rate ratio; latency: p50 ratio).
Latency cells are p50/p99.

## Reading the one loss

**Single-connection durable publish (0.34x):** every OxiDB confirm sits
behind a real `F_FULLFSYNC` of the batch it arrived in (write-before-confirm,
ADR-0016). RabbitMQ classic queues flush lazily on an interval, so its
persistent confirm does not, on its own, prove the message is on stable
storage — the same macOS durability caveat the PostgreSQL EF bench
documented. History: **264 msg/s** before publish batching (one fsync per
message) → **~53k** batching each connection's pipeline bursts (one
`insert_many` per burst).

**Under concurrency the loss flips**: with 8 publisher connections, a
cross-connection group commit (whoever finds no fsync in flight leads the
round; everyone arriving during it shares the next one) takes OxiDB to
103k msg/s aggregate while RabbitMQ *drops* to 93k — 1.11x ahead, honest
fsync included.

## How the latency loss became a win

End-to-end latency was **51 ms** p50 (a 50 ms poll tick), then **1.15 ms**
(adaptive 1 ms tick), and is now **0.02 ms** — ahead of RabbitMQ — via a
cross-thread wakeup: each consuming connection owns a nonblocking pipe whose
read end sits in its `poll(2)` set next to the TCP socket; a publish that
lands in a queue pokes every consumer's pipe, ending their kernel wait
instantly. Unix only; other platforms keep the adaptive tick.
