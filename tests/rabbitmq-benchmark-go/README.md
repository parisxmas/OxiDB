# OxiDB AMQP vs RabbitMQ — Go client benchmark

Same client (`rabbitmq/amqp091-go`), same code path, same scenarios; the only
variable is the broker behind the socket. OxiDB is spawned by the benchmark
itself (prefers `target/release/oxidb-server`; `cargo build --release -p
oxidb-server` first). RabbitMQ must already be running (`brew install
rabbitmq && rabbitmq-server`); if unreachable its column reads `n/a`.

```
go build . && ./rabbitmq-benchmark-go [-msgs N] [-durable-msgs N] [-lat-iters N] [-size BYTES] [-rabbit URL]
```

## Results — 2026-07-17, MacBook (arm64), OxiDB 0.36.11 vs RabbitMQ 4.x (brew)

body 100 B · throughput over 20 000 msgs (durable 5 000) · latency over 300 iters

| scenario                    |         OxiDB |      RabbitMQ | ratio |
|-----------------------------|--------------:|--------------:|------:|
| publish confirms, pipelined | 268 894 msg/s | 165 959 msg/s | 1.62x |
| publish confirm, sequential | 0.02/0.04 ms  | 0.03/0.08 ms  | 1.74x |
| durable publish, pipelined  |  47 983 msg/s | 149 830 msg/s | 0.32x |
| end-to-end throughput       | 352 214 msg/s | 215 835 msg/s | 1.63x |
| end-to-end latency          | 0.02/0.05 ms  | 0.03/0.05 ms  | 1.57x |

ratio > 1.00x = OxiDB faster (throughput: rate ratio; latency: p50 ratio).
Latency cells are p50/p99.

## Reading the one loss

**Durable publish (0.32x):** every OxiDB confirm sits behind a real
`F_FULLFSYNC` of the batch it arrived in (write-before-confirm, ADR-0016).
RabbitMQ classic queues flush lazily on an interval, so its persistent
confirm does not, on its own, prove the message is on stable storage — the
same macOS durability caveat the PostgreSQL EF bench documented.
Within-OxiDB history: this scenario ran at **264 msg/s** before publish
batching (one fsync per message); batching pipeline bursts into one
`insert_many` per burst took it to **~50k** (190x).

## How the latency loss became a win

End-to-end latency was **51 ms** p50 (a 50 ms poll tick), then **1.15 ms**
(adaptive 1 ms tick), and is now **0.02 ms** — ahead of RabbitMQ — via a
cross-thread wakeup: each consuming connection owns a nonblocking pipe whose
read end sits in its `poll(2)` set next to the TCP socket; a publish that
lands in a queue pokes every consumer's pipe, ending their kernel wait
instantly. Unix only; other platforms keep the adaptive tick.
