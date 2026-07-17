# OxiDB AMQP vs RabbitMQ — Go client benchmark

Same client (`rabbitmq/amqp091-go`), same code path, same scenarios; the only
variable is the broker behind the socket. OxiDB is spawned by the benchmark
itself (prefers `target/release/oxidb-server`; `cargo build --release -p
oxidb-server` first). RabbitMQ must already be running (`brew install
rabbitmq && rabbitmq-server`); if unreachable its column reads `n/a`.

```
go build . && ./rabbitmq-benchmark-go [-msgs N] [-durable-msgs N] [-lat-iters N] [-size BYTES] [-rabbit URL]
```

## Results — 2026-07-17, MacBook (arm64), OxiDB 0.36.10 vs RabbitMQ 4.x (brew)

body 100 B · throughput over 20 000 msgs (durable 5 000) · latency over 300 iters

| scenario                    |         OxiDB |      RabbitMQ | ratio |
|-----------------------------|--------------:|--------------:|------:|
| publish confirms, pipelined | 250 627 msg/s | 151 337 msg/s | 1.66x |
| publish confirm, sequential | 0.02/0.03 ms  | 0.03/0.07 ms  | 1.94x |
| durable publish, pipelined  |  52 665 msg/s | 155 365 msg/s | 0.34x |
| end-to-end throughput       | 349 929 msg/s | 248 458 msg/s | 1.41x |
| end-to-end latency          | 1.15/1.21 ms  | 0.10/0.23 ms  | 0.09x |

ratio > 1.00x = OxiDB faster (throughput: rate ratio; latency: p50 ratio).
Latency cells are p50/p99.

## Reading the two losses

- **durable publish (0.34x):** every OxiDB confirm sits behind a real
  `F_FULLFSYNC` of the batch it arrived in (write-before-confirm, ADR-0016).
  RabbitMQ classic queues flush lazily on an interval, so its persistent
  confirm does not, on its own, prove the message is on stable storage —
  the same macOS durability caveat the PostgreSQL EF bench documented.
  Within-OxiDB history: this scenario ran at **264 msg/s** before publish
  batching (one fsync per message); batching pipeline bursts into one
  `insert_many` per burst took it to **52.7k** (200x).
- **end-to-end latency (0.09x):** OxiDB's connection loop is a poll model —
  the delivery tick is 1 ms while deliveries flow (was 50 ms, then 5 ms).
  RabbitMQ pushes on the write path directly. Closing the last ~1 ms would
  take a cross-thread wakeup of the consumer's socket loop; the tick is the
  deliberate cheap version. History: 51 ms → 5.7 ms → 1.15 ms p50.
