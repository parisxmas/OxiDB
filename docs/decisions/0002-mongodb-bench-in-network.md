# ADR 0002 — MongoDB comparison benchmark runs in-network, not from the host

**Status:** Accepted
**Date:** 2026-05-14
**Related:** PR #5 (in-network benchmark mode + median-of-5 README),
PR #7 (remove latency probes after the artifact was diagnosed)

## Context

`tests/comparison-mongodb/` is a Go benchmark that compares OxiDB
vs MongoDB 7 over 100 000 documents. Originally its `run.sh` ran the Go
client **on the macOS host**, reaching the dockerized OxiDB and MongoDB
instances through Docker Desktop's published-port forward
(`127.0.0.1:4444`, `127.0.0.1:27017`).

The single-doc-insert benchmark consistently showed OxiDB ~5× slower
than MongoDB (~227 µs/op vs ~43 µs/op). Investigation traced the cause:

| Probe | From the macOS host (port-forward) | From inside the Docker network |
|---|---|---|
| OxiDB `ping`   | 205 µs | 46 µs |
| OxiDB `insert` | 218 µs | 55 µs |
| MongoDB `ping` | 25 µs  | 68 µs |
| MongoDB `InsertOne` | 43 µs | 84 µs |

The Docker Desktop port-forward adds **~160 µs per round-trip for OxiDB
but only ~10 µs for MongoDB through the same forward** — and is in fact
*faster* than the in-network path for MongoDB. The actual OxiDB engine
work per insert is **~9 µs** (`insert − ping` measured cleanly inside
the bridge network); ~95% of the host-mode benchmark number is the
proxy artifact.

Ruled out as causes (each tested directly):

- the client's two-write `sendRaw` (length then payload) vs a
  coalesced single write — no difference
- the client's two `io.ReadFull` recv vs a single buffered read — no
  difference
- `TCP_NODELAY` — set correctly on both ends

The asymmetry is almost certainly a TCP-level interaction between
OxiDB's small framed messages and Docker Desktop's userland port-
forward proxy (most likely delayed-ACK relaying OxiDB's tiny ~8–13 byte
frames; MongoDB's ~80-byte OP_MSG frames don't trip it). Unconfirmed —
proving it would need a packet capture inside the Docker VM. **It does
not affect production deployments**, where there is no host
port-forward.

## Decision

The benchmark runs the Go client **inside the compose network**
(container-to-container) as its primary, apples-to-apples mode. The
README's MongoDB comparison table reports the in-network medians (of
5 runs, fresh containers each time).

`tests/comparison-mongodb/run.sh` exposes a `BENCH_MODE` env var with
three values:

| Mode | Use it for |
|---|---|
| `innetwork` | Publishing numbers / fair engine-to-engine comparison |
| `host`      | Diagnosing or reproducing the port-forward artifact |
| `both`      | The default — runs each in sequence |

Outcome of switching to in-network: OxiDB wins **21 of 22** operations
vs MongoDB, including single-doc insert (it had been showing as a 5×
"loss" before).

## Rationale

The benchmark's job is to characterize the engines, not Docker
Desktop's networking. Running the client where users actually run
their apps — inside the same network as the database, or on the same
host in production — is the only measurement that reflects real engine
performance.

Reporting the host-mode numbers as the headline would publish a
measurement artifact as an OxiDB weakness, which is the opposite of
what a benchmark is for.

## Consequences

**Positive:**

- The README's published comparison reflects real engine performance,
  not Docker Desktop proxy quirks.
- The methodology (in-network, median-of-5, fresh containers per run)
  is reproducible — `./run.sh` produces the same shape of numbers on
  any machine with Docker.
- `BENCH_MODE=host` is still available for diagnosis — the artifact
  remains visible, just not the published number.

**Negative / accepted trade-offs:**

- Default `both` mode roughly doubles the benchmark runtime. Worth it
  for the cross-check, but users in a hurry can pass
  `BENCH_MODE=innetwork`.
- The `host` mode still produces a misleading number for OxiDB. The
  run script labels it clearly, and this ADR is the canonical
  explanation — but a casual reader who skims only the host output may
  still be confused. Mitigation is documentation, not numbers.

## Revisiting

Re-check the host-vs-in-network asymmetry if:

- Docker Desktop changes its port-forwarding implementation (e.g.,
  switches away from its current userland proxy)
- OxiDB's wire protocol changes its small-message distribution
- The benchmark moves to a non-Docker environment (e.g., bare-metal
  Linux), where Docker's proxy behaviour stops mattering

If the asymmetry goes away, `host` mode becomes a perfectly reasonable
headline mode again — at which point `both` is no longer needed as the
default.
