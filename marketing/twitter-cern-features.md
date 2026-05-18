# Twitter thread — CERN-grade testing features (2026-05)

Posting order: 1 → 9. Each tweet ≤ 280 chars. Links shorten
automatically; counts below assume t.co (~23 chars per link).

Free to remix / shorten / split into multiple threads. The "1/" through
"9/" prefixes are for posting reference; remove before posting.

---

## Thread

**1/** OxiDB just landed a CERN-grade testing program across all 8
categories of database hardening: ACID isolation, crash recovery,
scale, Raft, security, upgrade, DR, soak.

8/8 ✅ partial. Numbers in the thread 👇

🧵 https://github.com/parisxmas/OxiDB

---

**2/** ACID isolation — pinned, not assumed.

OxiDB = READ COMMITTED + OCC lost-update protection (≈ Postgres
READ COMMITTED + serializable-update guards).

Phantom-read & write-skew (A5B) DO occur. Pinned as tests so any
future SSI upgrade is intentional, not silent.

---

**3/** Crash recovery — 3 layers of escalating realism:

• `mem::forget(db)` (Rust-level "skip destructors")
• SIGKILL after N acks (OS kill, steady-state)
• SIGKILL at varying byte offsets 100µs–200ms (mid-init included)

Every ACKed write recovers. No phantoms. Reopen always succeeds.

---

**4/** Security — wire-protocol fuzz harness with 7 targets.

4 mutation-based + 3 structure-aware roundtrip (OxiWire / RESP /
MsgPack). The MsgPack one is cross-implementation: encoder under
test is OxiDB's hand-roll, decoder is `rmp_serde` — no shared
blind spots.

---

**5/** First 30 seconds of fuzzing in May 2026 found 4 unauth'd
DoS bugs:

• RESP UTF-8 panic (2 bytes)
• pg_wire i32 overflow (6 bytes)
• OxiWire array OOM (7 bytes)
• OxiWire map OOM (8 bytes)

All fixed in PRs #46/47/48 with regression tests pinning the
exact bytes.

---

**6/** Coverage proves structure-aware > mutation on the same parser:

  OxiWire:  43% → 55% (+12pp)
  RESP:     46% → 52% (+6pp)

Not theoretical — numbers from `fuzz/coverage.sh` running
`llvm-cov` over each target's corpus.

---

**7/** Scale — HEP-shaped workload at 100K docs:

  Insert (tx-batched):  56k docs/sec
  Index speedup:        30× over full scan
  Aggregation ($sum):   40ms over 100K
  Delete by predicate:  80k del/sec
  TOTAL test runtime:   2.37s

Knob to crank to 10⁹+ for dedicated runners.

---

**8/** Disaster recovery — backup → wipe → restore, end to end:

  3 collections × 500 docs + indexes + blobs + transactions
  RTO (wipe → verified): 34ms on dev hardware
  Backup file: 28 KB tar.gz

Indexes survive in-place. No rebuild. Aggregations correct
post-restore.

---

**9/** Every remaining gap is effort-estimated in ADR-0006:
Jepsen linearizability (m), differential fuzz vs Redis/Postgres
(m), external pentest (xxl), 10⁹-doc scale (l).

Pre-1.0, AGPL-3.0 + commercial. Honest about what's done and what
isn't. https://github.com/parisxmas/OxiDB

---

## Short version (single tweet)

For posting standalone without a thread:

> OxiDB just landed 8/8 CERN-grade testing categories at ✅ partial.
>
> Fuzz harness found 4 unauth'd DoS bugs in 30s of fuzzing — all
> fixed. Isolation level empirically pinned. Crash drill = SIGKILL
> byte-offset matrix. DR drill = 34ms RTO.
>
> Pre-1.0, honest about gaps: github.com/parisxmas/OxiDB

---

## Numbers to keep accurate when re-posting

- 8/8 categories ✅ partial (after PRs #42-#65 merged 2026-05-18)
- 9 fuzz targets (4 mutation + 3 structure-aware roundtrip + 2 cross-impl diff: vs `redis-rs` and the `pgwire` crate)
- 7 fuzz-found DoS bugs + 1 correctness bug:
    1. RESP UTF-8 panic (2 B)              PR #46
    2. pg_wire i32 overflow (6 B)          PR #47
    3. OxiWire array OOM (7 B)             PR #48
    4. OxiWire map OOM (8 B)               PR #48
    5. RESP bulk-string OOM (14 B)         PR #61
    6. pg_wire Bind/Parse i16 overflow     PR #65 (this batch)
    7. pg_wire Describe/Close empty body   PR #65 (this batch)
    + correctness: RESP SimpleString CR-truncation (PR #61)
- Server version: 0.28.6 (bumped 0.28.0 → 0.28.6 across the security/correctness fixes)
- ADR-0003 / 0005 / 0006 are the load-bearing decision documents
