# OxiDB fuzz harnesses

CERN-grade testing roadmap, category 5 (security). See
[`docs/testing-roadmap.md`](../docs/testing-roadmap.md).

The fuzz crate is **not part of the workspace** (the root
`Cargo.toml` does not list `fuzz` under `[workspace] members`, and
`fuzz/Cargo.toml` has its own empty `[workspace]` table so Cargo
doesn't auto-inherit). This is the cargo-fuzz convention — the
harness needs a sanitizer-friendly compile that the normal `cargo
build` shouldn't pay for.

## Prerequisites

```bash
rustup toolchain install nightly         # libfuzzer needs nightly
cargo install cargo-fuzz                 # 0.13+
```

## Targets

| Target | What it fuzzes | File |
|---|---|---|
| `wire_deserialize` | The top-level message dispatcher (`{`/`[` → JSON, `0xDB` → OxiWire, else → MsgPack) | `fuzz_targets/wire_deserialize.rs` |
| `wire_oxiwire` | OxiWire hand-rolled MsgPack-derived binary decoder | `fuzz_targets/wire_oxiwire.rs` |
| `wire_resp` | RESP (Redis-compatible) wire decoder used by OxiMem | `fuzz_targets/wire_resp.rs` |
| `wire_pg` | PostgreSQL frontend-message decoder | `fuzz_targets/wire_pg.rs` |

## Running

```bash
# Run one target indefinitely (Ctrl-C to stop). cargo-fuzz writes
# new interesting inputs to fuzz/corpus/<target>/ and any panic
# crashers to fuzz/artifacts/<target>/.
cargo +nightly fuzz run wire_deserialize

# Time-bounded smoke run (45 seconds):
cargo +nightly fuzz run wire_resp -- -max_total_time=45

# Parallel jobs:
cargo +nightly fuzz run wire_oxiwire -- -jobs=8 -workers=8

# Reproduce a crash from an artifact:
cargo +nightly fuzz run wire_resp fuzz/artifacts/wire_resp/crash-abcd1234
```

## What a finding looks like

`libfuzzer-sys` counts any panic / abort / sanitizer-detected UB as a
crash and writes the minimised input to
`fuzz/artifacts/<target>/crash-<sha>`. The exit message includes the
Rust panic message and a stack trace. Triage:

1. Re-run the target with the artifact path appended to reproduce
   deterministically.
2. Convert the input to a fixed regression test under
   `oxidb-server/tests/regression/`.
3. Fix the panic — TCP-facing decoders MUST return `Result`/`Option`,
   never `panic!` / `unwrap()` / `expect()` on input-derived values.
4. Commit the regression test + the fix in the same PR.

## What this harness explicitly does NOT do (yet)

- **Structure-aware fuzzing.** `libfuzzer-sys` does mutation-based
  bit-flipping fuzzing of raw bytes. A grammar-aware fuzzer (e.g.
  `arbitrary` + `Arbitrary` impls on each message type) would explore
  the message space far more efficiently. Follow-up.
- **Differential fuzzing against a reference impl.** RESP vs real
  Redis, pg_wire vs PostgreSQL — feed both, compare the parsed
  results, treat divergence as a finding. Multi-week effort.
- **Coverage reporting.** `cargo +nightly fuzz coverage <target>`
  exists but needs an `llvm-cov` setup; deliberately deferred.
- **Continuous OSS-Fuzz integration.** OSS-Fuzz runs cargo-fuzz
  targets 24/7 on Google infrastructure for free; needs a
  separate PR adding the OSS-Fuzz integration files
  (`projects/oxidb/` upstream).

## CI policy

The fuzz crate is excluded from the workspace, so `cargo build` and
`cargo test` ignore it entirely. A dedicated nightly CI stage should
run each target for a bounded time (suggested: 5 min per target on
PRs, longer on `master` push). That stage is not yet wired up;
follow-up.
