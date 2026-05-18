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

### Mutation-based (raw bytes → decoder)

| Target | What it fuzzes | File |
|---|---|---|
| `wire_deserialize` | The top-level message dispatcher (`{`/`[` → JSON, `0xDB` → OxiWire, else → MsgPack) | `fuzz_targets/wire_deserialize.rs` |
| `wire_oxiwire` | OxiWire hand-rolled MsgPack-derived binary decoder | `fuzz_targets/wire_oxiwire.rs` |
| `wire_resp` | RESP (Redis-compatible) wire decoder used by OxiMem | `fuzz_targets/wire_resp.rs` |
| `wire_pg` | PostgreSQL frontend-message decoder | `fuzz_targets/wire_pg.rs` |

### Structure-aware (typed grammar → encode → decode → equality)

| Target | What it checks | File |
|---|---|---|
| `oxiwire_roundtrip` | OxiWire encoder ↔ decoder mutual consistency. `Arbitrary` value tree → `encode_value` → `decode_request` → JSON-canonical equality. | `fuzz_targets/oxiwire_roundtrip.rs` |
| `resp_roundtrip` | RESP encoder ↔ decoder mutual consistency. `Arbitrary RespValue` → `write_value` → `read_value` → bytes-equal after re-encoding. CR/LF normalised out of SimpleString/Error at input (line-based framing constraint). | `fuzz_targets/resp_roundtrip.rs` |
| `msgpack_roundtrip` | OxiDB's hand-rolled MsgPack encoder (`protocol::value_to_msgpack`) ↔ canonical `rmp_serde::from_slice` decoder. Cross-implementation comparison surfaces encoder bugs the same-author decoder couldn't see. | `fuzz_targets/msgpack_roundtrip.rs` |
| `resp_diff_redis` | Decoder differential: OxiDB `resp::read_value` vs canonical `redis::parse_redis_value` (redis-rs). Scoped to length-prefixed scalar types (`:`, `$`) where RESP2 spec is unambiguous. Found a 12-byte bulk-string OOM + a SimpleString CR-truncation bug on its first smoke run (both fixed in PR #61). | `fuzz_targets/resp_diff_redis.rs` |
| `pg_diff_pgwire` | Decoder differential: OxiDB `pg_wire::codec::read_message` vs `pgwire` crate's `PgWireFrontendMessage::decode`. Scoped to `Q P D H C X` tagged frontend messages. Found two real OxiDB bugs on first smoke runs: signed-i16 cast → usize overflow in Bind/Parse decoders + empty-body panic in Describe/Close decoders (both fixed in this PR's branch). Includes a panic-hook filter that ignores known pgwire 0.40 truncated-message panic paths. | `fuzz_targets/pg_diff_pgwire.rs` |

Structure-aware fuzz runs **~6× faster** (~18k iter/s vs ~3k iter/s
for byte-flipping in 30s smoke runs) because every iteration starts
from a valid-by-construction input that libfuzzer can mutate
*meaningfully* instead of producing megabytes of garbage that bounce
off the decoder's first byte check.

## Coverage

Run `./coverage.sh` (or `./coverage.sh <target>` for a single one) to
build a coverage-instrumented binary, replay it against the saved
corpus, and render a compact per-file summary. Add `html` as the
second argument to also produce a clickable HTML report under
`coverage/<target>/html/index.html`.

Snapshot from a 30-second smoke run of each target (post-PR landing
this script):

| Target | File touched | Line cov |
|---|---|---|
| `wire_deserialize` | `oxiwire.rs` + `protocol.rs` | 42% / 19% |
| `wire_oxiwire` | `oxiwire.rs` | 43% |
| `wire_resp` | `resp.rs` | 46% |
| `wire_pg` | `pg_wire/codec.rs` | 18% |
| `oxiwire_roundtrip` | `oxiwire.rs` | **55%** |
| `resp_roundtrip` | `resp.rs` | **52%** |
| `msgpack_roundtrip` | `protocol.rs` | 31% |

**Structure-aware consistently outperforms mutation on the same
parser** (OxiWire 43% → 55%, RESP 46% → 52%) — empirical proof
that grammar-driven input generation reaches code paths bit-
flipping can't randomly hit. Longer fuzz runs grow these numbers
further; the 30s smoke results above are the floor, not the
ceiling.

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

- **Structure-aware fuzzing for pg_wire.** pg_wire is more decode-
  only than encode/decode-symmetric — there's no `value_to_pg_wire`
  to fuzz the inverse of. Differential fuzz vs real Postgres (next
  bullet) is the natural shape for pg_wire.
- **Differential fuzzing against a reference impl.** RESP vs real
  Redis, pg_wire vs PostgreSQL — feed both, compare the parsed
  results, treat divergence as a finding. Multi-week effort.
- **Coverage reporting.** `cargo +nightly fuzz coverage <target>`
  exists but needs an `llvm-cov` setup; deliberately deferred.
- **Continuous OSS-Fuzz integration.** Infrastructure files
  committed in [`infra/oss-fuzz/`](../infra/oss-fuzz/) — see that
  directory's README for the submission playbook. The upstream PR
  to `google/oss-fuzz` (adding `projects/oxidb/`) is a separate
  manual step.

## CI policy

The fuzz crate is excluded from the workspace, so `cargo build` and
`cargo test` ignore it entirely. A dedicated nightly CI stage should
run each target for a bounded time (suggested: 5 min per target on
PRs, longer on `master` push). That stage is not yet wired up;
follow-up.
