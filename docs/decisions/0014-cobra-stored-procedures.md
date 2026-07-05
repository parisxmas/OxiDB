# ADR-0014: Cobra as the compiled stored-procedure language

**Status:** Accepted — 2026-07-06 (Phase 0 shipped)

## Context

The SQL engine gained SQL-text stored procedures (`CREATE PROCEDURE ... AS
BEGIN dml; END`, shipped 2026-07-06). Their v1 body surface is deliberately
narrow: a parameterized batch of DML/SELECT statements — no variables, no
control flow, and the stored text is re-parsed on every `CALL`.

[Cobra](https://cobralang.baltavista.com) is our scripting language
(implemented in Go, same author): a full language with a bytecode compiler
and a 118-opcode stack VM. `cobra build` produces `.cobrac` files — a
versioned container (`COBRAC\0` + format byte) around the compiled
`Bytecode { Instructions, Lines, Constants, Bundle, GlobalNames }`. The
constant pool holds exactly four object kinds: `Integer`, `Float`,
`String`, `CompiledFunction`. Instruction operands are big-endian.

We want *compiled* stored procedures: parse+compile once (at `CREATE`),
execute bytecode on `CALL` — with real control flow, inside the calling
transaction. The document engine's OxiScript (JSON-step procedures) is
prior art in-repo; long-term, Cobra can subsume it so both engines share
one procedure language.

## Decision

Treat **`.cobrac` bytecode as the stored-procedure interchange format**,
and execute it server-side with a **compact Cobra VM written in Rust**
inside the SQL engine. The Go toolchain (`cobrac`) stays the only
compiler; the Rust side only *executes*.

Rejected alternatives:

- **cgo / `c-shared` Cobra runtime linked into the server** — brings the
  Go runtime (GC, signal handling) into the Rust process, double-FFI on
  every `sql()` callback, and a 5-platform build-matrix burden.
- **Sidecar process hosting the Go VM** — a `CALL` must run inside the
  caller's transaction; shuttling every statement over IPC makes
  transactional integration slow and fragile.
- **Growing SQL-text procedures into a procedural dialect** (PL/pgSQL
  style) — a second language to design, parse, and maintain, when a
  complete, tested language with a compiler already exists in-house.

## Constraints the design must honor

- **Bytecode is a cross-repo ABI.** The format byte in the magic is the
  contract; the server rejects versions it does not understand. Opcode or
  builtin changes in Cobra bump the format byte.
- **Portability.** The stock `.cobrac` payload is Go `encoding/gob` —
  effectively unreadable outside Go. Phase 0 adds a portable flat binary
  encoding to the Cobra compiler; that is what the server accepts.
- **Determinism (cluster).** Raft replicates `CALL` as a write statement,
  re-executed per node. Procedure bytecode must therefore be
  deterministic: the server's builtin allowlist excludes time, randomness,
  I/O, and parallelism; unknown opcodes/builtins are rejected **at
  `CREATE`**, not mid-call.
- **Sandbox.** Host access happens only through the functions we
  register (`sql(...)` first; document-engine ops later). No filesystem,
  network, or process access exists in the server VM.

## Phases

- **Phase 0 — portable bytecode** *(shipped, in the Cobra repo)*:
  `cobra build --portable` emits `COBRAP\0` v1 — a flat, little-endian,
  length-prefixed encoding of the same `Bytecode` struct (sections:
  instructions, line table, tagged constant pool, bundle, global names;
  `CompiledFunction` constants serialize their fields inline). The
  runtime auto-detects gob vs portable when loading, so `.cobrac` files
  in either encoding just run. Golden-bytes + round-trip + gob-equivalence
  tests pin the format.
- **Phase 1 — Rust VM core** (`oxidb-cobra` crate): decoder for
  `COBRAP\0` v1, value types mirroring Cobra semantics (int/float/string/
  bool/null/list/dict), frames/closures, the 118-opcode dispatch loop, and
  a curated builtin subset (pure functions only). Conformance: run Cobra's
  own example programs on both VMs and diff the output.
- **Phase 2 — server integration**: `CREATE PROCEDURE name(params)
  LANGUAGE COBRA AS '<base64 .cobrac>'` (wire clients upload the compiled
  payload; the CLI grows a helper), catalog + WAL storage next to SQL-text
  procedures, `CALL` dispatches on the procedure's language, host
  functions `sql(text, params...)` / `params[...]` bound to the calling
  transaction, result = the procedure's return value shaped as a result
  set.
- **Phase 3 — cluster + polish**: determinism validation at CREATE,
  Raft replication (rides the existing SQL-text write path), docs,
  `SHOW PROCEDURES` language column, and the OxiScript-convergence
  question for the document engine (separate ADR if pursued).

## Consequences

- `CALL` of a Cobra procedure skips parse and plan-shape work entirely —
  the VM starts on stored bytecode; only the `sql()` statements it issues
  go through the SQL executor.
- Two implementations of one VM must stay in lockstep. Mitigations: the
  format version byte, the conformance suite driven by Cobra's own tests,
  and a deliberately small server-side builtin allowlist.
- The SQL-text procedure surface (ADR-0013 era) stays — it is the
  zero-toolchain path; Cobra procedures are the power path.
