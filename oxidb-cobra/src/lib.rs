//! oxidb-cobra — a Rust VM for Cobra portable bytecode (ADR-0014, Phase 1).
//!
//! The Cobra language (github: parisxmas — Go implementation) compiles to a
//! stack-VM bytecode; `cobra build --portable` emits the `COBRAP\0` v1
//! container this crate decodes and executes. The Go toolchain stays the only
//! compiler; this crate only runs the result, deterministically, inside the
//! OxiDB SQL engine (stored procedures).
//!
//! Determinism contract: `validate` rejects async/import opcodes and I/O
//! builtins at load (CREATE PROCEDURE) — never mid-call.

pub mod builtins;
pub mod bytecode;
pub mod decimal;
pub mod methods;
pub mod ops;
pub mod validate;
pub mod value;
pub mod vm;

pub use bytecode::{Bytecode, decode};
pub use validate::validate;
pub use vm::{RuntimeError, Vm};
