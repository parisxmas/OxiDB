//! CREATE-time determinism validation (ADR-0014).
//!
//! A stored procedure replicates through Raft as a `CALL` re-executed on
//! every node, so its bytecode must be deterministic: no parallelism, no
//! module/file/plugin loading, no interactive input. This pass walks every
//! instruction stream (main + all function constants) and rejects banned
//! opcodes and builtins UP FRONT — at `CREATE PROCEDURE`, never mid-call.

use crate::bytecode::{Bytecode, Constant, Op};

/// Builtin indexes that are banned in the server VM (I/O). Filled from the
/// canonical builtin order in `builtins.rs`.
pub fn banned_builtin(idx: usize) -> Option<&'static str> {
    let name = crate::builtins::BUILTIN_NAMES.get(idx)?;
    if crate::builtins::is_banned(name) {
        Some(name)
    } else {
        None
    }
}

fn banned_op(op: Op) -> bool {
    matches!(op, Op::Await | Op::Import | Op::FromImport)
}

/// Validate one instruction stream.
fn scan(ins: &[u8], what: &str) -> Result<(), String> {
    let mut ip = 0;
    while ip < ins.len() {
        let Some(op) = Op::from_byte(ins[ip]) else {
            return Err(format!(
                "{what}: unknown opcode {} at {ip} — compiled with a newer Cobra?",
                ins[ip]
            ));
        };
        if banned_op(op) {
            return Err(format!(
                "{what}: {} is not allowed in a stored procedure (non-deterministic)",
                op.name()
            ));
        }
        let widths = op.operand_widths();
        let mut operands = [0usize; 5];
        let mut off = ip + 1;
        for (i, w) in widths.iter().enumerate() {
            if off + w > ins.len() {
                return Err(format!("{what}: truncated instruction at {ip}"));
            }
            operands[i] = match w {
                1 => ins[off] as usize,
                2 => ((ins[off] as usize) << 8) | ins[off + 1] as usize,
                _ => unreachable!(),
            };
            off += w;
        }
        if op == Op::GetBuiltin {
            if let Some(name) = banned_builtin(operands[0]) {
                return Err(format!(
                    "{what}: builtin '{name}' is not allowed in a stored procedure"
                ));
            }
            if operands[0] >= crate::builtins::BUILTIN_NAMES.len() {
                return Err(format!(
                    "{what}: unknown builtin index {} — compiled with a newer Cobra?",
                    operands[0]
                ));
            }
        }
        ip = off;
    }
    Ok(())
}

/// Validate a whole program: the main stream plus every function constant.
/// Also rejects async functions (they only run under OpAwait/task machinery).
pub fn validate(b: &Bytecode) -> Result<(), String> {
    scan(&b.instructions, "main")?;
    for (i, c) in b.constants.iter().enumerate() {
        if let Constant::Func(f) = c {
            let what = if f.name.is_empty() {
                format!("fn#{i}")
            } else {
                f.name.clone()
            };
            if f.is_async {
                return Err(format!(
                    "{what}: async functions are not allowed in a stored procedure"
                ));
            }
            scan(&f.instructions, &what)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytecode::CompiledFunction;

    fn program(instructions: Vec<u8>) -> Bytecode {
        Bytecode {
            instructions,
            ..Default::default()
        }
    }

    #[test]
    fn rejects_banned_opcodes() {
        for (op, name) in [
            (Op::Await, "OpAwait"),
            (Op::Import, "OpImport"),
            (Op::FromImport, "OpFromImport"),
        ] {
            let mut ins = vec![op as u8];
            ins.extend(vec![0u8; op.operand_widths().iter().sum()]);
            ins.push(Op::Halt as u8);
            let err = validate(&program(ins)).unwrap_err();
            assert!(err.contains(name), "{err}");
            assert!(err.contains("not allowed"), "{err}");
        }
    }

    #[test]
    fn rejects_banned_builtins() {
        // Mutex is index 20 in the canonical builtin order.
        let idx = crate::builtins::BUILTIN_NAMES
            .iter()
            .position(|&n| n == "Mutex")
            .unwrap();
        let ins = vec![Op::GetBuiltin as u8, idx as u8, Op::Halt as u8];
        let err = validate(&program(ins)).unwrap_err();
        assert!(err.contains("builtin 'Mutex' is not allowed"), "{err}");
    }

    #[test]
    fn rejects_async_functions() {
        let mut b = program(vec![Op::Halt as u8]);
        b.constants.push(Constant::Func(CompiledFunction {
            instructions: vec![Op::Return as u8],
            name: "worker".into(),
            is_async: true,
            ..Default::default()
        }));
        let err = validate(&b).unwrap_err();
        assert!(
            err.contains("worker: async functions are not allowed"),
            "{err}"
        );
    }

    #[test]
    fn accepts_clean_program() {
        let ins = vec![
            Op::GetBuiltin as u8,
            0, // print
            Op::Constant as u8,
            0,
            0,
            Op::Call as u8,
            1,
            Op::Pop as u8,
            Op::Halt as u8,
        ];
        let mut b = program(ins);
        b.constants.push(Constant::Str("hi".into()));
        assert!(validate(&b).is_ok());
    }
}
