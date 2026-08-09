//! COBRAP v1 portable bytecode: the decoder and the opcode table.
//!
//! Mirrors `compiler/portable.go` in the Cobra repo byte-for-byte. The file
//! container is little-endian with u32-length-prefixed strings; instruction
//! OPERANDS inside the opcode stream are big-endian (as produced by
//! `code.Make`). The format version byte is a cross-repo ABI: reject what we
//! don't know.

/// Magic prefix: `COBRAP\0`; the trailing byte is the format version.
/// v1 is the original layout; v2 (compiler 0.13+) appends one section —
/// `exported` names (u32 count + strings) — which this VM reads and
/// discards: procedures are invoked by symbol lookup, not module import,
/// so the export list carries nothing it needs.
pub const MAGIC: &[u8; 8] = b"COBRAP\x00\x01";
const MAGIC_PREFIX: &[u8; 7] = b"COBRAP\x00";
const VERSION_MIN: u8 = 1;
const VERSION_MAX: u8 = 2;

#[derive(Debug, Clone, PartialEq)]
pub enum Constant {
    Int(i64),
    Float(f64),
    Str(String),
    Func(CompiledFunction),
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct CompiledFunction {
    pub instructions: Vec<u8>,
    pub lines: Vec<(u32, u32)>, // (pos, line), run-length
    pub num_locals: usize,
    pub num_params: usize,
    pub name: String,
    pub is_async: bool,
    pub boxed_locals: Vec<usize>,
    pub kind: String, // "", "static", "property", "setter"
}

impl CompiledFunction {
    /// Source line for the instruction at `pos` (0 = unknown).
    pub fn line_at(&self, pos: usize) -> u32 {
        let mut line = 0;
        for &(p, l) in &self.lines {
            if p as usize > pos {
                break;
            }
            line = l;
        }
        line
    }
}

#[derive(Debug, Clone, Default)]
pub struct Bytecode {
    pub instructions: Vec<u8>,
    pub lines: Vec<(u32, u32)>,
    pub constants: Vec<Constant>,
    pub bundle: Vec<(String, String)>,
    pub global_names: Vec<String>,
}

/// Decode a portable compiled Cobra file (`cobra build --portable`).
pub fn decode(data: &[u8]) -> Result<Bytecode, String> {
    if data.len() < MAGIC.len() || &data[..MAGIC_PREFIX.len()] != MAGIC_PREFIX {
        return Err("not a portable compiled Cobra file (bad magic or version)".into());
    }
    let version = data[MAGIC_PREFIX.len()];
    if !(VERSION_MIN..=VERSION_MAX).contains(&version) {
        return Err(format!(
            "not a portable compiled Cobra file (bad magic or version): \
             format v{version}, this VM reads v{VERSION_MIN}..v{VERSION_MAX}"
        ));
    }
    let mut r = Reader {
        data,
        pos: MAGIC.len(),
    };

    let mut b = Bytecode {
        instructions: r.bytes()?,
        lines: r.line_table()?,
        ..Default::default()
    };

    let n_const = r.u32()? as usize;
    for _ in 0..n_const {
        let c = match r.u8()? {
            1 => Constant::Int(r.u64()? as i64),
            2 => Constant::Float(f64::from_bits(r.u64()?)),
            3 => Constant::Str(r.string()?),
            4 => {
                let mut f = CompiledFunction {
                    instructions: r.bytes()?,
                    lines: r.line_table()?,
                    num_locals: r.u32()? as usize,
                    num_params: r.u32()? as usize,
                    name: r.string()?,
                    is_async: false,
                    boxed_locals: vec![],
                    kind: String::new(),
                };
                f.is_async = r.u8()? == 1;
                let n_boxed = r.u32()? as usize;
                for _ in 0..n_boxed {
                    f.boxed_locals.push(r.u32()? as usize);
                }
                f.kind = r.string()?;
                Constant::Func(f)
            }
            t => return Err(format!("corrupt portable file: unknown constant tag {t}")),
        };
        b.constants.push(c);
    }

    let n_bundle = r.u32()? as usize;
    for _ in 0..n_bundle {
        let p = r.string()?;
        let s = r.string()?;
        b.bundle.push((p, s));
    }

    let n_globals = r.u32()? as usize;
    for _ in 0..n_globals {
        b.global_names.push(r.string()?);
    }

    // v2: the exported-names section. Read (so the trailing-bytes check
    // stays exact) and discard (see MAGIC docs).
    if version >= 2 {
        let n_exported = r.u32()? as usize;
        for _ in 0..n_exported {
            let _ = r.string()?;
        }
    }

    if r.pos != data.len() {
        return Err(format!(
            "corrupt portable file: {} trailing bytes",
            data.len() - r.pos
        ));
    }
    Ok(b)
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn need(&self, n: usize) -> Result<(), String> {
        if self.pos + n > self.data.len() {
            Err(format!(
                "corrupt portable file: truncated at byte {}",
                self.pos
            ))
        } else {
            Ok(())
        }
    }
    fn u8(&mut self) -> Result<u8, String> {
        self.need(1)?;
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }
    fn u32(&mut self) -> Result<u32, String> {
        self.need(4)?;
        let v = u32::from_le_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }
    fn u64(&mut self) -> Result<u64, String> {
        self.need(8)?;
        let v = u64::from_le_bytes(self.data[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }
    fn bytes(&mut self) -> Result<Vec<u8>, String> {
        let n = self.u32()? as usize;
        self.need(n)?;
        let v = self.data[self.pos..self.pos + n].to_vec();
        self.pos += n;
        Ok(v)
    }
    fn string(&mut self) -> Result<String, String> {
        String::from_utf8(self.bytes()?)
            .map_err(|e| format!("corrupt portable file: bad utf-8: {e}"))
    }
    fn line_table(&mut self) -> Result<Vec<(u32, u32)>, String> {
        let n = self.u32()? as usize;
        let mut t = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            let pos = self.u32()?;
            let line = self.u32()?;
            t.push((pos, line));
        }
        Ok(t)
    }
}

// ─── Opcodes (mirror code/code.go — same discriminants) ─────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[rustfmt::skip]
pub enum Op {
    Constant = 0, Pop, True, False, Null, Binary, Prefix, Dup,
    Jump, JumpNotTruthy, JumpTruthy, SetGlobal, GetGlobal, SetLocal, GetLocal,
    GetBuiltin, Closure, GetFree, SetFree, CurrentClosure, List, Dict, Index,
    Slice, SetIndex, Call, ReturnValue, Return, MethodCall, IterNew, IterNext,
    TryBegin, TryEnd, Throw, Struct, GetProperty, SetProperty, FinallyBegin,
    EndFinally, Import, Halt, Await, LocalConstBin, SuperCall, FromImport,
    Str, Destructure, GetLocalBox, SetLocalBox, GetLocalCell, GetFreeCell,
    LocalBoxConstBin,
}

pub const OP_COUNT: u8 = Op::LocalBoxConstBin as u8 + 1;

impl Op {
    pub fn from_byte(b: u8) -> Option<Op> {
        if b < OP_COUNT {
            // SAFETY: contiguous discriminants 0..OP_COUNT, checked above.
            Some(unsafe { std::mem::transmute::<u8, Op>(b) })
        } else {
            None
        }
    }

    /// Operand widths in bytes (operands are big-endian in the stream).
    pub fn operand_widths(self) -> &'static [usize] {
        use Op::*;
        match self {
            Pop | True | False | Null | Dup | CurrentClosure | Index | Slice | SetIndex
            | ReturnValue | Return | IterNew | TryEnd | Throw | EndFinally | Halt | Await | Str => {
                &[]
            }
            Binary | Prefix | SetLocal | GetLocal | GetBuiltin | GetFree | SetFree | Call
            | Destructure | GetLocalBox | SetLocalBox | GetLocalCell | GetFreeCell => &[1],
            Constant | Jump | JumpNotTruthy | JumpTruthy | SetGlobal | GetGlobal | List | Dict
            | IterNext | TryBegin | FinallyBegin | Import | GetProperty | SetProperty => &[2],
            Closure => &[2, 1],
            MethodCall | SuperCall => &[2, 1],
            FromImport => &[2, 2],
            LocalConstBin | LocalBoxConstBin => &[1, 2, 1],
            Struct => &[2, 1, 1, 1, 1],
        }
    }

    pub fn name(self) -> &'static str {
        use Op::*;
        match self {
            Constant => "OpConstant",
            Pop => "OpPop",
            True => "OpTrue",
            False => "OpFalse",
            Null => "OpNull",
            Binary => "OpBinary",
            Prefix => "OpPrefix",
            Dup => "OpDup",
            Jump => "OpJump",
            JumpNotTruthy => "OpJumpNotTruthy",
            JumpTruthy => "OpJumpTruthy",
            SetGlobal => "OpSetGlobal",
            GetGlobal => "OpGetGlobal",
            SetLocal => "OpSetLocal",
            GetLocal => "OpGetLocal",
            GetBuiltin => "OpGetBuiltin",
            Closure => "OpClosure",
            GetFree => "OpGetFree",
            SetFree => "OpSetFree",
            CurrentClosure => "OpCurrentClosure",
            List => "OpList",
            Dict => "OpDict",
            Index => "OpIndex",
            Slice => "OpSlice",
            SetIndex => "OpSetIndex",
            Call => "OpCall",
            ReturnValue => "OpReturnValue",
            Return => "OpReturn",
            MethodCall => "OpMethodCall",
            IterNew => "OpIterNew",
            IterNext => "OpIterNext",
            TryBegin => "OpTryBegin",
            TryEnd => "OpTryEnd",
            Throw => "OpThrow",
            Struct => "OpStruct",
            GetProperty => "OpGetProperty",
            SetProperty => "OpSetProperty",
            FinallyBegin => "OpFinallyBegin",
            EndFinally => "OpEndFinally",
            Import => "OpImport",
            Halt => "OpHalt",
            Await => "OpAwait",
            LocalConstBin => "OpLocalConstBin",
            SuperCall => "OpSuperCall",
            FromImport => "OpFromImport",
            Str => "OpStr",
            Destructure => "OpDestructure",
            GetLocalBox => "OpGetLocalBox",
            SetLocalBox => "OpSetLocalBox",
            GetLocalCell => "OpGetLocalCell",
            GetFreeCell => "OpGetFreeCell",
            LocalBoxConstBin => "OpLocalBoxConstBin",
        }
    }
}

/// Binary operator table — `OpBinary`'s operand indexes this.
pub const BINARY_OPS: [&str; 11] = ["+", "-", "*", "/", "%", "==", "!=", "<", ">", "<=", ">="];
pub const PREFIX_OPS: [&str; 2] = ["-", "!"];

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand-build a minimal portable file and decode it.
    #[test]
    fn decode_roundtrip_minimal() {
        let mut f = Vec::new();
        f.extend_from_slice(MAGIC);
        // instructions: OpTrue OpHalt
        f.extend_from_slice(&2u32.to_le_bytes());
        f.push(Op::True as u8);
        f.push(Op::Halt as u8);
        // line table: 1 entry (0, 1)
        f.extend_from_slice(&1u32.to_le_bytes());
        f.extend_from_slice(&0u32.to_le_bytes());
        f.extend_from_slice(&1u32.to_le_bytes());
        // constants: Int 42, Str "hi"
        f.extend_from_slice(&2u32.to_le_bytes());
        f.push(1);
        f.extend_from_slice(&42u64.to_le_bytes());
        f.push(3);
        f.extend_from_slice(&2u32.to_le_bytes());
        f.extend_from_slice(b"hi");
        // bundle: 0, globals: 1 "x"
        f.extend_from_slice(&0u32.to_le_bytes());
        f.extend_from_slice(&1u32.to_le_bytes());
        f.extend_from_slice(&1u32.to_le_bytes());
        f.extend_from_slice(b"x");

        let b = decode(&f).unwrap();
        assert_eq!(b.instructions, vec![Op::True as u8, Op::Halt as u8]);
        assert_eq!(
            b.constants,
            vec![Constant::Int(42), Constant::Str("hi".into())]
        );
        assert_eq!(b.global_names, vec!["x"]);
    }

    #[test]
    /// Format v2 (compiler 0.13+) appends an exported-names section; the VM
    /// reads it to keep the trailing-bytes check exact and discards it.
    /// Found the hard way: `cobra build --portable` moved to v2 and every
    /// freshly-compiled procedure was refused with "bad magic or version".
    #[test]
    fn decode_accepts_format_v2_and_ignores_exports() {
        let mut f = Vec::new();
        f.extend_from_slice(b"COBRAP\x00\x02");
        f.extend_from_slice(&2u32.to_le_bytes());
        f.push(Op::True as u8);
        f.push(Op::Halt as u8);
        f.extend_from_slice(&0u32.to_le_bytes()); // lines
        f.extend_from_slice(&0u32.to_le_bytes()); // constants
        f.extend_from_slice(&0u32.to_le_bytes()); // bundle
        f.extend_from_slice(&1u32.to_le_bytes()); // globals: "x"
        f.extend_from_slice(&1u32.to_le_bytes());
        f.extend_from_slice(b"x");
        // v2: exported = ["run"]
        f.extend_from_slice(&1u32.to_le_bytes());
        f.extend_from_slice(&3u32.to_le_bytes());
        f.extend_from_slice(b"run");

        let b = decode(&f).unwrap();
        assert_eq!(b.global_names, vec!["x"]);

        // The same bytes WITHOUT the exported section must fail the
        // trailing/length accounting rather than decode by luck.
        let mut torn = f.clone();
        torn.truncate(f.len() - 3);
        assert!(decode(&torn).is_err());
    }

    /// An unknown future version is refused, and the error names the range
    /// this VM reads — the operator's fix is a VM upgrade, and the message
    /// should say so.
    #[test]
    fn decode_refuses_future_versions_by_range() {
        let mut f = Vec::new();
        f.extend_from_slice(b"COBRAP\x00\x03");
        f.extend_from_slice(&0u32.to_le_bytes());
        let err = decode(&f).unwrap_err();
        assert!(err.contains("v3") && err.contains("v1..v2"), "{err}");
    }

    #[test]
    fn decode_rejects_bad_magic() {
        assert!(decode(b"COBRAP\x00\x02rest").is_err()); // future version
        assert!(decode(b"nope").is_err());
    }

    #[test]
    fn trailing_bytes_rejected() {
        let mut f = Vec::new();
        f.extend_from_slice(MAGIC);
        for _ in 0..5 {
            f.extend_from_slice(&0u32.to_le_bytes());
        }
        f.push(0xFF);
        assert!(decode(&f).unwrap_err().contains("trailing"));
    }
}
