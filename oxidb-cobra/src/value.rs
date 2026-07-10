//! The runtime value model — the port of `object/object.go`.
//!
//! Reference types (List, Dict, Instance, Cell, Iterator) are `Rc<RefCell>`
//! so two variables can alias the same collection, exactly like the Go
//! originals. Inspect/Repr, truthiness, equality and hash-key semantics are
//! the SEMANTICS.md contract, byte-for-byte.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::bytecode::CompiledFunction;
use crate::decimal::Decimal;

// ─── Value ───────────────────────────────────────────────────────────────

#[derive(Clone, Default)]
pub enum Value {
    /// Also the harmless filler `mem::take` leaves behind.
    #[default]
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(Rc<str>),
    Decimal(Rc<Decimal>),
    List(Rc<RefCell<Vec<Value>>>),
    Dict(Rc<RefCell<Dict>>),
    Range(Rc<Range>),
    Closure(Rc<Closure>),
    /// Index into the canonical builtin table.
    Builtin(usize),
    /// A function constant — only ever consumed by OpClosure.
    CompiledFn(Rc<CompiledFunction>),
    Struct(Rc<Struct>),
    Contract(Rc<Contract>),
    Instance(Rc<RefCell<Instance>>),
    /// Boxed local shared with capturing closures (upvalue).
    Cell(Rc<RefCell<Value>>),
    /// for-in cursor; never user-visible.
    Iterator(Rc<RefCell<Iter>>),
    /// VM-internal: rides the stack through a finally block on the error path.
    Rethrow(Rc<Thrown>),
}

/// A catchable runtime error: the message (for an uncaught crash) and the
/// value a catch clause binds.
#[derive(Clone)]
pub struct Thrown {
    pub value: Value,
    pub msg: String,
}

/// An error produced by a shared operation (builtin, method, property) with
/// no line attached yet — the CALLER wraps it with `line N: `. Mirrors
/// `*object.Error` (`value` = the thrown value when raised by `throw`).
pub struct NativeError {
    pub msg: String,
    pub value: Option<Value>,
}

impl NativeError {
    pub fn new(msg: impl Into<String>) -> NativeError {
        NativeError {
            msg: msg.into(),
            value: None,
        }
    }
}

impl std::fmt::Debug for NativeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NativeError({:?})", self.msg)
    }
}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}<{}>", self.type_name(), repr(self))
    }
}

macro_rules! native_err {
    ($($arg:tt)*) => { $crate::value::NativeError::new(format!($($arg)*)) };
}
pub(crate) use native_err;

// ─── Compound types ──────────────────────────────────────────────────────

/// Lazy integer sequence (the result of `range()`). Step is never zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Range {
    pub start: i64,
    pub stop: i64,
    pub step: i64,
}

impl Range {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> i64 {
        if self.step > 0 {
            if self.stop <= self.start {
                0
            } else {
                (self.stop - self.start + self.step - 1) / self.step
            }
        } else if self.stop >= self.start {
            0
        } else {
            (self.start - self.stop - self.step - 1) / -self.step
        }
    }

    pub fn at(&self, i: i64) -> i64 {
        self.start + i * self.step
    }
}

/// A compiled closure. `free` is mutable because OpSetFree may replace a
/// non-cell slot in place (a captured function name).
pub struct Closure {
    pub func: Rc<CompiledFunction>,
    pub free: RefCell<Vec<Value>>,
}

pub struct Struct {
    pub name: String,
    pub is_record: bool,
    pub parent: Option<Rc<Struct>>,
    pub methods: HashMap<String, Value>,
    pub statics: HashMap<String, Value>,
    pub getters: HashMap<String, Value>,
    pub setters: HashMap<String, Value>,
    pub consts: HashMap<String, Value>,
}

impl Struct {
    /// Walk the parent chain. Returns the method and its owner struct.
    pub fn find_method(self: &Rc<Struct>, name: &str) -> Option<(Value, Rc<Struct>)> {
        let mut s = Rc::clone(self);
        loop {
            if let Some(m) = s.methods.get(name) {
                return Some((m.clone(), Rc::clone(&s)));
            }
            match &s.parent {
                Some(p) => {
                    let p = Rc::clone(p);
                    s = p;
                }
                None => return None,
            }
        }
    }

    pub fn find_getter(self: &Rc<Struct>, name: &str) -> Option<Value> {
        let mut s = Rc::clone(self);
        loop {
            if let Some(m) = s.getters.get(name) {
                return Some(m.clone());
            }
            match &s.parent {
                Some(p) => s = Rc::clone(p),
                None => return None,
            }
        }
    }

    pub fn find_setter(self: &Rc<Struct>, name: &str) -> Option<Value> {
        let mut s = Rc::clone(self);
        loop {
            if let Some(m) = s.setters.get(name) {
                return Some(m.clone());
            }
            match &s.parent {
                Some(p) => s = Rc::clone(p),
                None => return None,
            }
        }
    }

    pub fn find_static(self: &Rc<Struct>, name: &str) -> Option<Value> {
        let mut s = Rc::clone(self);
        loop {
            if let Some(m) = s.statics.get(name) {
                return Some(m.clone());
            }
            match &s.parent {
                Some(p) => s = Rc::clone(p),
                None => return None,
            }
        }
    }

    pub fn find_const(self: &Rc<Struct>, name: &str) -> Option<Value> {
        let mut s = Rc::clone(self);
        loop {
            if let Some(m) = s.consts.get(name) {
                return Some(m.clone());
            }
            match &s.parent {
                Some(p) => s = Rc::clone(p),
                None => return None,
            }
        }
    }
}

pub struct Contract {
    pub name: String,
    pub methods: Vec<String>,
}

/// Fields preserve first-assignment order for deterministic printing.
pub struct Instance {
    pub struct_: Rc<Struct>,
    pub fields: Vec<(String, Value)>,
    /// A record instance, immutable after construction.
    pub frozen: bool,
    /// Field set fixed (init returned) — unknown-name writes are typos.
    pub sealed: bool,
}

impl Instance {
    pub fn new(s: Rc<Struct>) -> Instance {
        Instance {
            struct_: s,
            fields: Vec::with_capacity(4),
            frozen: false,
            sealed: false,
        }
    }

    pub fn get_field(&self, name: &str) -> Option<Value> {
        self.fields
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.clone())
    }

    pub fn set_field(&mut self, name: &str, val: Value) {
        for (n, v) in &mut self.fields {
            if n == name {
                *v = val;
                return;
            }
        }
        self.fields.push((name.to_string(), val));
    }

    /// Update an existing field only; report whether it existed.
    pub fn set_field_if_exists(&mut self, name: &str, val: Value) -> bool {
        for (n, v) in &mut self.fields {
            if n == name {
                *v = val;
                return true;
            }
        }
        false
    }

    /// Shallow mutable copy with the same struct and fields (record `with`).
    pub fn clone_fields(&self) -> Instance {
        Instance {
            struct_: Rc::clone(&self.struct_),
            fields: self.fields.clone(),
            frozen: false,
            sealed: self.sealed,
        }
    }
}

/// for-in cursor: a materialized item list, or a lazy Range.
pub struct Iter {
    pub items: Vec<Value>,
    pub pos: i64,
    pub rng: Option<Rc<Range>>,
}

impl Iter {
    #[allow(clippy::should_implement_trait)] // mirrors Go's Iterator.Next
    pub fn next(&mut self) -> Option<Value> {
        if let Some(r) = &self.rng {
            if self.pos >= r.len() {
                return None;
            }
            let v = Value::Int(r.at(self.pos));
            self.pos += 1;
            return Some(v);
        }
        if self.pos as usize >= self.items.len() {
            return None;
        }
        let v = self.items[self.pos as usize].clone();
        self.pos += 1;
        Some(v)
    }
}

// ─── Dict (insertion-ordered, tombstoned deletes) ────────────────────────

/// Dict key — the port of object.HK. The kind tag keeps 1, "1", 1.0 and
/// true distinct; Float hashes by IEEE-754 bits (1 vs 1.0 differ), Decimal
/// by its normalized string (1.0 == 1.00 collide).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HK {
    Int(i64),
    Float(u64),
    Str(Rc<str>),
    Dec(String),
    Bool(bool),
    Null,
}

/// Returns a dict key for `v`, or None if the type cannot be one.
pub fn hash_key(v: &Value) -> Option<HK> {
    match v {
        Value::Int(i) => Some(HK::Int(*i)),
        Value::Float(f) => Some(HK::Float(f.to_bits())),
        Value::Str(s) => Some(HK::Str(Rc::clone(s))),
        Value::Decimal(d) => Some(HK::Dec(d.normalized_key())),
        Value::Bool(b) => Some(HK::Bool(*b)),
        Value::Null => Some(HK::Null),
        _ => None,
    }
}

/// Insertion-ordered mutable mapping: an index into an entries vec whose
/// deleted slots become tombstones (None). Re-inserting an existing key
/// keeps its original position.
#[derive(Default)]
pub struct Dict {
    index: HashMap<HK, usize>,
    entries: Vec<Option<(Value, Value)>>,
    dead: usize,
}

impl Dict {
    pub fn new() -> Dict {
        Dict::default()
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn get(&self, k: &HK) -> Option<&(Value, Value)> {
        let pos = *self.index.get(k)?;
        self.entries[pos].as_ref()
    }

    pub fn set(&mut self, k: HK, key: Value, value: Value) {
        if let Some(&pos) = self.index.get(&k) {
            self.entries[pos] = Some((key, value));
            return;
        }
        self.index.insert(k, self.entries.len());
        self.entries.push(Some((key, value)));
    }

    pub fn delete(&mut self, k: &HK) -> bool {
        match self.index.remove(k) {
            Some(pos) => {
                self.entries[pos] = None;
                self.dead += 1;
                true
            }
            None => false,
        }
    }

    /// Live pairs in insertion order.
    pub fn in_order(&self) -> impl Iterator<Item = &(Value, Value)> {
        self.entries.iter().flatten()
    }
}

// ─── Type names ──────────────────────────────────────────────────────────

impl Value {
    /// The Type() string used in error messages. An instance reads as its
    /// struct name, a closure as FUNCTION (agreeing with the tree-walker).
    pub fn type_name(&self) -> String {
        match self {
            Value::Null => "NULL".into(),
            Value::Bool(_) => "BOOLEAN".into(),
            Value::Int(_) => "INTEGER".into(),
            Value::Float(_) => "FLOAT".into(),
            Value::Str(_) => "STRING".into(),
            Value::Decimal(_) => "DECIMAL".into(),
            Value::List(_) => "LIST".into(),
            Value::Dict(_) => "DICT".into(),
            Value::Range(_) => "RANGE".into(),
            Value::Closure(_) => "FUNCTION".into(),
            Value::Builtin(_) => "BUILTIN".into(),
            Value::CompiledFn(_) => "COMPILED_FUNCTION".into(),
            Value::Struct(_) => "STRUCT".into(),
            Value::Contract(_) => "CONTRACT".into(),
            Value::Instance(i) => i.borrow().struct_.name.clone(),
            Value::Cell(_) => "CELL".into(),
            Value::Iterator(_) => "ITERATOR".into(),
            Value::Rethrow(_) => "RETHROW".into(),
        }
    }
}

// ─── Truthiness ──────────────────────────────────────────────────────────

/// false: null, false, 0, 0.0, "", empty list, empty dict. Everything else
/// (Range, Decimal, Instance included) is true.
pub fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Int(i) => *i != 0,
        Value::Float(f) => *f != 0.0,
        Value::Str(s) => !s.is_empty(),
        Value::List(l) => !l.borrow().is_empty(),
        Value::Dict(d) => !d.borrow().is_empty(),
        _ => true,
    }
}

// ─── Equality ────────────────────────────────────────────────────────────

fn to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Decimal(d) => Some(d.to_f64()),
        _ => None,
    }
}

pub(crate) fn to_decimal_operand(v: &Value) -> Option<Decimal> {
    match v {
        Value::Decimal(d) => Some((**d).clone()),
        Value::Int(i) => Some(Decimal::from_int(*i)),
        _ => None,
    }
}

/// The port of eval.objectsEqual: numeric cross-type (1 == 1.0; int↔decimal
/// exact), String/Bool/Null by value, List elementwise, Dict order-
/// independent, record instances by struct+fields, everything else by
/// pointer identity.
pub fn objects_equal(left: &Value, right: &Value) -> bool {
    match left {
        Value::Decimal(l) => {
            if let Some(rd) = to_decimal_operand(right) {
                return l.cmp(&rd) == std::cmp::Ordering::Equal;
            }
            match to_f64(right) {
                Some(rf) => l.to_f64() == rf,
                None => false,
            }
        }
        Value::Int(_) | Value::Float(_) => {
            // An Integer compared to a Decimal stays exact (19 == 19.00).
            if let Value::Decimal(rd) = right
                && let Some(ld) = to_decimal_operand(left)
            {
                return ld.cmp(rd) == std::cmp::Ordering::Equal;
            }
            match (to_f64(left), to_f64(right)) {
                (Some(lf), Some(rf)) => lf == rf,
                _ => false,
            }
        }
        Value::Str(l) => matches!(right, Value::Str(r) if l == r),
        Value::Bool(l) => matches!(right, Value::Bool(r) if l == r),
        Value::Null => matches!(right, Value::Null),
        Value::List(l) => {
            let Value::List(r) = right else { return false };
            if Rc::ptr_eq(l, r) {
                return true;
            }
            let (l, r) = (l.borrow(), r.borrow());
            l.len() == r.len() && l.iter().zip(r.iter()).all(|(a, b)| objects_equal(a, b))
        }
        Value::Dict(l) => {
            let Value::Dict(r) = right else { return false };
            if Rc::ptr_eq(l, r) {
                return true;
            }
            let (l, r) = (l.borrow(), r.borrow());
            if l.len() != r.len() {
                return false;
            }
            l.in_order().all(|(lk, lv)| {
                let Some(hk) = hash_key(lk) else { return false };
                match r.get(&hk) {
                    Some((_, rv)) => objects_equal(lv, rv),
                    None => false,
                }
            })
        }
        Value::Instance(l) if l.borrow().struct_.is_record => {
            let Value::Instance(r) = right else {
                return false;
            };
            if Rc::ptr_eq(l, r) {
                return true;
            }
            let (l, r) = (l.borrow(), r.borrow());
            if !Rc::ptr_eq(&l.struct_, &r.struct_) || l.fields.len() != r.fields.len() {
                return false;
            }
            l.fields.iter().all(|(name, lv)| match r.get_field(name) {
                Some(rv) => objects_equal(lv, &rv),
                None => false,
            })
        }
        // Pointer identity for everything else.
        Value::Instance(l) => matches!(right, Value::Instance(r) if Rc::ptr_eq(l, r)),
        Value::Range(l) => matches!(right, Value::Range(r) if Rc::ptr_eq(l, r)),
        Value::Closure(l) => matches!(right, Value::Closure(r) if Rc::ptr_eq(l, r)),
        Value::Builtin(l) => matches!(right, Value::Builtin(r) if l == r),
        Value::Struct(l) => matches!(right, Value::Struct(r) if Rc::ptr_eq(l, r)),
        Value::Contract(l) => matches!(right, Value::Contract(r) if Rc::ptr_eq(l, r)),
        Value::CompiledFn(l) => matches!(right, Value::CompiledFn(r) if Rc::ptr_eq(l, r)),
        Value::Cell(l) => matches!(right, Value::Cell(r) if Rc::ptr_eq(l, r)),
        Value::Iterator(l) => matches!(right, Value::Iterator(r) if Rc::ptr_eq(l, r)),
        Value::Rethrow(l) => matches!(right, Value::Rethrow(r) if Rc::ptr_eq(l, r)),
    }
}

// ─── Inspect / Repr ──────────────────────────────────────────────────────

/// Float rendering per object.Float.Inspect: 3.0 stays "3.0", scientific
/// notation only below 1e-4 or at 1e16 and beyond (Go FormatFloat style).
pub fn format_float(v: f64) -> String {
    if v.is_nan() {
        return "NaN".into();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            "+Inf".into()
        } else {
            "-Inf".into()
        };
    }
    let abs = v.abs();
    #[allow(clippy::manual_range_contains)] // mirrors the Go condition verbatim
    if v != 0.0 && (abs >= 1e16 || abs < 1e-4) {
        return format_float_e(v);
    }
    let s = format!("{v}");
    if s.contains('.') { s } else { format!("{s}.0") }
}

/// Go strconv.FormatFloat(v, 'e', -1, 64): shortest mantissa, `e`, signed
/// exponent of at least two digits — "1e+16", "1.5e-05".
fn format_float_e(v: f64) -> String {
    let s = format!("{v:e}"); // e.g. "1e16", "1.5e-5"
    let (mant, exp) = s.split_once('e').expect("{:e} always contains e");
    let (sign, digits) = match exp.strip_prefix('-') {
        Some(d) => ('-', d),
        None => ('+', exp),
    };
    if digits.len() < 2 {
        format!("{mant}e{sign}0{digits}")
    } else {
        format!("{mant}e{sign}{digits}")
    }
}

/// Go strconv.Quote — double-quoted with Go escape sequences. Printable
/// characters (including non-ASCII letters) stay literal.
pub fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            '\x07' => out.push_str("\\a"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            '\x0b' => out.push_str("\\v"),
            c if go_is_print(c) => out.push(c),
            c => {
                let n = c as u32;
                if n < 0x80 {
                    out.push_str(&format!("\\x{n:02x}"));
                } else if n < 0x10000 {
                    out.push_str(&format!("\\u{n:04x}"));
                } else {
                    out.push_str(&format!("\\U{n:08x}"));
                }
            }
        }
    }
    out.push('"');
    out
}

/// Approximates Go unicode.IsPrint closely enough for quoting: ASCII space
/// through ~, plus non-control non-ASCII characters.
fn go_is_print(c: char) -> bool {
    if c.is_ascii() {
        (' '..='~').contains(&c)
    } else {
        !c.is_control() && c != '\u{feff}'
    }
}

/// Inspect: what `print` shows. Strings unquoted at top level.
pub fn inspect(v: &Value) -> String {
    inspect_seen(v, &mut Vec::new())
}

/// Repr: Inspect except a top-level string is quoted — used inside
/// containers and in error texts.
pub fn repr(v: &Value) -> String {
    repr_seen(v, &mut Vec::new())
}

fn repr_seen(v: &Value, seen: &mut Vec<usize>) -> String {
    if let Value::Str(s) = v {
        return go_quote(s);
    }
    inspect_seen(v, seen)
}

fn inspect_seen(v: &Value, seen: &mut Vec<usize>) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format_float(*f),
        Value::Str(s) => s.to_string(),
        Value::Decimal(d) => d.inspect(),
        Value::List(l) => {
            let ptr = Rc::as_ptr(l) as usize;
            if seen.contains(&ptr) {
                return "[...]".into();
            }
            seen.push(ptr);
            let parts: Vec<String> = l.borrow().iter().map(|e| repr_seen(e, seen)).collect();
            seen.pop();
            format!("[{}]", parts.join(", "))
        }
        Value::Dict(d) => {
            let ptr = Rc::as_ptr(d) as usize;
            if seen.contains(&ptr) {
                return "{...}".into();
            }
            seen.push(ptr);
            let parts: Vec<String> = d
                .borrow()
                .in_order()
                .map(|(k, val)| format!("{}: {}", repr_seen(k, seen), repr_seen(val, seen)))
                .collect();
            seen.pop();
            format!("{{{}}}", parts.join(", "))
        }
        Value::Instance(i) => {
            let ptr = Rc::as_ptr(i) as usize;
            let name = i.borrow().struct_.name.clone();
            if seen.contains(&ptr) {
                return format!("{name}{{...}}");
            }
            seen.push(ptr);
            let parts: Vec<String> = i
                .borrow()
                .fields
                .iter()
                .map(|(n, val)| format!("{n}: {}", repr_seen(val, seen)))
                .collect();
            seen.pop();
            format!("{name}{{{}}}", parts.join(", "))
        }
        Value::Range(r) => {
            let n = r.len();
            let parts: Vec<String> = (0..n).map(|i| r.at(i).to_string()).collect();
            format!("[{}]", parts.join(", "))
        }
        Value::Closure(c) => format!("def {}(...) ... end", c.func.name),
        Value::CompiledFn(f) => format!("def {}(...) ... end", f.name),
        Value::Builtin(idx) => format!(
            "builtin function {}",
            crate::builtins::BUILTIN_NAMES
                .get(*idx)
                .copied()
                .unwrap_or("?")
        ),
        Value::Struct(s) => format!("struct {}", s.name),
        Value::Contract(c) => format!("contract {}", c.name),
        Value::Cell(_) => "<cell>".into(),
        Value::Iterator(_) => "iterator".into(),
        Value::Rethrow(_) => "rethrow token".into(),
    }
}

// ─── FNV-1a (hash builtin + tests) ───────────────────────────────────────

pub fn fnv32a(s: &str) -> u32 {
    let mut h: u32 = 2166136261;
    for b in s.bytes() {
        h = (h ^ b as u32).wrapping_mul(16777619);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list(vals: Vec<Value>) -> Value {
        Value::List(Rc::new(RefCell::new(vals)))
    }

    fn s(v: &str) -> Value {
        Value::Str(Rc::from(v))
    }

    #[test]
    fn float_inspect_formats() {
        assert_eq!(format_float(3.0), "3.0");
        assert_eq!(format_float(0.5), "0.5");
        assert_eq!(format_float(1e16), "1e+16");
        assert_eq!(format_float(1e-5), "1e-05");
        assert_eq!(format_float(0.0001), "0.0001");
        assert_eq!(format_float(0.00009999), "9.999e-05");
        assert_eq!(format_float(21.333333333333332), "21.333333333333332");
        assert_eq!(format_float(-2.5), "-2.5");
        assert_eq!(format_float(0.0), "0.0");
        assert_eq!(format_float(1.5e17), "1.5e+17");
        assert_eq!(format_float(f64::NAN), "NaN");
        assert_eq!(format_float(f64::INFINITY), "+Inf");
        assert_eq!(format_float(f64::NEG_INFINITY), "-Inf");
    }

    #[test]
    fn inspect_containers() {
        let l = list(vec![Value::Int(1), s("x"), Value::Null]);
        assert_eq!(inspect(&l), "[1, \"x\", null]");
        assert_eq!(inspect(&s("plain")), "plain");
        assert_eq!(repr(&s("plain")), "\"plain\"");
    }

    #[test]
    fn inspect_cycles() {
        let inner = Rc::new(RefCell::new(vec![Value::Int(1)]));
        let l = Value::List(Rc::clone(&inner));
        inner.borrow_mut().push(l.clone());
        assert_eq!(inspect(&l), "[1, [...]]");
    }

    #[test]
    fn dict_insertion_order_and_reinsert() {
        let mut d = Dict::new();
        for (k, v) in [("a", 1), ("b", 2), ("c", 3)] {
            d.set(hash_key(&s(k)).unwrap(), s(k), Value::Int(v));
        }
        // Re-insert "a": keeps its original (first) position.
        d.set(hash_key(&s("a")).unwrap(), s("a"), Value::Int(9));
        let keys: Vec<String> = d.in_order().map(|(k, _)| inspect(k)).collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
        // Delete then check order and len.
        assert!(d.delete(&hash_key(&s("b")).unwrap()));
        assert!(!d.delete(&hash_key(&s("b")).unwrap()));
        let keys: Vec<String> = d.in_order().map(|(k, _)| inspect(k)).collect();
        assert_eq!(keys, vec!["a", "c"]);
        assert_eq!(d.len(), 2);
    }

    #[test]
    fn hash_key_kinds() {
        // 1 and 1.0 are DIFFERENT dict keys; decimal 1.0 == 1.00 same key.
        assert_ne!(hash_key(&Value::Int(1)), hash_key(&Value::Float(1.0)));
        let d1 = Value::Decimal(Rc::new(Decimal::parse("1.0").unwrap()));
        let d2 = Value::Decimal(Rc::new(Decimal::parse("1.00").unwrap()));
        assert_eq!(hash_key(&d1), hash_key(&d2));
        assert!(hash_key(&list(vec![])).is_none());
    }

    #[test]
    fn objects_equal_cross_type() {
        assert!(objects_equal(&Value::Int(1), &Value::Float(1.0)));
        assert!(!objects_equal(&Value::Int(1), &s("1")));
        assert!(!objects_equal(&Value::Bool(true), &Value::Int(1)));
        let d = Value::Decimal(Rc::new(Decimal::parse("19.00").unwrap()));
        assert!(objects_equal(&Value::Int(19), &d));
        assert!(objects_equal(&d, &Value::Int(19)));
        assert!(objects_equal(
            &list(vec![Value::Int(1), Value::Int(2)]),
            &list(vec![Value::Int(1), Value::Float(2.0)])
        ));
        // Two same-bounds ranges are pointer-distinct, hence unequal.
        let r1 = Value::Range(Rc::new(Range {
            start: 0,
            stop: 3,
            step: 1,
        }));
        let r2 = Value::Range(Rc::new(Range {
            start: 0,
            stop: 3,
            step: 1,
        }));
        assert!(!objects_equal(&r1, &r2));
        assert!(objects_equal(&r1, &r1.clone()));
    }

    #[test]
    fn truthiness() {
        assert!(!truthy(&Value::Null));
        assert!(!truthy(&Value::Int(0)));
        assert!(!truthy(&Value::Float(0.0)));
        assert!(!truthy(&s("")));
        assert!(!truthy(&list(vec![])));
        assert!(truthy(&Value::Range(Rc::new(Range {
            start: 0,
            stop: 0,
            step: 1
        }))));
        assert!(truthy(&s("x")));
    }
}
