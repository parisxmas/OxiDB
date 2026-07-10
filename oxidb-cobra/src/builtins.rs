//! Builtin functions — `OpGetBuiltin`'s operand indexes this table, so the
//! ORDER is ABI and must match `eval.Builtins` in the Cobra repo exactly
//! (eval/builtins.go base slice + functionalBuiltins() appended).
//!
//! Server policy (ADR-0014 determinism):
//! - `read_line` / `input` — banned (no stdin in a stored procedure).
//! - `Mutex` / `RWMutex` / `Channel` / `select` — banned (concurrency).
//! - `pmap` / `pfilter` / `pforeach` / `preduce` — implemented SEQUENTIALLY:
//!   same results as the Go parallel versions (which preserve order), fully
//!   deterministic.
//! - `print` writes into a captured notices buffer, not a real stream.

/// Canonical builtin order (ABI).
pub const BUILTIN_NAMES: [&str; 35] = [
    "print",      // 0
    "len",        // 1
    "push",       // 2
    "pop",        // 3
    "keys",       // 4
    "values",     // 5
    "has",        // 6
    "int",        // 7
    "float",      // 8
    "decimal",    // 9
    "hash",       // 10
    "range",      // 11
    "del",        // 12
    "type",       // 13
    "str",        // 14
    "implements", // 15
    "chr",        // 16
    "ord",        // 17
    "read_line",  // 18  banned
    "input",      // 19  banned
    "Mutex",      // 20  banned
    "RWMutex",    // 21  banned
    "Channel",    // 22  banned
    "select",     // 23  banned
    "map",        // 24
    "pmap",       // 25  sequential here
    "pfilter",    // 26  sequential here
    "pforeach",   // 27  sequential here
    "filter",     // 28
    "reduce",     // 29
    "preduce",    // 30  sequential here
    "zip",        // 31
    "enumerate",  // 32
    "any",        // 33
    "all",        // 34
];

/// Builtins rejected at validation (CREATE PROCEDURE) — I/O + concurrency.
pub fn is_banned(name: &str) -> bool {
    matches!(
        name,
        "read_line" | "input" | "Mutex" | "RWMutex" | "Channel" | "select"
    )
}

// ─── Implementations (ports of eval/builtins.go + eval/functional.go) ────
//
// Errors carry no line prefix; the VM's call site wraps them. The parallel
// p-variants run SEQUENTIALLY here — the Go versions preserve input order
// and surface the first error in index order, so a sequential run is
// observably identical (and deterministic, per ADR-0014).

use std::cell::RefCell;
use std::rc::Rc;

use crate::decimal::Decimal;
use crate::value::{
    NativeError, Range, Value, fnv32a, go_quote, hash_key, inspect, native_err, repr, truthy,
};
use crate::vm::Vm;

type BResult = Result<Value, NativeError>;

/// Dispatch a builtin by its ABI index.
pub fn call(vm: &Vm, idx: usize, args: &[Value]) -> BResult {
    let Some(&name) = BUILTIN_NAMES.get(idx) else {
        return Err(native_err!("unknown builtin index {idx}"));
    };
    if is_banned(name) {
        return Err(native_err!(
            "builtin '{name}' is not allowed in a stored procedure"
        ));
    }
    match name {
        "print" => {
            let parts: Vec<String> = args.iter().map(inspect).collect();
            vm.write_line(&parts.join(" "));
            Ok(Value::Null)
        }
        "len" => builtin_len(args),
        "push" => builtin_push(args),
        "pop" => builtin_pop(args),
        "keys" => builtin_keys_values(args, "keys", true),
        "values" => builtin_keys_values(args, "values", false),
        "has" => builtin_has(args),
        "int" => builtin_int(args),
        "float" => builtin_float(args),
        "decimal" => builtin_decimal(args),
        "hash" => builtin_hash(args),
        "range" => builtin_range(args),
        "del" => builtin_del(args),
        "type" => {
            want(name, args, 1)?;
            Ok(Value::Str(Rc::from(args[0].type_name())))
        }
        "str" => {
            want(name, args, 1)?;
            Ok(Value::Str(Rc::from(inspect(&args[0]))))
        }
        "implements" => builtin_implements(args),
        "chr" => builtin_chr(args),
        "ord" => builtin_ord(args),
        "map" | "pmap" => builtin_map(vm, name, args),
        "filter" | "pfilter" => builtin_filter(vm, name, args),
        "pforeach" => builtin_pforeach(vm, args),
        "reduce" | "preduce" => builtin_reduce(vm, name, args),
        "zip" => builtin_zip(args),
        "enumerate" => builtin_enumerate(args),
        "any" => builtin_any_all(args, "any", true),
        "all" => builtin_any_all(args, "all", false),
        _ => Err(native_err!("builtin '{name}' is not implemented")),
    }
}

fn want(name: &str, args: &[Value], n: usize) -> Result<(), NativeError> {
    if args.len() != n {
        return Err(native_err!(
            "wrong number of arguments to {name}: want={n}, got={}",
            args.len()
        ));
    }
    Ok(())
}

fn builtin_len(args: &[Value]) -> BResult {
    want("len", args, 1)?;
    match &args[0] {
        // Characters, not bytes — matches indexing, slicing and for-in.
        Value::Str(s) => Ok(Value::Int(s.chars().count() as i64)),
        Value::List(l) => Ok(Value::Int(l.borrow().len() as i64)),
        Value::Range(r) => Ok(Value::Int(r.len())),
        Value::Dict(d) => Ok(Value::Int(d.borrow().len() as i64)),
        v => Err(native_err!(
            "argument to len not supported: {}",
            v.type_name()
        )),
    }
}

fn builtin_push(args: &[Value]) -> BResult {
    want("push", args, 2)?;
    let Value::List(l) = &args[0] else {
        return Err(native_err!(
            "first argument to push must be LIST, got {}",
            args[0].type_name()
        ));
    };
    l.borrow_mut().push(args[1].clone());
    Ok(Value::Null)
}

fn builtin_pop(args: &[Value]) -> BResult {
    want("pop", args, 1)?;
    let Value::List(l) = &args[0] else {
        return Err(native_err!(
            "argument to pop must be LIST, got {}",
            args[0].type_name()
        ));
    };
    l.borrow_mut()
        .pop()
        .ok_or_else(|| native_err!("pop from empty list"))
}

fn builtin_keys_values(args: &[Value], name: &str, keys: bool) -> BResult {
    want(name, args, 1)?;
    let Value::Dict(d) = &args[0] else {
        return Err(native_err!(
            "argument to {name} must be DICT, got {}",
            args[0].type_name()
        ));
    };
    let elements: Vec<Value> = d
        .borrow()
        .in_order()
        .map(|(k, v)| if keys { k.clone() } else { v.clone() })
        .collect();
    Ok(Value::List(Rc::new(RefCell::new(elements))))
}

fn builtin_has(args: &[Value]) -> BResult {
    want("has", args, 2)?;
    let Value::Dict(d) = &args[0] else {
        return Err(native_err!(
            "first argument to has must be DICT, got {}",
            args[0].type_name()
        ));
    };
    let Some(hk) = hash_key(&args[1]) else {
        return Err(native_err!(
            "not a hashable dict key: {}",
            args[1].type_name()
        ));
    };
    Ok(Value::Bool(d.borrow().get(&hk).is_some()))
}

fn builtin_int(args: &[Value]) -> BResult {
    want("int", args, 1)?;
    match &args[0] {
        Value::Int(i) => Ok(Value::Int(*i)),
        Value::Float(f) => {
            if f.is_nan() || f.is_infinite() {
                return Err(native_err!(
                    "cannot convert {} to int",
                    crate::value::format_float(*f)
                ));
            }
            Ok(Value::Int(*f as i64)) // truncate toward zero
        }
        Value::Decimal(d) => Ok(Value::Int(d.to_i64())),
        Value::Bool(b) => Ok(Value::Int(i64::from(*b))),
        Value::Str(s) => match parse_go_int(s.trim()) {
            Some(n) => Ok(Value::Int(n)),
            None => Err(native_err!("cannot convert {} to int", go_quote(s))),
        },
        v => Err(native_err!(
            "argument to int not supported: {}",
            v.type_name()
        )),
    }
}

/// Go strconv.ParseInt(s, 10, 64) — optional sign, decimal digits only.
fn parse_go_int(s: &str) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    s.parse::<i64>().ok()
}

fn builtin_float(args: &[Value]) -> BResult {
    want("float", args, 1)?;
    match &args[0] {
        Value::Float(f) => Ok(Value::Float(*f)),
        Value::Int(i) => Ok(Value::Float(*i as f64)),
        Value::Bool(b) => Ok(Value::Float(if *b { 1.0 } else { 0.0 })),
        Value::Str(s) => match s.trim().parse::<f64>() {
            Ok(f) => Ok(Value::Float(f)),
            Err(_) => Err(native_err!("cannot convert {} to float", go_quote(s))),
        },
        v => Err(native_err!(
            "argument to float not supported: {}",
            v.type_name()
        )),
    }
}

fn builtin_decimal(args: &[Value]) -> BResult {
    want("decimal", args, 1)?;
    match &args[0] {
        Value::Decimal(d) => Ok(Value::Decimal(Rc::clone(d))),
        Value::Int(i) => Ok(Value::Decimal(Rc::new(Decimal::from_int(*i)))),
        Value::Str(s) => match Decimal::parse(s) {
            Some(d) => Ok(Value::Decimal(Rc::new(d))),
            None => Err(native_err!("cannot convert {} to decimal", go_quote(s))),
        },
        Value::Float(f) => {
            // Round-trip through the shortest string so decimal(0.1) is 0.1.
            match Decimal::parse(&format!("{f}")) {
                Some(d) => Ok(Value::Decimal(Rc::new(d))),
                None => Err(native_err!("cannot convert float {f} to decimal")),
            }
        }
        v => Err(native_err!(
            "argument to decimal not supported: {}",
            v.type_name()
        )),
    }
}

fn builtin_hash(args: &[Value]) -> BResult {
    want("hash", args, 1)?;
    let Value::Str(s) = &args[0] else {
        return Err(native_err!(
            "hash: argument must be STRING, got {}",
            args[0].type_name()
        ));
    };
    Ok(Value::Str(Rc::from(format!("{:x}", fnv32a(s)))))
}

fn builtin_range(args: &[Value]) -> BResult {
    if args.is_empty() || args.len() > 3 {
        return Err(native_err!(
            "wrong number of arguments to range: want=1..3, got={}",
            args.len()
        ));
    }
    let mut nums = [0i64; 3];
    for (i, a) in args.iter().enumerate() {
        let Value::Int(n) = a else {
            return Err(native_err!(
                "arguments to range must be INTEGER, got {}",
                a.type_name()
            ));
        };
        nums[i] = *n;
    }
    let (start, stop, step) = match args.len() {
        1 => (0, nums[0], 1),
        2 => (nums[0], nums[1], 1),
        _ => (nums[0], nums[1], nums[2]),
    };
    if step == 0 {
        return Err(native_err!("range step must not be zero"));
    }
    Ok(Value::Range(Rc::new(Range { start, stop, step })))
}

fn builtin_del(args: &[Value]) -> BResult {
    want("del", args, 2)?;
    let Value::Dict(d) = &args[0] else {
        return Err(native_err!(
            "first argument to del must be DICT, got {}",
            args[0].type_name()
        ));
    };
    let Some(hk) = hash_key(&args[1]) else {
        return Err(native_err!(
            "not a hashable dict key: {}",
            args[1].type_name()
        ));
    };
    if !d.borrow_mut().delete(&hk) {
        return Err(native_err!("key not found: {}", repr(&args[1])));
    }
    Ok(Value::Null)
}

fn builtin_implements(args: &[Value]) -> BResult {
    want("implements", args, 2)?;
    let Value::Contract(contract) = &args[1] else {
        return Err(native_err!(
            "implements: second argument must be a contract, got {}",
            args[1].type_name()
        ));
    };
    let def = match &args[0] {
        Value::Instance(i) => Rc::clone(&i.borrow().struct_),
        Value::Struct(s) => Rc::clone(s),
        v => {
            return Err(native_err!(
                "implements: first argument must be a struct or instance, got {}",
                v.type_name()
            ));
        }
    };
    let ok = contract
        .methods
        .iter()
        .all(|m| def.find_method(m).is_some());
    Ok(Value::Bool(ok))
}

fn builtin_chr(args: &[Value]) -> BResult {
    want("chr", args, 1)?;
    let Value::Int(n) = &args[0] else {
        return Err(native_err!(
            "argument to chr must be INTEGER, got {}",
            args[0].type_name()
        ));
    };
    if *n < 0 || *n > 0x10FFFF {
        return Err(native_err!(
            "chr: code point out of range (0..0x10FFFF): {n}"
        ));
    }
    // Go string(rune) maps invalid code points (surrogates) to U+FFFD.
    let c = char::from_u32(*n as u32).unwrap_or('\u{FFFD}');
    Ok(Value::Str(Rc::from(c.to_string())))
}

fn builtin_ord(args: &[Value]) -> BResult {
    want("ord", args, 1)?;
    let Value::Str(s) = &args[0] else {
        return Err(native_err!(
            "argument to ord must be STRING, got {}",
            args[0].type_name()
        ));
    };
    match s.chars().next() {
        Some(c) => Ok(Value::Int(c as i64)),
        None => Err(native_err!("ord: empty string")),
    }
}

// ─── Higher-order list builtins ──────────────────────────────────────────

fn is_callable(v: &Value) -> bool {
    matches!(v, Value::Closure(_) | Value::Builtin(_) | Value::Struct(_))
}

/// A lazy range materializes when a list builtin needs random access.
fn want_list(name: &str, arg: &Value, pos: &str) -> Result<Vec<Value>, NativeError> {
    match arg {
        Value::List(l) => Ok(l.borrow().clone()),
        Value::Range(r) => Ok((0..r.len()).map(|i| Value::Int(r.at(i))).collect()),
        v => Err(native_err!(
            "{name}: {pos} argument must be a list, got {}",
            v.type_name()
        )),
    }
}

fn builtin_map(vm: &Vm, name: &str, args: &[Value]) -> BResult {
    want(name, args, 2)?;
    if !is_callable(&args[0]) {
        return Err(native_err!(
            "{name}: first argument must be a function, got {}",
            args[0].type_name()
        ));
    }
    let list = want_list(name, &args[1], "second")?;
    let mut out = Vec::with_capacity(list.len());
    for e in &list {
        out.push(vm.call_callable(&args[0], std::slice::from_ref(e))?);
    }
    Ok(Value::List(Rc::new(RefCell::new(out))))
}

fn builtin_filter(vm: &Vm, name: &str, args: &[Value]) -> BResult {
    want(name, args, 2)?;
    if !is_callable(&args[0]) {
        return Err(native_err!(
            "{name}: first argument must be a function, got {}",
            args[0].type_name()
        ));
    }
    let list = want_list(name, &args[1], "second")?;
    let mut out = Vec::new();
    for e in &list {
        let verdict = vm.call_callable(&args[0], std::slice::from_ref(e))?;
        if truthy(&verdict) {
            out.push(e.clone());
        }
    }
    Ok(Value::List(Rc::new(RefCell::new(out))))
}

fn builtin_pforeach(vm: &Vm, args: &[Value]) -> BResult {
    want("pforeach", args, 2)?;
    if !is_callable(&args[0]) {
        return Err(native_err!(
            "pforeach: first argument must be a function, got {}",
            args[0].type_name()
        ));
    }
    let list = want_list("pforeach", &args[1], "second")?;
    for e in &list {
        vm.call_callable(&args[0], std::slice::from_ref(e))?;
    }
    Ok(Value::Null)
}

fn builtin_reduce(vm: &Vm, name: &str, args: &[Value]) -> BResult {
    if args.len() < 2 || args.len() > 3 {
        return Err(native_err!(
            "{name}(fn, list, init?) takes 2 or 3 arguments, got {}",
            args.len()
        ));
    }
    if !is_callable(&args[0]) {
        return Err(native_err!(
            "{name}: first argument must be a function, got {}",
            args[0].type_name()
        ));
    }
    let list = want_list(name, &args[1], "second")?;
    let has_init = args.len() == 3;

    if name == "preduce" {
        // Sequential (single-chunk) preduce: fold the elements, then combine
        // init on the LEFT of the result, exactly like the Go chunk-combine.
        if list.is_empty() {
            if has_init {
                return Ok(args[2].clone());
            }
            return Err(native_err!("preduce of empty list with no initial value"));
        }
        let mut acc = list[0].clone();
        for e in &list[1..] {
            acc = vm.call_callable(&args[0], &[acc, e.clone()])?;
        }
        if has_init {
            acc = vm.call_callable(&args[0], &[args[2].clone(), acc])?;
        }
        return Ok(acc);
    }

    let (mut acc, start) = if has_init {
        (args[2].clone(), 0)
    } else {
        if list.is_empty() {
            return Err(native_err!("reduce of empty list with no initial value"));
        }
        (list[0].clone(), 1)
    };
    for e in &list[start..] {
        acc = vm.call_callable(&args[0], &[acc, e.clone()])?;
    }
    Ok(acc)
}

fn ordinal(n: usize) -> &'static str {
    match n {
        1 => "first",
        2 => "second",
        3 => "third",
        _ => "an",
    }
}

fn builtin_zip(args: &[Value]) -> BResult {
    if args.is_empty() {
        return Err(native_err!("zip takes at least one list"));
    }
    let mut lists = Vec::with_capacity(args.len());
    let mut min_len = usize::MAX;
    for (i, a) in args.iter().enumerate() {
        let l = want_list("zip", a, ordinal(i + 1))?;
        min_len = min_len.min(l.len());
        lists.push(l);
    }
    let mut out = Vec::with_capacity(min_len);
    for i in 0..min_len {
        let tuple: Vec<Value> = lists.iter().map(|l| l[i].clone()).collect();
        out.push(Value::List(Rc::new(RefCell::new(tuple))));
    }
    Ok(Value::List(Rc::new(RefCell::new(out))))
}

fn builtin_enumerate(args: &[Value]) -> BResult {
    if args.is_empty() || args.len() > 2 {
        return Err(native_err!(
            "enumerate(list, start?) takes 1 or 2 arguments, got {}",
            args.len()
        ));
    }
    let list = want_list("enumerate", &args[0], "first")?;
    let start = if args.len() == 2 {
        match &args[1] {
            Value::Int(n) => *n,
            v => {
                return Err(native_err!(
                    "enumerate: start must be an integer, got {}",
                    v.type_name()
                ));
            }
        }
    } else {
        0
    };
    let out: Vec<Value> = list
        .into_iter()
        .enumerate()
        .map(|(i, e)| Value::List(Rc::new(RefCell::new(vec![Value::Int(start + i as i64), e]))))
        .collect();
    Ok(Value::List(Rc::new(RefCell::new(out))))
}

fn builtin_any_all(args: &[Value], name: &str, any: bool) -> BResult {
    want(name, args, 1)?;
    let list = want_list(name, &args[0], "first")?;
    if any {
        Ok(Value::Bool(list.iter().any(truthy)))
    } else {
        Ok(Value::Bool(list.iter().all(truthy)))
    }
}
