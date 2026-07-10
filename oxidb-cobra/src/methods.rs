//! Method dispatch, properties and record `with` — the port of
//! eval/methods.go and eval.GetProperty / SetProperty / RecordWith.
//!
//! Errors carry no line prefix; the VM wraps them at the call site.
//! Per Go's CallMethod, a method's inner error is RE-wrapped (dropping any
//! thrown value); property getter/setter errors pass through intact.

use std::cmp::Ordering;
use std::rc::Rc;

use crate::value::{Instance, NativeError, Value, hash_key, native_err, objects_equal, repr};
use crate::vm::Vm;

type OpResult = Result<Value, NativeError>;

fn want_args(name: &str, args: &[Value], want: usize) -> Result<(), NativeError> {
    if args.len() != want {
        return Err(native_err!(
            "wrong number of arguments to {name}: want={want}, got={}",
            args.len()
        ));
    }
    Ok(())
}

fn one_string_arg<'a>(name: &str, args: &'a [Value]) -> Result<&'a str, NativeError> {
    want_args(name, args, 1)?;
    match &args[0] {
        Value::Str(s) => Ok(s),
        v => Err(native_err!(
            "argument to {name} must be STRING, got {}",
            v.type_name()
        )),
    }
}

/// receiver.name(args) for String / List / Dict / Decimal / Struct statics.
/// Instances and record `with` are handled in the VM before reaching here.
pub fn call_method(vm: &Vm, receiver: &Value, name: &str, args: &[Value]) -> OpResult {
    let result: Option<OpResult> = match receiver {
        Value::Str(s) => string_method(s, name, args),
        Value::List(_) => list_method(receiver, name, args),
        Value::Dict(_) => dict_method(receiver, name, args),
        Value::Decimal(d) => decimal_method(d, name, args),
        // Host objects (the stored-procedure `db` handle) dispatch to their
        // own implementation; it reports unknown methods itself.
        Value::Native(obj) => Some(obj.call_method(name, args)),
        // StructName.method(args): a static, called with no self.
        Value::Struct(def) => match def.find_static(name) {
            Some(f) => Some(vm.call_callable(&f, args)),
            None => {
                return Err(native_err!(
                    "struct '{}' has no static method '{}'",
                    def.name,
                    name
                ));
            }
        },
        _ => None,
    };
    match result {
        None => Err(native_err!(
            "{} has no method '{}'",
            receiver.type_name(),
            name
        )),
        // Re-wrap: the thrown value (if any) is dropped, matching Go's
        // errorfAt(line, "%s", err.Message).
        Some(Err(e)) => Err(NativeError::new(e.msg)),
        Some(Ok(v)) => Ok(v),
    }
}

// ─── String methods (rune-aware) ─────────────────────────────────────────

fn string_method(s: &str, name: &str, args: &[Value]) -> Option<OpResult> {
    let r = match name {
        "upper" => want_args("upper", args, 0).map(|_| str_val(s.to_uppercase())),
        "lower" => want_args("lower", args, 0).map(|_| str_val(s.to_lowercase())),
        "strip" => want_args("strip", args, 0).map(|_| str_val(s.trim().to_string())),
        "lstrip" => want_args("lstrip", args, 0).map(|_| str_val(s.trim_start().to_string())),
        "rstrip" => want_args("rstrip", args, 0).map(|_| str_val(s.trim_end().to_string())),
        "split" => str_split(s, args),
        "join" => str_join(s, args),
        "replace" => str_replace(s, args),
        "slice" => str_slice(s, args),
        "substr" => str_substr(s, args),
        "contains" => one_string_arg("contains", args).map(|sub| Value::Bool(s.contains(sub))),
        "startswith" => one_string_arg("startswith", args).map(|p| Value::Bool(s.starts_with(p))),
        "endswith" => one_string_arg("endswith", args).map(|p| Value::Bool(s.ends_with(p))),
        "find" => one_string_arg("find", args).map(|sub| match s.find(sub) {
            // Rune offset, not byte offset.
            Some(idx) => Value::Int(s[..idx].chars().count() as i64),
            None => Value::Int(-1),
        }),
        "count" => {
            one_string_arg("count", args).map(|sub| Value::Int(s.matches(sub).count() as i64))
        }
        _ => return None,
    };
    Some(r)
}

fn str_val(s: String) -> Value {
    Value::Str(Rc::from(s))
}

fn str_split(s: &str, args: &[Value]) -> OpResult {
    if args.is_empty() {
        // No argument splits on runs of whitespace (Go strings.Fields).
        let elems: Vec<Value> = s
            .split_whitespace()
            .map(|f| str_val(f.to_string()))
            .collect();
        return Ok(Value::List(Rc::new(std::cell::RefCell::new(elems))));
    }
    if args.len() > 1 {
        return Err(native_err!(
            "wrong number of arguments to split: want=0..1, got={}",
            args.len()
        ));
    }
    let Value::Str(sep) = &args[0] else {
        return Err(native_err!(
            "argument to split must be STRING, got {}",
            args[0].type_name()
        ));
    };
    if sep.is_empty() {
        return Err(native_err!("empty separator"));
    }
    let elems: Vec<Value> = s.split(&**sep).map(|p| str_val(p.to_string())).collect();
    Ok(Value::List(Rc::new(std::cell::RefCell::new(elems))))
}

fn str_join(sep: &str, args: &[Value]) -> OpResult {
    want_args("join", args, 1)?;
    let Value::List(list) = &args[0] else {
        return Err(native_err!(
            "argument to join must be LIST, got {}",
            args[0].type_name()
        ));
    };
    let list = list.borrow();
    let mut parts = Vec::with_capacity(list.len());
    for e in list.iter() {
        match e {
            Value::Str(s) => parts.push(s.to_string()),
            v => {
                return Err(native_err!(
                    "join requires a list of strings, got {}",
                    v.type_name()
                ));
            }
        }
    }
    Ok(str_val(parts.join(sep)))
}

fn str_replace(s: &str, args: &[Value]) -> OpResult {
    if args.len() != 2 && args.len() != 3 {
        return Err(native_err!(
            "wrong number of arguments to replace: want=2 or 3, got={}",
            args.len()
        ));
    }
    let (Value::Str(old), Value::Str(new)) = (&args[0], &args[1]) else {
        return Err(native_err!("first two arguments to replace must be STRING"));
    };
    let n: i64 = if args.len() == 3 {
        match &args[2] {
            Value::Int(c) => *c,
            _ => return Err(native_err!("third argument to replace (count) must be INT")),
        }
    } else {
        -1 // replace all
    };
    let out = if n < 0 {
        s.replace(&**old, new)
    } else {
        s.replacen(&**old, new, n as usize)
    };
    Ok(str_val(out))
}

fn str_slice(s: &str, args: &[Value]) -> OpResult {
    if args.len() != 1 && args.len() != 2 {
        return Err(native_err!(
            "wrong number of arguments to slice: want=1 or 2, got={}",
            args.len()
        ));
    }
    let Value::Int(start) = &args[0] else {
        return Err(native_err!("slice: start must be INT"));
    };
    let r: Vec<char> = s.chars().collect();
    let n = r.len() as i64;
    let mut i = *start;
    let mut j = n;
    if args.len() == 2 {
        let Value::Int(end) = &args[1] else {
            return Err(native_err!("slice: end must be INT"));
        };
        j = *end;
    }
    if i < 0 {
        i += n;
    }
    if j < 0 {
        j += n;
    }
    i = i.clamp(0, n);
    j = j.min(n).max(i);
    Ok(str_val(r[i as usize..j as usize].iter().collect()))
}

fn str_substr(s: &str, args: &[Value]) -> OpResult {
    if args.len() != 1 && args.len() != 2 {
        return Err(native_err!(
            "wrong number of arguments to substr: want=1 or 2, got={}",
            args.len()
        ));
    }
    let Value::Int(start) = &args[0] else {
        return Err(native_err!("substr: start must be INT"));
    };
    let r: Vec<char> = s.chars().collect();
    let n = r.len() as i64;
    let mut i = *start;
    if i < 0 {
        i += n;
    }
    i = i.clamp(0, n);
    let mut j = n;
    if args.len() == 2 {
        let Value::Int(length) = &args[1] else {
            return Err(native_err!("substr: length must be INT"));
        };
        j = i + length;
    }
    j = j.min(n).max(i);
    Ok(str_val(r[i as usize..j as usize].iter().collect()))
}

// ─── List methods ────────────────────────────────────────────────────────

fn list_method(receiver: &Value, name: &str, args: &[Value]) -> Option<OpResult> {
    let Value::List(l) = receiver else {
        return None;
    };
    let r = match name {
        "push" => want_args("push", args, 1).map(|_| {
            l.borrow_mut().push(args[0].clone());
            Value::Null
        }),
        "pop" => want_args("pop", args, 0).and_then(|_| {
            l.borrow_mut()
                .pop()
                .ok_or_else(|| native_err!("pop from empty list"))
        }),
        "contains" => want_args("contains", args, 1)
            .map(|_| Value::Bool(l.borrow().iter().any(|e| objects_equal(e, &args[0])))),
        "find" => want_args("find", args, 1).map(|_| {
            match l.borrow().iter().position(|e| objects_equal(e, &args[0])) {
                Some(i) => Value::Int(i as i64),
                None => Value::Int(-1),
            }
        }),
        "count" => want_args("count", args, 1).map(|_| {
            Value::Int(
                l.borrow()
                    .iter()
                    .filter(|e| objects_equal(e, &args[0]))
                    .count() as i64,
            )
        }),
        "reverse" => want_args("reverse", args, 0).map(|_| {
            l.borrow_mut().reverse();
            Value::Null
        }),
        "sort" => want_args("sort", args, 0).and_then(|_| list_sort(l)),
        _ => return None,
    };
    Some(r)
}

fn list_sort(l: &Rc<std::cell::RefCell<Vec<Value>>>) -> OpResult {
    let mut elems = l.borrow_mut();
    let (mut all_num, mut all_str) = (true, true);
    for e in elems.iter() {
        match e {
            Value::Int(_) | Value::Float(_) => all_str = false,
            Value::Str(_) => all_num = false,
            other => {
                return Err(native_err!(
                    "cannot sort list containing {}",
                    other.type_name()
                ));
            }
        }
    }
    if !all_num && !all_str {
        return Err(native_err!("cannot sort list of mixed types"));
    }
    if all_num {
        elems.sort_by(|a, b| {
            let fa = match a {
                Value::Int(i) => *i as f64,
                Value::Float(f) => *f,
                _ => 0.0,
            };
            let fb = match b {
                Value::Int(i) => *i as f64,
                Value::Float(f) => *f,
                _ => 0.0,
            };
            fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
        });
    } else {
        elems.sort_by(|a, b| match (a, b) {
            (Value::Str(x), Value::Str(y)) => x.cmp(y),
            _ => Ordering::Equal,
        });
    }
    Ok(Value::Null)
}

// ─── Dict methods ────────────────────────────────────────────────────────

fn dict_method(receiver: &Value, name: &str, args: &[Value]) -> Option<OpResult> {
    let Value::Dict(d) = receiver else {
        return None;
    };
    let list_of = |v: Vec<Value>| Value::List(Rc::new(std::cell::RefCell::new(v)));
    let r = match name {
        "keys" => want_args("keys", args, 0)
            .map(|_| list_of(d.borrow().in_order().map(|(k, _)| k.clone()).collect())),
        "values" => want_args("values", args, 0)
            .map(|_| list_of(d.borrow().in_order().map(|(_, v)| v.clone()).collect())),
        "items" => want_args("items", args, 0).map(|_| {
            list_of(
                d.borrow()
                    .in_order()
                    .map(|(k, v)| list_of(vec![k.clone(), v.clone()]))
                    .collect(),
            )
        }),
        "has" => want_args("has", args, 1).and_then(|_| match hash_key(&args[0]) {
            Some(hk) => Ok(Value::Bool(d.borrow().get(&hk).is_some())),
            None => Err(native_err!(
                "not a hashable dict key: {}",
                args[0].type_name()
            )),
        }),
        "get" => {
            if args.is_empty() || args.len() > 2 {
                return Some(Err(native_err!(
                    "wrong number of arguments to get: want=1..2, got={}",
                    args.len()
                )));
            }
            match hash_key(&args[0]) {
                Some(hk) => Ok(match d.borrow().get(&hk) {
                    Some((_, v)) => v.clone(),
                    None if args.len() == 2 => args[1].clone(),
                    None => Value::Null,
                }),
                None => Err(native_err!(
                    "not a hashable dict key: {}",
                    args[0].type_name()
                )),
            }
        }
        "del" => want_args("del", args, 1).and_then(|_| match hash_key(&args[0]) {
            Some(hk) => {
                if d.borrow_mut().delete(&hk) {
                    Ok(Value::Null)
                } else {
                    Err(native_err!("key not found: {}", repr(&args[0])))
                }
            }
            None => Err(native_err!(
                "not a hashable dict key: {}",
                args[0].type_name()
            )),
        }),
        _ => return None,
    };
    Some(r)
}

// ─── Decimal methods ─────────────────────────────────────────────────────

fn decimal_method(d: &Rc<crate::decimal::Decimal>, name: &str, args: &[Value]) -> Option<OpResult> {
    let r = match name {
        "round" => {
            if args.len() != 1 {
                Err(native_err!(
                    "wrong number of arguments to round: want=1, got={}",
                    args.len()
                ))
            } else {
                match &args[0] {
                    Value::Int(n) => Ok(Value::Decimal(Rc::new(d.round(*n as i32)))),
                    _ => Err(native_err!("round: places must be INT")),
                }
            }
        }
        "to_float" => want_args("to_float", args, 0).map(|_| Value::Float(d.to_f64())),
        "scale" => want_args("scale", args, 0).map(|_| Value::Int(d.scale as i64)),
        _ => return None,
    };
    Some(r)
}

// ─── Properties (eval.go:1299+) ──────────────────────────────────────────

/// receiver.name — the slow path behind the VM's instance-field fast path.
pub fn get_property(vm: &Vm, receiver: &Value, name: &str) -> OpResult {
    if let Value::Struct(def) = receiver {
        if let Some(val) = def.find_const(name) {
            return Ok(val);
        }
        if let Some(f) = def.find_static(name) {
            return Ok(f);
        }
        return Err(native_err!(
            "struct '{}' has no static member '{}'",
            def.name,
            name
        ));
    }
    let Value::Instance(inst_rc) = receiver else {
        return Err(native_err!(
            "{} has no property '{}'",
            receiver.type_name(),
            name
        ));
    };
    // Pull everything out of the borrow before any closure runs.
    let (field, struct_) = {
        let inst = inst_rc.borrow();
        (inst.get_field(name), Rc::clone(&inst.struct_))
    };
    if let Some(val) = field {
        return Ok(val);
    }
    if let Some(getter) = struct_.find_getter(name) {
        // Getter errors pass through UNwrapped (thrown value preserved).
        let Value::Closure(cl) = &getter else {
            return Err(native_err!(
                "{}.{} is not a property",
                receiver.type_name(),
                name
            ));
        };
        return vm.call_closure_native(cl, std::slice::from_ref(receiver));
    }
    if let Some(val) = struct_.find_const(name) {
        return Ok(val);
    }
    if struct_.find_method(name).is_some() {
        return Err(native_err!(
            "{}.{} is a method — call it with parentheses",
            receiver.type_name(),
            name
        ));
    }
    let suggestion = field_suggestion(&inst_rc.borrow(), name);
    Err(native_err!(
        "{} has no field '{}'{}",
        receiver.type_name(),
        name,
        suggestion
    ))
}

/// receiver.name = value — the slow path (setters, frozen records, sealed
/// typos, non-instances).
pub fn set_property(vm: &Vm, receiver: &Value, name: &str, value: Value) -> OpResult {
    let Value::Instance(inst_rc) = receiver else {
        return Err(native_err!(
            "{} does not support property assignment",
            receiver.type_name()
        ));
    };
    let (frozen, sealed, struct_) = {
        let inst = inst_rc.borrow();
        (inst.frozen, inst.sealed, Rc::clone(&inst.struct_))
    };
    if frozen {
        return Err(native_err!(
            "cannot modify immutable record {}",
            struct_.name
        ));
    }
    if let Some(setter) = struct_.find_setter(name) {
        let Value::Closure(cl) = &setter else {
            return Err(native_err!(
                "{}.{} is not a property",
                receiver.type_name(),
                name
            ));
        };
        // Setter errors pass through UNwrapped.
        vm.call_closure_native(cl, &[receiver.clone(), value.clone()])?;
        return Ok(value);
    }
    if sealed
        && !inst_rc
            .borrow_mut()
            .set_field_if_exists(name, value.clone())
    {
        if struct_.find_getter(name).is_some() {
            return Err(native_err!(
                "{}.{} is a read-only property (it has a getter but no setter)",
                receiver.type_name(),
                name
            ));
        }
        let suggestion = field_suggestion(&inst_rc.borrow(), name);
        if !suggestion.is_empty() {
            return Err(native_err!(
                "{} has no field '{}'{}",
                receiver.type_name(),
                name,
                suggestion
            ));
        }
        return Err(native_err!(
            "{} has no field '{}' (fields are created in init)",
            receiver.type_name(),
            name
        ));
    }
    if !sealed {
        inst_rc.borrow_mut().set_field(name, value.clone());
    }
    Ok(value)
}

/// " (did you mean 'x'?)" when a field is within Levenshtein distance 2.
fn field_suggestion(inst: &Instance, name: &str) -> String {
    let (mut best, mut best_dist) = (String::new(), 3usize);
    for (field, _) in &inst.fields {
        let d = levenshtein(name, field);
        if d < best_dist {
            best = field.clone();
            best_dist = d;
        }
    }
    if best.is_empty() {
        String::new()
    } else {
        format!(" (did you mean '{best}'?)")
    }
}

/// Classic two-row edit distance over bytes (field names are short ASCII).
fn levenshtein(a: &str, b: &str) -> usize {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0usize; b.len() + 1];
    for i in 1..=a.len() {
        cur[0] = i;
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            cur[j] = (cur[j - 1] + 1).min(prev[j] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

// ─── Record `with` ───────────────────────────────────────────────────────

/// record.with(dict): a frozen copy with the named fields replaced. Only
/// existing fields may be set.
pub fn record_with(inst_rc: &Rc<std::cell::RefCell<Instance>>, args: &[Value]) -> OpResult {
    if args.len() != 1 {
        return Err(native_err!(
            "with expects 1 argument (a dict), got {}",
            args.len()
        ));
    }
    let Value::Dict(dict) = &args[0] else {
        return Err(native_err!(
            "with expects a dict, got {}",
            args[0].type_name()
        ));
    };
    let mut clone = inst_rc.borrow().clone_fields();
    let struct_name = clone.struct_.name.clone();
    for (k, v) in dict.borrow().in_order() {
        let Value::Str(key) = k else {
            return Err(native_err!(
                "with keys must be field-name strings, got {}",
                k.type_name()
            ));
        };
        if clone.get_field(key).is_none() {
            return Err(native_err!("{struct_name} has no field '{key}'"));
        }
        clone.set_field(key, v.clone());
    }
    clone.frozen = true;
    Ok(Value::Instance(Rc::new(std::cell::RefCell::new(clone))))
}
