//! Shared operations — the port of eval.BinaryOp / PrefixOp / IndexGet /
//! IndexSet / SliceGet / IterItems / NewIterator / DestructureValues
//! (eval/eval.go). All errors are produced WITHOUT a line prefix (Go calls
//! these with line 0 from the VM); the caller wraps them with `line N: `.

use std::cell::RefCell;
use std::rc::Rc;

use crate::decimal::Decimal;
use crate::value::{
    Iter, NativeError, Range, Value, hash_key, native_err, objects_equal, repr, to_decimal_operand,
    truthy,
};

type OpResult = Result<Value, NativeError>;

fn to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Decimal(d) => Some(d.to_f64()),
        _ => None,
    }
}

fn is_decimal_operand(v: &Value) -> bool {
    matches!(v, Value::Decimal(_) | Value::Int(_))
}

// ─── BinaryOp (eval.go:912) — dispatch order is the contract ─────────────

pub fn binary_op(op: &str, left: &Value, right: &Value) -> OpResult {
    // 1. int ⊕ int
    if let (Value::Int(l), Value::Int(r)) = (left, right) {
        return integer_op(op, *l, *r);
    }

    // 2. either DECIMAL and both decimal-operands (Decimal|Int)
    if (matches!(left, Value::Decimal(_)) || matches!(right, Value::Decimal(_)))
        && is_decimal_operand(left)
        && is_decimal_operand(right)
    {
        let ld = to_decimal_operand(left).expect("checked");
        let rd = to_decimal_operand(right).expect("checked");
        return decimal_op(op, &ld, &rd);
    }

    // 3. both float-able (Int|Float|Decimal)
    if let (Some(lf), Some(rf)) = (to_f64(left), to_f64(right)) {
        return float_op(op, lf, rf);
    }

    // 4. str ⊕ str
    if let (Value::Str(l), Value::Str(r)) = (left, right) {
        return string_op(op, l, r);
    }

    // 5. list + list
    if let (Value::List(l), Value::List(r), "+") = (left, right, op) {
        let mut out = l.borrow().clone();
        out.extend(r.borrow().iter().cloned());
        return Ok(Value::List(Rc::new(RefCell::new(out))));
    }

    // 6. equality crosses all types; different types are simply unequal.
    match op {
        "==" => return Ok(Value::Bool(objects_equal(left, right))),
        "!=" => return Ok(Value::Bool(!objects_equal(left, right))),
        _ => {}
    }

    let (lt, rt) = (left.type_name(), right.type_name());
    if lt != rt {
        Err(native_err!("type mismatch: {lt} {op} {rt}"))
    } else {
        Err(native_err!("unknown operator: {lt} {op} {rt}"))
    }
}

fn integer_op(op: &str, l: i64, r: i64) -> OpResult {
    Ok(match op {
        "+" => Value::Int(l.wrapping_add(r)),
        "-" => Value::Int(l.wrapping_sub(r)),
        "*" => Value::Int(l.wrapping_mul(r)),
        "/" => {
            if r == 0 {
                return Err(native_err!("division by zero"));
            }
            Value::Int(l.wrapping_div(r)) // Go trunc division
        }
        "%" => {
            if r == 0 {
                return Err(native_err!("division by zero"));
            }
            Value::Int(l.wrapping_rem(r)) // sign follows dividend
        }
        "==" => Value::Bool(l == r),
        "!=" => Value::Bool(l != r),
        "<" => Value::Bool(l < r),
        ">" => Value::Bool(l > r),
        "<=" => Value::Bool(l <= r),
        ">=" => Value::Bool(l >= r),
        _ => return Err(native_err!("unknown operator: INTEGER {op} INTEGER")),
    })
}

fn decimal_op(op: &str, a: &Decimal, b: &Decimal) -> OpResult {
    use std::cmp::Ordering;
    let cmp = || a.cmp(b);
    Ok(match op {
        "+" => Value::Decimal(Rc::new(a.add(b))),
        "-" => Value::Decimal(Rc::new(a.sub(b))),
        "*" => Value::Decimal(Rc::new(a.mul(b))),
        "/" => {
            // scale = max(scales), floored to working precision 6.
            let scale = a.scale.max(b.scale).max(6);
            match a.div(b, scale) {
                Some(d) => Value::Decimal(Rc::new(d)),
                None => return Err(native_err!("division by zero")),
            }
        }
        "%" => match a.rem(b) {
            Some(d) => Value::Decimal(Rc::new(d)),
            None => return Err(native_err!("division by zero")),
        },
        "==" => Value::Bool(cmp() == Ordering::Equal),
        "!=" => Value::Bool(cmp() != Ordering::Equal),
        "<" => Value::Bool(cmp() == Ordering::Less),
        ">" => Value::Bool(cmp() == Ordering::Greater),
        "<=" => Value::Bool(cmp() != Ordering::Greater),
        ">=" => Value::Bool(cmp() != Ordering::Less),
        _ => return Err(native_err!("unknown operator: DECIMAL {op} DECIMAL")),
    })
}

fn float_op(op: &str, l: f64, r: f64) -> OpResult {
    Ok(match op {
        "+" => Value::Float(l + r),
        "-" => Value::Float(l - r),
        "*" => Value::Float(l * r),
        "/" => {
            if r == 0.0 {
                return Err(native_err!("division by zero"));
            }
            Value::Float(l / r)
        }
        "%" => {
            if r == 0.0 {
                return Err(native_err!("division by zero"));
            }
            Value::Float(go_mod(l, r)) // math.Mod: sign follows dividend
        }
        "==" => Value::Bool(l == r),
        "!=" => Value::Bool(l != r),
        "<" => Value::Bool(l < r),
        ">" => Value::Bool(l > r),
        "<=" => Value::Bool(l <= r),
        ">=" => Value::Bool(l >= r),
        _ => return Err(native_err!("unknown operator: FLOAT {op} FLOAT")),
    })
}

/// Go math.Mod — result sign follows x (same as Rust's %, kept explicit).
fn go_mod(x: f64, y: f64) -> f64 {
    x % y
}

fn string_op(op: &str, l: &str, r: &str) -> OpResult {
    Ok(match op {
        "+" => Value::Str(Rc::from(format!("{l}{r}"))),
        "==" => Value::Bool(l == r),
        "!=" => Value::Bool(l != r),
        "<" => Value::Bool(l < r),
        ">" => Value::Bool(l > r),
        "<=" => Value::Bool(l <= r),
        ">=" => Value::Bool(l >= r),
        _ => return Err(native_err!("unknown operator: STRING {op} STRING")),
    })
}

// ─── PrefixOp ────────────────────────────────────────────────────────────

pub fn prefix_op(op: &str, right: &Value) -> OpResult {
    match op {
        "!" | "not" => Ok(Value::Bool(!truthy(right))),
        "-" => match right {
            Value::Int(i) => Ok(Value::Int(i.wrapping_neg())),
            Value::Float(f) => Ok(Value::Float(-f)),
            Value::Decimal(d) => Ok(Value::Decimal(Rc::new(d.mul(&Decimal::from_int(-1))))),
            _ => Err(native_err!("unknown operator: -{}", right.type_name())),
        },
        _ => Err(native_err!("unknown operator: {op}{}", right.type_name())),
    }
}

// ─── Index / IndexSet (eval.go:603+) ─────────────────────────────────────

/// Resolve a (possibly negative) index against a length.
fn list_index(index: &Value, length: i64) -> Option<i64> {
    let Value::Int(i) = index else { return None };
    let mut idx = *i;
    if idx < 0 {
        idx += length;
    }
    (idx >= 0 && idx < length).then_some(idx)
}

fn index_error(index: &Value, kind: &str) -> NativeError {
    if matches!(index, Value::Int(_)) {
        native_err!(
            "{kind} index out of range: {}",
            crate::value::inspect(index)
        )
    } else {
        native_err!("{kind} index must be an integer, got {}", index.type_name())
    }
}

pub fn index_get(left: &Value, index: &Value) -> OpResult {
    match left {
        Value::List(l) => {
            let l = l.borrow();
            match list_index(index, l.len() as i64) {
                Some(i) => Ok(l[i as usize].clone()),
                None => Err(index_error(index, "list")),
            }
        }
        Value::Str(s) => {
            let n = s.chars().count() as i64;
            match list_index(index, n) {
                Some(i) => {
                    let c = s.chars().nth(i as usize).expect("bounds checked");
                    Ok(Value::Str(Rc::from(c.to_string())))
                }
                None => Err(index_error(index, "string")),
            }
        }
        Value::Range(r) => match list_index(index, r.len()) {
            Some(i) => Ok(Value::Int(r.at(i))),
            None => Err(index_error(index, "range")),
        },
        Value::Dict(d) => {
            let Some(hk) = hash_key(index) else {
                return Err(native_err!(
                    "not a hashable dict key: {}",
                    index.type_name()
                ));
            };
            match d.borrow().get(&hk) {
                Some((_, v)) => Ok(v.clone()),
                None => Err(native_err!("key not found: {}", repr(index))),
            }
        }
        _ => Err(native_err!("type is not indexable: {}", left.type_name())),
    }
}

pub fn index_set(left: &Value, index: &Value, value: Value) -> OpResult {
    match left {
        Value::List(l) => {
            let mut l = l.borrow_mut();
            match list_index(index, l.len() as i64) {
                Some(i) => {
                    l[i as usize] = value.clone();
                    Ok(value)
                }
                None => Err(index_error(index, "list")),
            }
        }
        Value::Dict(d) => {
            let Some(hk) = hash_key(index) else {
                return Err(native_err!(
                    "not a hashable dict key: {}",
                    index.type_name()
                ));
            };
            d.borrow_mut().set(hk, index.clone(), value.clone());
            Ok(value)
        }
        Value::Str(_) => Err(native_err!("strings are immutable")),
        _ => Err(native_err!(
            "type does not support index assignment: {}",
            left.type_name()
        )),
    }
}

// ─── Slice (eval.go:691+) ────────────────────────────────────────────────

fn slice_bound(v: &Value, name: &str) -> Result<Option<i64>, NativeError> {
    match v {
        Value::Null => Ok(None),
        Value::Int(i) => Ok(Some(*i)),
        _ => Err(native_err!(
            "slice {name} must be INT, got {}",
            v.type_name()
        )),
    }
}

/// Python-like slice resolution against length n, bounds clamped.
fn resolve_slice(
    n: i64,
    low: Option<i64>,
    high: Option<i64>,
    step: Option<i64>,
) -> (i64, i64, i64) {
    let st = step.unwrap_or(1);
    let (lower, upper) = if st < 0 { (-1, n - 1) } else { (0, n) };
    let start = match low {
        None => {
            if st < 0 {
                upper
            } else {
                lower
            }
        }
        Some(mut s) => {
            if s < 0 {
                s += n;
                if s < lower {
                    s = lower;
                }
            } else if s > upper {
                s = upper;
            }
            s
        }
    };
    let stop = match high {
        None => {
            if st < 0 {
                lower
            } else {
                upper
            }
        }
        Some(mut s) => {
            if s < 0 {
                s += n;
                if s < lower {
                    s = lower;
                }
            } else if s > upper {
                s = upper;
            }
            s
        }
    };
    (start, stop, st)
}

fn slice_count(start: i64, stop: i64, step: i64) -> i64 {
    if step > 0 {
        if stop <= start {
            0
        } else {
            (stop - start + step - 1) / step
        }
    } else if stop >= start {
        0
    } else {
        (start - stop - step - 1) / -step
    }
}

pub fn slice_get(left: &Value, low_v: &Value, high_v: &Value, step_v: &Value) -> OpResult {
    let low = slice_bound(low_v, "low")?;
    let high = slice_bound(high_v, "high")?;
    let step = slice_bound(step_v, "step")?;
    if step == Some(0) {
        return Err(native_err!("slice step cannot be zero"));
    }

    match left {
        Value::List(l) => {
            let l = l.borrow();
            let n = l.len() as i64;
            let (start, stop, st) = resolve_slice(n, low, high, step);
            let mut out = Vec::with_capacity(slice_count(start, stop, st).max(0) as usize);
            let mut i = start;
            while (st > 0 && i < stop) || (st < 0 && i > stop) {
                out.push(l[i as usize].clone());
                i += st;
            }
            Ok(Value::List(Rc::new(RefCell::new(out))))
        }
        Value::Str(s) => {
            let r: Vec<char> = s.chars().collect();
            let n = r.len() as i64;
            let (start, stop, st) = resolve_slice(n, low, high, step);
            let mut out = String::new();
            let mut i = start;
            while (st > 0 && i < stop) || (st < 0 && i > stop) {
                out.push(r[i as usize]);
                i += st;
            }
            Ok(Value::Str(Rc::from(out)))
        }
        Value::Range(rg) => {
            let n = rg.len();
            let (start, stop, st) = resolve_slice(n, low, high, step);
            let count = slice_count(start, stop, st);
            if count == 0 {
                return Ok(Value::Range(Rc::new(Range {
                    start: 0,
                    stop: 0,
                    step: 1,
                })));
            }
            // A slice of an arithmetic progression is one — stay lazy.
            let new_start = rg.at(start);
            let new_step = rg.step * st;
            Ok(Value::Range(Rc::new(Range {
                start: new_start,
                stop: new_start + count * new_step,
                step: new_step,
            })))
        }
        _ => Err(native_err!("type is not sliceable: {}", left.type_name())),
    }
}

// ─── Iteration / destructuring ───────────────────────────────────────────

/// The sequence a for-in loop walks: list elements, range ints, string
/// runes, dict KEYS (insertion order).
pub fn iter_items(iterable: &Value) -> Result<Vec<Value>, NativeError> {
    match iterable {
        Value::List(l) => Ok(l.borrow().clone()),
        Value::Range(r) => Ok((0..r.len()).map(|i| Value::Int(r.at(i))).collect()),
        Value::Str(s) => Ok(s
            .chars()
            .map(|c| Value::Str(Rc::from(c.to_string())))
            .collect()),
        Value::Dict(d) => Ok(d.borrow().in_order().map(|(k, _)| k.clone()).collect()),
        _ => Err(native_err!(
            "type is not iterable: {}",
            iterable.type_name()
        )),
    }
}

/// A Range iterates lazily; every other iterable is materialized once.
pub fn new_iterator(iterable: &Value) -> Result<Iter, NativeError> {
    if let Value::Range(r) = iterable {
        return Ok(Iter {
            items: Vec::new(),
            pos: 0,
            rng: Some(Rc::clone(r)),
        });
    }
    Ok(Iter {
        items: iter_items(iterable)?,
        pos: 0,
        rng: None,
    })
}

/// Unpack val into exactly n elements for multi-value assignment.
pub fn destructure_values(val: &Value, n: usize) -> Result<Vec<Value>, NativeError> {
    let items = iter_items(val)?;
    if items.len() != n {
        return Err(native_err!(
            "cannot destructure {} values into {} names",
            items.len(),
            n
        ));
    }
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::inspect;

    fn list(vals: Vec<i64>) -> Value {
        Value::List(Rc::new(RefCell::new(
            vals.into_iter().map(Value::Int).collect(),
        )))
    }

    fn s(v: &str) -> Value {
        Value::Str(Rc::from(v))
    }

    #[test]
    fn slice_semantics() {
        let l = list(vec![0, 1, 2, 3, 4]);
        let sl = |lo: Value, hi: Value, st: Value| inspect(&slice_get(&l, &lo, &hi, &st).unwrap());
        assert_eq!(sl(Value::Int(1), Value::Int(3), Value::Null), "[1, 2]");
        assert_eq!(
            sl(Value::Null, Value::Null, Value::Int(-1)),
            "[4, 3, 2, 1, 0]"
        );
        assert_eq!(sl(Value::Int(-2), Value::Null, Value::Null), "[3, 4]");
        // Out-of-range bounds clamp, no error.
        assert_eq!(
            sl(Value::Int(-99), Value::Int(99), Value::Null),
            "[0, 1, 2, 3, 4]"
        );
        assert_eq!(sl(Value::Null, Value::Null, Value::Int(2)), "[0, 2, 4]");
        // String slice is rune-based.
        let st = s("çilek");
        assert_eq!(
            inspect(&slice_get(&st, &Value::Int(0), &Value::Int(2), &Value::Null).unwrap()),
            "çi"
        );
        // Range slice stays lazy but renders like a list.
        let r = Value::Range(Rc::new(Range {
            start: 0,
            stop: 10,
            step: 1,
        }));
        let out = slice_get(&r, &Value::Int(2), &Value::Int(8), &Value::Int(2)).unwrap();
        assert!(matches!(out, Value::Range(_)));
        assert_eq!(inspect(&out), "[2, 4, 6]");
        // step 0 errors.
        let err = slice_get(&l, &Value::Null, &Value::Null, &Value::Int(0)).unwrap_err();
        assert_eq!(err.msg, "slice step cannot be zero");
    }

    #[test]
    fn binary_dispatch() {
        assert_eq!(
            inspect(&binary_op("/", &Value::Int(4), &Value::Int(2)).unwrap()),
            "2"
        );
        assert_eq!(
            inspect(&binary_op("+", &Value::Int(1), &Value::Float(2.5)).unwrap()),
            "3.5"
        );
        let err = binary_op("+", &Value::Bool(true), &Value::Int(1)).unwrap_err();
        assert_eq!(err.msg, "type mismatch: BOOLEAN + INTEGER");
        let err = binary_op("-", &s("a"), &s("b")).unwrap_err();
        assert_eq!(err.msg, "unknown operator: STRING - STRING");
        // Go trunc division / sign-follows-dividend mod.
        assert_eq!(
            inspect(&binary_op("/", &Value::Int(-7), &Value::Int(2)).unwrap()),
            "-3"
        );
        assert_eq!(
            inspect(&binary_op("%", &Value::Int(-7), &Value::Int(2)).unwrap()),
            "-1"
        );
        // Cross-type equality.
        assert_eq!(
            inspect(&binary_op("==", &Value::Int(1), &s("1")).unwrap()),
            "false"
        );
        assert_eq!(
            inspect(&binary_op("!=", &Value::Null, &Value::Int(0)).unwrap()),
            "true"
        );
    }

    #[test]
    fn index_errors() {
        let l = list(vec![1, 2, 3]);
        assert_eq!(
            index_get(&l, &Value::Int(10)).unwrap_err().msg,
            "list index out of range: 10"
        );
        assert_eq!(inspect(&index_get(&l, &Value::Int(-1)).unwrap()), "3");
        assert_eq!(
            index_get(&l, &s("x")).unwrap_err().msg,
            "list index must be an integer, got STRING"
        );
        assert_eq!(
            index_set(&s("ab"), &Value::Int(0), Value::Int(1))
                .unwrap_err()
                .msg,
            "strings are immutable"
        );
        // Missing dict key errors with Repr.
        let d = Value::Dict(Rc::new(RefCell::new(crate::value::Dict::new())));
        assert_eq!(
            index_get(&d, &s("k")).unwrap_err().msg,
            "key not found: \"k\""
        );
    }
}
