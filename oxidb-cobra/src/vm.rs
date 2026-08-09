//! The bytecode VM — the port of `vm/vm.go`'s dispatch loop.
//!
//! Frames, basePointer, the handlers stack with recoverTo and the finally
//! rethrow token, boxed-local Cells, and the OpStruct / instantiate /
//! callInstanceMethod / callSuperMethod stack choreography all mirror the Go
//! original. `print` output is captured in a buffer owned by the VM.
//!
//! Closure re-entry from native code (functional builtins, property
//! getters/setters, struct statics) mirrors Go's callClosureFromNative: a
//! fresh micro-VM sharing this VM's constants/globals/output runs a
//! two-instruction driver (`OpCall argc; OpHalt`).

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use crate::bytecode::CompiledFunction;
use crate::bytecode::{BINARY_OPS, Bytecode, Constant, Op, PREFIX_OPS};
use crate::methods;
use crate::ops;
use crate::value::{
    Closure, Instance, NativeError, Struct, Thrown, Value, hash_key, inspect, repr, truthy,
};

pub const STACK_SIZE: usize = 2048;
pub const GLOBALS_SIZE: usize = 65536;
pub const MAX_FRAMES: usize = 1024;

/// The error `run` reports: an uncaught throw / runtime error (message
/// already carries its `line N: ` prefix) or a VM-fatal condition.
#[derive(Debug)]
pub struct RuntimeError {
    pub message: String,
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

pub(crate) enum VmError {
    /// Catchable: unwinds to the innermost registered try handler.
    Thrown(Thrown),
    /// Not catchable (Go's plain `error` path, e.g. push overflow).
    Fatal(String),
}

/// One function activation.
struct Frame {
    closure: Rc<Closure>,
    ip: i64,
    base_pointer: usize,
    /// Overrides the frame's return value — struct instantiation, where
    /// init runs but the call yields the new instance regardless.
    replace_result: Option<Value>,
}

/// A registered try (or finally) block.
struct Handler {
    catch_pos: usize,
    frames_idx: usize,
    sp: usize,
    is_finally: bool,
}

pub struct Vm {
    constants: Rc<Vec<Value>>,
    globals: Rc<RefCell<Vec<Option<Value>>>>,
    global_names: Rc<Vec<String>>,
    stack: Vec<Value>,
    sp: usize,
    frames: Vec<Frame>,
    handlers: Vec<Handler>,
    out: Rc<RefCell<String>>,
    /// Remaining instruction budget (`None` = unlimited). Shared with every
    /// micro-VM spawned for native re-entry, so nested closure calls cannot
    /// escape the limit.
    fuel: Option<Rc<Cell<u64>>>,
}

impl Vm {
    pub fn new(bytecode: &Bytecode) -> Vm {
        let constants: Vec<Value> = bytecode
            .constants
            .iter()
            .map(|c| match c {
                Constant::Int(i) => Value::Int(*i),
                Constant::Float(f) => Value::Float(*f),
                Constant::Str(s) => Value::Str(Rc::from(s.as_str())),
                Constant::Func(f) => Value::CompiledFn(Rc::new(f.clone())),
            })
            .collect();
        let main_fn = Rc::new(CompiledFunction {
            instructions: bytecode.instructions.clone(),
            lines: bytecode.lines.clone(),
            ..Default::default()
        });
        let main_closure = Rc::new(Closure {
            func: main_fn,
            free: RefCell::new(Vec::new()),
        });
        Vm {
            constants: Rc::new(constants),
            globals: Rc::new(RefCell::new(vec![None; GLOBALS_SIZE])),
            global_names: Rc::new(bytecode.global_names.clone()),
            stack: vec![Value::Null; STACK_SIZE],
            sp: 0,
            frames: vec![Frame {
                closure: main_closure,
                ip: -1,
                base_pointer: 0,
                replace_result: None,
            }],
            handlers: Vec::new(),
            out: Rc::new(RefCell::new(String::new())),
            fuel: None,
        }
    }

    /// Cap execution at `limit` instructions (`None` = unlimited, the
    /// default). Exceeding the budget raises the **non-catchable** runtime
    /// error `instruction limit exceeded` — it bypasses try/catch handlers
    /// so a runaway loop cannot swallow its own kill. The budget is shared
    /// with nested native re-entry (functional builtins, `run_procedure`'s
    /// closure call), and once exhausted it stays exhausted.
    pub fn set_fuel(&mut self, limit: Option<u64>) {
        self.fuel = limit.map(|n| Rc::new(Cell::new(n)));
    }

    /// Everything `print` wrote during the run.
    pub fn output(&self) -> String {
        self.out.borrow().clone()
    }

    pub(crate) fn write_line(&self, line: &str) {
        let mut out = self.out.borrow_mut();
        out.push_str(line);
        out.push('\n');
    }

    /// Execute to completion or the first uncaught runtime error.
    pub fn run(&mut self) -> Result<(), RuntimeError> {
        match self.run_internal() {
            Ok(()) => Ok(()),
            Err(VmError::Thrown(t)) => Err(RuntimeError { message: t.msg }),
            Err(VmError::Fatal(m)) => Err(RuntimeError { message: m }),
        }
    }

    fn run_internal(&mut self) -> Result<(), VmError> {
        loop {
            match self.run_loop() {
                Ok(()) => return Ok(()),
                Err(VmError::Thrown(t)) if !self.handlers.is_empty() => self.recover_to(t),
                Err(e) => return Err(e),
            }
        }
    }

    /// Unwind to the innermost handler: drop frames, restore the stack,
    /// push the caught value (or a rethrow token for a finally handler),
    /// resume at the handler's position.
    fn recover_to(&mut self, t: Thrown) {
        let h = self.handlers.pop().expect("recover_to requires a handler");
        self.frames.truncate(h.frames_idx);
        self.sp = h.sp;
        self.stack[self.sp] = if h.is_finally {
            Value::Rethrow(Rc::new(t))
        } else {
            t.value
        };
        self.sp += 1;
        self.frames.last_mut().expect("at least main frame").ip = h.catch_pos as i64 - 1;
    }

    /// Discard handlers registered by frames that no longer exist — a
    /// return inside a try block leaves its handler behind.
    fn drop_handlers(&mut self) {
        while let Some(h) = self.handlers.last() {
            if h.frames_idx > self.frames.len() {
                self.handlers.pop();
            } else {
                break;
            }
        }
    }

    fn push(&mut self, v: Value) -> Result<(), VmError> {
        if self.sp >= STACK_SIZE {
            return Err(VmError::Fatal("stack overflow".into()));
        }
        self.stack[self.sp] = v;
        self.sp += 1;
        Ok(())
    }

    fn pop(&mut self) -> Value {
        self.sp -= 1;
        std::mem::take(&mut self.stack[self.sp])
    }

    fn push_checked(&mut self, r: Result<Value, NativeError>) -> Result<(), VmError> {
        match r {
            Ok(v) => self.push(v),
            Err(e) => Err(self.wrap_native(e)),
        }
    }

    /// Like [`push_checked`](Self::push_checked) but hands the value back —
    /// for fused ops that consume it (a jump condition, a store).
    fn checked(&mut self, r: Result<Value, NativeError>) -> Result<Value, VmError> {
        r.map_err(|e| self.wrap_native(e))
    }

    /// Current source line via the executing function's line table.
    fn cur_line(&self) -> u32 {
        let f = self.frames.last().expect("at least main frame");
        if f.ip < 0 {
            return 0;
        }
        f.closure.func.line_at(f.ip as usize)
    }

    /// Catchable runtime error with the current line prefixed; the caught
    /// value is the full message string.
    fn rt_err(&self, msg: String) -> VmError {
        let msg = match self.cur_line() {
            0 => msg,
            n => format!("line {n}: {msg}"),
        };
        VmError::Thrown(Thrown {
            value: Value::Str(Rc::from(msg.as_str())),
            msg,
        })
    }

    /// A shared-operation error (line 0) becomes catchable with the current
    /// line prefixed; the thrown value is preserved when present.
    fn wrap_native(&self, e: NativeError) -> VmError {
        let msg = match self.cur_line() {
            0 => e.msg,
            n => format!("line {n}: {}", e.msg),
        };
        let value = e
            .value
            .unwrap_or_else(|| Value::Str(Rc::from(msg.as_str())));
        VmError::Thrown(Thrown { value, msg })
    }

    fn global_name(&self, idx: usize) -> &str {
        match self.global_names.get(idx) {
            Some(n) if !n.is_empty() => n,
            _ => "<global>",
        }
    }

    /// Activate a call frame; every captured local gets a fresh Cell for
    /// this activation (boxed params wrap the argument value).
    fn push_frame(&mut self, cl: Rc<Closure>, base_pointer: usize, replace_result: Option<Value>) {
        for &idx in &cl.func.boxed_locals {
            let slot = base_pointer + idx;
            let init = if idx < cl.func.num_params {
                std::mem::take(&mut self.stack[slot])
            } else {
                Value::Null
            };
            self.stack[slot] = Value::Cell(Rc::new(RefCell::new(init)));
        }
        self.frames.push(Frame {
            closure: cl,
            ip: -1,
            base_pointer,
            replace_result,
        });
    }

    fn sync_ip(&mut self, ip: i64) {
        self.frames.last_mut().expect("at least main frame").ip = ip;
    }

    // ─── The dispatch loop ───────────────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    fn run_loop(&mut self) -> Result<(), VmError> {
        let mut cl = Rc::clone(&self.frames.last().expect("frame").closure);
        let mut func = Rc::clone(&cl.func);
        let mut ip = self.frames.last().expect("frame").ip;
        let mut bp = self.frames.last().expect("frame").base_pointer;

        macro_rules! refresh_frame {
            () => {{
                let f = self.frames.last().expect("frame");
                cl = Rc::clone(&f.closure);
                func = Rc::clone(&cl.func);
                ip = f.ip;
                bp = f.base_pointer;
            }};
        }
        macro_rules! u8_operand {
            () => {{
                ip += 1;
                func.instructions[ip as usize] as usize
            }};
        }
        macro_rules! u16_operand {
            () => {{
                ip += 2;
                ((func.instructions[(ip - 1) as usize] as usize) << 8)
                    | func.instructions[ip as usize] as usize
            }};
        }

        loop {
            // Fuel: one unit per executed instruction. Exhaustion is Fatal
            // (not Thrown), so no handler in this VM can catch it, and the
            // shared zeroed counter re-kills any outer frame that tries.
            if let Some(fuel) = &self.fuel {
                let left = fuel.get();
                if left == 0 {
                    return Err(VmError::Fatal("instruction limit exceeded".into()));
                }
                fuel.set(left - 1);
            }
            ip += 1;
            let op = Op::from_byte(func.instructions[ip as usize]).ok_or_else(|| {
                VmError::Fatal(format!(
                    "unhandled opcode {}",
                    func.instructions[ip as usize]
                ))
            })?;

            match op {
                Op::Constant => {
                    let idx = u16_operand!();
                    self.push(self.constants[idx].clone())?;
                }
                Op::Pop => {
                    self.sp -= 1;
                    self.stack[self.sp] = Value::Null;
                }
                Op::True => self.push(Value::Bool(true))?,
                Op::False => self.push(Value::Bool(false))?,
                Op::Null => self.push(Value::Null)?,
                Op::Dup => {
                    let v = self.stack[self.sp - 1].clone();
                    self.push(v)?;
                }

                Op::Binary => {
                    let op_idx = u8_operand!();
                    let right = self.pop();
                    let left = self.pop();
                    // Integer fast path; div/mod by zero falls to the slow path.
                    if let (Value::Int(l), Value::Int(r)) = (&left, &right) {
                        if let Some(res) = fast_int_op(op_idx, *l, *r) {
                            self.push(res)?;
                            continue;
                        }
                    } else if let (Value::Str(l), Value::Str(r), 0) = (&left, &right, op_idx) {
                        // BinAdd string fast path.
                        self.push(Value::Str(Rc::from(format!("{l}{r}"))))?;
                        continue;
                    }
                    self.sync_ip(ip);
                    let result = ops::binary_op(BINARY_OPS[op_idx], &left, &right);
                    self.push_checked(result)?;
                }

                Op::Prefix => {
                    let op_idx = u8_operand!();
                    self.sync_ip(ip);
                    let right = self.pop();
                    let result = ops::prefix_op(PREFIX_OPS[op_idx], &right);
                    self.push_checked(result)?;
                }

                Op::Str => {
                    let v = self.pop();
                    self.push(Value::Str(Rc::from(inspect(&v))))?;
                }

                Op::Jump => {
                    let pos = u16_operand!();
                    ip = pos as i64 - 1;
                }
                Op::JumpNotTruthy => {
                    let pos = u16_operand!();
                    let v = self.pop();
                    if !truthy(&v) {
                        ip = pos as i64 - 1;
                    }
                }
                Op::JumpTruthy => {
                    let pos = u16_operand!();
                    let v = self.pop();
                    if truthy(&v) {
                        ip = pos as i64 - 1;
                    }
                }

                Op::SetGlobal => {
                    let idx = u16_operand!();
                    let v = self.pop();
                    self.globals.borrow_mut()[idx] = Some(v);
                }
                Op::GetGlobal => {
                    let idx = u16_operand!();
                    let val = self.globals.borrow()[idx].clone();
                    match val {
                        Some(v) => self.push(v)?,
                        None => {
                            // Hoisted slot read before its defining statement ran.
                            self.sync_ip(ip);
                            return Err(self
                                .rt_err(format!("undefined variable: {}", self.global_name(idx))));
                        }
                    }
                }

                Op::SetLocal => {
                    let idx = u8_operand!();
                    let v = self.pop();
                    self.stack[bp + idx] = v;
                }
                Op::GetLocal => {
                    let idx = u8_operand!();
                    let v = self.stack[bp + idx].clone();
                    self.push(v)?;
                }
                Op::SetLocalBox => {
                    let idx = u8_operand!();
                    let v = self.pop();
                    let Value::Cell(cell) = &self.stack[bp + idx] else {
                        return Err(VmError::Fatal("SetLocalBox on a non-cell slot".into()));
                    };
                    *cell.borrow_mut() = v;
                }
                Op::GetLocalBox => {
                    let idx = u8_operand!();
                    let Value::Cell(cell) = &self.stack[bp + idx] else {
                        return Err(VmError::Fatal("GetLocalBox on a non-cell slot".into()));
                    };
                    let v = cell.borrow().clone();
                    self.push(v)?;
                }
                Op::GetLocalCell => {
                    let idx = u8_operand!();
                    let v = self.stack[bp + idx].clone();
                    self.push(v)?;
                }
                Op::GetFreeCell => {
                    let idx = u8_operand!();
                    let v = cl.free.borrow()[idx].clone();
                    self.push(v)?;
                }

                Op::GetBuiltin => {
                    let idx = u8_operand!();
                    self.push(Value::Builtin(idx))?;
                }

                Op::GetFree => {
                    let idx = u8_operand!();
                    // A captured local is boxed in a Cell; a captured function
                    // name is stored directly. Handle both.
                    let val = cl.free.borrow()[idx].clone();
                    let val = match val {
                        Value::Cell(cell) => cell.borrow().clone(),
                        v => v,
                    };
                    self.push(val)?;
                }
                Op::SetFree => {
                    let idx = u8_operand!();
                    let v = self.pop();
                    let is_cell = matches!(&cl.free.borrow()[idx], Value::Cell(_));
                    if is_cell {
                        let free = cl.free.borrow();
                        let Value::Cell(cell) = &free[idx] else {
                            unreachable!()
                        };
                        *cell.borrow_mut() = v;
                    } else {
                        cl.free.borrow_mut()[idx] = v;
                    }
                }

                Op::CurrentClosure => {
                    self.push(Value::Closure(Rc::clone(&cl)))?;
                }

                Op::Closure => {
                    let const_idx = u16_operand!();
                    let num_free = u8_operand!();
                    let Value::CompiledFn(f) = &self.constants[const_idx] else {
                        return Err(VmError::Fatal(format!(
                            "not a function constant at {const_idx}"
                        )));
                    };
                    let f = Rc::clone(f);
                    let mut free = Vec::with_capacity(num_free);
                    for i in self.sp - num_free..self.sp {
                        free.push(std::mem::take(&mut self.stack[i]));
                    }
                    self.sp -= num_free;
                    self.push(Value::Closure(Rc::new(Closure {
                        func: f,
                        free: RefCell::new(free),
                    })))?;
                }

                Op::List => {
                    let n = u16_operand!();
                    let mut elements = Vec::with_capacity(n);
                    for i in self.sp - n..self.sp {
                        elements.push(std::mem::take(&mut self.stack[i]));
                    }
                    self.sp -= n;
                    self.push(Value::List(Rc::new(RefCell::new(elements))))?;
                }

                Op::Dict => {
                    let n = u16_operand!();
                    self.sync_ip(ip);
                    let mut dict = crate::value::Dict::new();
                    let base = self.sp - n;
                    let mut i = base;
                    while i < self.sp {
                        let key = std::mem::take(&mut self.stack[i]);
                        let value = std::mem::take(&mut self.stack[i + 1]);
                        let Some(hk) = hash_key(&key) else {
                            return Err(self
                                .rt_err(format!("not a hashable dict key: {}", key.type_name())));
                        };
                        dict.set(hk, key, value);
                        i += 2;
                    }
                    self.sp = base;
                    self.push(Value::Dict(Rc::new(RefCell::new(dict))))?;
                }

                Op::Index => {
                    let index = self.pop();
                    let left = self.pop();
                    // Fast paths for dict and list hits.
                    match &left {
                        Value::Dict(d) => {
                            if let Some(hk) = hash_key(&index) {
                                let hit = d.borrow().get(&hk).map(|(_, v)| v.clone());
                                if let Some(v) = hit {
                                    self.push(v)?;
                                    continue;
                                }
                            }
                        }
                        Value::List(l) => {
                            if let Value::Int(i) = index {
                                let l = l.borrow();
                                let mut idx = i;
                                if idx < 0 {
                                    idx += l.len() as i64;
                                }
                                if idx >= 0 && (idx as usize) < l.len() {
                                    let v = l[idx as usize].clone();
                                    drop(l);
                                    self.push(v)?;
                                    continue;
                                }
                            }
                        }
                        _ => {}
                    }
                    // Slow path covers strings and all error messages.
                    self.sync_ip(ip);
                    let result = ops::index_get(&left, &index);
                    self.push_checked(result)?;
                }

                Op::Slice => {
                    let step = self.pop();
                    let high = self.pop();
                    let low = self.pop();
                    let left = self.pop();
                    self.sync_ip(ip);
                    let result = ops::slice_get(&left, &low, &high, &step);
                    self.push_checked(result)?;
                }

                Op::SetIndex => {
                    let value = self.pop();
                    let index = self.pop();
                    let left = self.pop();
                    match &left {
                        Value::Dict(d) => {
                            if let Some(hk) = hash_key(&index) {
                                d.borrow_mut().set(hk, index, value.clone());
                                self.push(value)?;
                                continue;
                            }
                        }
                        Value::List(l) => {
                            if let Value::Int(i) = index {
                                let mut idx = i;
                                let len = l.borrow().len() as i64;
                                if idx < 0 {
                                    idx += len;
                                }
                                if idx >= 0 && idx < len {
                                    l.borrow_mut()[idx as usize] = value.clone();
                                    self.push(value)?;
                                    continue;
                                }
                            }
                        }
                        _ => {}
                    }
                    self.sync_ip(ip);
                    let result = ops::index_set(&left, &index, value);
                    self.push_checked(result)?;
                }

                Op::Call => {
                    let argc = u8_operand!();
                    self.sync_ip(ip);
                    let callee_is_fast = match &self.stack[self.sp - 1 - argc] {
                        Value::Closure(c) => {
                            !c.func.is_async
                                && argc == c.func.num_params
                                && self.frames.len() < MAX_FRAMES
                        }
                        _ => false,
                    };
                    if callee_is_fast {
                        let Value::Closure(c) = self.stack[self.sp - 1 - argc].clone() else {
                            unreachable!()
                        };
                        let base = self.sp - argc;
                        let num_locals = c.func.num_locals;
                        self.push_frame(c, base, None);
                        self.sp = base + num_locals;
                    } else {
                        self.call_value(argc)?;
                    }
                    refresh_frame!();
                }

                Op::ReturnValue | Op::Return => {
                    let rv = if op == Op::ReturnValue {
                        self.pop()
                    } else {
                        Value::Null
                    };
                    let f = self.frames.pop().expect("frame to return from");
                    if !self.handlers.is_empty() {
                        self.drop_handlers();
                    }
                    self.sp = f.base_pointer - 1;
                    let rv = match f.replace_result {
                        Some(inst) => {
                            finish_construction(&inst);
                            inst
                        }
                        None => rv,
                    };
                    self.push(rv)?;
                    refresh_frame!();
                }

                Op::MethodCall => {
                    let name_idx = u16_operand!();
                    let argc = u8_operand!();
                    self.sync_ip(ip);

                    let Value::Str(name) = &self.constants[name_idx] else {
                        return Err(VmError::Fatal(
                            "method name constant is not a string".into(),
                        ));
                    };
                    let name = Rc::clone(name);
                    let receiver = self.stack[self.sp - argc - 1].clone();

                    // list.push(v) fast path.
                    if let (Value::List(l), 1, "push") = (&receiver, argc, &*name) {
                        let v = self.pop();
                        l.borrow_mut().push(v);
                        self.sp -= 1;
                        self.push(Value::Null)?;
                        continue;
                    }
                    if let Value::Instance(inst) = &receiver {
                        let (is_record, has_with) = {
                            let b = inst.borrow();
                            (b.struct_.is_record, b.struct_.find_method("with").is_some())
                        };
                        if is_record && &*name == "with" && !has_with {
                            let args: Vec<Value> = self.stack[self.sp - argc..self.sp].to_vec();
                            let result = methods::record_with(inst, &args);
                            self.sp -= argc + 1;
                            self.push_checked(result)?;
                            continue;
                        }
                        let inst = Rc::clone(inst);
                        self.call_instance_method(&inst, &name, argc)?;
                        refresh_frame!();
                        continue;
                    }
                    // Everything else through the shared method tables.
                    let args: Vec<Value> = self.stack[self.sp - argc..self.sp].to_vec();
                    let result = methods::call_method(self, &receiver, &name, &args);
                    self.sp -= argc + 1;
                    self.push_checked(result)?;
                }

                Op::SuperCall => {
                    let name_idx = u16_operand!();
                    let argc = u8_operand!();
                    self.sync_ip(ip);
                    let Value::Str(name) = &self.constants[name_idx] else {
                        return Err(VmError::Fatal(
                            "method name constant is not a string".into(),
                        ));
                    };
                    let name = Rc::clone(name);
                    let Value::Struct(parent) = self.stack[self.sp - argc - 1].clone() else {
                        return Err(self.rt_err("super target is not a struct".into()));
                    };
                    let self_slot = match self.stack[bp].clone() {
                        // self was captured by a nested closure
                        Value::Cell(cell) => cell.borrow().clone(),
                        v => v,
                    };
                    let Value::Instance(selfi) = self_slot else {
                        return Err(
                            self.rt_err("super is only valid inside a struct method".into())
                        );
                    };
                    self.call_super_method(&parent, &selfi, &name, argc)?;
                    refresh_frame!();
                }

                Op::IterNew => {
                    self.sync_ip(ip);
                    let v = self.pop();
                    match ops::new_iterator(&v) {
                        Ok(it) => self.push(Value::Iterator(Rc::new(RefCell::new(it))))?,
                        Err(e) => return Err(self.wrap_native(e)),
                    }
                }
                Op::IterNext => {
                    let pos = u16_operand!();
                    let Value::Iterator(it) = &self.stack[self.sp - 1] else {
                        return Err(VmError::Fatal("IterNext without an iterator on top".into()));
                    };
                    let next = it.borrow_mut().next();
                    match next {
                        Some(v) => self.push(v)?,
                        None => ip = pos as i64 - 1,
                    }
                }

                Op::TryBegin => {
                    let pos = u16_operand!();
                    self.sync_ip(ip);
                    self.handlers.push(Handler {
                        catch_pos: pos,
                        frames_idx: self.frames.len(),
                        sp: self.sp,
                        is_finally: false,
                    });
                }
                Op::TryEnd => {
                    self.handlers.pop();
                }
                Op::FinallyBegin => {
                    let pos = u16_operand!();
                    self.sync_ip(ip);
                    self.handlers.push(Handler {
                        catch_pos: pos,
                        frames_idx: self.frames.len(),
                        sp: self.sp,
                        is_finally: true,
                    });
                }
                Op::EndFinally => {
                    let v = self.pop();
                    if let Value::Rethrow(t) = v {
                        // The finally ran; resume the original error.
                        self.sync_ip(ip);
                        return Err(VmError::Thrown((*t).clone()));
                    }
                    // normal-completion marker: nothing to do
                }

                Op::Import | Op::FromImport | Op::Await => {
                    self.sync_ip(ip);
                    return Err(self.rt_err(format!("{} is not allowed", op.name())));
                }

                Op::Destructure => {
                    let n = u8_operand!();
                    self.sync_ip(ip);
                    let v = self.pop();
                    match ops::destructure_values(&v, n) {
                        Ok(items) => {
                            for it in items {
                                self.push(it)?;
                            }
                        }
                        Err(e) => return Err(self.wrap_native(e)),
                    }
                }

                Op::Throw => {
                    self.sync_ip(ip);
                    let value = self.pop();
                    let mut msg = format!("uncaught throw: {}", repr(&value));
                    let line = self.cur_line();
                    if line > 0 {
                        msg = format!("line {line}: {msg}");
                    }
                    return Err(VmError::Thrown(Thrown { value, msg }));
                }

                Op::Struct => {
                    let name_idx = u16_operand!();
                    let closure_count = u8_operand!();
                    let const_count = u8_operand!();
                    let has_parent = u8_operand!();
                    let is_record = u8_operand!();

                    let Value::Str(name) = &self.constants[name_idx] else {
                        return Err(VmError::Fatal(
                            "struct name constant is not a string".into(),
                        ));
                    };
                    let name = name.to_string();

                    let mut methods_map = std::collections::HashMap::new();
                    let mut statics = std::collections::HashMap::new();
                    let mut getters = std::collections::HashMap::new();
                    let mut setters = std::collections::HashMap::new();
                    let cl_base = self.sp - closure_count;
                    for i in cl_base..self.sp {
                        let Value::Closure(mcl) = &self.stack[i] else {
                            return Err(VmError::Fatal("struct member is not a closure".into()));
                        };
                        let entry = Value::Closure(Rc::clone(mcl));
                        match mcl.func.kind.as_str() {
                            "static" => statics.insert(mcl.func.name.clone(), entry),
                            "property" => getters.insert(mcl.func.name.clone(), entry),
                            "setter" => setters.insert(mcl.func.name.clone(), entry),
                            _ => methods_map.insert(mcl.func.name.clone(), entry),
                        };
                    }
                    let mut consts = std::collections::HashMap::new();
                    let const_base = cl_base - 2 * const_count;
                    let mut i = const_base;
                    while i < cl_base {
                        let Value::Str(cname) = &self.stack[i] else {
                            return Err(VmError::Fatal("struct const name is not a string".into()));
                        };
                        consts.insert(cname.to_string(), self.stack[i + 1].clone());
                        i += 2;
                    }
                    let mut base = const_base;
                    let mut parent = None;
                    if has_parent == 1 {
                        let Value::Struct(p) = &self.stack[const_base - 1] else {
                            return Err(VmError::Fatal("struct parent is not a struct".into()));
                        };
                        parent = Some(Rc::clone(p));
                        base = const_base - 1;
                    }
                    self.sp = base;
                    self.push(Value::Struct(Rc::new(Struct {
                        name,
                        is_record: is_record == 1,
                        parent,
                        methods: methods_map,
                        statics,
                        getters,
                        setters,
                        consts,
                    })))?;
                }

                Op::GetProperty => {
                    let name_idx = u16_operand!();
                    let Value::Str(name) = &self.constants[name_idx] else {
                        return Err(VmError::Fatal(
                            "property name constant is not a string".into(),
                        ));
                    };
                    let name = Rc::clone(name);
                    let receiver = self.pop();
                    if let Value::Instance(inst) = &receiver {
                        let field = inst.borrow().get_field(&name);
                        if let Some(v) = field {
                            self.push(v)?;
                            continue;
                        }
                    }
                    self.sync_ip(ip);
                    let result = methods::get_property(self, &receiver, &name);
                    self.push_checked(result)?;
                }

                Op::SetProperty => {
                    let name_idx = u16_operand!();
                    let Value::Str(name) = &self.constants[name_idx] else {
                        return Err(VmError::Fatal(
                            "property name constant is not a string".into(),
                        ));
                    };
                    let name = Rc::clone(name);
                    let value = self.pop();
                    let receiver = self.pop();
                    if let Value::Instance(inst) = &receiver {
                        let (frozen, sealed, has_setter) = {
                            let b = inst.borrow();
                            (b.frozen, b.sealed, b.struct_.find_setter(&name).is_some())
                        };
                        if !frozen && !has_setter {
                            // Fast path for plain field writes. A sealed
                            // instance only updates existing fields here.
                            if !sealed {
                                inst.borrow_mut().set_field(&name, value.clone());
                                self.push(value)?;
                                continue;
                            }
                            if inst.borrow_mut().set_field_if_exists(&name, value.clone()) {
                                self.push(value)?;
                                continue;
                            }
                        }
                    }
                    self.sync_ip(ip);
                    let result = methods::set_property(self, &receiver, &name, value);
                    self.push_checked(result)?;
                }

                Op::LocalConstBin | Op::LocalBoxConstBin => {
                    let local_idx = u8_operand!();
                    let const_idx = u16_operand!();
                    let op_idx = u8_operand!();
                    let left = if op == Op::LocalConstBin {
                        self.stack[bp + local_idx].clone()
                    } else {
                        let Value::Cell(cell) = &self.stack[bp + local_idx] else {
                            return Err(VmError::Fatal(
                                "LocalBoxConstBin on a non-cell slot".into(),
                            ));
                        };
                        cell.borrow().clone()
                    };
                    let right = self.constants[const_idx].clone();
                    if let (Value::Int(l), Value::Int(r)) = (&left, &right)
                        && let Some(res) = fast_int_op(op_idx, *l, *r)
                    {
                        self.push(res)?;
                        continue;
                    }
                    self.sync_ip(ip);
                    let result = ops::binary_op(BINARY_OPS[op_idx], &left, &right);
                    self.push_checked(result)?;
                }

                // ─── 0.13 fusions — each mirrors its Go vm.go arm exactly ──
                Op::GetLocalProp => {
                    let local_idx = u8_operand!();
                    let name_idx = u16_operand!();
                    let Value::Str(name) = &self.constants[name_idx] else {
                        return Err(VmError::Fatal(
                            "property name constant is not a string".into(),
                        ));
                    };
                    let name = Rc::clone(name);
                    let receiver = local_value(&self.stack[bp + local_idx]);
                    if let Value::Instance(inst) = &receiver {
                        let field = inst.borrow().get_field(&name);
                        if let Some(v) = field {
                            self.push(v)?;
                            continue;
                        }
                    }
                    self.sync_ip(ip);
                    let result = methods::get_property(self, &receiver, &name);
                    self.push_checked(result)?;
                }

                Op::SetLocalProp => {
                    let local_idx = u8_operand!();
                    let name_idx = u16_operand!();
                    let Value::Str(name) = &self.constants[name_idx] else {
                        return Err(VmError::Fatal(
                            "property name constant is not a string".into(),
                        ));
                    };
                    let name = Rc::clone(name);
                    let receiver = local_value(&self.stack[bp + local_idx]);
                    let value = self.pop();
                    if let Value::Instance(inst) = &receiver
                        && inst.borrow_mut().set_field_if_exists(&name, value.clone())
                    {
                        self.push(value)?;
                        continue;
                    }
                    self.sync_ip(ip);
                    let result = methods::set_property(self, &receiver, &name, value);
                    self.push_checked(result)?;
                }

                Op::LocalLocalBin => {
                    let l_idx = u8_operand!();
                    let r_idx = u8_operand!();
                    let op_idx = u8_operand!();
                    let left = local_value(&self.stack[bp + l_idx]);
                    let right = local_value(&self.stack[bp + r_idx]);
                    if let (Value::Int(l), Value::Int(r)) = (&left, &right)
                        && let Some(res) = fast_int_op(op_idx, *l, *r)
                    {
                        self.push(res)?;
                        continue;
                    }
                    self.sync_ip(ip);
                    let result = ops::binary_op(BINARY_OPS[op_idx], &left, &right);
                    self.push_checked(result)?;
                }

                Op::IndexLL => {
                    let l_idx = u8_operand!();
                    let i_idx = u8_operand!();
                    let left = local_value(&self.stack[bp + l_idx]);
                    let index = local_value(&self.stack[bp + i_idx]);
                    match &left {
                        Value::List(l) => {
                            if let Value::Int(i) = index {
                                let l = l.borrow();
                                let mut idx = i;
                                if idx < 0 {
                                    idx += l.len() as i64;
                                }
                                if idx >= 0 && (idx as usize) < l.len() {
                                    let v = l[idx as usize].clone();
                                    drop(l);
                                    self.push(v)?;
                                    continue;
                                }
                            }
                        }
                        Value::Dict(d) => {
                            if let Some(hk) = hash_key(&index) {
                                let hit = d.borrow().get(&hk).map(|(_, v)| v.clone());
                                if let Some(v) = hit {
                                    self.push(v)?;
                                    continue;
                                }
                            }
                        }
                        _ => {}
                    }
                    self.sync_ip(ip);
                    let result = ops::index_get(&left, &index);
                    self.push_checked(result)?;
                }

                Op::JumpNotLL | Op::JumpNotLC => {
                    let a = u8_operand!();
                    let (left, right, op_idx, pos);
                    if op == Op::JumpNotLL {
                        let b = u8_operand!();
                        op_idx = u8_operand!();
                        pos = u16_operand!();
                        left = local_value(&self.stack[bp + a]);
                        right = local_value(&self.stack[bp + b]);
                    } else {
                        let const_idx = u16_operand!();
                        op_idx = u8_operand!();
                        pos = u16_operand!();
                        left = local_value(&self.stack[bp + a]);
                        right = self.constants[const_idx].clone();
                    }
                    if let (Value::Int(l), Value::Int(r)) = (&left, &right) {
                        if !int_cmp(op_idx, *l, *r) {
                            ip = pos as i64 - 1;
                        }
                        continue;
                    }
                    self.sync_ip(ip);
                    let result = ops::binary_op(BINARY_OPS[op_idx], &left, &right);
                    let v = self.checked(result)?;
                    if !truthy(&v) {
                        ip = pos as i64 - 1;
                    }
                }

                Op::LLBinStore | Op::LCBinStore => {
                    let dst = u8_operand!();
                    let a = u8_operand!();
                    let (left, right, op_idx);
                    if op == Op::LLBinStore {
                        let b = u8_operand!();
                        op_idx = u8_operand!();
                        left = local_value(&self.stack[bp + a]);
                        right = local_value(&self.stack[bp + b]);
                    } else {
                        let const_idx = u16_operand!();
                        op_idx = u8_operand!();
                        left = local_value(&self.stack[bp + a]);
                        right = self.constants[const_idx].clone();
                    }
                    let res = if let (Value::Int(l), Value::Int(r)) = (&left, &right)
                        && let Some(res) = fast_int_op(op_idx, *l, *r)
                    {
                        res
                    } else {
                        self.sync_ip(ip);
                        let result = ops::binary_op(BINARY_OPS[op_idx], &left, &right);
                        self.checked(result)?
                    };
                    store_local(&mut self.stack[bp + dst], res);
                }

                Op::ConstBin | Op::ConstBinRev => {
                    let const_idx = u16_operand!();
                    let op_idx = u8_operand!();
                    let konst = self.constants[const_idx].clone();
                    let tos = self.pop();
                    let (left, right) = if op == Op::ConstBin {
                        (tos, konst)
                    } else {
                        (konst, tos)
                    };
                    if let (Value::Int(l), Value::Int(r)) = (&left, &right)
                        && let Some(res) = fast_int_op(op_idx, *l, *r)
                    {
                        self.push(res)?;
                        continue;
                    }
                    self.sync_ip(ip);
                    let result = ops::binary_op(BINARY_OPS[op_idx], &left, &right);
                    self.push_checked(result)?;
                }

                Op::BinStoreL => {
                    let dst = u8_operand!();
                    let op_idx = u8_operand!();
                    let right = self.pop();
                    let left = self.pop();
                    let res = if let (Value::Int(l), Value::Int(r)) = (&left, &right)
                        && let Some(res) = fast_int_op(op_idx, *l, *r)
                    {
                        res
                    } else {
                        self.sync_ip(ip);
                        let result = ops::binary_op(BINARY_OPS[op_idx], &left, &right);
                        self.checked(result)?
                    };
                    store_local(&mut self.stack[bp + dst], res);
                }

                Op::Halt => {
                    self.sync_ip(ip);
                    return Ok(());
                }
            }
        }
    }

    // ─── Calls ───────────────────────────────────────────────────────────

    fn call_value(&mut self, argc: usize) -> Result<(), VmError> {
        let callee = self.stack[self.sp - 1 - argc].clone();
        match callee {
            Value::Closure(c) => {
                if c.func.is_async {
                    return Err(self.rt_err(format!(
                        "async function {} is not supported in a stored procedure",
                        c.func.name
                    )));
                }
                if argc != c.func.num_params {
                    return Err(self.rt_err(format!(
                        "wrong number of arguments to {}: want={}, got={}",
                        c.func.name, c.func.num_params, argc
                    )));
                }
                if self.frames.len() >= MAX_FRAMES {
                    return Err(self.rt_err("stack overflow".into()));
                }
                let base = self.sp - argc;
                let num_locals = c.func.num_locals;
                self.push_frame(c, base, None);
                self.sp = base + num_locals;
                Ok(())
            }
            Value::Builtin(idx) => {
                let args: Vec<Value> = self.stack[self.sp - argc..self.sp].to_vec();
                let result = crate::builtins::call(self, idx, &args);
                self.sp -= argc + 1;
                match result {
                    // Builtin messages carry no line; add the call site's.
                    // The thrown value is dropped (Go's runtimeErrorf path).
                    Err(e) => Err(self.rt_err(e.msg)),
                    Ok(v) => self.push(v),
                }
            }
            Value::Struct(def) => self.instantiate(&def, argc),
            other => Err(self.rt_err(format!("not a function: {}", other.type_name()))),
        }
    }

    /// Point(args): create the instance, then run init as a frame call
    /// whose result is replaced by the instance.
    fn instantiate(&mut self, def: &Rc<Struct>, argc: usize) -> Result<(), VmError> {
        let base = self.sp - argc - 1; // the struct's slot; becomes the result slot
        let init = def.find_method("init");
        let Some((init_obj, _)) = init else {
            if argc != 0 {
                return Err(self.rt_err(format!(
                    "wrong number of arguments to {}: want=0, got={argc}",
                    def.name
                )));
            }
            self.sp = base;
            let mut inst = Instance::new(Rc::clone(def));
            if def.is_record {
                inst.frozen = true;
            }
            return self.push(Value::Instance(Rc::new(RefCell::new(inst))));
        };

        let Value::Closure(c) = init_obj else {
            return Err(VmError::Fatal("init is not a closure".into()));
        };
        if argc + 1 != c.func.num_params {
            return Err(self.rt_err(format!(
                "wrong number of arguments to {}: want={}, got={argc}",
                def.name,
                c.func.num_params - 1
            )));
        }
        if self.frames.len() >= MAX_FRAMES || self.sp + 1 >= STACK_SIZE {
            return Err(self.rt_err("stack overflow".into()));
        }

        let inst = Value::Instance(Rc::new(RefCell::new(Instance::new(Rc::clone(def)))));
        // [Struct][a1..aN] → [Struct][self][a1..aN]; locals begin at self.
        let mut i = self.sp;
        while i > base + 1 {
            self.stack[i] = std::mem::take(&mut self.stack[i - 1]);
            i -= 1;
        }
        self.stack[base + 1] = inst.clone();
        self.sp += 1;

        let num_locals = c.func.num_locals;
        self.push_frame(c, base + 1, Some(inst));
        self.sp = base + 1 + num_locals;
        Ok(())
    }

    /// p.method(args): the receiver becomes the hidden self parameter.
    fn call_instance_method(
        &mut self,
        inst: &Rc<RefCell<Instance>>,
        name: &str,
        argc: usize,
    ) -> Result<(), VmError> {
        let found = inst.borrow().struct_.find_method(name);
        let struct_name = inst.borrow().struct_.name.clone();
        let Some((m, _)) = found else {
            return Err(self.rt_err(format!("{struct_name} has no method '{name}'")));
        };
        let Value::Closure(c) = m else {
            return Err(VmError::Fatal("method is not a closure".into()));
        };
        if argc + 1 != c.func.num_params {
            return Err(self.rt_err(format!(
                "wrong number of arguments to {struct_name}.{name}: want={}, got={argc}",
                c.func.num_params - 1
            )));
        }
        if self.frames.len() >= MAX_FRAMES || self.sp + 1 >= STACK_SIZE {
            return Err(self.rt_err("stack overflow".into()));
        }

        // [recv][a1..aN] → [recv][self=recv][a1..aN]; the original receiver
        // slot becomes the result slot.
        let base = self.sp - argc - 1;
        let mut i = self.sp;
        while i > base + 1 {
            self.stack[i] = std::mem::take(&mut self.stack[i - 1]);
            i -= 1;
        }
        self.stack[base + 1] = Value::Instance(Rc::clone(inst));
        self.sp += 1;

        let num_locals = c.func.num_locals;
        self.push_frame(c, base + 1, None);
        self.sp = base + 1 + num_locals;
        Ok(())
    }

    /// super.method(args): looked up starting at parent, self stays the
    /// current instance. Stack on entry: [parentStruct, a1..aN].
    fn call_super_method(
        &mut self,
        parent: &Rc<Struct>,
        selfi: &Rc<RefCell<Instance>>,
        name: &str,
        argc: usize,
    ) -> Result<(), VmError> {
        let Some((m, _)) = parent.find_method(name) else {
            return Err(self.rt_err(format!(
                "{} has no method '{name}' to call via super",
                parent.name
            )));
        };
        let Value::Closure(c) = m else {
            return Err(VmError::Fatal("method is not a closure".into()));
        };
        if argc + 1 != c.func.num_params {
            return Err(self.rt_err(format!(
                "wrong number of arguments to {}.{name}: want={}, got={argc}",
                parent.name,
                c.func.num_params - 1
            )));
        }
        if self.frames.len() >= MAX_FRAMES || self.sp + 1 >= STACK_SIZE {
            return Err(self.rt_err("stack overflow".into()));
        }

        // [parentStruct][a1..aN] → [parentStruct][self][a1..aN]; a super call
        // returns the method's real value (no replaceResult).
        let base = self.sp - argc - 1;
        let mut i = self.sp;
        while i > base + 1 {
            self.stack[i] = std::mem::take(&mut self.stack[i - 1]);
            i -= 1;
        }
        self.stack[base + 1] = Value::Instance(Rc::clone(selfi));
        self.sp += 1;

        let num_locals = c.func.num_locals;
        self.push_frame(c, base + 1, None);
        self.sp = base + 1 + num_locals;
        Ok(())
    }

    // ─── Native re-entry (Go callClosureFromNative) ──────────────────────

    /// Run a compiled closure from native code on a fresh micro-VM sharing
    /// this VM's constants, globals and output buffer.
    pub(crate) fn call_closure_native(
        &self,
        cl: &Rc<Closure>,
        args: &[Value],
    ) -> Result<Value, NativeError> {
        if args.len() != cl.func.num_params {
            return Err(NativeError::new(format!(
                "wrong number of arguments to {}: want={}, got={}",
                cl.func.name,
                cl.func.num_params,
                args.len()
            )));
        }
        // A two-instruction driver: call the closure, then halt with its
        // result on top.
        let driver_fn = Rc::new(CompiledFunction {
            instructions: vec![Op::Call as u8, args.len() as u8, Op::Halt as u8],
            ..Default::default()
        });
        let driver = Rc::new(Closure {
            func: driver_fn,
            free: RefCell::new(Vec::new()),
        });
        let mut micro = Vm {
            constants: Rc::clone(&self.constants),
            globals: Rc::clone(&self.globals),
            global_names: Rc::clone(&self.global_names),
            stack: vec![Value::Null; STACK_SIZE],
            sp: 1 + args.len(),
            frames: vec![Frame {
                closure: driver,
                ip: -1,
                base_pointer: 0,
                replace_result: None,
            }],
            handlers: Vec::new(),
            out: Rc::clone(&self.out),
            fuel: self.fuel.clone(),
        };
        micro.stack[0] = Value::Closure(Rc::clone(cl));
        micro.stack[1..1 + args.len()].clone_from_slice(args);

        match micro.run_internal() {
            Ok(()) => Ok(micro.stack[micro.sp - 1].clone()),
            Err(VmError::Thrown(t)) => Err(NativeError {
                msg: t.msg,
                value: Some(t.value),
            }),
            Err(VmError::Fatal(m)) => Err(NativeError::new(m)),
        }
    }

    /// Invoke any Cobra callable from native code (the port of
    /// eval.CallCallable + applyFunction for the VM's value kinds).
    pub(crate) fn call_callable(&self, f: &Value, args: &[Value]) -> Result<Value, NativeError> {
        match f {
            Value::Closure(cl) => self.call_closure_native(cl, args),
            Value::Builtin(idx) => match crate::builtins::call(self, *idx, args) {
                Ok(v) => Ok(v),
                // applyFunction re-wraps builtin errors, dropping any value.
                Err(e) => Err(NativeError::new(e.msg)),
            },
            Value::Struct(def) => {
                let inst = Rc::new(RefCell::new(Instance::new(Rc::clone(def))));
                if let Some((m, _)) = def.find_method("init") {
                    let Value::Closure(cl) = m else {
                        return Err(NativeError::new("init is not a closure"));
                    };
                    let mut with_self = Vec::with_capacity(args.len() + 1);
                    with_self.push(Value::Instance(Rc::clone(&inst)));
                    with_self.extend_from_slice(args);
                    self.call_closure_native(&cl, &with_self)?;
                    inst.borrow_mut().sealed = true;
                } else if !args.is_empty() {
                    return Err(NativeError::new(format!(
                        "wrong number of arguments to {}: want=0, got={}",
                        def.name,
                        args.len()
                    )));
                }
                if def.is_record {
                    inst.borrow_mut().frozen = true;
                }
                Ok(Value::Instance(inst))
            }
            other => Err(NativeError::new(format!(
                "not a function: {}",
                other.type_name()
            ))),
        }
    }
}

/// What [`run_procedure`] hands back: the called function's return value and
/// everything `print` wrote (the host surfaces it as notices).
#[derive(Debug)]
pub struct ProcOutcome {
    pub result: Value,
    pub notices: String,
}

/// Execute a stored procedure (ADR-0014 Phase 2): run the main program
/// (which defines globals, including the procedure's functions), then call
/// the global function `fn_name` with `args` via the VM's native closure
/// re-entry. `fuel` caps the *total* instruction count (main + the call);
/// exceeding it is the non-catchable error `instruction limit exceeded`.
///
/// Errors (as plain strings, ready for the host's error type):
/// - `procedure has no function '<fn_name>'` — no such global, or it never
///   got a value;
/// - `'<fn_name>' is not a function` — the global is not a closure;
/// - the usual `wrong number of arguments to <fn_name>: want=N, got=K` on
///   an arity mismatch;
/// - any runtime error the program raised and did not catch.
pub fn run_procedure(
    bc: &Bytecode,
    fn_name: &str,
    args: Vec<Value>,
    fuel: Option<u64>,
) -> Result<ProcOutcome, String> {
    let mut vm = Vm::new(bc);
    vm.set_fuel(fuel);
    vm.run().map_err(|e| e.message)?;

    let slot = bc.global_names.iter().position(|n| n == fn_name);
    let func = slot.and_then(|idx| vm.globals.borrow().get(idx).cloned().flatten());
    let Some(func) = func else {
        return Err(format!("procedure has no function '{fn_name}'"));
    };
    let Value::Closure(cl) = &func else {
        return Err(format!("'{fn_name}' is not a function"));
    };
    let result = vm.call_closure_native(cl, &args).map_err(|e| e.msg)?;
    Ok(ProcOutcome {
        result,
        notices: vm.output(),
    })
}

/// A constructor frame returned: init defined every field, so the instance
/// seals; a record also freezes.
fn finish_construction(v: &Value) {
    if let Value::Instance(inst) = v {
        let mut b = inst.borrow_mut();
        b.sealed = true;
        if b.struct_.is_record {
            b.frozen = true;
        }
    }
}

/// Integer-integer binary op without generic dispatch; div/mod by zero
/// report None so the slow path produces the proper error.
/// Unwrap the Cell of a boxed (captured) local; plain locals pass through —
/// the fused opcodes read locals this way, exactly as Go's `localValue`.
fn local_value(v: &Value) -> Value {
    if let Value::Cell(cell) = v {
        cell.borrow().clone()
    } else {
        v.clone()
    }
}

/// Write a local slot, writing THROUGH the Cell of a boxed local — the
/// store-fused opcodes' contract (Go `storeLocal`): a store fusion only runs
/// after the slot was initialized, so a Cell there is never stack garbage.
fn store_local(slot: &mut Value, v: Value) {
    if let Value::Cell(cell) = slot {
        *cell.borrow_mut() = v;
    } else {
        *slot = v;
    }
}

/// Integer comparison by BinaryOps index (5..=10) — the jump fusions' fast
/// path. Non-comparison indices never reach it (the compiler only fuses
/// comparisons into jumps).
fn int_cmp(op_idx: usize, l: i64, r: i64) -> bool {
    match op_idx {
        5 => l == r,
        6 => l != r,
        7 => l < r,
        8 => l > r,
        9 => l <= r,
        10 => l >= r,
        _ => false,
    }
}

fn fast_int_op(op_idx: usize, l: i64, r: i64) -> Option<Value> {
    Some(match op_idx {
        0 => Value::Int(l.wrapping_add(r)),
        1 => Value::Int(l.wrapping_sub(r)),
        2 => Value::Int(l.wrapping_mul(r)),
        3 => {
            if r == 0 {
                return None;
            }
            Value::Int(l.wrapping_div(r))
        }
        4 => {
            if r == 0 {
                return None;
            }
            Value::Int(l.wrapping_rem(r))
        }
        5 => Value::Bool(l == r),
        6 => Value::Bool(l != r),
        7 => Value::Bool(l < r),
        8 => Value::Bool(l > r),
        9 => Value::Bool(l <= r),
        10 => Value::Bool(l >= r),
        _ => return None,
    })
}
