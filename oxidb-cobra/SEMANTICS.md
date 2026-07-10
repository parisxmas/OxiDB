# Cobra VM semantics reference (port contract)

Extracted from the Go implementation at `~/source/cobra` (file:line refs).
The Rust VM must match these byte-for-byte — conformance = diffing program
output against `cobra run`. See also `vm/vm.go` (dispatch), `code/code.go`
(opcodes), `compiler/portable.go` (COBRAP format).

## Builtins (ORDER IS ABI — eval/builtins.go:39 + functionalBuiltins())

0 print, 1 len, 2 push, 3 pop, 4 keys, 5 values, 6 has, 7 int, 8 float,
9 decimal, 10 hash, 11 range, 12 del, 13 type, 14 str, 15 implements,
16 chr, 17 ord, 18 read_line, 19 input, 20 Mutex, 21 RWMutex, 22 Channel,
23 select, 24 map, 25 pmap, 26 pfilter, 27 pforeach, 28 filter, 29 reduce,
30 preduce, 31 zip, 32 enumerate, 33 any, 34 all.

- **print**: `parts[i]=arg.Inspect()`, join with `" "`, print + `\n`. Returns NULL. 0 args → just `\n`.
- **len**: String→rune count; List; Range.Len(); Dict.Len(). Err: `wrong number of arguments to len: want=1, got=%d` / `argument to len not supported: %s`.
- **push(list,v)** → append, NULL. Err: `first argument to push must be LIST, got %s`.
- **pop(list)** → remove+return last. `pop from empty list`.
- **keys/values(dict)** → insertion order. `argument to keys must be DICT, got %s`.
- **has(dict,k)** → bool. `first argument to has must be DICT, got %s`, `not a hashable dict key: %s`.
- **int(x)**: Int; Float→trunc toward zero, NaN/Inf → `cannot convert %s to int` (Inspect); Decimal→Int64; Bool→1/0; String→ParseInt(trimmed) err `cannot convert %q to int`. Else `argument to int not supported: %s`.
- **float(x)**: sym.; String err `cannot convert %q to float`. `argument to float not supported: %s`.
- **decimal(x)**: Decimal|Int|String(parse, err `cannot convert %q to decimal`)|Float via FormatFloat(v,'f',-1,64) err `cannot convert float %v to decimal`. `argument to decimal not supported: %s`.
- **hash(s)**: FNV-1a 32 over string, lowercase hex (`FormatUint(...,16)`). `hash: argument must be STRING, got %s`.
- **range(1..3 int args)** → LAZY Range{start,stop,step}. `wrong number of arguments to range: want=1..3, got=%d`, `arguments to range must be INTEGER, got %s`, `range step must not be zero`.
- **del(dict,k)** → NULL; missing → `key not found: %s` (Repr). `first argument to del must be DICT, got %s`.
- **type(x)** → Type() string. Instance → struct name.
- **str(x)** → Inspect().
- **implements(structOrInst, contract)** → bool; every contract method resolvable. Errs: `implements: second argument must be a contract, got %s` / `implements: first argument must be a struct or instance, got %s`.
- **chr(i)** → 1-char str; `chr: code point out of range (0..0x10FFFF): %d`; `argument to chr must be INTEGER, got %s`.
- **ord(s)** → first rune; `ord: empty string`; `argument to ord must be STRING, got %s`.
- **map/filter(fn,listish)**, **reduce(fn,listish[,init])**, **zip(...)**, **enumerate(x[,start])**, **any/all(list)** — see eval/functional.go for exact arg order/errors. p-variants = same semantics, order-preserving (implement sequentially).
- Builtin `*Error` results are re-wrapped by the CALLER with `line N: ` prefix.

## Value Inspect (object/object.go)

- Int: base-10. Float: NaN/Inf → `%g`; if v!=0 && (|v|>=1e16 || |v|<1e-4) → `FormatFloat('e',-1)` (e.g. `1e+16`, `1e-05`); else `FormatFloat('f',-1)` and append `.0` if no dot. String: raw. Bool: true/false. Null: `null`.
- List: `[` + Repr(elem) join `, ` + `]` (strings QUOTED via Go strconv.Quote). Cycle → `[...]`.
- Dict: `{` + `Repr(k): Repr(v)` join `, ` + `}` insertion order. Empty `{}`. Cycle `{...}`.
- Decimal: fixed-point, preserves scale (19.90 stays `19.90`).
- CompiledFunction/Closure: `def NAME(...) ... end`. Struct: `struct NAME`. Contract: `contract NAME`. Module: `module NAME`. Range: materialized like list `[0, 1, 2]`. Instance: `Name{f1: Repr(v1), ...}` insertion order, cycle `Name{...}`.
- **Repr** = Inspect except top-level String → strconv.Quote. Used inside containers + error texts (`key not found`, `uncaught throw`).

## Truthy (eval.go:1645)

false: Null, Bool(false), Int 0, Float 0, "" , empty List, empty Dict. Everything else true (Range/Decimal/Instance always true).

## BinaryOp (eval.go:912) dispatch order

1. int⊕int → int ops (div/mod by zero → `division by zero`; Go trunc div, sign-follows-dividend mod).
2. either DECIMAL and both decimal-operands (Decimal|Int) → decimal ops (div: scale=max floored 6; `division by zero`).
3. both toFloat-able (Int|Float|Decimal) → float ops (`math.Mod` for %, div-by-0.0 → `division by zero`).
4. str⊕str: `+`, comparisons lexicographic; else `unknown operator: STRING %s STRING`.
5. list+list → concat.
6. `==`/`!=` any remaining → objectsEqual (different types just unequal).
7. type mismatch → `type mismatch: %s %s %s`; same types → `unknown operator: %s %s %s`.

objectsEqual: numeric cross-type (1==1.0 true; int↔decimal exact); String/Bool/Null value; List elementwise; Dict order-independent; record Instance same-struct+fields; else pointer identity.

## PrefixOp

`!`/`not` → !Truthy. `-` Int/Float/Decimal. Else `unknown operator: -%s` / `unknown operator: %s%s`.

## Index / Slice (eval.go:603+)

- List/Range: int index, negative += len, else `list index out of range: %s` (Inspect) or `%s index must be an integer, got %s` (kind list/string/range).
- String: RUNE indexed, 1-char string.
- Dict: missing key → ERROR `key not found: %s` (Repr). Non-hashable: `not a hashable dict key: %s`.
- Else `type is not indexable: %s`.
- IndexSet: List (same rules), Dict set, String → `strings are immutable`, else `type does not support index assignment: %s`.
- Slice: Python-like, bounds clamped (no error OOR); NULL = omitted; `slice %s must be INT, got %s` (low/high/step), `slice step cannot be zero`. String slice = runes. Range slice stays lazy. Else `type is not sliceable: %s`.

## HashKey / Dict

Hashable: Int('i'), Float('f', bits — so 1 vs 1.0 DIFFERENT dict keys), String('s'), Decimal('d', normalized — 1.0==1.00 same key), Bool, Null. Dict = insertion-ordered entries + tombstoned deletes; re-insert keeps original position; iteration/Inspect/keys/values in insertion order.

## Methods (eval/methods.go)

Unknown → `%s has no method '%s'` (Type()). Method errors re-wrapped w/ line at call site.
- String (rune-aware): upper, lower, strip, lstrip, rstrip, split(0..1 — 0=whitespace Fields; `empty separator`), join(list of strings; `join requires a list of strings, got %s`), replace(2..3), slice(start[,end]), substr(start[,len]), contains, startswith, endswith, find (rune offset or -1), count. Arity: `wrong number of arguments to NAME: want=..., got=%d`; `argument to NAME must be STRING, got %s`.
- List: push, pop, contains, find, count, reverse (in place → NULL), sort (in place, no comparator; mixed → `cannot sort list of mixed types`, other types → `cannot sort list containing %s`; numeric by float, strings lexicographic; stable).
- Dict: keys, values, items ([k,v] pairs), has, get(k[,default]) → NULL if missing (NO error), del (missing → `key not found: %s`).
- Decimal: round(places) half-up, to_float(), scale().
- Struct receiver: statics via FindStatic → `struct '%s' has no static method '%s'`.
- Int/Float/Bool/Null: no methods.

## Iteration

IterItems: List elements; Range lazy ints; String runes; Dict KEYS (insertion order); else `type is not iterable: %s`. Destructure: any iterable; count mismatch → `cannot destructure %d values into %d names`.

## Properties (eval.go:1299+)

- GetProperty: Module member (`module '%s' has no member '%s'`); Struct consts→statics (`struct '%s' has no static member '%s'`); Instance: field → getter → const → method-hint (`%s.%s is a method — call it with parentheses`) → `%s has no field '%s'%s` (+ ` (did you mean 'x'?)` Levenshtein ≤2). Non-instance: `%s has no property '%s'` — NO list.length etc.
- SetProperty: Module → `cannot assign to module '%s'`; non-inst `%s does not support property assignment`; frozen record `cannot modify immutable record %s`; setter dispatch; sealed+unknown: getter-only → `%s.%s is a read-only property (it has a getter but no setter)` else `%s has no field '%s' (fields are created in init)`; unsealed → SetField.
- Instance: ordered fields; Seal() after init returns; records Freeze(). FindMethod/Getter/Setter/Static/Const walk parent chain.
- Instantiate: no init → 0 args (`wrong number of arguments to %s: want=0, got=%d`); with init: argc+1 == NumParams (self at local 0). Records: `with(dict)` → RecordWith: clone, fields must exist (`%s has no field '%s'`), keys strings (`with keys must be field-name strings, got %s`), 1 dict arg (`with expects 1 argument (a dict), got %d` / `with expects a dict, got %s`), freeze clone.

## Errors / try-catch

Runtime errors: `line N: msg` when line>0 (line = current fn's line table at ip). Caught value = the message String (or thrown value for OpThrow). `uncaught throw: %s` (Repr) for OpThrow. Finally: rethrow-token mechanics per vm.go. `wrong number of arguments to %s: want=%d, got=%d` for closures. `not a function: %s`. `stack overflow`. `undefined variable: %s` (OpGetGlobal on nil slot; name from global_names else `<global>`).

## Misc

- String interp OpStr → Inspect.
- Boxed locals: fresh Cell per activation; boxed params wrap arg value; others start Cell(NULL). OpGetFree unwraps Cell; OpSetFree writes through Cell if cell else replaces.
- OpReturn/OpReturnValue: dropHandlers (handlers with framesIdx > current after pop).
- Integer fast path semantics = general (interning invisible).
- `4/2`→2 int; `1+2.5`→3.5 float; `true+1`→err.
