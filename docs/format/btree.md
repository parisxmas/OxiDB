# `.btree` — B-tree storage file

**1.0 status:** Stable (per [ADR-0003](../decisions/0003-1.0-stability-scope.md))
**Written by:** `src/btree_storage.rs` — `BTreeStorage::persist`
**Read by:** `src/btree_storage.rs` — `BTreeStorage::open` →
`BTreeStorage::load_from_bytes`

## Purpose

One `.btree` file per collection, sitting at `<data_dir>/<collection_name>.btree`.
Holds the **canonical document image** for the collection: every
document the collection has, encoded by `DocumentId`. The
[`.wal`](wal.md) carries changes accumulated *since* the last persist;
recovery on startup replays the WAL on top of the `.btree` image.

## File layout

The file is a concatenated stream of fixed-shape records:

```
┌────────────────────┬────────────────────────────────┐
│ record 0           │ record 1            │  ...     │
└────────────────────┴────────────────────────────────┘

each record:
┌─────────────┬─────────────┬───────────────────────────┐
│  key u64 LE │  len u32 LE │  value bytes (`len` long) │
└─────────────┴─────────────┴───────────────────────────┘
   8 bytes      4 bytes      `len` bytes
```

- `key` — `DocumentId`, monotonic across the collection's lifetime.
- `len` — length in bytes of the `value` field.
- `value` — the JSON document encoded by `codec::encode_doc` (a binary
  JSON variant; see `src/codec.rs`).

There is **no file-level header** in the current format (pre-Phase 1).
Phase 1 will introduce one — see "Versioning" below.

## Encryption (when enabled)

When the engine is opened with an `EncryptionKey`, the entire `.btree`
file is encrypted as a single ciphertext blob (`crate::EncryptionKey::encrypt`
on write, `::decrypt` on read). The internal record structure above is
the *plaintext*. A decryption failure on read is logged and treated as
"start from empty; WAL replay will rebuild" — no partial-decrypt
attempts.

## Atomic persist

`BTreeStorage::persist` writes to `<name>.btree.tmp`, fsyncs the tmp
file, atomically renames onto `<name>.btree`, then fsyncs the parent
directory so the rename itself is durable across power loss. A crash
mid-persist leaves the previous `.btree` untouched; the stale
`.btree.tmp` is dropped on next open. See the `persist()` source for
the durability contract.

## Truncation tolerance

`load_from_bytes` is intentionally **tolerant of a truncated tail**:
if `len` would read past the end of the file, the load aborts but
keeps every record decoded so far (each upserted into the in-memory
tree as it parses). The engine logs `"[btree] {name}.btree partially
loaded …"` and proceeds — WAL replay reconciles. The reasoning is
"never refuse to boot for one bad shutdown"; the previous behaviour
was a bricked DB.

## Versioning

The current format has **no magic and no version byte**. This is the
state Phase 1 of [ADR-0003](../decisions/0003-1.0-stability-scope.md)
needs to fix.

**Planned for the Phase 1 version-header PR:**

```
┌──────────────┬──────────────┬──────────────┐
│  magic (4B)  │  version u16 │  flags u16   │   <- new 8-byte file header
│  "OXBT"      │  LE = 1      │  LE = 0      │
└──────────────┴──────────────┴──────────────┘
... then the existing record stream as above ...
```

- Engine reads version `1` (the post-Phase-1 header) AND legacy
  (no-header) files. On read, absence of `"OXBT"` magic = legacy ⇒
  load as today. On next `persist()`, the file is rewritten with a
  v1 header.
- Engine refuses to open versions newer than it knows.
- `flags` is reserved at 0 for now. Bit-0 will be `encrypted` (so the
  reader can fail fast on missing key without trying to decrypt
  garbage).

## Compatibility rules

Per [ADR-0004 §5](../decisions/0004-phase-0-answers.md) applied to
this file:

**Additive (1.X minor allowed):**
- Setting a new bit in the reserved `flags` (default 0; older readers
  ignore unknown bits *or* refuse if the bit is in a future-reserved
  range; the policy is recorded with the bit).
- Adding new record types BEFORE the existing `[key|len|value]` stream
  by introducing a wrapping section, *only if* the wrapping section is
  added under a new format-version (v2+) AND v1 readers see equivalent
  semantics.

**Breaking (requires 2.0):**
- Changing the layout of the `[key u64|len u32|value]` record.
- Changing the encoding of `value` from `codec::encode_doc`'s current
  shape.
- Repurposing the `OXBT` magic or any version byte.

## Code refs

- `src/btree_storage.rs:176` — `pub fn open(...)`
- `src/btree_storage.rs:268` — `pub fn persist(...)`
- `src/btree_storage.rs:236` — `fn load_from_bytes(...)` (the parser)
- `src/codec.rs` — `encode_doc` / `decode_doc` (the `value` encoding)
