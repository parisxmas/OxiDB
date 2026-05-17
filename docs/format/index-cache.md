# `.fidx` / `.cidx` — Index cache files

**1.0 status:** Stable (per [ADR-0003](../decisions/0003-1.0-stability-scope.md))
**Written by:** `src/index_persist.rs` — `save_field_indexes` (→ `.fidx`),
`save_composite_indexes` (→ `.cidx`)
**Read by:** `src/index_persist.rs` — `load_field_indexes` /
`load_composite_indexes`

## Purpose

`.fidx` and `.cidx` are **rebuild-skip caches** for a collection's
secondary indexes. They sit at:

- `<data_dir>/<collection_name>.fidx` — every single-field index on
  the collection, serialized as one file
- `<data_dir>/<collection_name>.cidx` — every composite (multi-field)
  index on the collection, in one file

Both are safe to delete — the engine will rebuild them on next open by
scanning the [`.btree`](btree.md) — but doing so is the slow path. The
cache makes startup `O(1)` instead of `O(collection size)` when the
cache is still valid for the current state.

## Shared file layout

Both `.fidx` and `.cidx` use the same `OXIX` cache-file framing
implemented in `src/index_persist.rs`. The only difference is what the
*body* contains.

### File header (36 bytes, all little-endian)

```
┌──────────┬────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
│ MAGIC 4B │ VERSION u32│ DOC_COUNT u64│ NEXT_ID  u64 │ BODY_CRC u32 │ BODY_LEN u64 │
│ "OXIX"   │ LE         │ LE           │ LE           │ LE           │ LE           │
└──────────┴────────────┴──────────────┴──────────────┴──────────────┴──────────────┘
```

| Field | Meaning |
|---|---|
| `MAGIC` | `b"OXIX"` — identifies the file as an OxiDB index cache |
| `VERSION` | Current = `1`. Engine refuses any other value (returns `None` from `validate_cache_file` ⇒ cache miss ⇒ rebuild from `.btree`) |
| `DOC_COUNT` | Document count at the moment the cache was written. **The validity check** — if it doesn't match the live `.btree`'s document count at open time, the cache is rejected as stale |
| `NEXT_ID` | Next `DocumentId` to allocate, at the moment the cache was written. Same validity-check role as `DOC_COUNT` (catches concurrent-write races more reliably than count alone) |
| `BODY_CRC` | `crc32fast` of the `final_body` bytes (after any encryption). Mismatch ⇒ cache miss |
| `BODY_LEN` | Length of `final_body`. If `data.len() < HEADER_SIZE + BODY_LEN`, cache miss |

### Body

#### `.fidx` body — field indexes

```
┌──────────────┬───────────────┬───────────────┬─── ... ───┐
│ COUNT u32 LE │ index #0      │ index #1      │           │
└──────────────┴───────────────┴───────────────┴───────────┘
```

Each `index #N` is the output of
`PagedFieldIndex::write_to(&mut writer)` — see
`src/paged_field_index.rs` for the per-index layout.

#### `.cidx` body — composite indexes

```
┌──────────────┬───────────────┬───────────────┬─── ... ───┐
│ COUNT u32 LE │ comp index #0 │ comp index #1 │           │
└──────────────┴───────────────┴───────────────┴───────────┘
```

Each `comp index #N` is the output of `CompositeIndex::write_to` —
see `src/index.rs`.

## Atomic persist

`write_cache_file` does the standard tmp + rename:

1. Write header + body to `<path>.tmp`.
2. `sync_data()` the tmp file.
3. `fs::rename` onto the target path.

A crash mid-persist leaves the previous cache intact. If no previous
cache exists, the engine just rebuilds from `.btree` on next open —
caches are advisory.

The persist also handles "no indexes" cleanly: if `indexes.is_empty()`,
the cache file is **removed** rather than written as an empty body.
Readers expecting a missing file ⇒ cache miss ⇒ rebuild.

## Encryption

When an `EncryptionKey` is configured, the *body* is encrypted before
the header is built; the CRC is computed over the encrypted bytes.
The header itself (magic, version, counts, CRC, length) is plaintext.
This matches the [`.btree`](btree.md#encryption-when-enabled)
"encrypt-as-blob" pattern but on the body only — the header has to
stay plaintext so the integrity check can run before decryption.

## Versioning

Already versioned: `OXIX` magic + `VERSION = 1` u32 LE header field.
This is the only file type in the 1.0-stable set that **does not need a
Phase 1b header retrofit** — it's already done. The current behavior
is exactly what the convention in
[`README.md`](README.md#versioning-convention-in-force-after-phase-1-closes)
prescribes:

- Engine reads version `1`.
- Engine treats any other version as a cache miss (silently rebuilds
  from `.btree`).

There is no "legacy / pre-header" form to support; the OXIX header
has been there from the file type's introduction.

(For consistency with the post-Phase-1 convention, future work may
widen the header to `magic + version u16 + flags u16` — but this
spec describes the current `version u32` shape that ships today.)

## Compatibility rules

Per [ADR-0004 §5](../decisions/0004-phase-0-answers.md):

**Additive (1.X minor allowed):**
- Adding a new index TYPE (e.g. a new "expression index" file) as its
  own file with its own magic — the existing `.fidx` / `.cidx`
  unaffected.
- Adding fields to the per-index serialization, *only if* the
  read side handles missing fields cleanly — in practice this means
  bumping `VERSION` to 2 and supporting both 1 and 2 read paths.

**Breaking (requires 2.0):**
- Changing the layout of `OXIX` header.
- Changing the meaning of `DOC_COUNT` / `NEXT_ID` (the validity-check
  semantics are part of the contract — they're the reason the cache is
  safe).
- Repurposing `MAGIC` or `VERSION` byte assignments already shipped.

## Validity model

The crucial bit about these files is they're **not authoritative**.
The [`.btree`](btree.md) is the truth; `.fidx` / `.cidx` are
write-through caches. Three rejection paths all degrade gracefully to
"rebuild from `.btree`":

1. File missing.
2. Magic / version / length / CRC mismatch.
3. `DOC_COUNT` or `NEXT_ID` don't match the live `.btree`'s state.

Any of these = cache miss = full rebuild = correct (just slower) boot.

## Code refs

- `src/index_persist.rs:15-19` — `MAGIC`, `VERSION`, `HEADER_SIZE`
- `src/index_persist.rs:26` — `pub fn save_field_indexes(...)`
- `src/index_persist.rs:49` — `pub fn load_field_indexes(...)`
- `src/index_persist.rs:75` — `pub fn save_composite_indexes(...)`
- `src/index_persist.rs:97` — `pub fn load_composite_indexes(...)`
- `src/index_persist.rs:173` — `fn write_cache_file(...)`
- `src/index_persist.rs:212` — `fn validate_cache_file(...)`
- `src/paged_field_index.rs` — `PagedFieldIndex::write_to` (body shape)
- `src/index.rs` — `CompositeIndex::write_to` (body shape)
