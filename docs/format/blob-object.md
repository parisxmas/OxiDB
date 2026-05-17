# `_blobs/<bucket>/<id>.{data,meta}` — Blob storage layout

**1.0 status:** Stable (per [ADR-0003](../decisions/0003-1.0-stability-scope.md))
**Written by:** `src/blob.rs` — `BlobStore::put_object`
**Read by:** `src/blob.rs` — `BlobStore::get_object`,
`BlobStore::scan_bucket`

## Purpose

OxiDB's S3-style blob storage. Each blob is two sibling files in
`<data_dir>/_blobs/<bucket>/`:

- `<id>.data` — the (optionally compressed and/or encrypted) payload
  bytes
- `<id>.meta` — a JSON document describing the object

`<id>` is an internal numeric id; the user-supplied object key is in
the `key` field of the meta. Lookup by key is via an in-memory index
built at scan time.

## `<id>.data`

A flat file containing the object payload, transformed by zero or
more of:

| Layer | When applied | Recorded as |
|---|---|---|
| zstd compression | when the heuristic in `try_compress_zstd` decides it's worthwhile (>= 5 % saving, ≥ 256 bytes, content type isn't already-compressed) | `storage_compression: "zstd"` on the meta |
| AES-GCM encryption | when an `EncryptionKey` is configured for the store | _not flagged on the meta — store-level setting_ |

The order is **compress, then encrypt**. The reader reverses it
(decrypt, then decompress) keyed off the meta's `storage_compression`
field.

There is no inline framing or header inside `<id>.data` — the entire
file IS the (transformed) payload. The original payload length is in
the meta's `size` field; the on-disk length post-transform is in
`stored_size`.

## `<id>.meta`

A UTF-8 JSON file. Serde-derived (`#[derive(Serialize, Deserialize)]`)
from `ObjectMeta` in `src/blob.rs`:

```json
{
  "key": "greeting.txt",
  "bucket": "files",
  "size": 6,
  "content_type": "text/plain",
  "etag": "12345abcde",
  "created_at": "2026-05-18T10:00:00Z",
  "metadata": { "author": "julia" },
  "storage_compression": "zstd",
  "stored_size": 11
}
```

| Field | Type | Notes |
|---|---|---|
| `key` | `string` | The user-supplied object key (path-like; arbitrary string). |
| `bucket` | `string` | Bucket name (also encoded in the parent directory; redundancy is intentional for crash forensics). |
| `size` | `u64` | Plaintext payload size in bytes. Authoritative for "how big is this object". |
| `content_type` | `string` | Caller-supplied MIME type. Drives the compression heuristic and is echoed back on GET. |
| `etag` | `string` | Currently a hash of the payload (see `src/blob.rs` for the precise scheme; CRC32-based — included in the meta so the on-disk file is self-describing for backup tools). |
| `created_at` | `string` | RFC 3339 timestamp. |
| `metadata` | `map<string,string>` | Arbitrary user key/value pairs (S3-style "user metadata"). |
| `storage_compression` | `string?` | `"zstd"` or absent. **Absent ⇒ uncompressed**. Drives decompression on read. |
| `stored_size` | `u64?` | Bytes the `.data` file actually consumes on disk (post compression/encryption). Absent on blobs written before this field existed; readers fall back to `size`. |

### Forward compatibility on `<id>.meta`

The meta JSON is read with default-on-missing semantics:

- New optional fields land with `#[serde(default, skip_serializing_if = "Option::is_none")]`,
  meaning a meta written by an older version (missing the field) parses
  cleanly under a newer reader (field deserialises to `None`).
- Existing fields are not renamed — the field-name → schema-meaning
  binding is part of the 1.0 contract.
- A new mandatory field would be a breaking change requiring 2.0; in
  practice we keep new fields optional with a documented
  absence-equals-prior-behavior default.

This is the project's de facto migration story for meta — already
demonstrated by `storage_compression` and `stored_size` being added
in 0.x without breaking older blobs.

## Write protocol (crash-safe ordering)

`BlobStore::put_object` writes to temp paths first and renames in a
specific order:

1. Write `<tmp_id>.data.tmp`, fsync the bytes.
2. Write `<tmp_id>.meta.tmp`, fsync the bytes.
3. Atomically rename `<tmp_id>.data.tmp` → `<id>.data`, **fsync the
   bucket directory**.
4. Atomically rename `<tmp_id>.meta.tmp` → `<id>.meta`, fsync the
   bucket directory (when `OXIDB_BLOB_SYNC=true`).

The `.data` rename must be durable *before* the `.meta` rename. This
is asymmetric on purpose: on recovery, an orphan `.data` (no matching
`.meta`) is swept on next open as garbage, but a `.meta` pointing at
a missing `.data` would be a "ghost" read returning corrupt data. So
the meta is the source of truth, and the data must always be there
before the meta is.

Crash recovery scans the bucket directory at open time
(`scan_bucket`):

1. Index every `<id>.meta` (= the authoritative set of objects).
2. Stat each matching `<id>.data` to confirm presence + recover
   `stored_size` if missing from older metas.
3. Delete orphan `<id>.data` files (no matching `.meta`) and any
   leftover `.tmp` files (crash mid-rename garbage).

## Versioning

`<id>.data` has **no header today** — it's the raw transformed
payload bytes. The meta's `storage_compression` flag is the only
versioning marker.

`<id>.meta` is **versioned by JSON schema additivity** (described
above) rather than a discrete version byte. This works in practice
because the meta is small and structured.

**Phase 1b — landed:**

- `<id>.data` gets no version header; the meta's
  `storage_compression` field is already the read-path discriminator.
  Adding new transforms (new compression codecs, new encryption
  modes) requires extending that field's vocabulary, which IS a
  versioning event but is already wired into how readers behave (an
  unknown codec value surfaces as an error, never silent corruption).
- `<id>.meta` carries an explicit `"format_version": 1` field
  (`src/blob.rs::CURRENT_BLOB_META_VERSION`). Absence ⇒ legacy ⇒
  treated as version 1 (serde `default = "default_blob_meta_version"`).
  Engine refuses to open metas declaring a version newer than it
  knows about (`Error::IncompatibleFormat`) — better to fail loudly
  than to best-effort read fields whose semantics may have changed.

## Compatibility rules

**Additive (1.X minor allowed):**
- New optional meta fields with `default` + skip-if-none.
- New `storage_compression` codec values, **provided** older readers
  reject them with a clean error (as they do today — see `decompress`
  in `src/blob.rs`). This is acceptable because compression is an
  intentional, per-blob, opt-in transformation.
- Tightening crash-recovery (sweeping more garbage shapes).

**Breaking (requires 2.0):**
- Renaming or removing any field from the meta JSON shape.
- Changing the meaning of `size` (it must always be "plaintext payload
  size in bytes") or `stored_size` (post-transform on-disk size).
- Changing the write protocol's ordering guarantees (the
  `.data`-before-`.meta` rule is part of the on-disk contract).
- Replacing the JSON encoding with another serialisation format.

## Code refs

- `src/blob.rs:11-38` — `ObjectMeta` struct (the meta JSON shape)
- `src/blob.rs:222` — `impl BlobStore`
- `src/blob.rs:457` — `pub fn put_object(...)`
- `src/blob.rs:586` — `pub fn get_object(...)`
- `src/blob.rs:333` — `fn scan_bucket(...)` (crash recovery)
- `src/blob.rs:77` — `fn try_compress_zstd(...)` (the compression
  heuristic)
- `src/blob.rs:97` — `fn decompress(...)` (read-path codec dispatch)
