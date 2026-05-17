# `.wal` — Write-Ahead Log file

**1.0 status:** Stable (per [ADR-0003](../decisions/0003-1.0-stability-scope.md))
**Written by:** `src/wal.rs` — `Wal::append_entries_locked`,
`encode_insert_record` / `encode_update_record` / `encode_delete_record`
**Read by:** `src/wal.rs` — `Wal::read_records` →
`Wal::read_records_prefix`

## Purpose

One `.wal` file per collection, sitting at `<data_dir>/<collection_name>.wal`,
holds **every mutation appended since the last `.btree` persist**.
Recovery on startup replays this on top of the
[`.btree`](btree.md) image to restore any writes that hadn't been
snapshotted yet. See the [Write-Ahead Log section in the README][readme-wal]
for the durability protocol; this doc covers the on-disk byte layout.

[readme-wal]: ../../README.md#write-ahead-log

## File layout

The WAL is an **append-only stream of length-prefixed records**. There
is no file-level header in the current format (pre-Phase 1).

```
┌──────────┬──────────┬──────────┬─── ... ────┐
│ record 0 │ record 1 │ record 2 │            │
└──────────┴──────────┴──────────┴────────────┘
```

### Record framing

Each record:

```
┌──────────────┬───────────────────┬─────────────────────────┐
│ crc32 u32 LE │ payload_len u32 LE│ payload (`payload_len`) │
└──────────────┴───────────────────┴─────────────────────────┘
   4 bytes        4 bytes              `payload_len` bytes
```

- `crc32` — `crc32fast::Hasher::write(payload).finalize()` over the
  payload bytes.
- `payload_len` — length of the payload field that follows.
- `payload` — see "Payload format" below.

If `payload_len` would extend past EOF the record is treated as a
truncated tail (incomplete write at crash time) and the WAL is
considered fully read at that offset. If the CRC doesn't match, the
same — replay stops there.

### Payload format

The first byte of the payload is an **op-type**. Two coexisting
generations:

| op-type byte | meaning | introduced |
|---|---|---|
| `0x01` | INSERT v1 | always |
| `0x02` | UPDATE v1 | always |
| `0x03` | DELETE v1 | always |
| `0x81` | INSERT v2 (GSN-stamped, for PITR) | with PITR |
| `0x82` | UPDATE v2 (GSN-stamped, for PITR) | with PITR |
| `0x83` | DELETE v2 (GSN-stamped, for PITR) | with PITR |

v2 op-types are distinguished by the high bit (`0x80`). Readers
recognise both side by side; a file written across an upgrade can mix
them with no separate format flag. v1 is still emitted by default —
v2 only when an `ArchiveSequencer` is attached (PITR mode).

**v1 INSERT / UPDATE payload (after the op-type byte):**

```
┌─────────────┬─────────────┬──────────────────────────┐
│ tx_id u64 LE│ doc_id u64 LE│ doc_bytes (rest of pld) │
└─────────────┴─────────────┴──────────────────────────┘
   8 bytes      8 bytes        payload_len - 17 bytes
```

**v1 DELETE payload:**

```
┌─────────────┬──────────────┐
│ tx_id u64 LE│ doc_id u64 LE│
└─────────────┴──────────────┘
   8 bytes      8 bytes          (payload_len == 17)
```

**v2 (INSERT / UPDATE / DELETE) — adds 16 bytes of PITR metadata
between `doc_id` and the body:**

```
┌─────────┬─────────┬─────────┬───────────────┬──────────┐
│ tx_id u64│ doc_id u64│ gsn u64 │ wall_clock_us │ doc_bytes│
│  LE      │  LE      │  LE      │  u64 LE      │ (if any) │
└─────────┴─────────┴─────────┴───────────────┴──────────┘
   8         8         8           8              variable
```

- `tx_id == 0` — non-transactional (auto-commit). Replay applies
  unconditionally.
- `tx_id != 0` — replay applies the record only if `tx_id` is in the
  committed-transactions set (`src/tx_log.rs`); otherwise the record
  is from a rolled-back / never-committed transaction and is
  discarded.
- `doc_bytes` — for INSERT and UPDATE, the document encoded by
  `codec::encode_doc`. Same encoding as the `value` field in
  [`.btree`](btree.md).
- `gsn` (v2 only) — global sequence number assigned by the
  `ArchiveSequencer`; monotonic across all collections in a PITR-enabled
  database.
- `wall_clock_us` (v2 only) — `SystemTime::now()` at write time in
  microseconds since Unix epoch.

## Encryption

The WAL is **not** wrapped in a single encryption blob the way
[`.btree`](btree.md#encryption-when-enabled) is — each *record's
payload* is encrypted individually when an `EncryptionKey` is
configured. The CRC + length-prefix header is plaintext (necessary
for framing); the payload bytes that follow are ciphertext. This keeps
recovery cheap (don't have to decrypt the whole WAL to find a record
boundary) without exposing document contents.

## Sealed segments (PITR)

When PITR is on, the live WAL is rotated periodically by
`Wal::seal()`: the live file is atomically renamed to
`<collection>.wal.<seq>` (a "sealed segment"), and a fresh empty
`.wal` starts. Sealed segments have **the same record format above**.
Their contents are eventually copied into `_archive/segments/*.seg`
by the archiver — the archive segment layout is a separate, currently
experimental format and is not specced here.

## Versioning

Like [`.btree`](btree.md), the current format has **no file-level
magic and no version byte** — only the per-record v1/v2 op-types.
Phase 1 will introduce a header:

```
┌──────────────┬──────────────┬──────────────┐
│  magic (4B)  │  version u16 │  flags u16   │   <- new 8-byte file header
│  "OXWA"      │  LE = 1      │  LE = 0      │
└──────────────┴──────────────┴──────────────┘
... then the existing record stream (mixed v1/v2) as above ...
```

- File version `1` = "the format that exists today, plus this header
  in front." Header-less files = legacy ⇒ readers treat them as
  version 1 by inference and the next append rewrites with a header
  prefix (which is safe because the WAL is append-only and the
  prefix-bytes are at offset 0, which is rewritten only by the seal /
  rotate path).
- Engine refuses file versions it doesn't know.
- `flags` reserved at 0; bit-0 will be `encrypted_records`.

## Compatibility rules

**Additive (1.X minor allowed):**
- New op-type bytes (e.g. v3 records) that older readers can
  recognise + skip cleanly. Adding op-types in the unused high-bit
  range is safe; the v1/v2 distinction is already by high bit.
- New optional fields appended to v2 record payloads, *only if*
  `payload_len` makes them self-describing (i.e., a v2 reader that
  doesn't know the new field still finds the doc_bytes correctly —
  which today it can't, because doc_bytes consumes "rest of payload").
  In practice this means a new format-version increment is required
  for any change to the v2 payload layout.

**Breaking (requires 2.0):**
- Changing the v1 or v2 payload layout above.
- Repurposing the `OXWA` magic, any version byte, or any committed
  op-type number.
- Changing the CRC algorithm or the framing header shape.

## Code refs

- `src/wal.rs:136` — `pub fn open(...)`
- `src/wal.rs:227` — `fn append_entries_locked(...)` (the write path)
- `src/wal.rs:559` — `fn encode_insert_record(...)` (v1 + v2 payload encoding)
- `src/wal.rs:658` — `pub fn read_entries(...)` / `read_records*`
- `src/wal.rs:281` — `pub fn seal(...)` (segment rotation)
- `src/tx_log.rs` — committed-transactions set used during replay
