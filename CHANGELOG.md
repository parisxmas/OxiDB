# Changelog

## v0.25.25

### Relicensed — AGPL-3.0 + commercial (dual-license)

OxiDB moves from `MIT OR Apache-2.0` to a **dual license**: the public,
open-source license is now **AGPL-3.0-only** (see [`LICENSE`](LICENSE)),
and a separate **commercial license** is available for closed-source /
proprietary use that the AGPL's copyleft does not permit — see
[`COMMERCIAL-LICENSE.md`](COMMERCIAL-LICENSE.md).

- `LICENSE-MIT` / `LICENSE-APACHE` removed; `LICENSE` is now the full
  AGPL-3.0 text. `license = "AGPL-3.0-only"` across every workspace
  crate's `Cargo.toml`.
- Prior releases remain under `MIT OR Apache-2.0` — that grant on those
  specific versions cannot be revoked. This and every future version is
  AGPL-3.0 / commercial.
- Contributions are accepted under **both** licenses, so the whole of
  OxiDB — contributed code included — can still be offered commercially.

### Versions

- `oxidb-server`: 0.25.24 → 0.25.25

## v0.25.24

### Blob store — opt-in durable writes with group-committed fsync

The blob store acknowledged a `put_object` as soon as the temp files
were renamed into place, leaving durability to a 1 Hz background fsync
thread — so a successful put could still be lost to a power cut for up
to a second. The document WAL has a real 3-fsync protocol; the blob
store, where large payloads (e.g. mail bodies behind the 16 MiB wire
cap) actually land, did not. This adds an opt-in durable path.

- **`OXIDB_BLOB_SYNC` / `BlobStore::with_sync_writes`** (`src/blob.rs`)
  — when set, `put_object` fsyncs the payload and meta temp files, then
  fsyncs the bucket directory between the `.data` and `.meta` renames
  and again after. The ordering is load-bearing: `scan_bucket` treats
  `.meta` as the source of truth, so a `.meta` made durable ahead of
  its `.data` would be a ghost read on recovery. The second dir fsync
  is the commit point — once it returns the put is durable and a
  caller (e.g. an SMTP server) may treat it as committed.

- **Durable `delete_object`** (`src/blob.rs`) — symmetric with `put`:
  fsyncs the bucket directory after unlinking the `.meta`, so an ack'd
  delete cannot be resurrected by a crash. In durable mode a real I/O
  error from the unlink is now propagated instead of swallowed;
  non-durable mode keeps its prior best-effort behavior.

- **Group-commit directory syncer** (`src/blob.rs`) — a naive per-put
  implementation would fsync the bucket dir twice per put, fully
  serial. `DirSyncer` mirrors the `tx_log` committer: a dedicated
  thread owns the fsync, callers `rename` then enqueue and block, and
  the thread coalesces every request waiting in its queue into one
  `fsync` per distinct directory (`MAX_BATCH = 512`). Because a caller
  always renames before it enqueues, any queued request corresponds to
  a completed rename — so N concurrent durable puts to one bucket cost
  ~2 directory fsyncs, not 4N. Single-put latency is unchanged (a
  channel round-trip is ~µs against an ~ms fsync).

- **Background `blob-sync` thread fix** (`src/blob.rs`) — the periodic
  fsync thread fsynced the `_blobs` root, but object renames/unlinks
  happen one level down in the per-bucket directories, and fsyncing a
  parent does not flush its children. It now enumerates the bucket
  dirs each tick and fsyncs every one. This is the best-effort path
  for deployments that leave `OXIDB_BLOB_SYNC` off.

- **Crash-consistency harness** (`tests/blob-crash-recovery-go/`) — a
  Go harness in the `crash-recovery-go` mould: put N objects, delete a
  subset, churn a second bucket so the SIGKILL lands mid-`put_object`,
  cold-boot the same data dir, and assert every ack'd put survived
  intact, every ack'd delete held, no stray `.tmp` files remain, and
  every listed object is readable (no ghost `.meta`). Scope is stated
  honestly in the file header: SIGKILL does not drop the page cache,
  so this proves crash *consistency*, not fsync *durability* — the
  latter needs block-layer fault injection (dm-log-writes), tracked
  separately. 3 new `blob::tests` unit tests cover the sync and
  group-commit paths plus reopen (9 → 12).

### Versions

- `oxidb-server`: 0.25.23 → 0.25.24

## v0.25.23

### Point-In-Time Recovery

OxiDB had only full-snapshot `backup`/`restore` — no way to recover to an
arbitrary moment (e.g. "just before the bad bulk delete"). The hard part:
OxiDB has **no global write ordering** — each collection's `.wal` is an
independent byte stream, `_tx_commit_log` is an unordered set, and most
writes carry `tx_id = 0`. PITR introduces that ordering and the machinery
to archive and replay against it. Opt-in via `OXIDB_PITR`; **zero cost
when off** — every record stays byte-identical v1, no extra threads.

- **WAL v2 record format** (`src/wal.rs`) — records gain v2 op-types
  (high bit) carrying an optional `[gsn][wall_clock_micros]` header.
  `read_entries` replays mixed v1/v2 files; v1 is still emitted unless a
  sequencer is attached.

- **Archive sequencer** (`src/pitr.rs`) — `ArchiveSequencer` hands every
  durable WAL write a global, monotonic, wall-clock-stamped GSN. It
  survives restarts via a leased `_gsn` file — one fsync per 10k GSNs,
  never reusing a number. GSN allocation happens **under the WAL lock**,
  so "the counter passed N" implies "N's record is in the file" — the
  invariant the base-backup watermark relies on.

- **WAL segment rotation** (`src/wal.rs`) — `Wal::seal()` atomically
  renames the live WAL to a numbered sealed segment and opens a fresh
  one, entirely under the WAL lock so it is atomic against every
  concurrent `log*` (closing the documented "lost acks across
  truncation" race). `log*` auto-seals past `OXIDB_WAL_SEGMENT_BYTES`
  (default 16 MiB). `replay_wal` now replays sealed segments + the live
  WAL, so a rotated WAL still recovers every acknowledged write. A
  6-writer-plus-sealer concurrency stress asserts zero loss/dup.

- **Archiver** (`src/archive.rs`) — a background `oxidb-archiver` thread
  copies sealed segments into `OXIDB_ARCHIVE_DIR/segments/*.seg` —
  verbatim WAL bytes (at-rest encryption preserved) plus a trailer with
  the GSN/time range + CRC. Crash-safe (`tmp → fsync → rename →
  fsync-dir`), idempotent (a segment is archived iff `<name>.seg`
  exists), with a `manifest.json` rebuilt from the `.seg` trailers so a
  torn manifest self-heals. Best-effort — sealed segments are immutable,
  read with no locking, never blocking a foreground write.

- **Base-backup watermark** (`src/engine.rs`, `src/pitr.rs`) — `backup()`
  reads the GSN counter, barriers every collection's WAL, and embeds a
  `base.meta` watermark in the tarball; the base is then guaranteed to
  contain every write below it.

- **`restore_to_point`** (`src/engine.rs`, `src/archive.rs`) — extracts a
  base backup, then `replay_into` advances it to a `Gsn` / `Timestamp` /
  `Latest` target: gathers every WAL record per collection, resolves the
  target GSN, applies a **two-pass transactionally-consistent cut** (a
  transaction is admitted only if its whole footprint — `max(gsn)` over
  all its records, across collections — fits under the target; one
  straddling the cut is excluded whole, never half-applied), rewrites
  each WAL with exactly the admitted records, and drops the stale index
  caches / FTS index / tx commit log. Idempotent and offline.

- **Server + retention** (`oxidb-server/src/handler.rs`,
  `src/archive.rs`) — admin commands `restore_to_point` and
  `archive_status`. `OXIDB_ARCHIVE_RETENTION_HOURS` prunes archived
  segments older than the window (`0` = disabled).

v1 limitations: blob objects restore to the base-backup point only (the
document set restores to the target); the FTS index is dropped and must
be rebuilt; create/drop-index DDL between the base and the target is not
replayed. SIGKILL preserves the page cache, so the included tests prove
crash *consistency* and idempotency, not fsync durability under power
loss — block-layer fault injection is tracked separately.

- 25 new unit tests across `wal`, `pitr`, and `archive`; the full lib
  suite is green except `wal_checkpoint_clears_wal` and
  `restore_from_backup`, which already fail on `master`.

### Versions

- `oxidb-server`: 0.25.22 → 0.25.23

## v0.25.22

### Engine — group-commit tx_log + lazy DocCache shards

- **Group commit on the transaction log** (`src/tx_log.rs`) —
  `mark_committed` was a global `Mutex<File>` + unconditional
  `sync_data()` per commit. Every transaction on every collection
  serialised through that single fsync, capping throughput at
  ~85 doc/s on a single-tenant DMS upload workload regardless of
  `OXIDB_LAZY_SYNC`. Moved the file behind a dedicated
  `oxidb-tx-commit` thread that owns an in-memory
  `HashSet<TransactionId>` + the file handle. Callers submit
  `Cmd::Mark / Remove / Clear / Read` over `mpsc` and block on a
  per-call `sync_channel(1)` reply. The committer drains the queue
  non-blockingly up to **`MAX_BATCH = 512`**, applies all mutations
  against the in-memory set, and emits **one** `persist + sync_data`
  per batch. Notifies every waiter only after the fsync, so
  durability semantics are unchanged. Reads are deferred until after
  the same batch's fsync — recovery invariant preserved.
  - File format unchanged: still a sequence of `[tx_id: u64 LE]`.
    Old logs are parsed once at `open()` into the in-memory set;
    the file is rewritten in full (sorted ids) at each batch
    boundary.
  - Bench (dms-bench, lazy_sync, Turkish FTS, real PDF + metadata
    uploads, pool=128):
    - single-tenant: **85 → 850 doc/s** @ 256 workers (~10×)
    - multitenant 10t × 10w: **73 → 527 doc/s** (~7×)
  - At 800+ doc/s the new bottleneck is per-collection
    `btree_storage::persist_mu` — expected, out of scope.
  - 9/9 tx_log unit tests pass, plus a new
    `concurrent_marks_all_durable` that fires 64 threads × 32 marks
    and verifies the post-run set holds all 2048 ids and survives
    close + reopen.

- **Lazy-allocate DocCache shards** (`src/doc_cache.rs`) — each
  `BTreeCollection` eagerly built a 16-shard LRU sized for 100K
  entries on creation. At scale that compounded to ~400 KiB of
  preallocated hashtable buckets per collection regardless of use:
  at 10K collections the dms-bench scale-load test hit **3.9 GiB
  RSS** with **161K anonymous mmap regions**. Switched the shard
  slots to `Mutex<Option<LruCache<…>>>` with a stored per-shard cap
  target (`AtomicUsize`). First `put()` to a shard materialises the
  inner `LruCache` at the cap; `clear()` drops it back to `None` to
  reclaim. Capacity ceiling unchanged — active collections still
  grow to the full 100K slots.
  - 10K populated collections: **3.9 GiB → 269 MiB** RSS (−93%),
    **161,348 → 6,316** anonymous mmap regions (−96%), insert p99
    unchanged.
  - 9/9 doc_cache unit tests pass, including a new
    `lazy_alloc_until_first_put` assertion.

### Combined effect

Unlocks the **collection-prefix multi-tenancy** primitive started
in v0.25.19 (`create_collection` 1260× speedup): with both
parallel-create AND the new memory + commit-throughput patches,
10K-collection deployments are now realistic — RSS, fsync
contention, and create-cost all in their right place.

### Versions

- `oxidb-server`: 0.25.21 → 0.25.22

## v0.25.21

### Decode — 5× faster cold-path wire response

- **`codec::decode_doc_to_text`** (`src/codec.rs`) — JSONB → JSON text via `RawJsonb::to_string`, skipping the `serde_json::Value` intermediate. One walk, no allocated Value tree. Legacy JSON-text bytes pass through unchanged.
- **`BTreeCollection::load_doc_text`** (`src/btree_collection.rs`) — cache-hit serializes the cached `Arc<Value>` (unchanged hot path); cache-miss reads raw bytes and calls `decode_doc_to_text` directly. Does NOT populate the doc cache on miss — decoding to Value just to cache it would undo the speedup, and the wire payload by itself doesn't carry the structured Value that filter paths need.
- Driven by measurement (`examples/measure_cache_hitrate.rs`): Zipfian sweep showed DocCache hit ratios of 60-92% for typical OLTP-shaped workloads (Zipf s ~ 1.0-1.2, 10-100× cache cap) and 12-35% for low-skew / big-working-set configs. Cold path is mixed but the absolute hit is biggest there.

  | Doc shape | Cold A (now) | Cold B (this) | Speedup | Hot A | Hot B |
  |---|---:|---:|---:|---:|---:|
  | small (4 fields) | 1 503 ns | 305 ns | 4.93× | 131 | 122 |
  | medium nested | 2 110 ns | 435 ns | 4.85× | 248 | 254 |
  | LARGE (50 events) | 31 001 ns | 5 752 ns | 5.39× | 4 001 | 3 875 |

- Next step (separate change): wire the server's find/find_one response handler to call `load_doc_text`. The engine-level building block is the prerequisite.

### Observability — DocCache hit/miss counters

- `DocCache` now tracks cumulative hits and misses (atomic Relaxed; cheap). `DocCache::stats()` returns a `CacheStats { hits, misses, hit_ratio() }` snapshot; `reset_stats()` clears the window.
- Exposed on the collection via `BTreeCollection::doc_cache_stats` / `doc_cache_stats_reset` / `doc_cache_clear`.

### Benches

- **`examples/measure_cache_hitrate.rs`** — Zipfian sweep that produced the hit-ratio data above.
- **`examples/profile_decode_wire.rs`** — micro-bench isolating the find→wire decode step (JSONB → Value → text vs `RawJsonb::to_string`).
- **`examples/profile_load_doc_text.rs`** — end-to-end load on a populated `BTreeCollection`, verifies the patch doesn't regress the hot path.

### Versions

- `oxidb-server`: 0.25.20 → 0.25.21

## v0.25.20

### Codec — 3-4× faster JSONB encode, ~40% smaller on-disk image

- **`codec::encode_doc` rewrite** (`src/codec.rs`) — route `Value` → `serde_json::to_writer` → `jsonb::parse_owned_jsonb_standard_mode` instead of `jsonb::to_owned_jsonb` (which uses the serde Serialize path).
  - The serde encoder in `jsonb` 0.5 allocates a fresh `Serializer` (with its own `Vec<u8>`) for every map value, materializes each child as an intermediate `OwnedJsonb`, then concatenates them in `ObjectBuilder::build → to_vec → buffer.append`. That was the dominant per-encode cost.
  - It also wrapped every scalar inside a container with a 4-byte `SCALAR_CONTAINER_TAG`, inflating the on-disk image by 30-50%.
  - The new path produces output bytes that are still a valid `OwnedJsonb` and decode through the same `jsonb::from_raw_jsonb` / `RawJsonb::get_by_*` calls. Legacy fat-format images from older writers continue to round-trip — `decode_doc` is unchanged.
- **Bench results** (median of 5, `--release`, M2):

  | Doc shape | Before | After | Speedup | Bytes before / after |
  |---|---:|---:|---:|---:|
  | flat scalars (5 fields) | 1 177 ns | 259 ns | 4.5× | 198 B / 83 B |
  | nested objects (2-level) | 2 913 ns | 742 ns | 3.9× | 474 B / 240 B |
  | array of 50 small objects | 48 348 ns | 12 336 ns | 3.9× | 8 682 B / 4 575 B |
  | LARGE realistic doc | 61 256 ns | 20 092 ns | 3.0× | 13 571 B / 9 306 B |

- **End-to-end smoke** — existing `wal_checkpoint` test logs the WAL size after 20 inserts: **3 821 B → 1 459 B (-62%)** with no change other than the encoder. Disk space, memory residency, and WAL replay cost all benefit.
- Standard mode is used (not extended) because `serde_json::to_writer` always emits strict JSON — no NaN/Infinity, no leading plus signs, no empty array elements to accommodate.

### Benches

- **`examples/profile_decode.rs`** — decomposes the decode hot path (full decode vs partial extract vs custom IndexValue extractor) so we can spot regressions on the next `jsonb` bump. Findings: partial extract is doc-size-independent (~90 ns); writing a custom IndexValue deserializer that bypasses serde turned out to be marginally *slower* — `get_by_keypath` is the dominant cost, not the scalar dispatch.
- **`examples/profile_encode.rs`** — decomposes encode by document shape; the basis for the `encode_doc` rewrite. Both run in a few seconds with `cargo run --release --example <name>`; no `criterion` / external harness.

### Versions

- `oxidb-server`: 0.25.19 → 0.25.20

## v0.25.19

### Engine — ACID hardening

- **Durability: "ack means on disk"** (`src/btree_storage.rs`, `src/btree_collection.rs`, `src/wal.rs`)
  - `persist` switched from non-atomic `fs::write` to tmp + `sync_data` + atomic rename + parent fsync; per-collection mutex prevents concurrent commits from trampling a shared `{name}.btree.tmp`
  - `OXIDB_LAZY_SYNC` default flipped from `true` → `false`; strict mode fsyncs every commit. Lazy mode still available opt-in.
  - `set_lazy_sync` wired through every write path (single + batch insert, update, delete, sync_writes) so the env flag actually selects fsync vs no_sync
  - tx-commit `log_wal_batch` on btree no longer a no-op — durability via WAL fsync at commit, not a synchronous full-file persist
  - `sync_writes` persists without truncating WAL (truncate was racy with concurrent writers and lost ~3/2000 acks under load); WAL truncation moved to a new `final_checkpoint` that runs only at shutdown
  - New `enable_periodic_snapshot` (strict mode default 1s cadence) so WAL doesn't grow unbounded between commits
- **Boot tolerance** — `btree_storage::open` tolerates partial / truncated images and leaves recovery to WAL replay instead of refusing to boot
- **Graceful shutdown** — `oxidb-server` SIGTERM/SIGINT handler flushes engine via `OxiDb::shutdown` then `process::exit` cleanly; SIGPIPE ignored
- **Multi-collection atomicity** (`src/transaction.rs`)
  - `tx_insert` reserves the doc id at buffering time and returns it to the client, so callers can wire the assigned id into sibling writes inside the same transaction
  - `WriteOp::Insert` carries the pre-allocated id; `prepare_tx_insert` consumes it instead of double-allocating
  - Wire-protocol `insert`/`insert_many` in tx mode now return `{"id": N}` matching the non-tx response shape (was `"buffered"` with no id)
  - `find_one` routes through `tx_find` when in a transaction so the read version is recorded for OCC validation — without this a read-then-write inside a tx would skip the conflict check
- **Crash-test harnesses**
  - `tests/crash-recovery-go`: 2000 inserts → SIGKILL → restart, every ack'd write must survive; no `.btree.tmp` leftovers; payload spot-check
  - `tests/atomicity-go`: 3 scenarios — pre-commit SIGKILL, post-commit SIGKILL, mid-tx SIGTERM (graceful) — across two collections with foreign-key linkage, asserting all-or-nothing recovery
- **Legacy `.dat` data-loss guard** — `OxiDb::open_internal` scans the data dir at startup and refuses to open non-empty `.dat` collection files without a matching `.btree`. Without this, upgrading a pre-BTree binary in place silently shadowed real records with empty collections. `OXIDB_ALLOW_LEGACY_DAT=1` keeps the old behavior for callers that explicitly accept the loss.

### Engine — concurrency

- **Parallel `create_collection`** (`src/engine.rs`) — hold the global write lock only to insert into the collections map, not while opening the `BTreeCollection` from disk. Mirrors `get_or_create_collection`: read-check, lock-free open, then short write lock with a race-loser re-check.
  - Bench (1000 collections, 8 workers): Phase 1 wall **4m18s → 205ms**, `CreateCollection` p99 **6.25s → 1ms** (~1260× speedup). At 10K collections: p99=11ms, RSS=1.24 GiB — viable as a per-tenant collection-prefix multi-tenancy primitive.
- **Lock-free blob `put` rename** (`src/blob.rs`) — `fs::rename` no longer held under the bucket write lock; split into: brief lock for id allocation, lock-free renames, brief lock for hashmap commit. Same-key races are safe for content-addressed callers (identical bytes → identical result). Brings 32-way concurrent put p50 down from ~900ms.

### Full-text search

- **Parallel indexing worker pool + introspection** (`src/fts.rs`)
  - `FtsRuntime` tracks queue depth, per-worker in-flight job, and a ring of recently completed/failed/skipped jobs
  - Engine: `bucket_fts_size` accessor (powers per-tenant FTS quota accounting); `fts_status` returns the runtime snapshot as JSON
  - Server: new `fts_status`, `bucket_fts_size`, `proc_status` commands; admin + reader RBAC roles get `fts_status` + `proc_status`

### S3

- **`aws-chunked` request body decoding** (`oxidb-server/src/s3/`) — AWS CLI / boto3 send streaming PUT bodies with `content-encoding: aws-chunked` or `x-amz-content-sha256: STREAMING-*`. Strip the chunk-size framing back to the original payload in both single PUT and multipart upload paths. Tolerates missing trailers and partial reads.

### Blobs

- **Skip zstd for already-compressed mime types** (`src/blob.rs`) — re-compressing image/video/audio buys ~nothing while costing CPU on every Put and Get. Detect the content-type prefix and store raw bytes when compression is futile; decode path stays forward-compatible with legacy zstd-stored blobs.

### OCR

- **Image crate + dockerized tesseract toolchain** — `ocr` feature now pulls `image` (png/jpeg/tiff/bmp/gif/webp) so the pipeline can decode the source file before handing pixels to leptess. Dockerfile installs `libtesseract-dev`/`libleptonica-dev` + libclang for leptess bindgen, plus runtime libs and `eng`/`tur` traineddata in the slim image. Build uses `--features cluster,ocr`.

### Process metrics

- **macOS dev-box `proc_status`** (`oxidb-server/src/proc_stats.rs`) — `getrusage(RUSAGE_SELF)`-backed `read_cpu_ticks` + `read_vm_rss_kb` so dashboards report real CPU% / RSS on Darwin. Linux prod path (`/proc/self/{stat,status}`) unchanged.
- **Real macOS thread count via Mach `task_threads`** — replaces the placeholder `0` with the live thread count (matches Activity Monitor); releases per-thread send rights and the returned array to avoid port-name leaks under the 5s admin probe.

### Go client

- **`Client.ProcStatus()` / `Client.FtsStatus()`** — typed wrappers so callers don't assemble raw `{"cmd": ...}` maps. `BucketFTSSize` added.
- **Bounded ping check on pooled connection checkout** (`go/oxidb/pool.go`) — without a deadline, `Ping` on a server-reaped TCP conn that didn't get a clean FIN could hang for the OS keepalive interval (~2h on macOS/Linux). 2s `pingTimeout`; checkout transparently dials a fresh replacement when the pooled conn fails Ping.

### Python client

- `fts_status`, `proc_status` added.

### Build / repo

- `oxidb-wasm`: drop member-level `[profile.release]` — Cargo only honors profile sections at the workspace root; the per-crate config was silently ignored and emitted a warning every build.
- Drop tracked `target/` symlink — external SSD disconnect left rustc processes hanging in `U` state during build. Cargo creates a fresh local `target/` now.

### Versions

- `oxidb-server`: 0.25.9 → 0.25.19

## v0.25.9

### Full-text search — BM25 ranking + multi-language stemmers + highlighting

- **BM25 ranking** replaces TF-IDF (`src/fts.rs`)
  - Length-normalized: long documents no longer outrank short ones on identical TF
  - Saturating: 10× term frequency does not yield 10× score (k1 saturation)
  - Lucene/Elasticsearch defaults `k1=1.2`, `b=0.75`; tunable via `OXIDB_FTS_K1` / `OXIDB_FTS_B`
  - Lazy migration: `_fts/index.json` files written before BM25 are auto-backfilled with `total_term_count` on first open — no rebuild required
  - Both `FtsIndex` (blob FTS) and `CollectionTextIndex` (per-collection FTS) on the new path
- **Snowball stemmers, 18 languages** via `OXIDB_FTS_LANG`
  - English (default), Turkish/`tr`, German, French, Spanish, Italian, Portuguese, Russian, Dutch, Danish, Finnish, Hungarian, Norwegian, Romanian, Greek, Arabic, Swedish, Tamil
  - Cached per-process via `OnceLock` — no overhead in the hot path
  - Verified in tests: `kitap` ↔ `kitaplar` ↔ `kitaplarda` collapse to a common stem under `OXIDB_FTS_LANG=tr`
- **Highlighted snippets** — `fts::highlight(text, query, snippet_chars, max_snippets)` returns `<mark>matched</mark>` snippets with offsets and matched-term counts
  - Same tokenization pipeline as the index, so a `running` query highlights `runs` (and `kitaplar` highlights `kitabı` under Turkish)
  - Custom tags via `highlight_with_tags(...)`
  - Multi-byte (UTF-8) safe: char-boundary snapping prevents panics on Turkish/CJK text
  - Wired through to:
    - `Collection::text_search_highlighted` / `BTreeCollection::text_search_highlighted`
    - `OxiDb::text_search_highlighted`, `OxiDb::search_highlighted`
    - Server `text_search` and `search` ops via optional `"highlight": true` or `"highlight": {"snippet_chars": N, "max_snippets": M}`
- **Multi-worker FTS pipeline** — `OXIDB_FTS_WORKERS` (default 1)
  - New `FtsDispatcher` round-robins jobs across N worker channels — CPU-bound `extract_text` (PDF/DOCX/OCR) parallelizes across cores
  - Round-robin with `try_send` fallback to blocking `send` so one slow worker doesn't backpressure the whole pool
- **Batched FTS persist** — `OXIDB_FTS_FLUSH_INTERVAL_MS` (default 1000 ms)
  - `FtsIndex` gains `set_batched(true)` / `flush()` — per-document mutations only mark the index dirty
  - Background flusher thread persists at most once per interval
  - Eliminates the previous N² disk write amplification on bulk ingestion (every doc previously rewrote the entire `_fts/index.json`)
  - Existing test path keeps `batched=false` so synchronous-persist guarantees still hold
- **Startup config dump** — `oxidb-server` now logs `FTS: lang=... k1=... b=... (BM25)` after the alert-evaluator line
- 38 new FTS unit tests + 3 new BTreeCollection integration tests; total lib suite 678 → 714

### Aggregation pipeline

- **`$dateHistogram`** stage (`src/pipeline.rs`)
  - Buckets a date field by `interval` and runs accumulators per bucket
  - Intervals: `Ns`/`Nm`/`Nh`/`Nd`/`Nw` (fixed-width) or `1M`/`1y` (calendar) plus long forms `minute`/`hour`/`day`/`week`/`month`/`year`
  - `min_doc_count: 0` fills empty buckets between observed min and max with `count: 0` — emitted as a synthetic `Stage::DateBucketFill` chained after the underlying `$group`
  - Accepts ISO 8601 / RFC 3339 strings or numeric epoch ms; output `_id` is always an ISO string in UTC
  - Implementation: new `Expression::DateBucket(expr, DateInterval)` reuses the standard `$group` execution path; index-accelerated group remains compatible
- **`$percentile`** accumulator
  - Exact percentile with linear interpolation between nearest ranks
  - Syntax: `{ "$percentile": { "input": "$score", "p": [0.5, 0.95, 0.99] } }` → returns array of values matching `p` order
  - Validates `p` is non-empty and each entry is in `[0, 1]`

### Server / handlers

- `text_search` op gains optional `"highlight"` field — collection FTS returns `<mark>` snippets per indexed string field plus `_score`
- `search` op gains optional `"highlight"` field — blob FTS re-extracts each hit's blob and emits snippet array (cost is paid only when requested)
- `oxidb-server/Cargo.toml` bumped: 0.25.3 → 0.25.9

### Tests / demo

- New `ftstests/` directory — end-to-end FTS smoke + live demo
  - `01_generate.py` — fetches three Project Gutenberg books (Alice / Pride and Prejudice / Sherlock Holmes) and splits each into ~33 minimal `.docx` chunks (uses `zipfile` only, no `python-docx` dependency)
  - `02_upload.py` — uploads via the in-tree Python TCP client
  - `03_search.py` — 15 ranking / stop-word / highlight assertions; 15/15 pass against a fresh server
  - `web.py` — stdlib HTTP proxy: `/api/search`, `/api/blob/<key>`, `/api/text/<key>`, `/api/stats`, `/api/upload/<filename>`, `/healthz`
  - `index.html` — single-page UI: search box, query chips, drag-and-drop upload, document viewer (PDF iframe / mammoth.js DOCX / SheetJS XLSX / sandboxed HTML / `<pre>` for text), find-in-doc toolbar with prev/next match navigation, auto-highlight of the search query inside the viewer, download button, live document count
  - `run.sh` — one-shot: build server + spawn + generate + upload + run search suite
  - `serve.sh` — long-running: persistent data dir + auto-seed + web client (default `http://127.0.0.1:8765/`)
  - `deploy/` — image-based deploy (`docker buildx --platform=linux/amd64` → `docker save` → scp tar → `docker load`); ships only image binaries to the remote, no source tree

## v0.25.3

### `oxipool/src/scatter.rs` — partial-shard errors no longer silent

- New `first_partial_error` helper — checks every shard response before merging.
- `merge_counts`, `merge_doc_arrays`, `merge_modified` now fail fast with `ok:false` and the failing shard's error message instead of silently summing/concatenating only the responding shards.
- Surfaced by the 100K load test: a stale-pool conn to a freshly-restarted follower returned an error → router silently dropped that shard → `count` returned ~2/3 of the actual rows. With the fix the client sees a real error and can retry, instead of getting an under-count.
- `oxipool` crate version bumped: 0.25.0 → 0.25.3.

### Cluster mode — Raft persistence: O(1) per mutation

- Rewrite `oxidb-server/src/raft/log_store.rs` persistence layer to scale
  - Split single `raft_state.json` into `raft_meta.json` (small, vote/committed/sm_data — rewritten on metadata changes) and `raft_log.jsonl` (append-only, one Entry per line)
  - `append_to_log` is now O(1) per entry instead of O(n) — single line append
  - `delete_conflict_logs_since` / `purge_logs_upto` rewrite the log file (rare events)
  - On startup, `raft_meta.json` + `raft_log.jsonl` are loaded line-by-line into the in-memory BTreeMap
  - Transparent migration from the v0.25.2 single-file format
- Unblocked 1M-record load tests under failover: 22.4 s end-to-end, 44,701 rec/s avg, 0 records lost (previously stalled at ~52% complete due to 14 MB-per-mutation rewrites)

## v0.25.2

### Cluster mode — Raft state persistence

- Add disk persistence for Raft storage in `oxidb-server/src/raft/log_store.rs`
  - `OxiDbStore` was previously in-memory only; nodes that restarted came back as `Learner term=0` and lost cluster membership, breaking failover scenarios
  - New `OxiDbStore::open(db, &data_dir)` constructor loads existing Raft state on startup
  - Atomic write-through (write-then-rename) on every mutation: `save_vote`, `save_committed`, `append_to_log`, `delete_conflict_logs_since`, `purge_logs_upto`, `apply_to_state_machine`, `install_snapshot`
  - Wired up from `oxidb-server/src/main.rs` cluster-mode startup
- `OxiDbStore::new(db)` retained as in-memory variant (used by tests)

### Tests

- Add `ShardReplicaRealWorldTest/` — full sharded + replicated cluster harness
  - 14-service `docker-compose.yml`: 9 oxidb-server nodes (3 Raft groups), 3 per-shard oxipool master/replica routers, 1 top-level shard-routing oxipool, 1 Go API tier, 1 cluster-init bootstrapper, 1 opt-in smoke harness
  - `cluster-init/` — one-shot Go tool that runs `raft_init` + `raft_add_learner` + `raft_change_membership` on each shard's leader candidate
  - `api/` — Go HTTP API with endpoints for browse, cart, checkout (TX-pinned), order history, scatter-gather queries, raft metrics
  - `smoke/` — 5-assertion Go smoke test covering health, sharding, replication, TX pinning, scatter-gather
  - `tests/test_cluster.py` — 8 Python integration tests (CRUD + sharding + aggregation)
  - `tests/test_failover.py` — 5 Python failover scenarios (network partition, follower down, recovery catch-up, two followers down, leader down)
  - `tests/test_load_failover.py` — load test with mid-stream failover (parameterized by `TOTAL`/`BATCH`/`FAILOVER_AT`)
  - Validated against 10K, 100K, and 1M record loads

## v0.25.1

### Query Engine

- Add `$not` operator — negate any field condition; missing fields return true (MongoDB-compatible)
- Add `$nor` top-level operator — match documents where none of the conditions are true
- Add `$all` operator — array must contain all specified values
- Add `$size` operator — match arrays with exact length
- Add `$type` operator — match by JSON type (string, number, bool, array, object, null, int)
- Add `$mod` operator — modulo arithmetic on numeric fields (`[divisor, remainder]`)
- Add `$expr` top-level operator — cross-field comparisons (`{"$expr": {"$gt": ["$sold", "$stock"]}}`)
- Add `$elemMatch` operator — match array elements against sub-queries with AND semantics
- Refactor `matches_doc` and `matches_value` into shared `eval_field_op` helper

### Go Client

- Add stored procedure methods: `CreateProcedure`, `CreateProcedureFromScript`, `CallProcedure`, `ListProcedures`, `GetProcedure`, `DeleteProcedure`, `CompileOxiScript`
- Add `CreateTTLIndex` for automatic document expiration
- Add retention policy methods: `SetRetention`, `GetRetention`, `DeleteRetention`, `ListRetentions`
- Add alerting methods: `CreateAlert`, `GetAlert`, `DeleteAlert`, `ListAlerts`, `TestAlert`, `ListAlertHistory`
- Add `ExtractText` for blob text extraction (PDF, DOCX, HTML)
- Add `Backup` and `Restore` for full database backup/restore
- Add `SetDialect` for SQL dialect switching (mysql, postgresql, mssql, generic)

### Tests

- 107 query engine tests (53 new), including 15 real-world scenario tests covering fraud detection, loan eligibility, matchmaking, property search, supply chain, content management, and more

## v0.25.0

- Bump all workspace crates to v0.25.0
- Add Python client retention, alerting, and TTL methods
- Add `oxidb-tail` TUI log viewer with table columns, stats toggle, and keyboard shortcuts

## v0.24.0

- Add WebAssembly support — OxiDB runs in the browser
- Add JavaScript/TypeScript SDK (`oxidb` npm package) — zero dependencies, REST + WebSocket
- Add JWT authentication for REST and WebSocket APIs
- Add WebSocket server with real-time subscriptions
- Add per-document security rules (Firebase-style access control)
- Add TTL indexes with automatic document expiration
- Add REST HTTP API with CORS and 64-thread pool

## v0.23.1

- Add TTL indexes, REST HTTP API, OxiScript tests, Julia client updates

## v0.23.0

- Add stored procedures (OxiScript and JavaScript)
- Add cron scheduling for procedures
- Add multi-database support (create, drop, use, list)
- Add SQL dialect support (MySQL, PostgreSQL, MSSQL, Generic)

## v0.22.2

- Add GELF ingestion, retention policies, and alerting system
- Add GELF chunked reassembly and FTS stemming with accent normalization
- Add GPU-accelerated vector search via wgpu compute shaders

## v0.22.0

- Add OxiMem in-memory key-value layer (RESP protocol, redis-cli compatible)
- Add MQTT v3.1.1 protocol with cross-protocol pub/sub
- Add sorted sets (ZADD, ZRANGE, etc.)

## v0.20.4

- Add vector similarity search (cosine, euclidean, dot product)
- Add pipeline command batching

## v0.20.0

- Add S3-compatible blob storage with full-text search
- Add backup and restore commands

## v0.19.0

- Add SCRAM-SHA-256 authentication
- Add RBAC (Admin, ReadWrite, Read roles)
- Add TLS support
- Add audit logging

## v0.18.0

- Initial public release
- Core document engine with append-only storage, WAL, field indexes
- SQL and JSON query support
- ACID transactions with OCC
- Aggregation pipeline
- Full-text search
- Python, Go, .NET, PHP, Swift, Julia client libraries
