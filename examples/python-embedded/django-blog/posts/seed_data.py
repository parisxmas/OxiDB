"""
Seed posts for /dev/oxide — 10 build-log entries about real OxiDB
features. Each one is grounded in actual code in the parent
repo; the timestamps are spread out over the last ten days so the
archive renders in a sensible reverse-chronological order.
"""

from datetime import datetime, timedelta, timezone

# Stamps go newest-first. Subtract `i` days from `now` so SEED_POSTS[0]
# (the newest) is "today" and SEED_POSTS[9] is ten days back.
_NOW = datetime.now(timezone.utc).replace(microsecond=0)


def _stamp(i: int) -> str:
    return (_NOW - timedelta(days=i, hours=(i * 3) % 24)).strftime("%Y-%m-%dT%H:%M:%SZ")


# Authors: a couple of personas so the archive isn't all-admin.
ADMIN = "admin"


SEED_POSTS = [
    # ──────────────────────────────────────────────────────────────────
    # 0 — newest
    {
        "title": "skipping the Value tree — 5× faster find→wire decode",
        "slug": "skipping-the-value-tree",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "Even after the encode rewrite, profiling the wire path on this "
            "blog showed half of every find response was spent inside "
            "`serde_json::Value`. The cold path looks like:\n\n"
            "  storage bytes (JSONB) → jsonb::from_raw_jsonb → Value tree → "
            "serde_json::to_writer → wire bytes\n\n"
            "On a 13 KB document that round-trip is ~30 µs. About 27 µs of "
            "that is materializing the recursive Value enum — every string "
            "is a fresh `String`, every array a `Vec<Value>`, every object "
            "a `Map<String, Value>`. The wire wants JSON text. The Value "
            "tree is pure overhead in between.\n\n"
            "`RawJsonb::to_string` walks the binary once and writes JSON "
            "straight to a buffer using `serde_json::ser::CompactFormatter`. "
            "No Value tree, no recursive allocation. Same 13 KB doc lands "
            "in ~5 µs — 5.4× faster — and the output bytes are identical to "
            "what the old path produced.\n\n"
            "Which workloads care? Hit rate matters. I ran a Zipfian sweep "
            "to find out: at typical OLTP skew (s=1.0–1.2, 10–100× cache "
            "cap), DocCache hit rate sits between 60% and 92%. At low skew "
            "with a working set that doesn't fit, hit rate drops to 12–35%. "
            "The cold path is where the absolute gain is biggest, so that's "
            "what got the bypass.\n\n"
            "The patch is a new `decode_doc_to_text` helper in `codec.rs` "
            "plus `BTreeCollection::load_doc_text` that calls it on cache "
            "miss and falls back to serializing the cached `Arc<Value>` on "
            "cache hit. The filter and update paths still use `load_doc_arc` "
            "because they actually need the Value tree."
        ),
        "created_at": _stamp(0),
    },
    # ──────────────────────────────────────────────────────────────────
    # 1
    {
        "title": "encode 3× faster, output 40% smaller — one-line codec rewrite",
        "slug": "encode-3x-faster-40-percent-smaller",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "Writes have always felt slower than reads on this engine and I "
            "never figured out why. So I wrote a micro-bench that breaks "
            "encode apart by document shape and ran it against six payload "
            "profiles — flat scalars, nested objects, arrays of strings, "
            "arrays of small objects, and a 'large realistic' doc that "
            "looks like the events shape we see in real-world traffic.\n\n"
            "The result was ugly: on the large doc, `codec::encode_doc` "
            "took 61 µs. Plain `serde_json::to_vec` of the same Value took "
            "4.4 µs. JSONB encode is **13× slower** than JSON text encode. "
            "And the output is 1.6× larger.\n\n"
            "Reading the jsonb 0.5 serializer made the cause obvious. "
            "`ObjectSerializer::serialize_value` allocates a fresh "
            "`Serializer` (with its own `Vec<u8>`) for every map value, "
            "materializes each child as an intermediate `OwnedJsonb`, then "
            "concatenates them in `ObjectBuilder::build → to_vec → "
            "buffer.append`. Two extra allocations and one memcpy per "
            "field. It also wraps every scalar inside a container with a "
            "4-byte `SCALAR_CONTAINER_TAG` — the cause of the 40% bloat.\n\n"
            "Fix is a one-line route change in `codec::encode_doc`: "
            "instead of `jsonb::to_owned_jsonb(value)`, do "
            "`serde_json::to_writer(...)` followed by "
            "`jsonb::parse_owned_jsonb_standard_mode(text)`. The output is "
            "still a valid `OwnedJsonb` and decodes through the same paths. "
            "Large-doc encode drops from 61 µs to 20 µs (3.0×); 50-event "
            "array drops from 48 µs to 12 µs (3.9×). The on-disk image "
            "shrinks 30–58% depending on shape. Even the WAL entries are "
            "smaller — same test, 20 inserts: WAL goes from 3821 B to "
            "1459 B, a 62% reduction with no other code change.\n\n"
            "Strict mode (not extended) because `serde_json` always emits "
            "strict JSON — no NaN, no Infinity, no leading plus signs to "
            "accommodate."
        ),
        "created_at": _stamp(1),
    },
    # ──────────────────────────────────────────────────────────────────
    # 2
    {
        "title": "from 4 minutes to 200 ms — fixing create_collection contention",
        "slug": "from-4-minutes-to-200-ms-create-collection",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "A scale-bench tenant-creation workload — 1000 collections, "
            "eight workers in parallel — hung for four and a half minutes "
            "before producing any throughput at all. CPU was idle. The "
            "p99 for a single `create_collection` call was 6.25 seconds.\n\n"
            "Stack trace pointed at the engine-wide `RwLock<HashMap>` that "
            "guards the collection map. Every call held the write lock "
            "for the entire `BTreeCollection::open` path, including disk "
            "I/O — touching the directory, reading the existing .btree if "
            "any, building the empty B-tree image, fsync'ing it into "
            "place. While that ran, every other worker waited.\n\n"
            "The fix mirrors what `get_or_create_collection` already did: "
            "a read-check, then a lock-free `open` off the hot path, then "
            "a short write lock with a race-loser recheck. If two workers "
            "race to create the same collection, the loser opens the disk "
            "image, then drops it on the floor when the recheck shows the "
            "winner's entry is already in the map.\n\n"
            "Same bench, after:\n\n"
            "  Phase 1 wall:        4m18s → 205 ms  (~1260× speedup)\n"
            "  CreateCollection p99: 6.25s → 1 ms\n\n"
            "We pushed harder: 10,000 collections, patched binary, same "
            "eight workers. p99 11 ms. RSS 1.24 GiB. ListCollections p99 "
            "22 ms. At that point the limit is the user's directory "
            "inodes, not OxiDB. This puts collection-prefix multi-tenancy "
            "on the table as a real strategy: one collection per "
            "tenant, indexed and isolated, with create-cost on the order "
            "of a single `fs::rename`."
        ),
        "created_at": _stamp(2),
    },
    # ──────────────────────────────────────────────────────────────────
    # 3
    {
        "title": "ACID hardening — atomic persist, WAL fsync, graceful shutdown",
        "slug": "acid-hardening-atomic-persist-wal-fsync",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "Durability used to be 'mostly, probably'. Now it's 'ack means "
            "on disk', verified by two crash-test harnesses.\n\n"
            "The persist path was `fs::write` — not atomic, not durable. "
            "It got rewritten to: write a `.tmp` file, fsync its data, "
            "atomic rename onto the canonical name, fsync the parent "
            "directory. A per-collection mutex serializes concurrent "
            "commits so two writers can't both `truncate(true).open()` "
            "the same tmp file and mangle each other's bytes.\n\n"
            "`OXIDB_LAZY_SYNC` defaulted to true. Strict mode now fsyncs "
            "every commit; the env flag is still there for benchmark "
            "configurations that explicitly want lazy. The tx-commit "
            "path used to no-op `log_wal_batch` on the B-tree backend — "
            "now durability runs through WAL fsync at commit, not a "
            "synchronous full-image persist. `sync_writes` no longer "
            "truncates the WAL inline; the truncate moved to a new "
            "`final_checkpoint` that runs at shutdown only. The old "
            "inline truncate was racy with concurrent writers and lost "
            "about three of two thousand acks under load.\n\n"
            "Graceful shutdown: `oxidb-server` installs a SIGTERM/SIGINT "
            "handler that calls `OxiDb::shutdown` (flush + final "
            "checkpoint) before `process::exit`. SIGPIPE is ignored — "
            "otherwise a dropped client kills the worker mid-write.\n\n"
            "Two new harnesses verify it:\n\n"
            "  tests/crash-recovery-go:  2000 inserts → SIGKILL → reopen.\n"
            "                            Every acked write must survive.\n"
            "                            No `.btree.tmp` leftovers.\n"
            "  tests/atomicity-go:       Three scenarios — pre-commit\n"
            "                            SIGKILL, post-commit SIGKILL,\n"
            "                            mid-tx SIGTERM. Two collections\n"
            "                            with a foreign-key link, must\n"
            "                            recover all-or-nothing.\n\n"
            "Both green. Multi-collection transactions actually buy you "
            "atomicity now, not just isolation hopefully."
        ),
        "created_at": _stamp(3),
    },
    # ──────────────────────────────────────────────────────────────────
    # 4
    {
        "title": "S3 without the IAM dance — a blob bucket inside the engine",
        "slug": "s3-without-the-iam-dance",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "The blog images on this site are stored exactly the same way "
            "an S3 bucket stores objects, except the bucket is a directory "
            "inside the OxiDB data dir and the API is a Python function "
            "call instead of an AWS SDK.\n\n"
            "Layout per object is two files in `_blobs/<bucket>/`:\n\n"
            "  <key>.data    raw payload (optionally zstd-compressed)\n"
            "  <key>.meta    JSON: content-type, etag, metadata, size\n\n"
            "Etag is the CRC32 of the data. List/Head/Get/Put/Delete all "
            "exist on the same `OxiDb` handle as the document API. "
            "Buckets are top-level, like S3 — `create_bucket(name)`, "
            "`list_buckets()`, `delete_bucket(name)`.\n\n"
            "The S3 wire side speaks the AWS HTTP API when `OXIDB_S3_PORT` "
            "is set. Recent additions:\n\n"
            "* **aws-chunked decode.** AWS CLI and boto3 send streaming "
            "  PUTs with `content-encoding: aws-chunked` or "
            "  `x-amz-content-sha256: STREAMING-*`. Without decoding, the "
            "  chunk-size headers were ending up in the stored object. "
            "  The framing is now stripped back to the original payload, "
            "  in both single PUT and multipart upload paths. Missing "
            "  trailers and partial reads are tolerated.\n\n"
            "* **Skip zstd for already-compressed types.** Re-compressing "
            "  image/video/audio buys nothing while costing CPU on every "
            "  Put and Get. The encoder now detects the content-type "
            "  prefix and stores raw bytes for those. Decode stays "
            "  forward-compatible with legacy zstd-stored blobs.\n\n"
            "* **SSE.** Optional AES-256-GCM at rest, with both SSE-S3 "
            "  (server-managed key) and SSE-C (per-request key) styles. "
            "  `OXIDB_S3_DEFAULT_ENCRYPTION=true` encrypts every object "
            "  without the client having to know.\n\n"
            "For this blog the embedded mode is enough — no S3 port, just "
            "`db.put_object('blog-images', key, data, content_type=...)` "
            "from the Django view that handles uploads."
        ),
        "created_at": _stamp(4),
    },
    # ──────────────────────────────────────────────────────────────────
    # 5
    {
        "title": "BM25 + 18 stemmers — ranking that doesn't lie",
        "slug": "bm25-18-stemmers",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "Full-text search ranking moved from TF-IDF to BM25. The "
            "old scorer told you what words appeared, not which "
            "documents were actually relevant. Long documents with "
            "high raw term frequency consistently outranked short, "
            "focused ones — the classic TF-IDF failure mode.\n\n"
            "BM25 fixes both:\n\n"
            "* **Length normalization** — long docs no longer outrank "
            "  short ones at the same TF.\n"
            "* **TF saturation** — 10× the term frequency does not give "
            "  10× the score. The k1 parameter controls how fast scoring "
            "  saturates; Lucene/Elasticsearch defaults k1=1.2, b=0.75 "
            "  ship by default. Tunable per-process via "
            "  `OXIDB_FTS_K1` and `OXIDB_FTS_B`.\n\n"
            "Migration is lazy. An `_fts/index.json` written by an older "
            "binary is missing the `total_term_count` BM25 needs — it's "
            "backfilled on first open, no rebuild required.\n\n"
            "Eighteen Snowball stemmers ship with the engine: English, "
            "Turkish, German, French, Spanish, Italian, Portuguese, "
            "Russian, Dutch, Danish, Finnish, Hungarian, Norwegian, "
            "Romanian, Greek, Arabic, Swedish, Tamil. Pick one via "
            "`OXIDB_FTS_LANG=tr` and `kitap`, `kitaplar`, and `kitaplarda` "
            "all collapse to the same stem — the analyzer is cached "
            "per-process via `OnceLock`, no overhead in the hot path.\n\n"
            "Highlights ride on the same tokenizer, so a query for "
            "`running` correctly marks `runs` in the response. "
            "`text_search_highlighted` returns `<mark>matched</mark>` "
            "snippets with offsets and matched-term counts. Multi-byte "
            "safe — char-boundary snapping prevents panics on Turkish "
            "and CJK input.\n\n"
            "Indexing itself runs on a parallel worker pool — `FtsDispatcher` "
            "round-robins extraction jobs across N workers, with try_send "
            "fallback to blocking send so one slow worker (think OCR on a "
            "200-page PDF) doesn't backpressure the entire pool. "
            "Persistence is batched via `OXIDB_FTS_FLUSH_INTERVAL_MS` "
            "(default 1000 ms), which killed the previous N² disk-write "
            "amplification on bulk ingestion."
        ),
        "created_at": _stamp(5),
    },
    # ──────────────────────────────────────────────────────────────────
    # 6
    {
        "title": "OCC transactions across collections — prepare, validate, commit",
        "slug": "occ-transactions-across-collections",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "Transactions are optimistic and three-phase. No locks held "
            "between operations, no deadlocks because there's nothing to "
            "deadlock on — readers and writers never block each other "
            "until commit time.\n\n"
            "API:\n\n"
            "  tx_id = db.begin_tx()\n"
            "  db.tx_insert(tx_id, 'users', {'name': 'Alice'})\n"
            "  db.tx_update(tx_id, 'wallets', {'user': 'Alice'}, {'$inc': {'bal': 100}})\n"
            "  db.commit_tx(tx_id)\n\n"
            "Phase 1 (prepare): writes are buffered into a transaction-local "
            "scratch space. Each write records the version of the document "
            "it observed. Reads inside the transaction route through "
            "`tx_find`, which serves from the scratch space when there's "
            "a pending change for that key and falls back to the live "
            "store otherwise — so the transaction sees its own writes.\n\n"
            "Phase 2 (validate): commit takes a sorted lock on every "
            "collection touched by the transaction. Sorting is alphabetical "
            "via `BTreeSet`, which keeps lock acquisition deadlock-free "
            "across concurrent multi-collection commits — they'll always "
            "grab in the same order. With the locks held, the validator "
            "walks each buffered write and compares observed version "
            "against the current version on the live row. Any mismatch "
            "raises `TransactionConflictError` — the client retries.\n\n"
            "Phase 3 (commit): the buffered writes are applied to live "
            "state, the version map is bumped, indexes get updated, the "
            "WAL gets a batch entry, and the locks are released.\n\n"
            "Recent change: `tx_insert` now returns the assigned doc id "
            "from phase 1, so the client can wire that id into sibling "
            "writes inside the same transaction (the DMS upload path "
            "needs this for the version row's `document_id` FK). The "
            "embedded FFI in this blog picked that up in v0.25.1.\n\n"
            "Recovery on startup replays the transaction log first, then "
            "the WAL. The atomicity-go test harness verifies all-or-nothing "
            "behavior across two linked collections under three crash "
            "scenarios — pre-commit SIGKILL, post-commit SIGKILL, mid-tx "
            "SIGTERM. All three recover cleanly."
        ),
        "created_at": _stamp(6),
    },
    # ──────────────────────────────────────────────────────────────────
    # 7
    {
        "title": "indexes — single-field, composite, unique, TTL, and when each pays",
        "slug": "indexes-single-composite-unique-ttl",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "Indexes are `BTreeMap<IndexValue, BTreeSet<DocumentId>>` "
            "behind a `RwLock`. That structure carries the whole feature "
            "set:\n\n"
            "**Single-field.** Point lookup is `O(log n)` map probe + "
            "`O(1)` set head. The bigger win is index-backed sort: a "
            "`find` with `sort:{created_at:-1}` iterates the BTreeMap "
            "in reverse, which is `O(limit)` instead of the `O(n log n)` "
            "full scan you'd need without one. That's how the archive "
            "page on this blog returns in ~100 µs regardless of how "
            "many posts you've written.\n\n"
            "**Composite.** Multi-field B-tree where each tuple is "
            "`(field1, field2, ...)`. Prefix scans work — `{city: \"X\", "
            "age: {$gte: 18}}` on a `(city, age)` index hits a tight "
            "range. The order of fields in the index matters; the order "
            "in the query doesn't.\n\n"
            "**Unique.** Same shape as single-field but inserts that "
            "would create a duplicate raise `Error::Unique` at write "
            "time — before the doc lands. The admins collection in this "
            "blog uses one on `username` so two workers can't seed the "
            "same default admin during a race.\n\n"
            "**TTL.** A regular index plus an `expireAfterSeconds` knob. "
            "A background scanner walks the index by date order — TTL "
            "documents are always dated — and deletes anything past the "
            "expiry. Index-backed scanning means it doesn't touch live "
            "rows; it only loads the ones it's about to delete. The "
            "SQL surface is `CREATE TTL INDEX ... EXPIRE AFTER N`.\n\n"
            "**Index-only count.** When a query is fully satisfiable by "
            "an index (no projection needed, no post-filter operators), "
            "`count` returns the set size without touching documents at "
            "all. A million-document collection with a covering index "
            "returns `count({active: true})` in microseconds.\n\n"
            "**Cross-type ordering.** `IndexValue` enforces a stable "
            "total order across types: Null < Bool < Number < DateTime "
            "< String. Date strings (ISO 8601, RFC 3339, YYYY-MM-DD) "
            "are auto-detected and stored as epoch milliseconds — so "
            "comparison on a date field is integer comparison, not "
            "lexicographic string comparison. That alone moved time-range "
            "queries from 'unusable' to 'fastest path in the system'."
        ),
        "created_at": _stamp(7),
    },
    # ──────────────────────────────────────────────────────────────────
    # 8
    {
        "title": "running OxiDB inside this gunicorn worker",
        "slug": "running-oxidb-inside-this-gunicorn-worker",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "This whole blog is one process. Gunicorn worker, eight "
            "threads, Django on top, OxiDB linked in as a shared library "
            "and called via ctypes. There is no oxidb-server running "
            "next to it. There is no SQLite next to it. Every `find`, "
            "every `put_object`, every `count` is a function call inside "
            "the same address space.\n\n"
            "The wrapping is small. `oxidb-embedded-ffi` is a Rust crate "
            "that exports five C symbols: `oxidb_open`, "
            "`oxidb_open_encrypted`, `oxidb_close`, `oxidb_execute`, "
            "`oxidb_free_string`. The Python package "
            "(`oxidb-embedded`, on PyPI) is a single file of ctypes that "
            "binds those symbols and ships the `.dylib`/`.so`/`.dll` "
            "inside the wheel.\n\n"
            "The wire format on the FFI side is the same length-prefixed "
            "JSON envelope the TCP server speaks — `{\"cmd\": \"insert\", "
            "\"collection\": \"posts\", \"doc\": {...}}` in, "
            "`{\"ok\": true, \"data\": {\"id\": 7}}` out. So the embedded "
            "client and the TCP client share their dispatch tables; new "
            "commands ship to both at once.\n\n"
            "What it gives you:\n\n"
            "* Zero IPC. No socket setup per request.\n"
            "* One process to crash-recover, one log to tail.\n"
            "* Deploys with the app — `tar czf` your data dir, scp it.\n\n"
            "What it costs:\n\n"
            "* One process per data dir, no exceptions. The B-tree, the "
            "  WAL, the doc cache, and the FTS workers are all in-process "
            "  state. Two processes opening the same dir race on writes "
            "  and corrupt the tree.\n\n"
            "For Django that translates to one rule: `gunicorn "
            "--workers 1 --threads N`. The Rust engine is internally "
            "thread-safe — per-collection RwLocks, scc::HashMap for "
            "lock-free bucket-level concurrency — so threads inside the "
            "one process are the right concurrency knob. The `bin/start.sh` "
            "in this repo runs `gunicorn --workers 1 --threads 8` and gets "
            "comfortable four-digit concurrent reads/sec on a laptop.\n\n"
            "If you actually need multiple OS processes serving an OxiDB, "
            "boot `oxidb-server` and use the pure-Python `oxidb` TCP "
            "client. Different example, same API surface."
        ),
        "created_at": _stamp(8),
    },
    # ──────────────────────────────────────────────────────────────────
    # 9
    {
        "title": "why JSONB and not JSON text",
        "slug": "why-jsonb-and-not-json-text",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "OxiDB stores documents as JSONB — Postgres-style binary "
            "JSON — not as JSON text. That choice pays off in exactly "
            "one place and it pays a *lot*: partial extraction.\n\n"
            "JSONB has a header section that's an offset table. Field "
            "lookups jump straight to the value's offset without parsing "
            "the rest of the document. JSON text can't do this — the "
            "parser has no way to know where field N lives without "
            "scanning past fields 0 through N-1.\n\n"
            "Concretely, on a realistic 13 KB document with 50 nested "
            "events plus scalars and arrays:\n\n"
            "  serde_json::from_slice → Value (full decode):    27 µs\n"
            "  jsonb full decode → Value:                       27 µs\n"
            "  jsonb partial extract → IndexValue:              90 ns\n\n"
            "Three hundred times faster, doc-size-independent. That's "
            "what index build, sort-key extraction, group-key extraction, "
            "and `$eq`/`$gt`/`$exists` predicate evaluation all run on "
            "now. The hot path of the query engine never touches the "
            "Value tree.\n\n"
            "The decode codec auto-detects: if `bytes[0]` is `{` (0x7B) "
            "or `[` (0x5B), it's JSON text and goes through "
            "`serde_json::from_slice`. Otherwise it's JSONB binary and "
            "goes through `jsonb::RawJsonb`. That lets us read legacy "
            "JSON-text `.dat` files written by older binaries without a "
            "migration step.\n\n"
            "Custom deserializer? I tried it. Profiled a "
            "`from_raw_jsonb_to_index_value` that skips serde entirely. "
            "Came out **3–5% slower**, not faster. The reason: "
            "`get_by_keypath` is the dominant cost (~90 ns), and the "
            "scalar dispatch after it is so cheap that the serde "
            "Deserialize indirection doesn't even register. The lesson: "
            "measure before you write a custom anything. The bench code "
            "is in `examples/profile_decode.rs` in the repo, runs in "
            "five seconds, deletes opinions."
        ),
        "created_at": _stamp(9),
    },
    # ──────────────────────────────────────────────────────────────────
    # 10
    {
        "title": "OxiMem — a Redis wire on the same engine",
        "slug": "oximem-a-redis-wire-on-the-same-engine",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "OxiMem speaks the Redis RESP protocol on a separate port "
            "(`OXIDB_OXIMEM_PORT`) and shares process memory with the "
            "document engine. `redis-cli`, `node-redis`, `go-redis`, "
            "every Redis client just works. No translation shim.\n\n"
            "What's implemented: strings (GET/SET/INCR/EXPIRE), hashes "
            "(HSET/HGET/HDEL), sorted sets (ZADD/ZRANGEBYSCORE/ZINCRBY "
            "— full O(log N) skip-list semantics), lists, sets, pub/sub "
            "(SUBSCRIBE/PUBLISH/PSUBSCRIBE), Lua-free server-side eval "
            "via OxiScript. RESP2 and RESP3. The store lives in a "
            "single struct (`OxiMemStore`) inside `oxidb-server` — same "
            "binary, no extra service.\n\n"
            "**The cross-protocol trick.** MQTT v3.1.1 (`OXIDB_MQTT_PORT`) "
            "shares pub/sub channels with OxiMem. An MQTT PUBLISH on "
            "`sensors/temp` is received by a redis-cli `SUBSCRIBE "
            "sensors/temp` — and vice versa. One protocol topology, two "
            "wire formats, zero forwarding overhead.\n\n"
            "**SQL mirror.** Set `OXIDB_OXIMEM_SQL=true` and writes "
            "shadow-publish into OxiDB collections. `SET user:42 "
            "{json...}` lands as a document in `_kv_user` and shows up "
            "to the SQL parser. So you can dashboard your hot cache with "
            "`SELECT count(*) FROM _kv_user WHERE active=true`. The KV "
            "side stays the source of truth; the doc side is read-only.\n\n"
            "Benchmark notes: single-cmd OxiMem hits 93–101% of Redis "
            "8.6 throughput; pipelined writes beat Redis on the same "
            "hardware (one fewer fsync per batch). Bench script under "
            "`tests/comparison-redis/`."
        ),
        "created_at": _stamp(10),
    },
    # ──────────────────────────────────────────────────────────────────
    # 11
    {
        "title": "aggregation pipeline — $match, $group, $sort, $dateHistogram, $percentile",
        "slug": "aggregation-pipeline-deep-dive",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "The aggregation surface is MongoDB-style — same operator "
            "names, same input/output shape, same `db.aggregate(coll, "
            "[stages...])` API. Stages execute as an iterator chain so "
            "intermediate results never need to fit in memory.\n\n"
            "Stages implemented today: `$match`, `$group`, `$sort`, "
            "`$skip`, `$limit`, `$project`, `$count`, `$unwind`, "
            "`$addFields`, `$lookup`, `$dateHistogram`, `$percentile`. "
            "Group accumulators: `$sum`, `$avg`, `$min`, `$max`, "
            "`$count`, `$push`, `$addToSet`, `$first`, `$last`, plus "
            "`$percentile` with exact linear interpolation over the "
            "full input.\n\n"
            "**$dateHistogram** buckets a date field by interval and "
            "applies accumulators per bucket. Intervals: `Ns`/`Nm`/"
            "`Nh`/`Nd`/`Nw` (fixed-width) or `1M`/`1y` (calendar). "
            "`min_doc_count: 0` fills empty buckets between observed "
            "min and max — emitted as a synthetic `$group` chain.\n\n"
            "**Index-backed $sort** when the sort field is indexed: "
            "the pipeline iterates the BTreeMap directly instead of "
            "collecting everything and sorting. `O(limit)` for "
            "`$sort + $limit`, no `O(n log n)` price.\n\n"
            "**Index-backed $group** when the group key is indexed: "
            "buckets are read off the index in key order. The accumulator "
            "doesn't need a hash map.\n\n"
            "Aggregation is where embedded mode shines hardest. The "
            "stages are Rust functions hitting in-process state. No "
            "network round-trip per stage, no serialization between "
            "them. A four-stage pipeline on 100K docs returns in tens "
            "of milliseconds on a laptop."
        ),
        "created_at": _stamp(11),
    },
    # ──────────────────────────────────────────────────────────────────
    # 12
    {
        "title": "REST + JWT + per-document security rules (Firebase, but smaller)",
        "slug": "rest-jwt-security-rules-firebase-but-smaller",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "Set `OXIDB_HTTP_PORT` and OxiDB exposes a REST surface for "
            "CRUD, SQL, aggregation, procedures, and blob ops. JSON "
            "in, JSON out, CORS, 64-thread pool. The whole Firebase "
            "experience without the Google account.\n\n"
            "**JWT auth.** `OXIDB_JWT_SECRET` flips on `/auth/signup`, "
            "`/auth/login`, `/auth/verify`. HMAC-SHA256 tokens with the "
            "`{sub, exp, iat}` claims that everybody expects. Passwords "
            "go through Argon2id; the user docs land in the `_auth_users` "
            "collection. Verify endpoint returns the claims so a "
            "front-end can short-circuit a re-auth round-trip.\n\n"
            "**Security rules** live in `_security_rules` and run on "
            "every read/write. Each rule is a small expression — "
            "`auth.username == doc.owner`, `doc.public == true && "
            "request.method == 'read'`. The evaluator is hand-rolled "
            "OxiScript, sandboxed (no I/O, no allocations beyond the "
            "rule), and per-request cached so the same query against "
            "100 docs evaluates the rule once.\n\n"
            "**TTL indexes** with `expireAfterSeconds` let you write "
            "Firebase-style ephemeral docs (session tokens, OTPs) "
            "directly. SQL surface: `CREATE TTL INDEX ... EXPIRE "
            "AFTER N`. Background scanner walks the date-ordered "
            "index and deletes from the top.\n\n"
            "**WebSocket on the side.** `OXIDB_WS_PORT` exposes the "
            "same change-stream the JS SDK consumes. RFC 6455 with "
            "no extension negotiation, so any client works.\n\n"
            "JS SDK: `npm i oxidb`. Zero deps. Auth + CRUD + SQL + "
            "aggregation + onSnapshot, browser AND Node."
        ),
        "created_at": _stamp(12),
    },
    # ──────────────────────────────────────────────────────────────────
    # 13
    {
        "title": "change streams — onSnapshot without polling",
        "slug": "change-streams-onsnapshot-without-polling",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "Every successful write — insert, update, delete, "
            "`commit_tx` — emits a `ChangeEvent` to a process-wide "
            "broker. Subscribers receive a stream of events filtered by "
            "collection, document id, or a small predicate. No polling, "
            "no `LISTEN/NOTIFY` ceremony, no Postgres trigger.\n\n"
            "The broker is a single `tokio::sync::broadcast` channel "
            "wrapped in a per-collection filter map. New subscriber "
            "→ `subscribe(filter)` returns a `WatchHandle` that holds "
            "an mpsc receiver. Drop the handle, the slot frees. "
            "Internal capacity is a ring; if a subscriber falls "
            "behind, it gets a `Lagged(n)` event and resubscribes from "
            "the new head. No silent missed events.\n\n"
            "**Resume tokens.** Each event carries an opaque `resume_id`. "
            "Pass it back to `watch(filter, resume_from=...)` to pick "
            "up where you left off after a process restart. The token "
            "is a monotonically increasing 64-bit counter stamped at "
            "WAL append time, so it survives crash recovery.\n\n"
            "**Wire surface.** Three transports, same event shape:\n\n"
            "* WebSocket — JS SDK consumes this. Subscribe-by-collection, "
            "  multiplex on one socket.\n"
            "* Server protocol `watch` op — Python / Go / .NET clients.\n"
            "* In-process — Swift embedded mode (`addMutationObserver`) "
            "  and the Python embedded wrapper observe directly via the "
            "  same broker, no socket required.\n\n"
            "Event types: `insert`, `insert_many`, `update`, `delete`, "
            "`commit_tx`. Each carries the collection, the doc id, the "
            "timestamp, the before/after fragment (for updates), and a "
            "metadata bag where the SDK can pin per-watch state."
        ),
        "created_at": _stamp(13),
    },
    # ──────────────────────────────────────────────────────────────────
    # 14
    {
        "title": "GELF log ingestion — auto-index, retention, alerts",
        "slug": "gelf-log-ingestion-auto-index-retention-alerts",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "OxiDB doubles as a log destination. Set "
            "`OXIDB_GELF_PORT=12201` and it speaks GELF UDP. Graylog "
            "clients, Docker's `gelf` log driver, fluent-bit's gelf "
            "plugin — all just point and stream. Auto-decompresses "
            "chunked + gzipped GELF.\n\n"
            "**Auto-indexing.** Incoming records get scanned for keys "
            "across the first N messages (`OXIDB_GELF_INDEX_FIRST`). "
            "Any field that appears in ≥ 50% of those messages gets a "
            "single-field index. New keys discovered later trigger an "
            "incremental index build in the background — no rebuild, no "
            "rescan. So queries like `find _gelf_logs where level=4 "
            "and host=...` are O(log n) from the moment they're "
            "useful.\n\n"
            "**Retention.** TTL index on `_gelf_logs.timestamp` with "
            "`expireAfterSeconds=$OXIDB_GELF_RETENTION` does the "
            "delete-old-stuff job. Background scanner walks the index "
            "by date, deletes from the head. No cron, no separate "
            "pruner.\n\n"
            "**Alerting.** `OXIDB_ALERT_INTERVAL=15` runs the alert "
            "evaluator every 15 s. Rules live in `_alerts`. Each rule "
            "is a query + a threshold + a webhook URL. The evaluator "
            "uses the same indexes the log queries do, so a rule like "
            "`count(_gelf_logs, level <= 3, last 60s) > 50` is a single "
            "index range scan.\n\n"
            "**Companion CLI.** `oxidb-tail` (workspace crate) is a "
            "ratatui-based TUI that subscribes to the GELF collection's "
            "change stream and renders a colorized live tail. Looks "
            "like `lnav` but talks to OxiDB directly."
        ),
        "created_at": _stamp(14),
    },
    # ──────────────────────────────────────────────────────────────────
    # 15
    {
        "title": "encryption at rest — AES-256-GCM in the storage layer",
        "slug": "encryption-at-rest-aes-256-gcm-storage",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "Pass an `EncryptionKey` to `OxiDb::open_with_options` (or "
            "the env var `OXIDB_ENCRYPTION_KEY=/path/to/keyfile`) and "
            "every byte that leaves the in-memory representation is "
            "encrypted before it hits the disk. AES-256-GCM, per-write "
            "random 96-bit nonce, authenticated, no chained mode "
            "surprises.\n\n"
            "**Where it sits.** The crypto layer wraps `BTreeStorage` "
            "and the blob store. `persist()` encrypts the serialized "
            "image; `open()` decrypts on load. WAL entries are encrypted "
            "individually so a partial write doesn't poison the whole "
            "image. Indexes (the BTreeMap structures) live in memory "
            "only, get rebuilt from the encrypted store on restart — "
            "so an attacker with disk access never sees the structure "
            "either.\n\n"
            "**Key handling.** The key is a 32-byte file. Mode is 0o600 "
            "or we refuse to open. No key derivation step — feed it the "
            "raw entropy, mark it as such in your secret manager. KMS "
            "integration is a small layer above (encrypt the key file "
            "with KMS, decrypt at boot) that we don't ship in the "
            "engine.\n\n"
            "**Operational.** Encrypted and unencrypted data dirs are "
            "wire-incompatible — you can't mix. Backup is `tar czf` of "
            "the data dir plus a separate, never-on-disk-together copy "
            "of the key. To rotate: re-open with new key flag, persist, "
            "swap in. (We're working on a non-blocking rotation; not "
            "shipped yet.)\n\n"
            "**S3.** `OXIDB_S3_ENCRYPTION_KEY` enables SSE-S3 for the "
            "S3-compatible bucket API — same AES-256-GCM, server-managed "
            "key. SSE-C also implemented for per-request key clients."
        ),
        "created_at": _stamp(15),
    },
    # ──────────────────────────────────────────────────────────────────
    # 16
    {
        "title": "WebAssembly — OxiDB in the browser tab",
        "slug": "webassembly-oxidb-in-the-browser-tab",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "`oxidb-wasm` is a workspace crate that compiles the core "
            "engine to `wasm32-unknown-unknown` via `wasm-bindgen`. The "
            "output is a 1.4 MB `.wasm` file plus a tiny JS shim. Drop "
            "it into a `<script type=\"module\">`, get a working "
            "document database in the page.\n\n"
            "**API.** `init()`, `insert()`, `find()`, `update()`, "
            "`delete()`, `count()`, `sql()`, `aggregate()`. Same JSON "
            "shapes as the server protocol — the wasm side is the same "
            "Rust handler. No new surface to learn.\n\n"
            "**In-memory only.** WASM has no filesystem and no threads "
            "(yet — wasm-threads spec exists, browser support uneven). "
            "Native deps that assume both (`memmap2`, `rayon`, `zstd`, "
            "etc.) sit behind a target-specific `[dependencies]` block; "
            "the wasm build never sees them. `parking_lot` swaps to "
            "`spin` for locks via `src/locks.rs`.\n\n"
            "**Use cases.** Offline-first PWAs (data in IndexedDB if "
            "you want persistence, mirrored through OxiDB's API). "
            "Demo sites that don't need a server. Browser-side "
            "interview tools. The wasm-pack output ships as an npm "
            "package candidate (not on npm yet); local example at "
            "`oxidb-wasm/example/index.html`.\n\n"
            "**Build.** `cd oxidb-wasm && ./build.sh` → drops "
            "`pkg/oxidb_wasm_bg.wasm`, `oxidb_wasm.js`, types. "
            "Roughly 4 s on a warm cache. The build feature-flags out "
            "anything that needs a syscall, so cargo cycles between "
            "native and wasm targets without conflicts."
        ),
        "created_at": _stamp(16),
    },
    # ──────────────────────────────────────────────────────────────────
    # 17
    {
        "title": "Raft replication — persistent log_store, no membership amnesia",
        "slug": "raft-replication-persistent-log-store",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "OxiDB-server speaks Raft for replication. Set "
            "`OXIDB_NODE_ID=<num>` plus `OXIDB_RAFT_PEERS=\"1=host1:"
            "4445,2=host2:4445,3=host3:4445\"` and the node joins a "
            "quorum. Leader handles writes; followers tail the log. "
            "Reads can be served from any node with `read_index` for "
            "linearizability or stale-OK from a follower if you ask.\n\n"
            "**The log_store rewrite.** Raft state used to be in "
            "memory only. Nodes lost cluster membership on restart — "
            "rejoined as a fresh peer, ran a fresh election, and "
            "occasionally split-brained while the operator stitched "
            "things back together. The recent fix moved it to disk:\n\n"
            "  raft_log.jsonl    append-only, one Entry per line.\n"
            "                    O(1) per append, no full-file rewrite.\n"
            "  raft_meta.json    small, vote + committed + state_machine.\n"
            "                    Rewritten on each metadata update — a\n"
            "                    few hundred bytes, cheap.\n\n"
            "**Result.** A node restart picks up its term, its vote, "
            "and its log offset from disk and rejoins without an "
            "election storm. The 1M-record load test under failover "
            "(which we previously failed) now averages 44K rec/s with "
            "leader kills every 30s — log catch-up is fast because the "
            "follower already knows what it has.\n\n"
            "**Membership changes.** Joint consensus, not unsafe "
            "single-server change. Add/remove nodes via `raft_admin` "
            "ops on the leader; the cluster catches up the new member "
            "before promoting it to voter.\n\n"
            "Limitation worth flagging: no log compaction yet. The "
            "jsonl grows until manually checkpointed. On the roadmap."
        ),
        "created_at": _stamp(17),
    },
    # ──────────────────────────────────────────────────────────────────
    # 18
    {
        "title": "OxiScript — stored procedures and a cron scheduler",
        "slug": "oxiscript-stored-procedures-and-cron",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "OxiScript is a small embedded language for server-side "
            "logic. Looks like a stripped-down JavaScript with database "
            "primitives — `db.find`, `db.insert`, `db.put_object`, "
            "control flow, arithmetic, string ops. No I/O outside the "
            "engine, no allocations beyond what the script holds, no "
            "infinite loops (step counter caps execution).\n\n"
            "**Stored procedures.** `CREATE PROCEDURE recharge_wallet "
            "(user_id, amount) AS ...`. The body is OxiScript, "
            "compiled to a tiny bytecode, persisted in `_procedures`, "
            "callable from any client (`db.run_procedure(name, args)`) "
            "or via SQL. Procedures run in-process at FFI speed — a "
            "10-statement procedure is microseconds, not milliseconds.\n\n"
            "**Why a custom language?** Lua would have worked. So would "
            "deno_core or boa. Both pull in ~5–10 MB of runtime "
            "infrastructure and the security model becomes 'sandbox a "
            "general-purpose language', which is hard. OxiScript is "
            "~2K LOC and has no syscalls to sandbox — the design choice "
            "is the security boundary.\n\n"
            "**Cron scheduler.** `CREATE SCHEDULE rotate_caches CRON "
            "'0 */6 * * *' AS rotate_caches(...)`. The scheduler runs "
            "in a background thread, reads `_schedules` at startup, "
            "wakes on each cron-tick, invokes the procedure. Misfire "
            "handling: skipped runs are not retroactively executed (we "
            "do scheduled jobs, not at-least-once delivery).\n\n"
            "**Patterns documented.** `examples/oxiscript/` ships "
            "a few — banking ledger (atomic transfer), audit log "
            "(append-only with rule check), inventory (oversell guard), "
            "leaderboard (sorted set wrapper), rate limiter (sliding "
            "window). All copy-pasteable."
        ),
        "created_at": _stamp(18),
    },
    # ──────────────────────────────────────────────────────────────────
    # 19 — oldest
    {
        "title": "oxipool — a sharded connection pooler for OxiDB",
        "slug": "oxipool-sharded-connection-pooler",
        "author": ADMIN,
        "image_key": None,
        "body": (
            "PgBouncer for OxiDB. Single binary, written in Tokio. "
            "Accepts client connections on one port, multiplexes them "
            "onto a smaller pool of backend connections to one or more "
            "oxidb-server nodes. Two modes: pooling (route any client "
            "to any free backend) and sharded (hash the doc id, pick "
            "the right backend).\n\n"
            "**Hash sharding.** `OXIPOOL_BACKENDS=\"1=host1:4444,2=host2:"
            "4444,3=host3:4444\"`, `OXIPOOL_SHARD_KEY=\"_id\"` and you "
            "get consistent-hashing routing. Each shard runs its own "
            "OxiDB. Cross-shard queries (find without a shard-key "
            "predicate) fan out + merge; same-shard queries stay on a "
            "single backend with full transaction support.\n\n"
            "**The recent merge bug.** Found by the 100K load test: "
            "stale-pool conn to a freshly-restarted follower returned "
            "an error → router silently dropped that shard's results "
            "→ `count(*)` returned ~2/3 of actual rows. The fix added "
            "`first_partial_error` to every merge — `merge_counts`, "
            "`merge_doc_arrays`, `merge_modified` — so the router "
            "fails fast with the offending shard's message instead of "
            "silently producing wrong answers.\n\n"
            "**Health checks.** Background pinger drops dead backends "
            "out of the pool; replacements re-handshake when they come "
            "back. A bounded `Ping` timeout (2s) on pool checkout "
            "catches half-dead TCP connections that didn't get a clean "
            "FIN — without it, you'd wait the OS keepalive interval "
            "(~2h on macOS/Linux) before noticing.\n\n"
            "**oxipool itself is a workspace crate** — same repo, same "
            "release cycle. Latest tag is 0.25.3."
        ),
        "created_at": _stamp(19),
    },
]
