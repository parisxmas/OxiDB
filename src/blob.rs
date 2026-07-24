use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, RwLock};

use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};

use crate::crypto::EncryptionKey;
use crate::error::{Error, Result};

/// Highest blob meta JSON schema version this engine knows how to
/// read. Bump (and add a migration path) whenever a backwards-
/// incompatible meta field is introduced. Specced in
/// `docs/format/blob-object.md` (the "Versioning" section).
pub const CURRENT_BLOB_META_VERSION: u32 = 1;

/// Serde default for `ObjectMeta::format_version` — used when
/// deserialising a meta file written before the field existed.
/// Per the format spec, absence means legacy ⇒ treat as v1.
fn default_blob_meta_version() -> u32 {
    1
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ObjectMeta {
    pub key: String,
    pub bucket: String,
    pub size: u64,
    pub content_type: String,
    pub etag: String,
    pub created_at: String,
    pub metadata: HashMap<String, String>,
    /// Codec used to compress the on-disk `.data` payload, if any.
    /// `None` (or the field's absence in older meta files) means the
    /// payload is stored as-is. Only `"zstd"` is currently emitted.
    /// Decompression on read is keyed off this field, so the format
    /// is forward-compatible: old uncompressed blobs keep working
    /// after the upgrade because their meta defaults this to None.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_compression: Option<String>,
    /// Bytes the `.data` file actually consumes on disk (post
    /// compression / encryption). `None` for blobs written before
    /// this field existed; readers should fall back to `size`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stored_size: Option<u64>,
    /// JSON schema version of this meta file. Newly-written metas
    /// always carry the current version constant; metas written
    /// before this field existed parse cleanly thanks to `default`
    /// and are treated as version 1 (specced in
    /// `docs/format/blob-object.md`). Readers REFUSE versions newer
    /// than they know about — a forward-compatibility tripwire so a
    /// newer engine writing semantics-changing fields can't
    /// silently corrupt data when read by an older binary.
    #[serde(default = "default_blob_meta_version")]
    pub format_version: u32,
}

/// Returns true for content types that are already entropy-saturated
/// (image/audio/video, PDF, ZIP-based Office docs, etc). Re-compressing
/// these wastes CPU on every put + get for at most a few percent of
/// savings, so the compressor short-circuits before invoking zstd.
fn is_already_compressed(content_type: &str) -> bool {
    let ct = content_type.to_ascii_lowercase();
    if ct.starts_with("image/") || ct.starts_with("video/") || ct.starts_with("audio/") {
        return true;
    }
    if ct.starts_with("application/vnd.openxmlformats-officedocument.")
        || ct.starts_with("application/vnd.oasis.opendocument.")
    {
        return true;
    }
    matches!(
        ct.as_str(),
        "application/pdf"
            | "application/zip"
            | "application/x-zip-compressed"
            | "application/gzip"
            | "application/x-gzip"
            | "application/x-bzip2"
            | "application/x-xz"
            | "application/x-7z-compressed"
            | "application/x-rar-compressed"
            | "application/x-tar"
            | "application/zstd"
    )
}

/// Try to zstd-compress a payload. Keeps the compressed bytes only when
/// they save at least 5% — otherwise the original is returned and the
/// per-blob compression label stays None. Files under 256 bytes always
/// pass through (zstd framing alone overshoots small payloads), and
/// content types known to be pre-compressed short-circuit so we don't
/// waste CPU re-encoding entropy.
fn try_compress_zstd(data: &[u8], content_type: &str) -> (Vec<u8>, Option<&'static str>) {
    const MIN_SIZE: usize = 256;
    if data.len() < MIN_SIZE || is_already_compressed(content_type) {
        return (data.to_vec(), None);
    }
    match zstd::stream::encode_all(data, 3) {
        Ok(c) if c.len() * 100 < data.len() * 95 => (c, Some("zstd")),
        _ => (data.to_vec(), None),
    }
}

/// Decompress a payload according to the codec recorded on the meta.
/// Unknown codecs surface as an error so silent corruption is
/// impossible if a future version writes a new label.
fn decompress(payload: Vec<u8>, codec: Option<&str>) -> Result<Vec<u8>> {
    match codec {
        None => Ok(payload),
        Some("zstd") => zstd::stream::decode_all(payload.as_slice())
            .map_err(|e| Error::Io(std::io::Error::other(format!("zstd decode: {e}")))),
        Some(other) => Err(Error::Io(std::io::Error::other(format!(
            "unknown storage_compression codec: {other}"
        )))),
    }
}

/// Write `bytes` to `path`, replacing any existing file. When `sync` is
/// set, fsync the file before returning so the contents survive a crash —
/// `std::fs::write` alone leaves them sitting in the page cache.
fn write_file(path: &Path, bytes: &[u8], sync: bool) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)?;
    f.write_all(bytes)?;
    if sync {
        f.sync_all()?;
    }
    Ok(())
}

/// fsync a directory so a prior `rename` (or create/unlink) of an entry
/// inside it is durable. POSIX semantics: on Unix a directory opens as a
/// read-only `File` whose `sync_all` flushes the directory inode.
fn fsync_dir(path: &Path) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}

/// A request to the group-commit directory syncer: "fsync `dir`, then
/// signal `reply`". The error is carried as a `String` because one
/// fsync result fans out to every waiter in a batch and `io::Error`
/// is not `Clone`.
struct DirSyncReq {
    dir: PathBuf,
    reply: mpsc::SyncSender<std::result::Result<(), String>>,
}

/// Group-commit fsync for blob directories. Mirrors the `tx_log`
/// committer: a dedicated thread owns the fsync, callers submit a
/// request and block until a fsync that *started after* their request
/// was enqueued completes. Because a caller always `rename`s before it
/// enqueues, any request the syncer sees in its queue corresponds to a
/// rename that already happened — so a single `fsync` per directory
/// makes the whole batch durable. N concurrent durable puts to one
/// bucket therefore cost one directory fsync, not N.
struct DirSyncer {
    tx: mpsc::Sender<DirSyncReq>,
}

impl DirSyncer {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel::<DirSyncReq>();
        std::thread::Builder::new()
            .name("blob-dir-sync".into())
            .spawn(move || Self::run(rx))
            .expect("failed to spawn blob-dir-sync thread");
        Self { tx }
    }

    fn run(rx: mpsc::Receiver<DirSyncReq>) {
        // Cap a single batch so a flood of writers can't starve the
        // fsync indefinitely; matches the tx_log committer's bound.
        const MAX_BATCH: usize = 512;
        while let Ok(first) = rx.recv() {
            let mut batch = vec![first];
            while batch.len() < MAX_BATCH {
                match rx.try_recv() {
                    Ok(req) => batch.push(req),
                    Err(_) => break,
                }
            }
            // One fsync per distinct directory in the batch, then fan
            // the result out to every waiter that asked for that dir.
            let mut done: HashMap<PathBuf, std::result::Result<(), String>> = HashMap::new();
            for req in &batch {
                done.entry(req.dir.clone())
                    .or_insert_with(|| fsync_dir(&req.dir).map_err(|e| e.to_string()));
            }
            for req in batch {
                let result = done.get(&req.dir).cloned().unwrap_or(Ok(()));
                let _ = req.reply.send(result);
            }
        }
    }

    /// Block until `dir` has been fsynced by a batch that started after
    /// this call enqueued its request.
    fn sync(&self, dir: &Path) -> Result<()> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.tx
            .send(DirSyncReq {
                dir: dir.to_path_buf(),
                reply: reply_tx,
            })
            .map_err(|_| Error::Io(std::io::Error::other("blob dir-syncer thread is gone")))?;
        match reply_rx.recv() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(msg)) => Err(Error::Io(std::io::Error::other(msg))),
            Err(_) => Err(Error::Io(std::io::Error::other(
                "blob dir-syncer dropped the reply",
            ))),
        }
    }
}

struct BucketState {
    keys: HashMap<String, u64>,
    metas: HashMap<u64, ObjectMeta>,
    next_id: u64,
}

pub struct BlobStore {
    base_dir: PathBuf,
    /// Per-bucket locking: the outer RwLock protects the bucket map (create/delete bucket),
    /// inner RwLock protects individual bucket state (object CRUD).
    buckets: RwLock<HashMap<String, Arc<RwLock<BucketState>>>>,
    encryption: Option<Arc<EncryptionKey>>,
    /// Opt-in zstd compression for blob payloads. Read path always
    /// honors `meta.storage_compression`, so disabling this after
    /// blobs were written compressed still works — only new uploads
    /// are affected. Toggled via `OXIDB_BLOB_COMPRESS=1` at startup.
    compression: bool,
    /// Opt-in per-put fsync. When set, `put_object` and `delete_object`
    /// fsync their writes (payload, meta, and the bucket directory)
    /// before returning, so a successful write is durable on disk — not
    /// merely scheduled for the 1 Hz background flush. The directory
    /// fsyncs are group-committed across concurrent writers (see
    /// `DirSyncer`). Enable it when a caller treats a successful write
    /// as a commit (e.g. an SMTP server that must not `250 OK` a mail it
    /// could still lose to a power cut). Toggled via `OXIDB_BLOB_SYNC=1`.
    sync_writes: bool,
    /// Group-commit directory syncer, present iff `sync_writes` is set.
    dir_syncer: Option<DirSyncer>,
    delete_tx: mpsc::Sender<PathBuf>,
}

impl BlobStore {
    pub fn open(data_dir: &Path) -> Result<Self> {
        Self::open_with_encryption(data_dir, None)
    }

    pub fn open_with_encryption(
        data_dir: &Path,
        encryption: Option<Arc<EncryptionKey>>,
    ) -> Result<Self> {
        let base_dir = data_dir.join("_blobs");
        std::fs::create_dir_all(&base_dir)?;

        let mut buckets: HashMap<String, Arc<RwLock<BucketState>>> = HashMap::new();

        if base_dir.exists() {
            for entry in std::fs::read_dir(&base_dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    let bucket_name = entry.file_name().to_string_lossy().to_string();
                    let state = Self::scan_bucket(&entry.path(), &encryption)?;
                    buckets.insert(bucket_name, Arc::new(RwLock::new(state)));
                }
            }
        }

        // Background thread for async file deletion
        let (delete_tx, delete_rx) = mpsc::channel::<PathBuf>();
        std::thread::Builder::new()
            .name("blob-gc".into())
            .spawn(move || {
                for path in delete_rx {
                    let _ = std::fs::remove_file(&path);
                }
            })
            .expect("failed to spawn blob-gc thread");

        // Background thread for periodic fsync. This is the best-effort
        // durability path for deployments that leave `OXIDB_BLOB_SYNC` off;
        // when it's on, `put`/`delete` are already durable on return and
        // this thread is just a redundant safety net.
        //
        // A rename/unlink inside a bucket dir is only durable once *that
        // bucket dir* is fsynced — fsyncing the `_blobs` root does NOT
        // flush its subdirectories. So each tick we fsync the root (which
        // catches bucket create/delete) AND every bucket dir under it.
        {
            let root = base_dir.clone();
            std::thread::Builder::new()
                .name("blob-sync".into())
                .spawn(move || {
                    loop {
                        std::thread::sleep(std::time::Duration::from_secs(1));
                        let _ = fsync_dir(&root);
                        if let Ok(entries) = std::fs::read_dir(&root) {
                            for entry in entries.flatten() {
                                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                                    let _ = fsync_dir(&entry.path());
                                }
                            }
                        }
                    }
                })
                .expect("failed to spawn blob-sync thread");
        }

        let env_flag = |name: &str| {
            std::env::var(name)
                .ok()
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on"))
                .unwrap_or(false)
        };
        let compression = env_flag("OXIDB_BLOB_COMPRESS");
        let sync_writes = env_flag("OXIDB_BLOB_SYNC");
        let dir_syncer = sync_writes.then(DirSyncer::new);

        Ok(Self {
            base_dir,
            buckets: RwLock::new(buckets),
            encryption,
            compression,
            sync_writes,
            dir_syncer,
            delete_tx,
        })
    }

    /// Override the per-put fsync setting, ignoring `OXIDB_BLOB_SYNC`.
    /// Builder-style so embedders can configure durability in code
    /// rather than through the environment:
    /// `BlobStore::open(dir)?.with_sync_writes(true)`.
    pub fn with_sync_writes(mut self, sync: bool) -> Self {
        self.sync_writes = sync;
        if sync && self.dir_syncer.is_none() {
            self.dir_syncer = Some(DirSyncer::new());
        }
        self
    }

    /// Make a prior rename/unlink inside `dir` durable. When the
    /// group-commit syncer is running the fsync is batched with other
    /// concurrent durable writes; the `None` arm is unreachable while
    /// `sync_writes` is set but falls back to a direct fsync rather than
    /// silently skipping durability.
    fn sync_dir(&self, dir: &Path) -> Result<()> {
        match &self.dir_syncer {
            Some(s) => s.sync(dir),
            None => fsync_dir(dir).map_err(Error::Io),
        }
    }

    fn scan_bucket(
        bucket_path: &Path,
        encryption: &Option<Arc<EncryptionKey>>,
    ) -> Result<BucketState> {
        let mut keys = HashMap::new();
        let mut metas = HashMap::new();
        let mut max_id: u64 = 0;
        let mut valid_ids = std::collections::HashSet::new();

        // First pass: load all .meta files (the source of truth)
        for entry in std::fs::read_dir(bucket_path)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(id_str) = name.strip_suffix(".meta") {
                if let Ok(id) = id_str.parse::<u64>() {
                    let raw_meta = std::fs::read(entry.path())?;
                    // A single torn/undecryptable .meta (crash mid-write in
                    // non-sync mode, bit rot) must not brick the whole
                    // database open. Quarantine it and keep going — only
                    // that one object becomes unavailable.
                    let meta_bytes = match encryption {
                        Some(key) => match key.decrypt(&raw_meta) {
                            Ok(b) => b,
                            Err(e) => {
                                eprintln!(
                                    "[blob] {}: undecryptable meta ({e}); quarantining",
                                    entry.path().display()
                                );
                                let _ = std::fs::rename(
                                    entry.path(),
                                    entry.path().with_extension("meta.corrupt"),
                                );
                                // Keep the .data file out of the orphan sweep —
                                // the payload may still be recoverable — and
                                // keep the id allocated so a future put can't
                                // reuse it and overwrite the payload.
                                valid_ids.insert(id);
                                if id >= max_id {
                                    max_id = id + 1;
                                }
                                continue;
                            }
                        },
                        None => raw_meta,
                    };
                    let mut meta: ObjectMeta = match serde_json::from_slice(&meta_bytes) {
                        Ok(m) => m,
                        Err(e) => {
                            eprintln!(
                                "[blob] {}: corrupt meta ({e}); quarantining",
                                entry.path().display()
                            );
                            let _ = std::fs::rename(
                                entry.path(),
                                entry.path().with_extension("meta.corrupt"),
                            );
                            // Keep the .data file out of the orphan sweep —
                            // the payload may still be recoverable — and keep
                            // the id allocated so a future put can't reuse it
                            // and overwrite the quarantined payload.
                            valid_ids.insert(id);
                            if id >= max_id {
                                max_id = id + 1;
                            }
                            continue;
                        }
                    };
                    // Forward-compat tripwire: refuse meta files
                    // written by a future engine with a schema
                    // version we don't recognise. `serde(default)`
                    // backfills v1 for legacy metas missing the
                    // field, so this only fires on metas that
                    // explicitly declare a higher version.
                    if meta.format_version > CURRENT_BLOB_META_VERSION {
                        return Err(Error::IncompatibleFormat(format!(
                            "blob meta for id {id} has format_version={} which \
                             is newer than this engine supports (max {}). \
                             Refusing to open to prevent silent corruption.",
                            meta.format_version, CURRENT_BLOB_META_VERSION
                        )));
                    }
                    // Backfill `stored_size` for blobs written before
                    // the field existed. We stat the matching .data
                    // file rather than rewriting the on-disk meta.
                    // The cached value is enough for head_object /
                    // API consumers; persistence catches up the next
                    // time the blob is rewritten.
                    if meta.stored_size.is_none() {
                        let data_path = bucket_path.join(format!("{id}.data"));
                        if let Ok(md) = std::fs::metadata(&data_path) {
                            meta.stored_size = Some(md.len());
                        }
                    }
                    keys.insert(meta.key.clone(), id);
                    metas.insert(id, meta);
                    valid_ids.insert(id);
                    if id >= max_id {
                        max_id = id + 1;
                    }
                }
            }
        }

        // Second pass: clean orphan .data and .tmp files (crash recovery)
        let mut orphans = 0;
        for entry in std::fs::read_dir(bucket_path)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            // Clean up temp files from interrupted writes
            if name.ends_with(".tmp") {
                let _ = std::fs::remove_file(entry.path());
                orphans += 1;
            }
            // Clean orphan .data files (no matching .meta)
            else if let Some(id_str) = name.strip_suffix(".data") {
                if let Ok(id) = id_str.parse::<u64>() {
                    if !valid_ids.contains(&id) {
                        let _ = std::fs::remove_file(entry.path());
                        orphans += 1;
                    }
                }
            }
        }
        if orphans > 0 {
            eprintln!(
                "[blob] cleaned {orphans} orphan/temp files from {}",
                bucket_path.display()
            );
        }

        Ok(BucketState {
            keys,
            metas,
            next_id: max_id,
        })
    }

    fn bucket_path(&self, bucket: &str) -> PathBuf {
        self.base_dir.join(bucket)
    }

    fn data_path(&self, bucket: &str, id: u64) -> PathBuf {
        self.base_dir.join(bucket).join(format!("{}.data", id))
    }

    fn meta_path(&self, bucket: &str, id: u64) -> PathBuf {
        self.base_dir.join(bucket).join(format!("{}.meta", id))
    }

    fn validate_bucket_name(name: &str) -> Result<()> {
        if name.is_empty() || name.len() > 63 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "bucket name must be 1-63 characters",
            )));
        }
        if name.contains("..") || name.contains('/') || name.contains('\\') || name.contains('\0') {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "bucket name contains invalid characters",
            )));
        }
        // "." passes every check above yet resolves to the _blobs root
        // itself: create_bucket(".") registers the root as a bucket and
        // delete_bucket(".") would remove_dir_all the ENTIRE blob store.
        // S3-style: require the name to start and end alphanumeric.
        if name == "."
            || !name.as_bytes()[0].is_ascii_alphanumeric()
            || !name.as_bytes()[name.len() - 1].is_ascii_alphanumeric()
        {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "bucket name must start and end with an alphanumeric character",
            )));
        }
        if !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.' || b == b'_')
        {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "bucket name must be alphanumeric, hyphens, dots, or underscores",
            )));
        }
        Ok(())
    }

    pub fn create_bucket(&self, name: &str) -> Result<()> {
        Self::validate_bucket_name(name)?;
        std::fs::create_dir_all(self.bucket_path(name))?;
        let mut buckets = self.buckets.write().unwrap();
        buckets.entry(name.to_string()).or_insert_with(|| {
            Arc::new(RwLock::new(BucketState {
                keys: HashMap::new(),
                metas: HashMap::new(),
                next_id: 0,
            }))
        });
        Ok(())
    }

    pub fn list_buckets(&self) -> Vec<String> {
        let map = self.buckets.read().unwrap();
        let mut names: Vec<String> = map.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn delete_bucket(&self, name: &str) -> Result<()> {
        let path = {
            let mut map = self.buckets.write().unwrap();
            if !map.contains_key(name) {
                return Err(Error::BucketNotFound(name.to_string()));
            }
            map.remove(name);
            self.bucket_path(name)
        }; // outer lock released before disk I/O
        if path.exists() {
            std::fs::remove_dir_all(path)?;
        }
        Ok(())
    }

    pub fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: HashMap<String, String>,
    ) -> Result<ObjectMeta> {
        self.put_object_with_etag(bucket, key, data, content_type, metadata, None)
    }

    /// `put_object`, but the caller supplies the ETag.
    ///
    /// Only multipart needs this. S3 does not define a completed multipart
    /// object's ETag as the MD5 of the object — it is the MD5 of the
    /// concatenated part MD5s, suffixed `-<partcount>`, and that is not
    /// recoverable from the assembled bytes once the part boundaries are gone.
    /// The tag has to persist, not just be returned: clients keep it from
    /// CompleteMultipartUpload and send it back as `If-Match`.
    pub fn put_object_with_etag(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        content_type: &str,
        metadata: HashMap<String, String>,
        etag_override: Option<String>,
    ) -> Result<ObjectMeta> {
        Self::validate_bucket_name(bucket)?;
        // Auto-create bucket if it doesn't exist
        std::fs::create_dir_all(self.bucket_path(bucket))?;

        // Get or create the per-bucket lock
        let bucket_lock = {
            let mut map = self.buckets.write().unwrap();
            map.entry(bucket.to_string())
                .or_insert_with(|| {
                    Arc::new(RwLock::new(BucketState {
                        keys: HashMap::new(),
                        metas: HashMap::new(),
                        next_id: 0,
                    }))
                })
                .clone()
        }; // outer lock released — only this bucket is locked below

        // Phase 1: expensive work outside any lock (hash, encrypt, write temp files)
        //
        // S3 defines a single-part object's ETag as the hex MD5 of its bytes, and
        // real clients enforce that: the AWS SDK for .NET re-computes the MD5 of
        // what it sent and throws when the response disagrees. This used to be the
        // first 16 bytes of a SHA-256, which is the same SHAPE as an MD5 — 32 hex
        // characters — so clients could not tell it apart and simply concluded the
        // upload had corrupted. A tag that looks like an MD5 and isn't is worse
        // than one that looks like neither.
        //
        // MD5 is broken for signatures; an ETag is not one. It identifies bytes.
        // Callers who want an integrity guarantee want a checksum, not this.
        let etag: String = etag_override.unwrap_or_else(|| {
            Md5::digest(data)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect()
        });
        let created_at = now_rfc3339();

        // Compression first (so encryption — if any — operates on the
        // smaller payload; encrypted bytes have near-maximum entropy
        // and won't compress at all).
        let (compressed_data, codec) = if self.compression {
            try_compress_zstd(data, content_type)
        } else {
            (data.to_vec(), None)
        };

        let data_to_write = match &self.encryption {
            Some(key) => key.encrypt(&compressed_data)?,
            None => compressed_data,
        };

        let meta = ObjectMeta {
            key: key.to_string(),
            bucket: bucket.to_string(),
            size: data.len() as u64,
            content_type: content_type.to_string(),
            etag,
            created_at,
            metadata,
            storage_compression: codec.map(|s| s.to_string()),
            stored_size: Some(data_to_write.len() as u64),
            format_version: CURRENT_BLOB_META_VERSION,
        };
        let meta_json = serde_json::to_vec(&meta)?;
        let meta_to_write = match &self.encryption {
            Some(key) => key.encrypt(&meta_json)?,
            None => meta_json,
        };

        // Write to temp files with random names (no lock needed — unique per
        // call). With `sync_writes` the temp files are fsynced here, so by the
        // time they're renamed into place their contents are already durable.
        let tmp_id = rand::random::<u64>();
        let bucket_dir = self.bucket_path(bucket);
        let data_tmp = bucket_dir.join(format!("{tmp_id}.data.tmp"));
        let meta_tmp = bucket_dir.join(format!("{tmp_id}.meta.tmp"));
        write_file(&data_tmp, &data_to_write, self.sync_writes)?;
        write_file(&meta_tmp, &meta_to_write, self.sync_writes)?;

        // Phase 2a: brief lock to allocate / look up the id. We DON'T
        // hold the lock during the renames — concurrent puts to the
        // same bucket would otherwise serialize on fs::rename, and
        // under load that's the dominant tail-latency contributor
        // (observed ~900ms p50 on 32-way concurrent put workloads).
        let id = {
            let mut state = bucket_lock.write().unwrap();
            if let Some(&existing_id) = state.keys.get(key) {
                existing_id
            } else {
                let id = state.next_id;
                state.next_id += 1;
                id
            }
        };

        // Phase 2b: renames outside any lock. Two distinct concurrent
        // writers to the SAME key would race here, but that case is
        // effectively impossible for content-addressed callers (the
        // key is the etag (an MD5 of the data) → identical bytes → identical
        // result). Mismatched keys can't collide on id because the
        // counter increment under the lock above guarantees uniqueness.
        //
        // Crash-recovery ordering (see `scan_bucket`): `.meta` is the
        // source of truth and an orphan `.data` is swept on the next
        // open, but a `.meta` with no `.data` is a ghost read. So the
        // `.data` rename must be durable *before* the `.meta` rename.
        // With `sync_writes` we fsync the bucket dir between the two
        // renames to enforce that order; the second fsync is the
        // commit point — once it returns the put is durable.
        let data_path = self.data_path(bucket, id);
        let meta_path = self.meta_path(bucket, id);
        std::fs::rename(&data_tmp, &data_path)?;
        if self.sync_writes {
            self.sync_dir(&bucket_dir)?;
        }
        std::fs::rename(&meta_tmp, &meta_path)?;
        if self.sync_writes {
            self.sync_dir(&bucket_dir)?;
        }

        // Phase 2c: commit hashmap entries under a brief lock.
        {
            let mut state = bucket_lock.write().unwrap();
            state.keys.insert(key.to_string(), id);
            state.metas.insert(id, meta.clone());
        }

        // Durability: with `sync_writes` (OXIDB_BLOB_SYNC=1) a successful
        // return means the payload and meta are fsynced to disk — a caller
        // may treat the put as a commit. The two directory fsyncs are
        // group-committed (see `DirSyncer`), so concurrent puts to the same
        // bucket share fsyncs instead of each paying its own. Otherwise
        // rename() is only metadata-atomic and the bytes reach disk via the
        // 1 Hz background sync thread (or the kernel's normal writeback).

        Ok(meta)
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> Result<(Vec<u8>, ObjectMeta)> {
        let (id, cached_meta, bucket_lock) = {
            let map = self.buckets.read().unwrap();
            let bl = map
                .get(bucket)
                .ok_or_else(|| Error::BucketNotFound(bucket.to_string()))?
                .clone();
            let state = bl.read().unwrap();
            let &id = state.keys.get(key).ok_or_else(|| Error::BlobNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;
            (id, state.metas.get(&id).cloned(), bl.clone())
        }; // all locks released before disk I/O

        let raw_data = std::fs::read(self.data_path(bucket, id))?;
        let decrypted = match &self.encryption {
            Some(key) => key.decrypt(&raw_data)?,
            None => raw_data,
        };

        let meta = if let Some(m) = cached_meta {
            m
        } else {
            let raw_meta = std::fs::read(self.meta_path(bucket, id))?;
            let meta_bytes = match &self.encryption {
                Some(key) => key.decrypt(&raw_meta)?,
                None => raw_meta,
            };
            let m: ObjectMeta = serde_json::from_slice(&meta_bytes)?;
            // Backfill cache
            if let Ok(mut state) = bucket_lock.write() {
                state.metas.insert(id, m.clone());
            }
            m
        };

        // Read path is never gated on `self.compression`. Even if the
        // operator turned the feature off after this blob was written,
        // the meta still tells us how to reverse the transformation.
        let data = decompress(decrypted, meta.storage_compression.as_deref())?;

        Ok((data, meta))
    }

    pub fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMeta> {
        let (id, cached, bucket_lock) = {
            let map = self.buckets.read().unwrap();
            let bl = map
                .get(bucket)
                .ok_or_else(|| Error::BucketNotFound(bucket.to_string()))?
                .clone();
            let state = bl.read().unwrap();
            let &id = state.keys.get(key).ok_or_else(|| Error::BlobNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;
            (id, state.metas.get(&id).cloned(), bl.clone())
        };

        if let Some(meta) = cached {
            return Ok(meta);
        }

        let raw_meta = std::fs::read(self.meta_path(bucket, id))?;
        let meta_bytes = match &self.encryption {
            Some(key) => key.decrypt(&raw_meta)?,
            None => raw_meta,
        };
        let meta: ObjectMeta = serde_json::from_slice(&meta_bytes)?;
        if let Ok(mut state) = bucket_lock.write() {
            state.metas.insert(id, meta.clone());
        }
        Ok(meta)
    }

    pub fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        // Hold bucket write lock only for in-memory removal
        let (data_path, meta_path) = {
            let map = self.buckets.read().unwrap();
            let bl = map
                .get(bucket)
                .ok_or_else(|| Error::BucketNotFound(bucket.to_string()))?
                .clone();
            let mut state = bl.write().unwrap();
            let id = state.keys.remove(key).ok_or_else(|| Error::BlobNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;
            state.metas.remove(&id);
            (self.data_path(bucket, id), self.meta_path(bucket, id))
        }; // all locks released here

        // Delete .meta synchronously — this is the commit point for scan_bucket.
        // On crash recovery, absent .meta = object is deleted (no ghost reads).
        // With `sync_writes` we fsync the bucket dir after the unlink so the
        // deletion itself is durable: otherwise a crash could resurrect a
        // "deleted" object when scan_bucket sees the .meta reappear.
        match std::fs::remove_file(&meta_path) {
            Ok(()) => {}
            // Already gone — that's the desired end state, treat as success.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            // In durable mode a real I/O error must not be reported as a
            // successful delete; non-sync mode keeps its prior best-effort
            // behavior (the in-memory state is already updated either way).
            Err(e) if self.sync_writes => return Err(Error::Io(e)),
            Err(_) => {}
        }
        if self.sync_writes {
            self.sync_dir(&self.bucket_path(bucket))?;
        }
        // Defer .data deletion to background thread (large file, slow I/O).
        // An orphan .data with no .meta is harmless — scan_bucket sweeps it.
        let _ = self.delete_tx.send(data_path);

        Ok(())
    }

    pub fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<ObjectMeta>> {
        let map = self.buckets.read().unwrap();
        let bl = map
            .get(bucket)
            .ok_or_else(|| Error::BucketNotFound(bucket.to_string()))?
            .clone();
        let state = bl.read().unwrap();

        let mut matching_keys: Vec<(&String, &u64)> = state
            .keys
            .iter()
            .filter(|(k, _)| match prefix {
                Some(p) => k.starts_with(p),
                None => true,
            })
            .collect();

        matching_keys.sort_by(|a, b| a.0.cmp(b.0));

        let limit = limit.unwrap_or(1000);
        let mut results = Vec::with_capacity(limit.min(matching_keys.len()));

        for (_, &id) in matching_keys.into_iter().take(limit) {
            if let Some(meta) = state.metas.get(&id) {
                results.push(meta.clone());
                continue;
            }
            let raw_meta = std::fs::read(self.meta_path(bucket, id))?;
            let meta_bytes = match &self.encryption {
                Some(key) => key.decrypt(&raw_meta)?,
                None => raw_meta,
            };
            let meta: ObjectMeta = serde_json::from_slice(&meta_bytes)?;
            results.push(meta);
        }

        Ok(results)
    }
}

fn now_rfc3339() -> String {
    use std::time::SystemTime;
    let dur = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let mins = (time_secs % 3600) / 60;
    let s = time_secs % 60;

    // Compute year/month/day from days since epoch
    let mut y = 1970i64;
    let mut remaining = days as i64;
    loop {
        let days_in_year = if is_leap(y) { 366 } else { 365 };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        y += 1;
    }
    let leap = is_leap(y);
    let month_days = if leap {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut m = 0;
    for (i, &md) in month_days.iter().enumerate() {
        if remaining < md as i64 {
            m = i + 1;
            break;
        }
        remaining -= md as i64;
    }
    let d = remaining + 1;
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y, m, d, hours, mins, s
    )
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> (tempfile::TempDir, BlobStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::open(dir.path()).unwrap();
        (dir, store)
    }

    #[test]
    fn create_and_list_buckets() {
        let (_dir, store) = temp_store();
        store.create_bucket("images").unwrap();
        store.create_bucket("docs").unwrap();
        let buckets = store.list_buckets();
        assert_eq!(buckets, vec!["docs", "images"]);
    }

    #[test]
    fn put_and_get_object() {
        let (_dir, store) = temp_store();
        let data = b"Hello World";
        let meta = store
            .put_object("docs", "hello.txt", data, "text/plain", HashMap::new())
            .unwrap();
        assert_eq!(meta.key, "hello.txt");
        assert_eq!(meta.bucket, "docs");
        assert_eq!(meta.size, 11);
        assert_eq!(meta.content_type, "text/plain");
        assert!(!meta.etag.is_empty());
        // S3 defines it as the hex MD5 of the bytes, and clients enforce that —
        // the AWS SDK for .NET recomputes it and throws on a mismatch.
        assert_eq!(meta.etag, format!("{:x}", Md5::digest(data)));
        assert_eq!(meta.etag.len(), 32, "an ETag is 32 hex characters");

        let (got_data, got_meta) = store.get_object("docs", "hello.txt").unwrap();
        assert_eq!(got_data, data);
        assert_eq!(got_meta.key, "hello.txt");
        assert_eq!(got_meta.etag, meta.etag);
    }

    #[test]
    fn head_object_returns_meta_only() {
        let (_dir, store) = temp_store();
        store
            .put_object("docs", "f.txt", b"abc", "text/plain", HashMap::new())
            .unwrap();
        let meta = store.head_object("docs", "f.txt").unwrap();
        assert_eq!(meta.size, 3);
    }

    #[test]
    fn delete_object_then_not_found() {
        let (_dir, store) = temp_store();
        store
            .put_object("docs", "f.txt", b"abc", "text/plain", HashMap::new())
            .unwrap();
        store.delete_object("docs", "f.txt").unwrap();
        let err = store.get_object("docs", "f.txt").unwrap_err();
        assert!(err.to_string().contains("blob not found"));
    }

    #[test]
    fn list_objects_with_prefix() {
        let (_dir, store) = temp_store();
        store
            .put_object("b", "images/a.png", b"a", "image/png", HashMap::new())
            .unwrap();
        store
            .put_object("b", "images/b.png", b"b", "image/png", HashMap::new())
            .unwrap();
        store
            .put_object("b", "docs/c.txt", b"c", "text/plain", HashMap::new())
            .unwrap();

        let list = store.list_objects("b", Some("images/"), None).unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].key, "images/a.png");
        assert_eq!(list[1].key, "images/b.png");
    }

    #[test]
    fn list_objects_sorted_by_key() {
        let (_dir, store) = temp_store();
        store
            .put_object("b", "c.txt", b"c", "text/plain", HashMap::new())
            .unwrap();
        store
            .put_object("b", "a.txt", b"a", "text/plain", HashMap::new())
            .unwrap();
        store
            .put_object("b", "b.txt", b"b", "text/plain", HashMap::new())
            .unwrap();

        let list = store.list_objects("b", None, None).unwrap();
        let keys: Vec<&str> = list.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, vec!["a.txt", "b.txt", "c.txt"]);
    }

    #[test]
    fn overwrite_existing_key_reuses_id() {
        let (_dir, store) = temp_store();
        store
            .put_object("b", "f.txt", b"v1", "text/plain", HashMap::new())
            .unwrap();
        store
            .put_object("b", "f.txt", b"v2-longer", "text/plain", HashMap::new())
            .unwrap();

        let (data, meta) = store.get_object("b", "f.txt").unwrap();
        assert_eq!(data, b"v2-longer");
        assert_eq!(meta.size, 9);

        // Only one object in the bucket
        let list = store.list_objects("b", None, None).unwrap();
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn get_from_missing_bucket() {
        let (_dir, store) = temp_store();
        let err = store.get_object("nonexistent", "f.txt").unwrap_err();
        assert!(err.to_string().contains("bucket not found"));
    }

    #[test]
    fn delete_bucket_removes_everything() {
        let (_dir, store) = temp_store();
        store
            .put_object("b", "f.txt", b"data", "text/plain", HashMap::new())
            .unwrap();
        store.delete_bucket("b").unwrap();

        let buckets = store.list_buckets();
        assert!(buckets.is_empty());

        let err = store.get_object("b", "f.txt").unwrap_err();
        assert!(err.to_string().contains("bucket not found"));
    }

    #[test]
    fn sync_writes_put_get_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::open(dir.path()).unwrap().with_sync_writes(true);
        let data = b"durable payload";
        store
            .put_object("mail", "msg-1", data, "message/rfc822", HashMap::new())
            .unwrap();

        // Visible immediately through the same store.
        let (got, meta) = store.get_object("mail", "msg-1").unwrap();
        assert_eq!(got, data);
        assert_eq!(meta.size, data.len() as u64);

        // ...and after a fresh open that rebuilds state from disk,
        // exercising the temp-write → fsync → rename → dir-fsync commit path.
        drop(store);
        let reopened = BlobStore::open(dir.path()).unwrap();
        let (got2, _) = reopened.get_object("mail", "msg-1").unwrap();
        assert_eq!(got2, data);
    }

    #[test]
    fn sync_writes_delete_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::open(dir.path()).unwrap().with_sync_writes(true);
        store
            .put_object("mail", "msg-1", b"body", "message/rfc822", HashMap::new())
            .unwrap();
        store.delete_object("mail", "msg-1").unwrap();
        assert!(store.get_object("mail", "msg-1").is_err());

        // The deletion must hold across a fresh open — the .meta unlink was
        // fsynced, so scan_bucket won't resurrect the object from a stale dir
        // entry. (A leftover .data with no .meta is swept as an orphan.)
        drop(store);
        let reopened = BlobStore::open(dir.path()).unwrap();
        assert!(reopened.get_object("mail", "msg-1").is_err());
    }

    #[test]
    fn sync_writes_group_commit_concurrent_puts() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(BlobStore::open(dir.path()).unwrap().with_sync_writes(true));

        // 32 concurrent durable puts to the same bucket — the directory
        // fsyncs are group-committed, but every put must still come back
        // durable and intact.
        let mut handles = Vec::new();
        for i in 0..32 {
            let store = Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                let body = format!("body-{i}").into_bytes();
                store
                    .put_object(
                        "mail",
                        &format!("msg-{i}"),
                        &body,
                        "message/rfc822",
                        HashMap::new(),
                    )
                    .unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        drop(store);
        let reopened = BlobStore::open(dir.path()).unwrap();
        for i in 0..32 {
            let (got, _) = reopened.get_object("mail", &format!("msg-{i}")).unwrap();
            assert_eq!(got, format!("body-{i}").into_bytes());
        }
    }

    /// New metas MUST carry the explicit format_version field on the
    /// wire so a future reader can tell at a glance what schema it
    /// is looking at. Pins both the field-name and the current value.
    #[test]
    fn new_meta_serialises_format_version() {
        let (_dir, store) = temp_store();
        store
            .put_object("b", "k", b"hello", "text/plain", HashMap::new())
            .unwrap();
        let meta_path = _dir.path().join("_blobs").join("b");
        // Find the single .meta file in the bucket dir.
        let meta_file = std::fs::read_dir(&meta_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().ends_with(".meta"))
            .expect("at least one .meta written");
        let raw = std::fs::read(meta_file.path()).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(
            parsed["format_version"], CURRENT_BLOB_META_VERSION,
            "newly-written meta must declare its schema version explicitly"
        );
    }

    /// Legacy metas written before the field existed (no
    /// `format_version` key) MUST still parse — the field is
    /// additive and defaults to v1. Open() with such metas in the
    /// bucket dir must succeed.
    #[test]
    fn legacy_meta_without_format_version_still_parses() {
        let (dir, store) = temp_store();
        store
            .put_object("b", "k", b"x", "text/plain", HashMap::new())
            .unwrap();
        let bucket_dir = dir.path().join("_blobs").join("b");
        let meta_file = std::fs::read_dir(&bucket_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().ends_with(".meta"))
            .unwrap();

        // Rewrite the meta with the format_version stripped — exactly
        // what a pre-upgrade engine would have written.
        let raw = std::fs::read(meta_file.path()).unwrap();
        let mut parsed: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        parsed.as_object_mut().unwrap().remove("format_version");
        std::fs::write(meta_file.path(), serde_json::to_vec(&parsed).unwrap()).unwrap();

        // Reopen — must succeed (default = v1).
        drop(store);
        let reopened = BlobStore::open(dir.path()).unwrap();
        let (got, _) = reopened.get_object("b", "k").unwrap();
        assert_eq!(got, b"x");
    }

    /// Forward-compat tripwire: a meta declaring a higher version
    /// MUST be refused at open(). Better to error loudly than to
    /// best-effort read fields with potentially different semantics.
    #[test]
    fn meta_with_future_version_refused_on_open() {
        let (dir, store) = temp_store();
        store
            .put_object("b", "k", b"y", "text/plain", HashMap::new())
            .unwrap();
        let bucket_dir = dir.path().join("_blobs").join("b");
        let meta_file = std::fs::read_dir(&bucket_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().ends_with(".meta"))
            .unwrap();

        // Bump the version to one past what we support.
        let raw = std::fs::read(meta_file.path()).unwrap();
        let mut parsed: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        parsed["format_version"] = serde_json::json!(CURRENT_BLOB_META_VERSION + 1);
        std::fs::write(meta_file.path(), serde_json::to_vec(&parsed).unwrap()).unwrap();

        drop(store);
        let res = BlobStore::open(dir.path());
        match res {
            Ok(_) => panic!("expected IncompatibleFormat, got Ok(_)"),
            Err(Error::IncompatibleFormat(msg)) => {
                assert!(msg.contains("format_version"), "msg was: {msg}");
                assert!(msg.contains("newer"), "msg was: {msg}");
            }
            Err(other) => panic!("expected IncompatibleFormat, got {other:?}"),
        }
    }
}
