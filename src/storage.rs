/// Location of a document in the data file.
#[derive(Debug, Clone, Copy)]
pub struct DocLocation {
    pub offset: u64,
    pub length: u32,
}

// Everything below is native-only (filesystem, compression, mmap).
#[cfg(not(target_arch = "wasm32"))]
use crate::locks::Mutex;
#[cfg(not(target_arch = "wasm32"))]
use std::borrow::Cow;
#[cfg(not(target_arch = "wasm32"))]
use std::fs::{self, File, OpenOptions};
#[cfg(not(target_arch = "wasm32"))]
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use crate::crypto::EncryptionKey;
#[cfg(not(target_arch = "wasm32"))]
use crate::error::Result;

#[cfg(not(target_arch = "wasm32"))]
const RECORD_ACTIVE: u8 = 0;
#[cfg(not(target_arch = "wasm32"))]
const RECORD_DELETED: u8 = 1;

#[cfg(not(target_arch = "wasm32"))]
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Hard ceiling on the buffer we will pre-allocate to decompress a single
/// stored record. Far above any realistic document, but bounded so a corrupt
/// or hostile zstd frame header declaring an enormous content size cannot
/// drive an out-of-memory allocation during a read.
#[cfg(not(target_arch = "wasm32"))]
const MAX_DECOMPRESSED_RECORD: usize = 1 << 30; // 1 GiB

/// Build an `InvalidData` I/O error for a corrupt/overflowing record location.
#[cfg(not(target_arch = "wasm32"))]
fn storage_corrupt(msg: &str) -> crate::error::Error {
    crate::error::Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, msg))
}
#[cfg(not(target_arch = "wasm32"))]
const LZ4_MAGIC: [u8; 4] = [0x04, 0x22, 0x4D, 0x18];
#[cfg(not(target_arch = "wasm32"))]
const ZSTD_LEVEL: i32 = 3;

/// Whether a stored payload carries a zstd/lz4 frame header — i.e. it was
/// stored compressed and must be decompressed on read. Used by the zero-copy
/// scan path to decide whether it can borrow the mmap bytes directly.
#[cfg(not(target_arch = "wasm32"))]
#[inline]
fn payload_is_compressed(data: &[u8]) -> bool {
    data.len() >= 4 && (data[..4] == ZSTD_MAGIC || data[..4] == LZ4_MAGIC)
}

#[cfg(not(target_arch = "wasm32"))]
thread_local! {
    static ZSTD_COMPRESSOR: std::cell::RefCell<zstd::bulk::Compressor<'static>> = std::cell::RefCell::new(
        zstd::bulk::Compressor::new(ZSTD_LEVEL).expect("failed to create zstd compressor")
    );
    static ZSTD_DECOMPRESSOR: std::cell::RefCell<zstd::bulk::Decompressor<'static>> = std::cell::RefCell::new(
        zstd::bulk::Decompressor::new().expect("failed to create zstd decompressor")
    );
}

#[cfg(not(target_arch = "wasm32"))]
struct StorageInner {
    file: File,
    current_offset: u64,
}

#[cfg(not(target_arch = "wasm32"))]
pub struct Storage {
    _path: PathBuf,
    inner: Mutex<StorageInner>,
    read_file: File,
    encryption: Option<Arc<EncryptionKey>>,
    /// Whether new records are zstd-compressed before writing. Default `true`.
    /// Disk-first storage can disable it (`OXIDB_DISK_UNCOMPRESSED`) to trade
    /// disk space for CPU: uncompressed records skip the per-record compress on
    /// write *and* the decompress on read, and (when unencrypted) become
    /// zero-copy reads from the mmap. Reads stay adaptive either way — records
    /// are decoded by their magic bytes — so flipping this needs no migration
    /// and mixed compressed/uncompressed files read back correctly.
    compress: bool,
    /// Memory-mapped view of the data file for zero-syscall reads.
    /// Re-mapped when the file grows significantly.
    read_mmap: parking_lot::RwLock<Option<memmap2::Mmap>>,
    /// Size of the file when mmap was last created.
    mmap_len: std::sync::atomic::AtomicU64,
}

#[cfg(not(target_arch = "wasm32"))]
impl Storage {
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_encryption(path, None)
    }

    pub fn open_with_encryption(
        path: &Path,
        encryption: Option<Arc<EncryptionKey>>,
    ) -> Result<Self> {
        Self::open_with_options(path, encryption, true)
    }

    /// Like [`open_with_encryption`](Self::open_with_encryption) but lets the
    /// caller disable zstd compression of new records (`compress = false`).
    pub fn open_with_options(
        path: &Path,
        encryption: Option<Arc<EncryptionKey>>,
        compress: bool,
    ) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;

        let current_offset = file.metadata()?.len();

        // Separate read-only handle for lockfree pread operations.
        let read_file = File::open(path)?;

        // Create initial mmap for reads (if file is non-empty)
        let mmap = if current_offset > 0 {
            unsafe { memmap2::Mmap::map(&read_file).ok() }
        } else {
            None
        };
        let mmap_len = mmap.as_ref().map_or(0, |m| m.len() as u64);

        Ok(Self {
            _path: path.to_path_buf(),
            inner: Mutex::new(StorageInner {
                file,
                current_offset,
            }),
            read_file,
            encryption,
            compress,
            read_mmap: parking_lot::RwLock::new(mmap),
            mmap_len: std::sync::atomic::AtomicU64::new(mmap_len),
        })
    }

    /// Compress with zstd using a reusable thread-local compressor.
    /// Only returns compressed form if it actually shrinks the data.
    /// No-op when compression is disabled for this store.
    fn maybe_compress<'a>(&self, data: &'a [u8]) -> Cow<'a, [u8]> {
        if !self.compress {
            return Cow::Borrowed(data);
        }
        ZSTD_COMPRESSOR.with(|c| match c.borrow_mut().compress(data) {
            Ok(compressed) if compressed.len() < data.len() => Cow::Owned(compressed),
            _ => Cow::Borrowed(data),
        })
    }

    /// Decompress if the payload starts with zstd or lz4 magic bytes.
    fn maybe_decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.len() < 4 {
            return Ok(data.to_vec());
        }

        // zstd frame
        if data[..4] == ZSTD_MAGIC {
            let exact = zstd::zstd_safe::get_frame_content_size(data);
            // The declared frame content size is attacker/corruption-
            // controlled (a single flipped byte can fake the zstd magic in
            // the data file, which has no per-record CRC). Clamp the buffer
            // hint so a bogus multi-exabyte size can't trigger an OOM
            // `Vec::with_capacity` while merely reading a record. Legit
            // records decompress fine under the ceiling; a frame that truly
            // needs more will just fail to decompress.
            let capacity = match exact {
                Ok(Some(size)) => (size as usize).min(MAX_DECOMPRESSED_RECORD),
                _ => std::cmp::max(data.len() * 16, 65536).min(MAX_DECOMPRESSED_RECORD),
            };
            return ZSTD_DECOMPRESSOR.with(|d| {
                d.borrow_mut().decompress(data, capacity).map_err(|e| {
                    crate::error::Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                })
            });
        }

        // LZ4 frame (backward compat if data was written with lz4)
        if data[..4] == LZ4_MAGIC {
            use std::io::Read as _;
            let mut decoder = lz4_flex::frame::FrameDecoder::new(data);
            let mut result = Vec::with_capacity(data.len() * 4);
            decoder.read_to_end(&mut result).map_err(|e| {
                crate::error::Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            })?;
            return Ok(result);
        }

        // Uncompressed
        Ok(data.to_vec())
    }

    /// Prepare payload for writing: compress → encrypt.
    /// Returns `Cow::Borrowed` when no transformation needed (zero-copy).
    fn prepare_payload<'a>(&self, doc_bytes: &'a [u8]) -> Result<Cow<'a, [u8]>> {
        let compressed = self.maybe_compress(doc_bytes);
        match &self.encryption {
            Some(key) => Ok(Cow::Owned(key.encrypt(&compressed)?)),
            None => Ok(compressed),
        }
    }

    /// Decode payload after reading: decrypt → decompress.
    fn decode_payload(&self, payload: &[u8]) -> Result<Vec<u8>> {
        let decrypted = match &self.encryption {
            Some(key) => key.decrypt(payload)?,
            None => return self.maybe_decompress(payload),
        };
        self.maybe_decompress(&decrypted)
    }

    /// Append a document to the data file, returns its location.
    pub fn append(&self, doc_bytes: &[u8]) -> Result<DocLocation> {
        let payload = self.prepare_payload(doc_bytes)?;
        let mut inner = self.inner.lock();
        let offset = inner.current_offset;
        let length = payload.len() as u32;

        inner.file.seek(SeekFrom::End(0))?;
        inner.file.write_all(&[RECORD_ACTIVE])?;
        inner.file.write_all(&length.to_le_bytes())?;
        inner.file.write_all(&payload)?;
        inner.file.sync_data()?;

        inner.current_offset += 1 + 4 + length as u64;

        Ok(DocLocation { offset, length })
    }

    /// Read a document's bytes from the data file.
    pub fn read(&self, loc: DocLocation) -> Result<Vec<u8>> {
        let mut inner = self.inner.lock();
        inner.file.seek(SeekFrom::Start(loc.offset + 5))?;
        let mut buf = vec![0u8; loc.length as usize];
        inner.file.read_exact(&mut buf)?;
        drop(inner);
        self.decode_payload(&buf)
    }

    /// Read a document without acquiring the Mutex.
    /// Uses mmap when available (zero syscall), falls back to pread.
    pub fn read_lockfree(&self, loc: DocLocation) -> Result<Vec<u8>> {
        // Compute the byte range in u64 with overflow checks. `loc.offset` is
        // u64 and `loc.length` u32; casting to usize first would truncate on
        // 32-bit targets and let a corrupt DocLocation wrap back inside the
        // mmap bounds, returning the wrong bytes past the `<= mmap.len()` guard.
        let data_start_u64 = loc
            .offset
            .checked_add(5)
            .ok_or_else(|| storage_corrupt("document offset overflow"))?;
        let data_end_u64 = data_start_u64
            .checked_add(loc.length as u64)
            .ok_or_else(|| storage_corrupt("document length overflow"))?;

        // Fast path: read from mmap (no syscall)
        {
            let guard = self.read_mmap.read();
            if let Some(ref mmap) = *guard {
                if data_end_u64 <= mmap.len() as u64 {
                    let data_start = data_start_u64 as usize;
                    let data_end = data_end_u64 as usize;
                    return self.decode_payload(&mmap[data_start..data_end]);
                }
            }
        }

        // Slow path: pread for data beyond mmap range (recent writes)
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            let mut buf = vec![0u8; loc.length as usize];
            self.read_file.read_at(&mut buf, loc.offset + 5)?;
            self.decode_payload(&buf)
        }
        #[cfg(not(unix))]
        {
            self.read(loc)
        }
    }

    /// Scan the payloads at `locs` **in the given order**, locking the read
    /// mmap once for the whole scan and invoking `f(index, bytes)` per record
    /// (`index` is the position in `locs`). `f` returns `false` to stop early.
    ///
    /// Two wins over a per-record `read_lockfree` loop, which is why the
    /// full-collection scan path (aggregation, unindexed find) uses this:
    ///
    /// - **Zero-copy** for records that need no decode (no encryption *and* not
    ///   compressed): `f` receives a slice **borrowed straight from the mmap**,
    ///   with no per-record `Vec` allocation or memcpy. Compressed/encrypted
    ///   records are still decoded into an owned buffer.
    /// - **Sequential access**: callers pass `locs` sorted by offset, turning
    ///   the random per-record page faults of index-order traversal into a
    ///   readahead-friendly forward sweep of the data file.
    ///
    /// Records past the live mmap (recent appends) fall back to `pread` into a
    /// reused scratch buffer.
    pub fn for_each_payload<F>(&self, locs: &[DocLocation], mut f: F) -> Result<()>
    where
        F: FnMut(usize, &[u8]) -> Result<bool>,
    {
        let guard = self.read_mmap.read();
        let mmap = guard.as_ref();
        let mmap_len = mmap.map_or(0u64, |m| m.len() as u64);
        let encrypted = self.encryption.is_some();
        let mut scratch: Vec<u8> = Vec::new();

        for (i, loc) in locs.iter().enumerate() {
            let data_start = loc
                .offset
                .checked_add(5)
                .ok_or_else(|| storage_corrupt("document offset overflow"))?;
            let data_end = data_start
                .checked_add(loc.length as u64)
                .ok_or_else(|| storage_corrupt("document length overflow"))?;

            // Fast path: data is within the mmap.
            if let Some(m) = mmap {
                if data_end <= mmap_len {
                    let raw = &m[data_start as usize..data_end as usize];
                    if !encrypted && !payload_is_compressed(raw) {
                        // Zero-copy: hand the mmap slice straight to the caller.
                        if !f(i, raw)? {
                            return Ok(());
                        }
                    } else {
                        let decoded = self.decode_payload(raw)?;
                        if !f(i, &decoded)? {
                            return Ok(());
                        }
                    }
                    continue;
                }
            }

            // Slow path: recent append beyond the mmap — pread into scratch.
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                scratch.clear();
                scratch.resize(loc.length as usize, 0);
                self.read_file.read_at(&mut scratch, loc.offset + 5)?;
                let decoded = self.decode_payload(&scratch)?;
                if !f(i, &decoded)? {
                    return Ok(());
                }
            }
            #[cfg(not(unix))]
            {
                let decoded = self.read(*loc)?;
                if !f(i, &decoded)? {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Batch-read multiple documents without the Mutex.
    /// Uses mmap when possible, falls back to pread for recent data.
    pub fn read_batch_lockfree(
        &self,
        locs: &mut [(usize, DocLocation)],
    ) -> Result<Vec<(usize, Vec<u8>)>> {
        locs.sort_unstable_by_key(|&(_, loc)| loc.offset);
        let guard = self.read_mmap.read();
        let mmap_ref = guard.as_ref();
        let mmap_len = mmap_ref.map_or(0, |m| m.len());

        let mut results = Vec::with_capacity(locs.len());
        for &(idx, loc) in locs.iter() {
            // u64 + checked math, as in `read_lockfree`, so a truncated/wrapped
            // offset can't pass the mmap-bounds check and read wrong bytes.
            let data_start_u64 = loc
                .offset
                .checked_add(5)
                .ok_or_else(|| storage_corrupt("document offset overflow"))?;
            let data_end_u64 = data_start_u64
                .checked_add(loc.length as u64)
                .ok_or_else(|| storage_corrupt("document length overflow"))?;

            let decoded = if let Some(ref mmap) = mmap_ref {
                if data_end_u64 <= mmap_len as u64 {
                    // Zero-syscall mmap read
                    self.decode_payload(&mmap[data_start_u64 as usize..data_end_u64 as usize])?
                } else {
                    self.pread_decode(loc)?
                }
            } else {
                self.pread_decode(loc)?
            };
            results.push((idx, decoded));
        }
        Ok(results)
    }

    /// Fallback pread + decode for data beyond mmap range.
    fn pread_decode(&self, loc: DocLocation) -> Result<Vec<u8>> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            let mut buf = vec![0u8; loc.length as usize];
            self.read_file.read_at(&mut buf, loc.offset + 5)?;
            self.decode_payload(&buf)
        }
        #[cfg(not(unix))]
        {
            self.read(loc)
        }
    }

    #[allow(dead_code)]
    fn read_batch_lockfree_legacy(
        &self,
        locs: &mut [(usize, DocLocation)],
    ) -> Result<Vec<(usize, Vec<u8>)>> {
        let mut inner = self.inner.lock();
        locs.sort_unstable_by_key(|&(_, loc)| loc.offset);
        let mut results = Vec::with_capacity(locs.len());
        for &(idx, loc) in locs.iter() {
            inner.file.seek(SeekFrom::Start(loc.offset + 5))?;
            let mut buf = vec![0u8; loc.length as usize];
            inner.file.read_exact(&mut buf)?;
            results.push((idx, buf));
        }
        drop(inner);
        results
            .into_iter()
            .map(|(idx, buf)| Ok((idx, self.decode_payload(&buf)?)))
            .collect()
    }

    /// Soft-delete a record by flipping its status byte.
    pub fn mark_deleted(&self, loc: DocLocation) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.file.seek(SeekFrom::Start(loc.offset))?;
        inner.file.write_all(&[RECORD_DELETED])?;
        inner.file.sync_data()?;
        Ok(())
    }

    /// Append a document without fsync (caller must call `sync()` after batch).
    pub fn append_no_sync(&self, doc_bytes: &[u8]) -> Result<DocLocation> {
        let payload = self.prepare_payload(doc_bytes)?;
        let mut inner = self.inner.lock();
        let offset = inner.current_offset;
        let length = payload.len() as u32;

        inner.file.seek(SeekFrom::End(0))?;
        inner.file.write_all(&[RECORD_ACTIVE])?;
        inner.file.write_all(&length.to_le_bytes())?;
        inner.file.write_all(&payload)?;

        inner.current_offset += 1 + 4 + length as u64;

        Ok(DocLocation { offset, length })
    }

    /// Append multiple documents without fsync, acquiring the mutex only once.
    /// Returns a location for each item. Caller must call `sync()` after.
    pub fn append_batch_no_sync(&self, items: &[&[u8]]) -> Result<Vec<DocLocation>> {
        // Pre-encrypt all items outside the lock
        let payloads: Vec<Cow<'_, [u8]>> = items
            .iter()
            .map(|doc_bytes| self.prepare_payload(doc_bytes))
            .collect::<Result<Vec<_>>>()?;

        let mut inner = self.inner.lock();
        inner.file.seek(SeekFrom::End(0))?;

        let mut locations = Vec::with_capacity(payloads.len());
        for payload in &payloads {
            let offset = inner.current_offset;
            let length = payload.len() as u32;

            inner.file.write_all(&[RECORD_ACTIVE])?;
            inner.file.write_all(&length.to_le_bytes())?;
            inner.file.write_all(payload)?;

            inner.current_offset += 1 + 4 + length as u64;
            locations.push(DocLocation { offset, length });
        }

        Ok(locations)
    }

    /// Compress + encrypt a batch of items in parallel using thread::scope.
    /// Falls back to sequential for small batches or when encryption is enabled
    /// (encryption uses per-call random nonces which are cheap, but we keep it simple).
    fn prepare_payloads_parallel(&self, items: &[&[u8]]) -> Result<Vec<Vec<u8>>> {
        const PAR_THRESHOLD: usize = 256;
        let n = items.len();

        if n < PAR_THRESHOLD || self.encryption.is_some() || !self.compress {
            // Sequential path. Also taken when compression is disabled —
            // `prepare_payload` then just copies the bytes (no zstd), so there's
            // nothing to parallelize.
            return items
                .iter()
                .map(|doc_bytes| {
                    let cow = self.prepare_payload(doc_bytes)?;
                    Ok(cow.into_owned())
                })
                .collect();
        }

        // Parallel compression (no encryption, compression enabled)
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let chunk_size = (n + cpus - 1) / cpus;

        let results: Vec<Result<Vec<Vec<u8>>>> = std::thread::scope(|s| {
            let handles: Vec<_> = items
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(|| {
                        chunk
                            .iter()
                            .map(|doc_bytes| {
                                let compressed = ZSTD_COMPRESSOR.with(|c| {
                                    match c.borrow_mut().compress(doc_bytes) {
                                        Ok(comp) if comp.len() < doc_bytes.len() => comp,
                                        _ => doc_bytes.to_vec(),
                                    }
                                });
                                Ok(compressed)
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                })
                .collect();

            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        let mut payloads = Vec::with_capacity(n);
        for chunk_result in results {
            payloads.extend(chunk_result?);
        }
        Ok(payloads)
    }

    /// Append multiple documents without fsync using a single write_all call.
    /// Builds the entire batch into one buffer, reducing syscalls from 3*N to 1.
    /// For large batches, compression is parallelized across available CPU cores.
    /// Returns a location for each item. Caller must call `sync()` after.
    pub fn append_batch_no_sync_buffered(&self, items: &[&[u8]]) -> Result<Vec<DocLocation>> {
        if items.is_empty() {
            return Ok(vec![]);
        }
        // Compress + encrypt all items outside the lock (parallel for large batches)
        let payloads = self.prepare_payloads_parallel(items)?;

        // Build a single buffer for all records (also outside the lock)
        let total_size: usize = payloads.iter().map(|p| 5 + p.len()).sum();
        let mut buf = Vec::with_capacity(total_size);
        // We'll compute offsets relative to 0, then shift by current_offset under lock
        let mut relative_locations = Vec::with_capacity(payloads.len());
        let mut rel_offset: u64 = 0;

        for payload in &payloads {
            let length = payload.len() as u32;
            buf.push(RECORD_ACTIVE);
            buf.extend_from_slice(&length.to_le_bytes());
            buf.extend_from_slice(payload);
            relative_locations.push((rel_offset, length));
            rel_offset += 1 + 4 + length as u64;
        }

        // Single lock acquisition, single write
        let mut inner = self.inner.lock();
        let base_offset = inner.current_offset;
        inner.file.seek(SeekFrom::End(0))?;
        inner.file.write_all(&buf)?;
        inner.current_offset = base_offset + rel_offset;

        let locations = relative_locations
            .into_iter()
            .map(|(rel, length)| DocLocation {
                offset: base_offset + rel,
                length,
            })
            .collect();

        Ok(locations)
    }

    /// Soft-delete without fsync (caller must call `sync()` after batch).
    pub fn mark_deleted_no_sync(&self, loc: DocLocation) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.file.seek(SeekFrom::Start(loc.offset))?;
        inner.file.write_all(&[RECORD_DELETED])?;
        Ok(())
    }

    /// Soft-delete multiple records in a single lock acquisition.
    /// Caller must call `sync()` after.
    pub fn mark_deleted_batch_no_sync(&self, locs: &[DocLocation]) -> Result<()> {
        if locs.is_empty() {
            return Ok(());
        }
        let mut inner = self.inner.lock();
        for loc in locs {
            inner.file.seek(SeekFrom::Start(loc.offset))?;
            inner.file.write_all(&[RECORD_DELETED])?;
        }
        Ok(())
    }

    /// Flush and fsync the data file.
    pub fn sync(&self) -> Result<()> {
        let inner = self.inner.lock();
        inner.file.sync_data()?;
        let current_size = inner.current_offset;
        drop(inner);
        self.remap_if_grown(current_size);
        Ok(())
    }

    /// Re-create mmap if the file has grown beyond the current mmap range.
    fn remap_if_grown(&self, current_size: u64) {
        let old_len = self.mmap_len.load(std::sync::atomic::Ordering::Relaxed);
        if current_size > old_len && current_size > 0 {
            if let Ok(new_mmap) = unsafe { memmap2::Mmap::map(&self.read_file) } {
                let new_len = new_mmap.len() as u64;
                *self.read_mmap.write() = Some(new_mmap);
                self.mmap_len
                    .store(new_len, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    /// Returns the total file size in bytes.
    pub fn file_size(&self) -> u64 {
        let inner = self.inner.lock();
        inner.current_offset
    }

    /// Returns the path this storage was opened with.
    pub fn path(&self) -> &Path {
        &self._path
    }

    /// Iterate all active records. Returns (DocLocation, plaintext_bytes) pairs.
    /// The DocLocation contains the correct on-disk payload length (which may
    /// differ from plaintext length when encryption is enabled).
    pub fn iter_active(&self) -> Result<Vec<(DocLocation, Vec<u8>)>> {
        let mut inner = self.inner.lock();
        inner.file.seek(SeekFrom::Start(0))?;
        let file_len = inner.file.metadata()?.len();
        let mut results = Vec::new();
        let mut pos = 0u64;

        while pos < file_len {
            // A torn final record (process crashed mid-append) leaves fewer
            // than a full header+payload at the tail. Treat that as a clean
            // end-of-data boundary rather than erroring out — the data file is
            // reconstructable from the WAL, so an unopenable file would be far
            // worse than dropping a never-acknowledged trailing write.
            if pos + 5 > file_len {
                break;
            }
            let mut header = [0u8; 5];
            inner.file.read_exact(&mut header)?;

            let status = header[0];
            let length = u32::from_le_bytes([header[1], header[2], header[3], header[4]]);

            if pos + 5 + length as u64 > file_len {
                break; // torn payload — stop at last valid record
            }

            if status == RECORD_ACTIVE {
                let mut data = vec![0u8; length as usize];
                inner.file.read_exact(&mut data)?;
                let plaintext = self.decode_payload(&data)?;
                results.push((
                    DocLocation {
                        offset: pos,
                        length,
                    },
                    plaintext,
                ));
            } else {
                inner.file.seek(SeekFrom::Current(length as i64))?;
            }

            pos += 5 + length as u64;
        }

        Ok(results)
    }

    /// Stream active records one at a time via callback, avoiding the large
    /// Vec allocation of `iter_active`.
    pub fn for_each_active<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(DocLocation, Vec<u8>) -> Result<()>,
    {
        let mut inner = self.inner.lock();
        inner.file.seek(SeekFrom::Start(0))?;
        let file_len = inner.file.metadata()?.len();
        let mut pos = 0u64;

        while pos < file_len {
            // Stop cleanly at a torn final record (see `iter_active`).
            if pos + 5 > file_len {
                break;
            }
            let mut header = [0u8; 5];
            inner.file.read_exact(&mut header)?;

            let status = header[0];
            let length = u32::from_le_bytes([header[1], header[2], header[3], header[4]]);

            if pos + 5 + length as u64 > file_len {
                break; // torn payload — stop at last valid record
            }

            if status == RECORD_ACTIVE {
                let mut data = vec![0u8; length as usize];
                inner.file.read_exact(&mut data)?;
                let plaintext = self.decode_payload(&data)?;
                // Drop inner lock before callback (callback may need to read storage)
                drop(inner);
                f(
                    DocLocation {
                        offset: pos,
                        length,
                    },
                    plaintext,
                )?;
                inner = self.inner.lock();
                // Re-seek to continue after this record
                inner.file.seek(SeekFrom::Start(pos + 5 + length as u64))?;
            } else {
                inner.file.seek(SeekFrom::Current(length as i64))?;
            }

            pos += 5 + length as u64;
        }

        Ok(())
    }
    /// Sequential scan using a separate read-only file handle.
    /// Does NOT hold the main mutex — other reads/writes can proceed concurrently.
    /// Uses BufReader for efficient sequential I/O (OS read-ahead).
    /// The callback receives raw (decrypted) bytes and returns Ok(true) to continue
    /// or Ok(false) to stop early.
    pub fn scan_readonly_while<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<bool>,
    {
        use std::io::BufReader;

        let file = File::open(&self._path)?;
        let file_len = file.metadata()?.len();
        let mut reader = BufReader::with_capacity(256 * 1024, file);
        let mut pos = 0u64;
        let mut buf = Vec::with_capacity(4096);

        while pos < file_len {
            // Stop cleanly at a torn final record (see `iter_active`).
            if pos + 5 > file_len {
                break;
            }
            let mut header = [0u8; 5];
            reader.read_exact(&mut header)?;
            let status = header[0];
            let length = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;

            if pos + 5 + length as u64 > file_len {
                break; // torn payload — stop at last valid record
            }

            if status == RECORD_ACTIVE {
                buf.resize(length, 0);
                reader.read_exact(&mut buf)?;
                let decoded = self.decode_payload(&buf)?;
                if !f(&decoded)? {
                    break;
                }
            } else {
                reader.seek(SeekFrom::Current(length as i64))?;
            }

            pos += 5 + length as u64;
        }

        Ok(())
    }

    /// Scan a segment [start_offset, end_offset) of the data file using its own
    /// read-only file descriptor + BufReader. Safe to call from multiple threads
    /// concurrently — each invocation opens an independent handle.
    /// The callback returns Ok(true) to continue or Ok(false) to stop early.
    pub fn scan_segment_readonly_while<F>(
        &self,
        start_offset: u64,
        end_offset: u64,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<bool>,
    {
        use std::io::BufReader;

        let file = File::open(&self._path)?;
        let mut reader = BufReader::with_capacity(256 * 1024, file);
        reader.seek(SeekFrom::Start(start_offset))?;
        let mut pos = start_offset;
        let mut buf = Vec::with_capacity(4096);

        while pos < end_offset {
            if pos + 5 > end_offset {
                break; // torn header at the segment tail
            }
            let mut header = [0u8; 5];
            if reader.read_exact(&mut header).is_err() {
                break;
            }
            let status = header[0];
            let length = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;

            // Bound the resize against the segment so a bogus length from a
            // torn record can't drive an arbitrary allocation.
            if pos + 5 + length as u64 > end_offset {
                break; // torn payload — stop at last valid record
            }

            if status == RECORD_ACTIVE {
                buf.resize(length, 0);
                reader.read_exact(&mut buf)?;
                let decoded = self.decode_payload(&buf)?;
                if !f(&decoded)? {
                    break;
                }
            } else {
                reader.seek(SeekFrom::Current(length as i64))?;
            }

            pos += 5 + length as u64;
        }

        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_storage(dir: &TempDir) -> Storage {
        Storage::open(&dir.path().join("test.dat")).unwrap()
    }

    #[test]
    fn append_and_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let storage = test_storage(&dir);
        let data = b"hello world";
        let loc = storage.append(data).unwrap();
        let read_back = storage.read(loc).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn uncompressed_mode_stores_raw_and_reads_adaptively() {
        let dir = TempDir::new().unwrap();
        // A highly compressible payload that zstd would normally shrink.
        let payload = vec![b'x'; 4096];

        // compress = false: the record must be stored verbatim (no zstd frame).
        let path = dir.path().join("raw.dat");
        let raw = Storage::open_with_options(&path, None, false).unwrap();
        let loc = raw.append(&payload).unwrap();
        assert_eq!(raw.read(loc).unwrap(), payload, "uncompressed round-trip");
        // On-disk length == payload length (+ no shrink), i.e. not compressed.
        assert_eq!(loc.length as usize, payload.len(), "stored raw, not zstd-shrunk");

        // A compressed store shrinks the same payload.
        let cpath = dir.path().join("zstd.dat");
        let comp = Storage::open_with_options(&cpath, None, true).unwrap();
        let cloc = comp.append(&payload).unwrap();
        assert!((cloc.length as usize) < payload.len(), "compressed store shrinks it");

        // Reads are adaptive: an uncompressed-mode handle still decodes a
        // previously-compressed file correctly (mixed files need no migration).
        drop(comp);
        let reopened_uncompressed = Storage::open_with_options(&cpath, None, false).unwrap();
        assert_eq!(
            reopened_uncompressed.read(cloc).unwrap(),
            payload,
            "uncompressed-mode handle still reads compressed records"
        );
    }

    #[test]
    fn torn_tail_record_is_not_a_hard_error() {
        // A crash mid-append can leave a partial header or a header with a
        // payload that runs past EOF. Scanning must stop cleanly at the last
        // valid record rather than erroring (which would make the data file —
        // reconstructable from the WAL — unopenable) or allocating from a
        // bogus length.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.dat");
        {
            let storage = Storage::open(&path).unwrap();
            storage.append(b"first").unwrap();
            storage.append(b"second").unwrap();
        }
        // Simulate a torn tail: a status byte + a length field claiming a huge
        // payload that does not exist on disk.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[RECORD_ACTIVE]).unwrap();
            f.write_all(&u32::MAX.to_le_bytes()).unwrap(); // bogus 4 GiB length
            f.write_all(b"xx").unwrap(); // only a couple of payload bytes
        }

        let storage = Storage::open(&path).unwrap();
        let active = storage.iter_active().unwrap();
        assert_eq!(
            active.len(),
            2,
            "torn tail must be ignored, valid records kept"
        );
        assert_eq!(active[0].1, b"first");
        assert_eq!(active[1].1, b"second");
    }

    #[test]
    fn append_multiple_records() {
        let dir = TempDir::new().unwrap();
        let storage = test_storage(&dir);
        let loc1 = storage.append(b"first").unwrap();
        let loc2 = storage.append(b"second").unwrap();
        let loc3 = storage.append(b"third").unwrap();

        assert_eq!(storage.read(loc1).unwrap(), b"first");
        assert_eq!(storage.read(loc2).unwrap(), b"second");
        assert_eq!(storage.read(loc3).unwrap(), b"third");
        assert_ne!(loc1.offset, loc2.offset);
        assert_ne!(loc2.offset, loc3.offset);
    }

    #[test]
    fn soft_delete_hides_record() {
        let dir = TempDir::new().unwrap();
        let storage = test_storage(&dir);
        let loc1 = storage.append(b"keep").unwrap();
        let loc2 = storage.append(b"delete_me").unwrap();
        let loc3 = storage.append(b"also_keep").unwrap();

        storage.mark_deleted(loc2).unwrap();

        let active = storage.iter_active().unwrap();
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].1, b"keep");
        assert_eq!(active[1].1, b"also_keep");

        // Deleted record's bytes are still readable by direct offset
        let raw = storage.read(loc2).unwrap();
        assert_eq!(raw, b"delete_me");

        // But loc1 and loc3 are still fine
        assert_eq!(storage.read(loc1).unwrap(), b"keep");
        assert_eq!(storage.read(loc3).unwrap(), b"also_keep");
    }

    #[test]
    fn file_size_grows_correctly() {
        let dir = TempDir::new().unwrap();
        let storage = test_storage(&dir);
        assert_eq!(storage.file_size(), 0);

        let data = b"test";
        storage.append(data).unwrap();
        // header (1 status + 4 length) + payload
        assert_eq!(storage.file_size(), 5 + data.len() as u64);
    }

    #[test]
    fn iter_active_on_empty_file() {
        let dir = TempDir::new().unwrap();
        let storage = test_storage(&dir);
        let active = storage.iter_active().unwrap();
        assert!(active.is_empty());
    }

    #[test]
    fn append_no_sync_and_manual_sync() {
        let dir = TempDir::new().unwrap();
        let storage = test_storage(&dir);

        let loc1 = storage.append_no_sync(b"batch1").unwrap();
        let loc2 = storage.append_no_sync(b"batch2").unwrap();
        storage.sync().unwrap();

        assert_eq!(storage.read(loc1).unwrap(), b"batch1");
        assert_eq!(storage.read(loc2).unwrap(), b"batch2");
    }

    #[test]
    fn mark_deleted_no_sync() {
        let dir = TempDir::new().unwrap();
        let storage = test_storage(&dir);

        let loc = storage.append(b"will_delete").unwrap();
        storage.mark_deleted_no_sync(loc).unwrap();
        storage.sync().unwrap();

        let active = storage.iter_active().unwrap();
        assert!(active.is_empty());
    }

    #[test]
    fn encrypted_storage_roundtrip() {
        let dir = TempDir::new().unwrap();
        let key_path = dir.path().join("test.key");
        std::fs::write(&key_path, &[0x42u8; 32]).unwrap();
        let enc_key = EncryptionKey::load_from_file(&key_path).unwrap();

        let storage =
            Storage::open_with_encryption(&dir.path().join("encrypted.dat"), Some(enc_key))
                .unwrap();

        let data = b"secret document payload";
        let loc = storage.append(data).unwrap();
        let read_back = storage.read(loc).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn encrypted_data_not_plaintext() {
        let dir = TempDir::new().unwrap();
        let key_path = dir.path().join("test.key");
        std::fs::write(&key_path, &[0x42u8; 32]).unwrap();
        let enc_key = EncryptionKey::load_from_file(&key_path).unwrap();

        let data_path = dir.path().join("encrypted.dat");
        let storage = Storage::open_with_encryption(&data_path, Some(enc_key)).unwrap();

        let data = b"secret document payload";
        storage.append(data).unwrap();

        // Read raw file and verify plaintext is not visible
        let raw = std::fs::read(&data_path).unwrap();
        assert!(!raw.windows(data.len()).any(|w| w == data));
    }

    #[test]
    fn encrypted_iter_active() {
        let dir = TempDir::new().unwrap();
        let key_path = dir.path().join("test.key");
        std::fs::write(&key_path, &[0x42u8; 32]).unwrap();
        let enc_key = EncryptionKey::load_from_file(&key_path).unwrap();

        let storage =
            Storage::open_with_encryption(&dir.path().join("encrypted.dat"), Some(enc_key))
                .unwrap();

        storage.append(b"doc_a").unwrap();
        let loc_b = storage.append(b"doc_b").unwrap();
        storage.append(b"doc_c").unwrap();

        storage.mark_deleted(loc_b).unwrap();

        let active = storage.iter_active().unwrap();
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].1, b"doc_a");
        assert_eq!(active[1].1, b"doc_c");
    }

    #[test]
    fn reopen_preserves_data() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("persist.dat");

        let loc;
        {
            let storage = Storage::open(&path).unwrap();
            loc = storage.append(b"persistent").unwrap();
        }

        // Reopen
        let storage = Storage::open(&path).unwrap();
        let data = storage.read(loc).unwrap();
        assert_eq!(data, b"persistent");
        let active = storage.iter_active().unwrap();
        assert_eq!(active.len(), 1);
    }

    #[test]
    fn large_payload() {
        let dir = TempDir::new().unwrap();
        let storage = test_storage(&dir);
        let data = vec![0xABu8; 100_000];
        let loc = storage.append(&data).unwrap();
        assert_eq!(storage.read(loc).unwrap(), data);
    }
}
