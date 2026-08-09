//! Disk-backed base layer for the collection full-text index (`.mtidx`).
//!
//! The collection text index was the one index type with no disk form: the
//! whole inverted index — measured **785 MB per 1M documents against 38 MB
//! for the documents themselves** (`examples/fts_mem_bench.rs`) — lived in
//! anonymous memory, and was rebuilt by a full collection scan at every open.
//! This file is the `.mfidx` model applied to postings: an immutable mmap'd
//! **base** the searches read, a small in-RAM overlay for documents written
//! since the last persist (owned by `CollectionTextIndex`, which keeps the
//! single scoring implementation), and a whole-file rewrite at persist time.
//!
//! Because a document's postings are scattered across many terms, the removal
//! unit is the **document**, not the (term, id) pair: the base is never
//! edited — `CollectionTextIndex` keeps a set of dead doc ids and search
//! skips their base postings, exactly Lucene's deleted-docs bitmap. A persist
//! folds the overlay in and drops the dead.
//!
//! ## File layout (v1, little-endian)
//!
//! ```text
//! MAGIC "OXTI" (4) | VERSION u32 | field_count u16 | field: len u16 + utf8 …
//! total_term_count u64                 corpus token count as of this file
//! doc_count u32 | term_count u32
//! postings_len u64 | string_blob_len u64
//! DOC TABLE   doc_count  × (doc_id u64, dl u32)          sorted by doc_id
//! POSTINGS    per entry (doc_id u64, freq u32) = 12 B    doc-id order per term
//! TERM TABLE  term_count × (str_off u64, str_len u16,
//!                           post_off u64, post_cnt u32)  sorted by term bytes
//! STRING BLOB                                            concatenated terms
//! ```
//!
//! Postings sit **before** the term table on purpose: a persist streams
//! term-by-term, and each term arrives carrying its postings — with postings
//! second, they append straight to the file while the (vocabulary-sized) term
//! entries and strings buffer until the end. Nothing document-proportional is
//! ever held in memory to write the file.
//!
//! Doc and term tables are fixed-stride, so both lookups are binary searches
//! straight over the mmap; postings are sequential 12-byte entries. Terms
//! compare as raw UTF-8 bytes — a total order is all a lookup needs.
//!
//! Crash safety is the open path's: the file is written to a temp name and
//! renamed, and a missing/torn/short file fails `open` — the caller falls
//! back to the rebuild-from-scan it always did.

#![cfg(not(target_arch = "wasm32"))]

use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use memmap2::Mmap;

const MAGIC: &[u8; 4] = b"OXTI";
const VERSION: u32 = 1;
const DOC_ENTRY: usize = 12; // doc_id u64 + dl u32
const TERM_ENTRY: usize = 22; // str_off u64 + str_len u16 + post_off u64 + post_cnt u32
const POST_ENTRY: usize = 12; // doc_id u64 + freq u32

/// The immutable mmap'd base. Read-only by construction — every mutation
/// lives in `CollectionTextIndex`'s overlay until a persist rewrites this.
pub struct MmapTextIndex {
    mmap: Mmap,
    fields: Vec<String>,
    total_term_count: u64,
    doc_count: u32,
    term_count: u32,
    doc_table: usize,   // byte offset of the doc table
    postings: usize,    // byte offset of the postings blob
    term_table: usize,  // byte offset of the term table
    string_blob: usize, // byte offset of the string blob
    len: usize,
}

fn rd_u16(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(b[at..at + 2].try_into().unwrap())
}
fn rd_u32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}
fn rd_u64(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

impl MmapTextIndex {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = fs::File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let b: &[u8] = &mmap;
        let bad = |what: &str| io::Error::new(io::ErrorKind::InvalidData, what.to_string());

        if b.len() < 10 || &b[0..4] != MAGIC {
            return Err(bad("not an OXTI file"));
        }
        if rd_u32(b, 4) != VERSION {
            return Err(bad("unsupported OXTI version"));
        }
        let mut at = 8;
        let field_count = rd_u16(b, at) as usize;
        at += 2;
        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            if at + 2 > b.len() {
                return Err(bad("truncated field table"));
            }
            let len = rd_u16(b, at) as usize;
            at += 2;
            if at + len > b.len() {
                return Err(bad("truncated field name"));
            }
            fields.push(
                std::str::from_utf8(&b[at..at + len])
                    .map_err(|_| bad("field name not UTF-8"))?
                    .to_string(),
            );
            at += len;
        }
        if at + 8 + 4 + 4 + 8 + 8 > b.len() {
            return Err(bad("truncated header"));
        }
        let total_term_count = rd_u64(b, at);
        at += 8;
        let doc_count = rd_u32(b, at);
        at += 4;
        let term_count = rd_u32(b, at);
        at += 4;
        let postings_len = rd_u64(b, at) as usize;
        at += 8;
        let string_blob_len = rd_u64(b, at) as usize;
        at += 8;

        let doc_table = at;
        let postings = doc_table + doc_count as usize * DOC_ENTRY;
        let term_table = postings + postings_len;
        let string_blob = term_table + term_count as usize * TERM_ENTRY;
        let len = string_blob + string_blob_len;
        if len != b.len() {
            return Err(bad("OXTI length mismatch"));
        }

        Ok(Self {
            mmap,
            fields,
            total_term_count,
            doc_count,
            term_count,
            doc_table,
            postings,
            term_table,
            string_blob,
            len,
        })
    }

    pub fn fields(&self) -> &[String] {
        &self.fields
    }
    pub fn doc_count(&self) -> u32 {
        self.doc_count
    }
    pub fn total_term_count(&self) -> u64 {
        self.total_term_count
    }
    /// On-disk size — what this index costs instead of resident memory.
    pub fn file_len(&self) -> usize {
        self.len
    }

    /// The indexed term count (`dl`) of `doc_id`, if the base knows the doc.
    pub fn doc_len(&self, doc_id: u64) -> Option<u32> {
        let b: &[u8] = &self.mmap;
        let (mut lo, mut hi) = (0usize, self.doc_count as usize);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let at = self.doc_table + mid * DOC_ENTRY;
            match rd_u64(b, at).cmp(&doc_id) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(rd_u32(b, at + 8)),
            }
        }
        None
    }

    fn term_at(&self, i: usize) -> (&[u8], u64, u32) {
        let b: &[u8] = &self.mmap;
        let at = self.term_table + i * TERM_ENTRY;
        let str_off = rd_u64(b, at) as usize;
        let str_len = rd_u16(b, at + 8) as usize;
        let post_off = rd_u64(b, at + 10);
        let post_cnt = rd_u32(b, at + 18);
        (
            &b[self.string_blob + str_off..self.string_blob + str_off + str_len],
            post_off,
            post_cnt,
        )
    }

    /// The base postings for `term`: an iterator of `(doc_id, frequency)` and
    /// the entry count (the term's base document frequency). `None` when the
    /// base has no such term.
    pub fn postings(&self, term: &str) -> Option<(PostingsIter<'_>, u32)> {
        let (mut lo, mut hi) = (0usize, self.term_count as usize);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (bytes, post_off, post_cnt) = self.term_at(mid);
            match bytes.cmp(term.as_bytes()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    return Some((
                        PostingsIter {
                            b: &self.mmap,
                            at: self.postings + post_off as usize,
                            left: post_cnt,
                        },
                        post_cnt,
                    ));
                }
            }
        }
        None
    }

    /// Walk every term with its postings, in term order — the persist merge's
    /// base side.
    pub fn for_each_term(&self, mut f: impl FnMut(&str, PostingsIter<'_>)) {
        for i in 0..self.term_count as usize {
            let (bytes, post_off, post_cnt) = self.term_at(i);
            // Written from &str; unreadable bytes would mean a torn file,
            // which `open`'s length check already rejects.
            let term = std::str::from_utf8(bytes).unwrap_or("");
            f(
                term,
                PostingsIter {
                    b: &self.mmap,
                    at: self.postings + post_off as usize,
                    left: post_cnt,
                },
            );
        }
    }

    /// Walk the doc table in doc-id order — the persist merge's base side.
    pub fn for_each_doc(&self, mut f: impl FnMut(u64, u32)) {
        let b: &[u8] = &self.mmap;
        for i in 0..self.doc_count as usize {
            let at = self.doc_table + i * DOC_ENTRY;
            f(rd_u64(b, at), rd_u32(b, at + 8));
        }
    }
}

/// Sequential reader over one term's postings in the mmap.
pub struct PostingsIter<'a> {
    b: &'a [u8],
    at: usize,
    left: u32,
}

impl Iterator for PostingsIter<'_> {
    type Item = (u64, u32);
    fn next(&mut self) -> Option<(u64, u32)> {
        if self.left == 0 {
            return None;
        }
        let doc_id = rd_u64(self.b, self.at);
        let freq = rd_u32(self.b, self.at + 8);
        self.at += POST_ENTRY;
        self.left -= 1;
        Some((doc_id, freq))
    }
}

/// Streaming writer: docs first (sorted by id), then terms in ascending byte
/// order, each with its postings — which append straight to the file, so
/// nothing document-proportional is buffered. Temp-file + rename at `finish`;
/// a crash leaves the previous file in force.
pub struct MtidxWriter {
    path: PathBuf,
    tmp: PathBuf,
    out: BufWriter<fs::File>,
    stats_at: usize,
    doc_count: u32,
    docs_pushed: u32,
    term_count: u32,
    term_entries: Vec<u8>,
    string_blob: Vec<u8>,
    postings_entries: u64,
}

impl MtidxWriter {
    /// Begin a write. `total_term_count` and `doc_count` must be exact —
    /// they are BM25's corpus statistics.
    pub fn create(
        path: &Path,
        fields: &[String],
        total_term_count: u64,
        doc_count: u32,
    ) -> io::Result<Self> {
        let tmp = path.with_extension("mtidx.tmp");
        let mut header = Vec::new();
        header.extend_from_slice(MAGIC);
        header.extend_from_slice(&VERSION.to_le_bytes());
        header.extend_from_slice(&(fields.len() as u16).to_le_bytes());
        for f in fields {
            header.extend_from_slice(&(f.len() as u16).to_le_bytes());
            header.extend_from_slice(f.as_bytes());
        }
        header.extend_from_slice(&total_term_count.to_le_bytes());
        header.extend_from_slice(&doc_count.to_le_bytes());
        // term_count / postings_len / string_blob_len are back-patched at
        // finish; write zeros now so every later offset is final.
        let stats_at = header.len();
        header.extend_from_slice(&0u32.to_le_bytes());
        header.extend_from_slice(&0u64.to_le_bytes());
        header.extend_from_slice(&0u64.to_le_bytes());

        let mut out = BufWriter::new(fs::File::create(&tmp)?);
        out.write_all(&header)?;
        Ok(Self {
            path: path.to_path_buf(),
            tmp,
            out,
            stats_at,
            doc_count,
            docs_pushed: 0,
            term_count: 0,
            term_entries: Vec::new(),
            string_blob: Vec::new(),
            postings_entries: 0,
        })
    }

    /// Docs must arrive sorted by id, exactly `doc_count` of them, before any
    /// term.
    pub fn push_doc(&mut self, doc_id: u64, dl: u32) -> io::Result<()> {
        self.docs_pushed += 1;
        self.out.write_all(&doc_id.to_le_bytes())?;
        self.out.write_all(&dl.to_le_bytes())?;
        Ok(())
    }

    /// Terms must arrive in ascending byte order, each with at least one
    /// posting in doc-id order (a term nobody has is not a term).
    pub fn push_term(
        &mut self,
        term: &str,
        postings: impl Iterator<Item = (u64, u32)>,
    ) -> io::Result<()> {
        debug_assert_eq!(self.docs_pushed, self.doc_count, "terms before all docs");
        let str_off = self.string_blob.len() as u64;
        self.string_blob.extend_from_slice(term.as_bytes());
        let post_off = self.postings_entries * POST_ENTRY as u64;
        let mut cnt: u32 = 0;
        for (doc_id, freq) in postings {
            self.out.write_all(&doc_id.to_le_bytes())?;
            self.out.write_all(&freq.to_le_bytes())?;
            cnt += 1;
        }
        debug_assert!(cnt > 0, "empty posting list pushed");
        self.postings_entries += cnt as u64;

        let mut e = [0u8; TERM_ENTRY];
        e[0..8].copy_from_slice(&str_off.to_le_bytes());
        e[8..10].copy_from_slice(&(term.len() as u16).to_le_bytes());
        e[10..18].copy_from_slice(&post_off.to_le_bytes());
        e[18..22].copy_from_slice(&cnt.to_le_bytes());
        self.term_entries.extend_from_slice(&e);
        self.term_count += 1;
        Ok(())
    }

    /// Append the buffered term table + strings, back-patch the header stats,
    /// fsync, and atomically publish over `path`.
    pub fn finish(mut self) -> io::Result<()> {
        if self.docs_pushed != self.doc_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "doc table incomplete: {} of {} pushed",
                    self.docs_pushed, self.doc_count
                ),
            ));
        }
        self.out.write_all(&self.term_entries)?;
        self.out.write_all(&self.string_blob)?;
        let mut file = self.out.into_inner()?;
        // Back-patch the three lengths reserved in the header.
        use std::io::Seek;
        file.seek(io::SeekFrom::Start(self.stats_at as u64))?;
        file.write_all(&self.term_count.to_le_bytes())?;
        file.write_all(&(self.postings_entries * POST_ENTRY as u64).to_le_bytes())?;
        file.write_all(&(self.string_blob.len() as u64).to_le_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&self.tmp, &self.path)?;
        // Durability of the rename itself, same protocol as compact().
        if let Some(dir) = self.path.parent()
            && let Ok(d) = fs::File::open(dir)
        {
            let _ = d.sync_all();
        }
        Ok(())
    }
}

// ─── Blob (bucket/key) variant ──────────────────────────────────────────────

const BLOB_MAGIC: &[u8; 4] = b"OXTB";
const BLOB_DOC_ENTRY: usize = 24; // name_off u64 + bucket_len u16 + key_len u16 + dl u32 + text_bytes u64
const BLOB_POST_ENTRY: usize = 8; // doc ordinal u32 + freq u32

/// The blob search index's mmap'd base — the same model as [`MmapTextIndex`]
/// with two differences forced by the domain: documents are named by
/// `(bucket, key)` strings, so the doc table is (offset, lengths) into a
/// names blob and postings reference doc **ordinals** (u32, 8 bytes an entry
/// instead of 12); and each doc carries `text_bytes`, because per-bucket FTS
/// quota accounting reads it. The doc table is sorted by `(bucket, key)`, so
/// a name lookup is a binary search and a whole bucket is one contiguous run
/// — `bucket_text_size` walks exactly its own range.
pub struct MmapBlobTextIndex {
    mmap: Mmap,
    total_term_count: u64,
    doc_count: u32,
    term_count: u32,
    doc_table: usize,
    postings: usize,
    term_table: usize,
    names_blob: usize,
    string_blob: usize,
    len: usize,
}

impl MmapBlobTextIndex {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = fs::File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let b: &[u8] = &mmap;
        let bad = |what: &str| io::Error::new(io::ErrorKind::InvalidData, what.to_string());

        if b.len() < 8 || &b[0..4] != BLOB_MAGIC {
            return Err(bad("not an OXTB file"));
        }
        if rd_u32(b, 4) != VERSION {
            return Err(bad("unsupported OXTB version"));
        }
        let mut at = 8;
        if at + 8 + 4 + 4 + 8 + 8 + 8 > b.len() {
            return Err(bad("truncated header"));
        }
        let total_term_count = rd_u64(b, at);
        at += 8;
        let doc_count = rd_u32(b, at);
        at += 4;
        let term_count = rd_u32(b, at);
        at += 4;
        let postings_len = rd_u64(b, at) as usize;
        at += 8;
        let names_blob_len = rd_u64(b, at) as usize;
        at += 8;
        let string_blob_len = rd_u64(b, at) as usize;
        at += 8;

        let doc_table = at;
        let postings = doc_table + doc_count as usize * BLOB_DOC_ENTRY;
        let term_table = postings + postings_len;
        let names_blob = term_table + term_count as usize * TERM_ENTRY;
        let string_blob = names_blob + names_blob_len;
        let len = string_blob + string_blob_len;
        if len != b.len() {
            return Err(bad("OXTB length mismatch"));
        }

        Ok(Self {
            mmap,
            total_term_count,
            doc_count,
            term_count,
            doc_table,
            postings,
            term_table,
            names_blob,
            string_blob,
            len,
        })
    }

    pub fn doc_count(&self) -> u32 {
        self.doc_count
    }
    pub fn total_term_count(&self) -> u64 {
        self.total_term_count
    }
    pub fn file_len(&self) -> usize {
        self.len
    }

    /// (bucket, key, dl, text_bytes) of the doc at `ord`.
    pub fn doc_at(&self, ord: u32) -> (&str, &str, u32, u64) {
        let b: &[u8] = &self.mmap;
        let at = self.doc_table + ord as usize * BLOB_DOC_ENTRY;
        let name_off = rd_u64(b, at) as usize;
        let bucket_len = rd_u16(b, at + 8) as usize;
        let key_len = rd_u16(b, at + 10) as usize;
        let dl = rd_u32(b, at + 12);
        let text_bytes = rd_u64(b, at + 16);
        let name =
            &b[self.names_blob + name_off..self.names_blob + name_off + bucket_len + key_len];
        (
            std::str::from_utf8(&name[..bucket_len]).unwrap_or(""),
            std::str::from_utf8(&name[bucket_len..]).unwrap_or(""),
            dl,
            text_bytes,
        )
    }

    /// Ordinal of `(bucket, key)`, if the base knows it.
    pub fn find_doc(&self, bucket: &str, key: &str) -> Option<u32> {
        let (mut lo, mut hi) = (0usize, self.doc_count as usize);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (mb, mk, _, _) = self.doc_at(mid as u32);
            match (mb, mk).cmp(&(bucket, key)) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid as u32),
            }
        }
        None
    }

    /// Walk the docs of exactly one bucket — a contiguous run of the sorted
    /// doc table, located by binary search.
    pub fn for_each_bucket_doc(&self, bucket: &str, mut f: impl FnMut(u32, &str, u32, u64)) {
        // Lower bound: first entry with bucket >= target.
        let (mut lo, mut hi) = (0usize, self.doc_count as usize);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.doc_at(mid as u32).0 < bucket {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        while lo < self.doc_count as usize {
            let (b, k, dl, bytes) = self.doc_at(lo as u32);
            if b != bucket {
                break;
            }
            f(lo as u32, k, dl, bytes);
            lo += 1;
        }
    }

    fn term_at(&self, i: usize) -> (&[u8], u64, u32) {
        let b: &[u8] = &self.mmap;
        let at = self.term_table + i * TERM_ENTRY;
        let str_off = rd_u64(b, at) as usize;
        let str_len = rd_u16(b, at + 8) as usize;
        let post_off = rd_u64(b, at + 10);
        let post_cnt = rd_u32(b, at + 18);
        (
            &b[self.string_blob + str_off..self.string_blob + str_off + str_len],
            post_off,
            post_cnt,
        )
    }

    /// Base postings for `term` as `(doc ordinal, frequency)`, plus the df.
    pub fn postings(&self, term: &str) -> Option<(BlobPostingsIter<'_>, u32)> {
        let (mut lo, mut hi) = (0usize, self.term_count as usize);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (bytes, post_off, post_cnt) = self.term_at(mid);
            match bytes.cmp(term.as_bytes()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    return Some((
                        BlobPostingsIter {
                            b: &self.mmap,
                            at: self.postings + post_off as usize,
                            left: post_cnt,
                        },
                        post_cnt,
                    ));
                }
            }
        }
        None
    }

    /// Walk every term with its postings, in term order — the persist merge's
    /// base side.
    pub fn for_each_term(&self, mut f: impl FnMut(&str, BlobPostingsIter<'_>)) {
        for i in 0..self.term_count as usize {
            let (bytes, post_off, post_cnt) = self.term_at(i);
            let term = std::str::from_utf8(bytes).unwrap_or("");
            f(
                term,
                BlobPostingsIter {
                    b: &self.mmap,
                    at: self.postings + post_off as usize,
                    left: post_cnt,
                },
            );
        }
    }
}

/// Sequential reader over one term's blob postings.
pub struct BlobPostingsIter<'a> {
    b: &'a [u8],
    at: usize,
    left: u32,
}

impl Iterator for BlobPostingsIter<'_> {
    type Item = (u32, u32);
    fn next(&mut self) -> Option<(u32, u32)> {
        if self.left == 0 {
            return None;
        }
        let ord = rd_u32(self.b, self.at);
        let freq = rd_u32(self.b, self.at + 4);
        self.at += BLOB_POST_ENTRY;
        self.left -= 1;
        Some((ord, freq))
    }
}

/// Streaming writer for the blob variant. Docs first, sorted by
/// `(bucket, key)` — their arrival order IS the ordinal assignment the
/// caller's postings must reference. Then terms in ascending byte order.
pub struct BtidxWriter {
    path: PathBuf,
    tmp: PathBuf,
    out: BufWriter<fs::File>,
    stats_at: usize,
    doc_count: u32,
    docs_pushed: u32,
    term_count: u32,
    term_entries: Vec<u8>,
    names_blob: Vec<u8>,
    string_blob: Vec<u8>,
    postings_entries: u64,
}

impl BtidxWriter {
    pub fn create(path: &Path, total_term_count: u64, doc_count: u32) -> io::Result<Self> {
        let tmp = path.with_extension("mtidx.tmp");
        let mut header = Vec::new();
        header.extend_from_slice(BLOB_MAGIC);
        header.extend_from_slice(&VERSION.to_le_bytes());
        header.extend_from_slice(&total_term_count.to_le_bytes());
        header.extend_from_slice(&doc_count.to_le_bytes());
        let stats_at = header.len();
        header.extend_from_slice(&0u32.to_le_bytes()); // term_count
        header.extend_from_slice(&0u64.to_le_bytes()); // postings_len
        header.extend_from_slice(&0u64.to_le_bytes()); // names_blob_len
        header.extend_from_slice(&0u64.to_le_bytes()); // string_blob_len

        let mut out = BufWriter::new(fs::File::create(&tmp)?);
        out.write_all(&header)?;
        Ok(Self {
            path: path.to_path_buf(),
            tmp,
            out,
            stats_at,
            doc_count,
            docs_pushed: 0,
            term_count: 0,
            term_entries: Vec::new(),
            names_blob: Vec::new(),
            string_blob: Vec::new(),
            postings_entries: 0,
        })
    }

    /// Docs must arrive sorted by `(bucket, key)`; the Nth call defines
    /// ordinal N. Returns that ordinal.
    pub fn push_doc(
        &mut self,
        bucket: &str,
        key: &str,
        dl: u32,
        text_bytes: u64,
    ) -> io::Result<u32> {
        let ord = self.docs_pushed;
        self.docs_pushed += 1;
        let name_off = self.names_blob.len() as u64;
        self.names_blob.extend_from_slice(bucket.as_bytes());
        self.names_blob.extend_from_slice(key.as_bytes());
        self.out.write_all(&name_off.to_le_bytes())?;
        self.out.write_all(&(bucket.len() as u16).to_le_bytes())?;
        self.out.write_all(&(key.len() as u16).to_le_bytes())?;
        self.out.write_all(&dl.to_le_bytes())?;
        self.out.write_all(&text_bytes.to_le_bytes())?;
        Ok(ord)
    }

    /// Terms in ascending byte order; postings in ordinal order, non-empty.
    pub fn push_term(
        &mut self,
        term: &str,
        postings: impl Iterator<Item = (u32, u32)>,
    ) -> io::Result<()> {
        debug_assert_eq!(self.docs_pushed, self.doc_count, "terms before all docs");
        let str_off = self.string_blob.len() as u64;
        self.string_blob.extend_from_slice(term.as_bytes());
        let post_off = self.postings_entries * BLOB_POST_ENTRY as u64;
        let mut cnt: u32 = 0;
        for (ord, freq) in postings {
            self.out.write_all(&ord.to_le_bytes())?;
            self.out.write_all(&freq.to_le_bytes())?;
            cnt += 1;
        }
        debug_assert!(cnt > 0, "empty posting list pushed");
        self.postings_entries += cnt as u64;

        let mut e = [0u8; TERM_ENTRY];
        e[0..8].copy_from_slice(&str_off.to_le_bytes());
        e[8..10].copy_from_slice(&(term.len() as u16).to_le_bytes());
        e[10..18].copy_from_slice(&post_off.to_le_bytes());
        e[18..22].copy_from_slice(&cnt.to_le_bytes());
        self.term_entries.extend_from_slice(&e);
        self.term_count += 1;
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<()> {
        if self.docs_pushed != self.doc_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "doc table incomplete: {} of {} pushed",
                    self.docs_pushed, self.doc_count
                ),
            ));
        }
        self.out.write_all(&self.term_entries)?;
        self.out.write_all(&self.names_blob)?;
        self.out.write_all(&self.string_blob)?;
        let mut file = self.out.into_inner()?;
        use std::io::Seek;
        file.seek(io::SeekFrom::Start(self.stats_at as u64))?;
        file.write_all(&self.term_count.to_le_bytes())?;
        file.write_all(&(self.postings_entries * BLOB_POST_ENTRY as u64).to_le_bytes())?;
        file.write_all(&(self.names_blob.len() as u64).to_le_bytes())?;
        file.write_all(&(self.string_blob.len() as u64).to_le_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&self.tmp, &self.path)?;
        if let Some(dir) = self.path.parent()
            && let Ok(d) = fs::File::open(dir)
        {
            let _ = d.sync_all();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_fixture(path: &Path) {
        // 3 docs; vocabulary chosen unsorted here to prove the CALLER sorts.
        let mut w = MtidxWriter::create(path, &["body".into()], 10, 3).unwrap();
        w.push_doc(1, 4).unwrap();
        w.push_doc(5, 3).unwrap();
        w.push_doc(9, 3).unwrap();
        w.push_term("alpha", [(1u64, 2u32), (5, 1)].into_iter())
            .unwrap();
        w.push_term("beta", [(5u64, 2u32)].into_iter()).unwrap();
        w.push_term("gamma", [(1u64, 1u32), (5, 1), (9, 3)].into_iter())
            .unwrap();
        w.finish().unwrap();
    }

    #[test]
    fn roundtrip_docs_terms_postings() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.mtidx");
        write_fixture(&path);

        let idx = MmapTextIndex::open(&path).unwrap();
        assert_eq!(idx.fields(), &["body".to_string()]);
        assert_eq!(idx.doc_count(), 3);
        assert_eq!(idx.total_term_count(), 10);
        assert_eq!(idx.doc_len(1), Some(4));
        assert_eq!(idx.doc_len(5), Some(3));
        assert_eq!(idx.doc_len(2), None);

        let (it, df) = idx.postings("gamma").unwrap();
        assert_eq!(df, 3);
        assert_eq!(it.collect::<Vec<_>>(), vec![(1, 1), (5, 1), (9, 3)]);
        let (it, df) = idx.postings("beta").unwrap();
        assert_eq!(df, 1);
        assert_eq!(it.collect::<Vec<_>>(), vec![(5, 2)]);
        assert!(idx.postings("delta").is_none());

        let mut walked = Vec::new();
        idx.for_each_term(|t, it| walked.push((t.to_string(), it.count())));
        assert_eq!(
            walked,
            vec![("alpha".into(), 2), ("beta".into(), 1), ("gamma".into(), 3)]
        );
    }

    #[test]
    fn a_torn_file_is_refused_not_misread() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.mtidx");
        write_fixture(&path);
        let bytes = fs::read(&path).unwrap();
        // Truncate anywhere: the length check must refuse it.
        fs::write(&path, &bytes[..bytes.len() - 5]).unwrap();
        assert!(MmapTextIndex::open(&path).is_err());
        // Garbage magic likewise.
        fs::write(&path, b"NOPE").unwrap();
        assert!(MmapTextIndex::open(&path).is_err());
    }

    #[test]
    fn finish_refuses_an_incomplete_doc_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.mtidx");
        let mut w = MtidxWriter::create(&path, &["body".into()], 10, 3).unwrap();
        w.push_doc(1, 4).unwrap();
        assert!(w.finish().is_err());
        assert!(!path.exists(), "a refused finish must not publish");
    }
}
