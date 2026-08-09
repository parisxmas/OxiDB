use std::collections::HashMap;

#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};

#[cfg(not(target_arch = "wasm32"))]
use serde::{Deserialize, Serialize};

use crate::error::Result;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Serialize, Deserialize, Clone)]
struct Posting {
    doc_id: String,
    frequency: u32,
    // `positions` existed here once (4 bytes per token OCCURRENCE, resident)
    // and nothing read it: search scores on frequency, and highlights
    // re-extract the blob. Legacy index.json files that carry it still parse
    // — serde ignores unknown fields.
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Serialize, Deserialize, Clone)]
struct DocInfo {
    bucket: String,
    key: String,
    total_terms: u32,
    /// Byte length of the extracted text that was passed to index_document.
    /// Used to attribute FTS storage cost per-bucket for tenant quota
    /// reporting. Optional/default-0 so indexes written before this field
    /// existed deserialize cleanly — they fall back to a term-count-based
    /// estimate via FtsIndex::bucket_text_size.
    #[serde(default)]
    text_bytes: u64,
    /// The distinct terms this doc contributed postings for. Lets
    /// remove/re-index touch only those posting lists instead of sweeping
    /// the entire inverted index per document. Default-empty for indexes
    /// written before the field existed — those docs fall back to the full
    /// sweep once, then carry the list after re-indexing.
    #[serde(default)]
    terms: Vec<String>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Serialize, Deserialize, Default)]
struct IndexData {
    postings: HashMap<String, Vec<Posting>>,
    docs: HashMap<String, DocInfo>,
    #[serde(default)]
    total_term_count: u64,
}

// BM25 scoring parameters. Defaults match Lucene/Elasticsearch.
// k1 controls term-frequency saturation (higher = more linear).
// b controls length normalization (0 = ignore length, 1 = full).
// Both can be overridden at process start via env vars
// (OXIDB_FTS_K1, OXIDB_FTS_B); resolved once and cached.
const BM25_K1_DEFAULT: f64 = 1.2;
const BM25_B_DEFAULT: f64 = 0.75;

fn parse_k1(raw: Option<&str>) -> f64 {
    raw.and_then(|s| s.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v > 0.0)
        .unwrap_or(BM25_K1_DEFAULT)
}

fn parse_b(raw: Option<&str>) -> f64 {
    raw.and_then(|s| s.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v >= 0.0 && *v <= 1.0)
        .unwrap_or(BM25_B_DEFAULT)
}

fn parse_lang(raw: Option<&str>) -> rust_stemmers::Algorithm {
    use rust_stemmers::Algorithm;
    match raw.map(|s| s.trim().to_lowercase()).as_deref() {
        Some("turkish") | Some("tr") | Some("tur") => Algorithm::Turkish,
        Some("german") | Some("de") | Some("ger") => Algorithm::German,
        Some("french") | Some("fr") | Some("fra") => Algorithm::French,
        Some("spanish") | Some("es") | Some("spa") => Algorithm::Spanish,
        Some("italian") | Some("it") | Some("ita") => Algorithm::Italian,
        Some("portuguese") | Some("pt") | Some("por") => Algorithm::Portuguese,
        Some("russian") | Some("ru") | Some("rus") => Algorithm::Russian,
        Some("dutch") | Some("nl") | Some("nld") => Algorithm::Dutch,
        Some("danish") | Some("da") | Some("dan") => Algorithm::Danish,
        Some("finnish") | Some("fi") | Some("fin") => Algorithm::Finnish,
        Some("hungarian") | Some("hu") | Some("hun") => Algorithm::Hungarian,
        Some("norwegian") | Some("no") | Some("nor") => Algorithm::Norwegian,
        Some("romanian") | Some("ro") | Some("ron") => Algorithm::Romanian,
        Some("greek") | Some("el") | Some("ell") => Algorithm::Greek,
        Some("arabic") | Some("ar") | Some("ara") => Algorithm::Arabic,
        Some("swedish") | Some("sv") | Some("swe") => Algorithm::Swedish,
        Some("tamil") | Some("ta") | Some("tam") => Algorithm::Tamil,
        _ => Algorithm::English,
    }
}

fn fts_k1() -> f64 {
    static CACHED: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| parse_k1(std::env::var("OXIDB_FTS_K1").ok().as_deref()))
}

fn fts_b() -> f64 {
    static CACHED: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| parse_b(std::env::var("OXIDB_FTS_B").ok().as_deref()))
}

/// Hard ceiling on how many DISTINCT documents one search will score.
///
/// Term-at-a-time BM25 accumulates a score entry per document matching any
/// query term BEFORE `limit` applies, so a common term made a search's
/// transient proportional to the corpus: measured 25 MB per query at 500k
/// matches, 51 MB at 1M (`examples/fts_mem_bench.rs`) — multiplied by
/// concurrent searches, an OOM. The cap bounds that at ~10 MB; terms are
/// processed rarest-first so what the cap cuts is the tail of the most
/// common (least informative) term, never the docs matching a rare one.
/// `OXIDB_FTS_SCORE_CAP` overrides; `0` = unbounded (the old behavior).
const FTS_SCORE_CAP_DEFAULT: usize = 200_000;

fn fts_score_cap() -> usize {
    static CACHED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        match std::env::var("OXIDB_FTS_SCORE_CAP")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(0) => usize::MAX,
            Some(n) => n,
            None => FTS_SCORE_CAP_DEFAULT,
        }
    })
}

fn fts_stemmer() -> &'static rust_stemmers::Stemmer {
    static CACHED: std::sync::OnceLock<rust_stemmers::Stemmer> = std::sync::OnceLock::new();
    CACHED.get_or_init(|| {
        rust_stemmers::Stemmer::create(parse_lang(std::env::var("OXIDB_FTS_LANG").ok().as_deref()))
    })
}

fn algorithm_label(a: rust_stemmers::Algorithm) -> &'static str {
    use rust_stemmers::Algorithm;
    match a {
        Algorithm::Arabic => "arabic",
        Algorithm::Danish => "danish",
        Algorithm::Dutch => "dutch",
        Algorithm::English => "english",
        Algorithm::Finnish => "finnish",
        Algorithm::French => "french",
        Algorithm::German => "german",
        Algorithm::Greek => "greek",
        Algorithm::Hungarian => "hungarian",
        Algorithm::Italian => "italian",
        Algorithm::Norwegian => "norwegian",
        Algorithm::Portuguese => "portuguese",
        Algorithm::Romanian => "romanian",
        Algorithm::Russian => "russian",
        Algorithm::Spanish => "spanish",
        Algorithm::Swedish => "swedish",
        Algorithm::Tamil => "tamil",
        Algorithm::Turkish => "turkish",
    }
}

/// One-line summary of the current FTS configuration, suitable for
/// startup logs. Reads (and caches) env vars on first call.
pub fn fts_config_summary() -> String {
    let lang = parse_lang(std::env::var("OXIDB_FTS_LANG").ok().as_deref());
    format!(
        "FTS: lang={} k1={} b={} (BM25)",
        algorithm_label(lang),
        fts_k1(),
        fts_b()
    )
}

#[inline]
fn bm25_idf(total_docs: f64, docs_with_term: f64) -> f64 {
    ((total_docs - docs_with_term + 0.5) / (docs_with_term + 0.5) + 1.0).ln()
}

#[inline]
fn bm25_score(tf: f64, dl: f64, avgdl: f64, idf: f64) -> f64 {
    let k1 = fts_k1();
    let b = fts_b();
    let norm = 1.0 - b + b * (dl / avgdl);
    let comp = (tf * (k1 + 1.0)) / (tf + k1 * norm);
    idf * comp
}

#[cfg(not(target_arch = "wasm32"))]
pub struct FtsIndex {
    /// Legacy `_fts/index.json` — read once for migration, then removed.
    index_path: PathBuf,
    /// The mmap'd base (`_fts/index.mtidx`); `data` is its overlay.
    mtidx_path: PathBuf,
    /// OVERLAY since the last persist (in legacy/fresh states, everything).
    data: IndexData,
    /// Immutable persisted base — same model as the collection index's.
    base: Option<crate::mmap_text_index::MmapBlobTextIndex>,
    /// Base doc ORDINALS whose postings are void (removed or re-indexed
    /// since the base was written). Integer-keyed on purpose: the search
    /// path checks it per posting, and a name-keyed set would build a
    /// String per check.
    dead: std::collections::HashSet<u32>,
    /// Σ dl of the dead — keeps live corpus stats exact.
    dead_terms: u64,
    /// When true, mutations only mark the index dirty; persistence
    /// happens lazily via an external flusher calling `flush()`.
    /// When false (default), every mutation writes the full index
    /// file synchronously — convenient for tests and embedded use,
    /// but not appropriate for high-volume ingestion.
    batched: bool,
    /// Set on every mutation, cleared by `persist`/`flush`.
    dirty: bool,
}

#[cfg(not(target_arch = "wasm32"))]
pub struct SearchResult {
    pub bucket: String,
    pub key: String,
    pub score: f64,
}

/// English stop words.
const STOP_WORDS_EN: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "from", "had", "has", "have",
    "he", "her", "his", "if", "in", "into", "is", "it", "its", "no", "not", "of", "on", "or",
    "she", "so", "that", "the", "this", "to", "was", "we", "with", "you",
];

/// Turkish stop words.
const STOP_WORDS_TR: &[&str] = &[
    "bir", "ve", "bu", "da", "de", "ile", "mi", "mu", "ne", "o", "ya", "ben", "sen", "biz", "siz",
    "ama", "her", "ki", "en", "var", "yok", "olan", "gibi", "daha", "icin", "kadar", "sonra",
    "once", "ise", "hem", "veya", "sadece",
];

/// Check if a word is a stop word (English or Turkish).
fn is_stop_word(word: &str) -> bool {
    STOP_WORDS_EN.contains(&word) || STOP_WORDS_TR.contains(&word)
}

/// Apply Snowball stemming to a word using the language configured
/// via OXIDB_FTS_LANG (defaults to English).
fn stem(word: &str) -> String {
    fts_stemmer().stem(word).to_string()
}

/// Strip Unicode accents/diacritics: é→e, ü→u, ç→c, etc.
fn strip_accents(text: &str) -> String {
    text.chars()
        .map(|c| match c {
            'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' => 'a',
            'è' | 'é' | 'ê' | 'ë' => 'e',
            'ì' | 'í' | 'î' | 'ï' => 'i',
            'ò' | 'ó' | 'ô' | 'õ' | 'ö' => 'o',
            'ù' | 'ú' | 'û' | 'ü' => 'u',
            'ç' => 'c',
            'ñ' => 'n',
            'ş' => 's',
            'ğ' => 'g',
            'ı' => 'i',
            _ => c,
        })
        .collect()
}

#[cfg(not(target_arch = "wasm32"))]
fn make_doc_id(bucket: &str, key: &str) -> String {
    format!("{}\t{}", bucket, key)
}

/// Tokenize text: lowercase → strip accents → split → stop words → stem.
///
/// Produces stemmed tokens for both indexing and querying, ensuring
/// that "running" and "runs" both match "run".
pub(crate) fn tokenize(text: &str) -> Vec<String> {
    let lower = text.to_lowercase();
    let normalized = strip_accents(&lower);
    normalized
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() > 1)
        .filter(|w| !is_stop_word(w))
        .map(stem)
        .collect()
}

#[cfg(not(target_arch = "wasm32"))]
impl FtsIndex {
    pub fn open(data_dir: &Path) -> Result<Self> {
        let fts_dir = data_dir.join("_fts");
        std::fs::create_dir_all(&fts_dir)?;
        let index_path = fts_dir.join("index.json");
        let mtidx_path = fts_dir.join("index.mtidx");

        // The mmap'd base wins when present: instant open, nothing resident.
        // (If a legacy index.json also survives — a crash between migration
        // and its removal — the base is the complete one; the rename that
        // published it is atomic.)
        if let Ok(base) = crate::mmap_text_index::MmapBlobTextIndex::open(&mtidx_path) {
            let _ = std::fs::remove_file(&index_path);
            return Ok(Self {
                index_path,
                mtidx_path,
                data: IndexData::default(),
                base: Some(base),
                dead: std::collections::HashSet::new(),
                dead_terms: 0,
                batched: false,
                dirty: false,
            });
        }

        let mut data: IndexData = if index_path.exists() {
            let bytes = std::fs::read(&index_path)?;
            // The FTS index is DERIVED data — a torn/corrupt index.json
            // (e.g. from a crash mid-write before the atomic-rename persist
            // existed) must not brick `OxiDb::open`. Start empty and warn;
            // documents/blobs are intact and can be reindexed.
            match serde_json::from_slice(&bytes) {
                Ok(parsed) => parsed,
                Err(e) => {
                    eprintln!(
                        "[fts] {} is corrupt ({e}); starting with an empty FTS index",
                        index_path.display()
                    );
                    let quarantine = index_path.with_extension("json.corrupt");
                    let _ = std::fs::rename(&index_path, &quarantine);
                    IndexData::default()
                }
            }
        } else {
            IndexData::default()
        };

        // Lazy migration: backfill total_term_count for indexes
        // written before BM25 was added. New empty indexes also have
        // total_term_count == 0 but docs is empty, so this is safe.
        if data.total_term_count == 0 && !data.docs.is_empty() {
            data.total_term_count = data.docs.values().map(|d| d.total_terms as u64).sum();
        }

        let mut idx = Self {
            index_path,
            mtidx_path,
            data,
            base: None,
            dead: std::collections::HashSet::new(),
            dead_terms: 0,
            batched: false,
            dirty: false,
        };
        // Migrate a legacy JSON index to the mmap'd form now — it is fully
        // resident at this point either way, and the persist folds it to
        // disk and frees it. On failure keep running resident; the JSON
        // stays until a persist succeeds.
        if !idx.data.docs.is_empty() {
            match idx.persist() {
                Ok(()) => {
                    let _ = std::fs::remove_file(&idx.index_path);
                }
                Err(e) => eprintln!("[fts] mtidx migration failed ({e}); staying resident"),
            }
        }
        Ok(idx)
    }

    /// Switch the index into batched mode (no per-mutation persist).
    /// The caller is then responsible for invoking `flush()` periodically.
    pub fn set_batched(&mut self, batched: bool) {
        self.batched = batched;
    }

    /// Persist the index if it has unwritten changes. No-op otherwise.
    pub fn flush(&mut self) -> Result<()> {
        if self.dirty {
            self.persist()?;
            self.dirty = false;
        }
        Ok(())
    }

    pub fn index_document(&mut self, bucket: &str, key: &str, text: &str) -> Result<()> {
        // Remove any existing entry for this doc first — overlay postings,
        // and the base version (which cannot be edited) via the dead set.
        let doc_id = make_doc_id(bucket, key);
        self.remove_postings(&doc_id);
        self.kill_base_doc(bucket, key);

        let tokens = tokenize(text);
        let total_terms = tokens.len() as u32;

        if total_terms == 0 {
            return Ok(());
        }

        // Count term frequencies
        let mut term_freq: HashMap<String, u32> = HashMap::new();
        for token in &tokens {
            *term_freq.entry(token.clone()).or_insert(0) += 1;
        }

        // Add postings (keep the distinct-term list for targeted removal)
        let doc_terms: Vec<String> = term_freq.keys().cloned().collect();
        for (term, freq) in term_freq {
            let posting = Posting {
                doc_id: doc_id.clone(),
                frequency: freq,
            };
            self.data.postings.entry(term).or_default().push(posting);
        }

        // Maintain total_term_count cache for BM25 avgdl: subtract old
        // entry (if re-indexing) before inserting the new one.
        if let Some(prev) = self.data.docs.get(&doc_id) {
            self.data.total_term_count = self
                .data
                .total_term_count
                .saturating_sub(prev.total_terms as u64);
        }
        self.data.total_term_count += total_terms as u64;

        // Store doc info
        self.data.docs.insert(
            doc_id,
            DocInfo {
                bucket: bucket.to_string(),
                key: key.to_string(),
                total_terms,
                text_bytes: text.len() as u64,
                terms: doc_terms,
            },
        );

        self.dirty = true;
        if !self.batched {
            self.persist()?;
            self.dirty = false;
        }
        Ok(())
    }

    /// Returns the total bytes of extracted text indexed under `bucket`.
    /// Powers per-tenant FTS quota accounting in DMS — sum is taken across
    /// all docs whose `bucket` field matches.
    ///
    /// For DocInfo entries written before `text_bytes` existed (default
    /// value 0), falls back to estimating bytes from `total_terms` × 6
    /// (rough average word length post-tokenization). New writes carry
    /// the exact byte count, so the estimate phases out as docs are
    /// re-indexed.
    pub fn bucket_text_size(&self, bucket: &str) -> u64 {
        const ESTIMATED_BYTES_PER_TERM: u64 = 6;
        let overlay: u64 = self
            .data
            .docs
            .values()
            .filter(|d| d.bucket == bucket)
            .map(|d| {
                if d.text_bytes > 0 {
                    d.text_bytes
                } else {
                    d.total_terms as u64 * ESTIMATED_BYTES_PER_TERM
                }
            })
            .sum();
        // The doc table is (bucket, key)-sorted, so the bucket is one
        // contiguous run of it; dead docs are the overlay's problem (their
        // current size was counted above if re-indexed, nothing if removed).
        let mut base_sum = 0u64;
        if let Some(base) = &self.base {
            base.for_each_bucket_doc(bucket, |ord, _key, dl, bytes| {
                if !self.dead.contains(&ord) {
                    base_sum += if bytes > 0 {
                        bytes
                    } else {
                        dl as u64 * ESTIMATED_BYTES_PER_TERM
                    };
                }
            });
        }
        overlay + base_sum
    }

    /// Void the base's copy of `(bucket, key)`, if it has one.
    fn kill_base_doc(&mut self, bucket: &str, key: &str) {
        if let Some(base) = &self.base
            && let Some(ord) = base.find_doc(bucket, key)
            && self.dead.insert(ord)
        {
            self.dead_terms += base.doc_at(ord).2 as u64;
        }
    }

    pub fn remove_document(&mut self, bucket: &str, key: &str) -> Result<()> {
        let doc_id = make_doc_id(bucket, key);
        self.remove_postings(&doc_id);
        self.kill_base_doc(bucket, key);
        if let Some(removed) = self.data.docs.remove(&doc_id) {
            self.data.total_term_count = self
                .data
                .total_term_count
                .saturating_sub(removed.total_terms as u64);
        }
        self.dirty = true;
        if !self.batched {
            self.persist()?;
            self.dirty = false;
        }
        Ok(())
    }

    fn remove_postings(&mut self, doc_id: &str) {
        // Targeted removal: only the posting lists of the doc's own terms
        // are touched — O(doc terms) instead of a sweep over the ENTIRE
        // inverted index per document update/remove.
        match self.data.docs.get(doc_id) {
            None => {} // never indexed — nothing to remove
            Some(info) if !info.terms.is_empty() || info.total_terms == 0 => {
                let terms = info.terms.clone();
                for term in &terms {
                    if let Some(postings) = self.data.postings.get_mut(term) {
                        postings.retain(|p| p.doc_id != doc_id);
                        if postings.is_empty() {
                            self.data.postings.remove(term);
                        }
                    }
                }
            }
            Some(_) => {
                // Legacy DocInfo written before the per-doc term list
                // existed: full sweep, once — re-indexing stores the list.
                let mut empty_terms = Vec::new();
                for (term, postings) in self.data.postings.iter_mut() {
                    postings.retain(|p| p.doc_id != doc_id);
                    if postings.is_empty() {
                        empty_terms.push(term.clone());
                    }
                }
                for term in empty_terms {
                    self.data.postings.remove(&term);
                }
            }
        }
    }

    pub fn search(&self, bucket: Option<&str>, query: &str, limit: usize) -> Vec<SearchResult> {
        let mut query_terms = tokenize(query);
        let base_docs = self.base.as_ref().map_or(0, |b| b.doc_count() as u64);
        let total_docs = self.data.docs.len() as u64 + base_docs - self.dead.len() as u64;
        if query_terms.is_empty() || total_docs == 0 {
            return Vec::new();
        }
        // Rarest term first + the distinct-doc scoring cap — see
        // `CollectionTextIndex::search_capped`. Base df counts dead docs
        // until the next persist, the same documented approximation.
        let term_df = |t: &String| -> usize {
            self.data.postings.get(t).map_or(0, Vec::len)
                + self
                    .base
                    .as_ref()
                    .and_then(|b| b.postings(t).map(|(_, df)| df as usize))
                    .unwrap_or(0)
        };
        query_terms.sort_by_key(term_df);
        let cap = fts_score_cap();

        let total_term_count = self.data.total_term_count
            + self.base.as_ref().map_or(0, |b| b.total_term_count())
            - self.dead_terms;
        let avgdl = (total_term_count as f64 / total_docs as f64).max(1.0);

        // Scores are keyed by a small integer, never a String: base docs by
        // their ordinal, overlay docs by base_count + position in a
        // per-search overlay list. A String key would allocate once per
        // scored posting — the exact transient this path is trying to bound.
        let base_count = self.base.as_ref().map_or(0u32, |b| b.doc_count());
        let overlay_ids: Vec<&String> = self.data.docs.keys().collect();
        let overlay_slot: HashMap<&str, u32> = overlay_ids
            .iter()
            .enumerate()
            .map(|(i, id)| (id.as_str(), base_count + i as u32))
            .collect();
        let mut scores: HashMap<u32, f64> = HashMap::new();

        for term in &query_terms {
            let docs_with_term = term_df(term) as f64;
            if docs_with_term == 0.0 {
                continue;
            }
            let idf = bm25_idf(total_docs as f64, docs_with_term);
            let mut score_one = |slot: u32, freq: u32, dl: u32| {
                let contribution = bm25_score(freq as f64, dl as f64, avgdl, idf);
                let at_cap = scores.len() >= cap;
                match scores.entry(slot) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        *e.get_mut() += contribution;
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        if !at_cap {
                            e.insert(contribution);
                        }
                    }
                }
            };
            if let Some(base) = &self.base
                && let Some((iter, _)) = base.postings(term)
            {
                for (ord, freq) in iter {
                    if self.dead.contains(&ord) {
                        continue;
                    }
                    let (doc_bucket, _, dl, _) = base.doc_at(ord);
                    if let Some(b) = bucket
                        && doc_bucket != b
                    {
                        continue;
                    }
                    score_one(ord, freq, dl);
                }
            }
            if let Some(postings) = self.data.postings.get(term.as_str()) {
                for posting in postings {
                    let Some(doc_info) = self.data.docs.get(&posting.doc_id) else {
                        continue;
                    };
                    if let Some(b) = bucket
                        && doc_info.bucket != b
                    {
                        continue;
                    }
                    let slot = overlay_slot[posting.doc_id.as_str()];
                    score_one(slot, posting.frequency, doc_info.total_terms);
                }
            }
        }

        let mut results: Vec<SearchResult> = scores
            .into_iter()
            .filter_map(|(slot, score)| {
                if slot < base_count {
                    let (b, k, _, _) = self.base.as_ref().unwrap().doc_at(slot);
                    Some(SearchResult {
                        bucket: b.to_string(),
                        key: k.to_string(),
                        score,
                    })
                } else {
                    let doc_id = overlay_ids[(slot - base_count) as usize];
                    self.data.docs.get(doc_id).map(|info| SearchResult {
                        bucket: info.bucket.clone(),
                        key: info.key.clone(),
                        score,
                    })
                }
            })
            .collect();

        // Same deterministic tie-break as the collection index.
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| (&a.bucket, &a.key).cmp(&(&b.bucket, &b.key)))
        });
        results.truncate(limit);
        results
    }

    /// Fold the overlay + dead set into a fresh `.mtidx` base and free them.
    /// (The pre-mtidx implementation serialized the WHOLE index to one JSON
    /// buffer per flush tick — a resident copy plus a 2-3x write transient.)
    fn persist(&mut self) -> Result<()> {
        if self.base.is_some()
            && self.data.docs.is_empty()
            && self.dead.is_empty()
            && self.mtidx_path.exists()
        {
            return Ok(());
        }
        let base_docs = self.base.as_ref().map_or(0, |b| b.doc_count() as u64);
        let doc_count = self.data.docs.len() as u64 + base_docs - self.dead.len() as u64;
        let total_terms = self.data.total_term_count
            + self.base.as_ref().map_or(0, |b| b.total_term_count())
            - self.dead_terms;

        let mut w = crate::mmap_text_index::BtidxWriter::create(
            &self.mtidx_path,
            total_terms,
            doc_count as u32,
        )?;

        // Doc table merge, (bucket, key) order; ordinals are assigned by
        // arrival, so record the remapping both layers' postings need.
        let mut overlay_docs: Vec<(&String, &DocInfo)> = self.data.docs.iter().collect();
        overlay_docs.sort_unstable_by_key(|(_, d)| (&d.bucket, &d.key));
        let base_count = self.base.as_ref().map_or(0u32, |b| b.doc_count());
        const UNMAPPED: u32 = u32::MAX;
        let mut ord_map: Vec<u32> = vec![UNMAPPED; base_count as usize];
        let mut overlay_ord: HashMap<&str, u32> = HashMap::new();
        {
            let mut ov = overlay_docs.iter().peekable();
            let mut err: Option<std::io::Error> = None;
            if let Some(base) = &self.base {
                for old in 0..base_count {
                    if err.is_some() {
                        break;
                    }
                    if self.dead.contains(&old) {
                        continue;
                    }
                    let (b, k, dl, bytes) = base.doc_at(old);
                    while let Some((id, d)) = ov.peek() {
                        if (d.bucket.as_str(), d.key.as_str()) < (b, k) {
                            match w.push_doc(&d.bucket, &d.key, d.total_terms, d.text_bytes) {
                                Ok(ord) => {
                                    overlay_ord.insert(id.as_str(), ord);
                                }
                                Err(e) => {
                                    err = Some(e);
                                    break;
                                }
                            }
                            ov.next();
                        } else {
                            break;
                        }
                    }
                    if err.is_some() {
                        break;
                    }
                    match w.push_doc(b, k, dl, bytes) {
                        Ok(ord) => ord_map[old as usize] = ord,
                        Err(e) => err = Some(e),
                    }
                }
            }
            if let Some(e) = err {
                return Err(e.into());
            }
            for (id, d) in ov {
                let ord = w.push_doc(&d.bucket, &d.key, d.total_terms, d.text_bytes)?;
                overlay_ord.insert(id.as_str(), ord);
            }
        }

        // Term merge-join, base file order against the overlay's sorted
        // vocabulary; per-term postings remap to the new ordinals.
        let merged = |term: &str,
                      base_iter: Option<crate::mmap_text_index::BlobPostingsIter<'_>>|
         -> Vec<(u32, u32)> {
            let mut out: Vec<(u32, u32)> = Vec::new();
            if let Some(iter) = base_iter {
                out.extend(iter.filter_map(|(old, freq)| {
                    let new = ord_map[old as usize];
                    (new != UNMAPPED).then_some((new, freq))
                }));
            }
            if let Some(list) = self.data.postings.get(term) {
                out.extend(list.iter().filter_map(|p| {
                    overlay_ord
                        .get(p.doc_id.as_str())
                        .map(|&o| (o, p.frequency))
                }));
            }
            out.sort_unstable_by_key(|&(ord, _)| ord);
            out
        };

        let mut overlay_terms: Vec<&String> = self.data.postings.keys().collect();
        overlay_terms.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        let mut ov = overlay_terms.iter().peekable();
        let mut err: Option<std::io::Error> = None;
        if let Some(base) = &self.base {
            base.for_each_term(|term, base_iter| {
                if err.is_some() {
                    return;
                }
                while let Some(o) = ov.peek() {
                    if o.as_bytes() < term.as_bytes() {
                        let o = ov.next().unwrap();
                        let m = merged(o, None);
                        if !m.is_empty()
                            && let Err(e) = w.push_term(o, m.into_iter())
                        {
                            err = Some(e);
                            return;
                        }
                    } else {
                        break;
                    }
                }
                let in_overlay = ov.peek().is_some_and(|o| o.as_bytes() == term.as_bytes());
                let m = merged(term, Some(base_iter));
                if in_overlay {
                    ov.next();
                }
                if !m.is_empty()
                    && let Err(e) = w.push_term(term, m.into_iter())
                {
                    err = Some(e);
                }
            });
        }
        if let Some(e) = err {
            return Err(e.into());
        }
        for o in ov {
            let m = merged(o, None);
            if !m.is_empty() {
                w.push_term(o, m.into_iter())?;
            }
        }
        w.finish()?;

        // Reopen; REPLACE the overlay (clear keeps bucket capacity).
        self.base = Some(crate::mmap_text_index::MmapBlobTextIndex::open(
            &self.mtidx_path,
        )?);
        self.data = IndexData::default();
        self.dead = std::collections::HashSet::new();
        self.dead_terms = 0;
        Ok(())
    }
}

// ------------------------------------------------------------------
// Per-collection document full-text search
// ------------------------------------------------------------------

use crate::document::DocumentId;

/// Posting entry for a single document in the collection text index.
#[derive(Clone)]
struct DocPosting {
    doc_id: DocumentId,
    frequency: u32,
}

/// In-memory inverted index for full-text search on collection documents.
/// Indexes specified string fields using BM25 scoring.
pub struct CollectionTextIndex {
    fields: Vec<String>,
    /// term → list of postings
    postings: HashMap<std::sync::Arc<str>, Vec<DocPosting>>,
    /// doc_id → total indexed terms count
    doc_term_counts: HashMap<DocumentId, u32>,
    /// doc_id → the distinct terms it contributed, so removal touches only
    /// those posting lists instead of sweeping the whole inverted index.
    ///
    /// The terms are `Arc<str>` SHARED with the postings keys, not owned
    /// `String`s: a copy per document was, measured, **55% of the whole
    /// index** — 558 MB of a 1 GB index at 1M documents
    /// (`examples/fts_mem_bench.rs`) — to store a vocabulary the postings map
    /// already holds. Interning keeps removal O(doc terms) at 8 bytes per
    /// doc-term instead of a String.
    doc_terms: HashMap<DocumentId, Vec<std::sync::Arc<str>>>,
    /// Sum of the OVERLAY's per-doc term counts (in resident mode, of
    /// everything); cached for BM25 avgdl.
    total_term_count: u64,
    /// Disk-first base: the mmap'd `.mtidx` written at the last persist.
    /// Searches merge it with the overlay above; RAM holds only what was
    /// written since. `None` = resident mode (or no persist yet).
    #[cfg(not(target_arch = "wasm32"))]
    base: Option<crate::mmap_text_index::MmapTextIndex>,
    /// Documents whose base postings are void: removed or re-indexed since
    /// the base was written. The base is immutable, so search skips these ids
    /// (Lucene's deleted-docs bitmap); a persist folds them away.
    #[cfg(not(target_arch = "wasm32"))]
    dead: std::collections::HashSet<DocumentId>,
    /// Σ dl of the dead base docs — keeps live corpus stats exact.
    #[cfg(not(target_arch = "wasm32"))]
    dead_terms: u64,
    /// Where the base lives; `None` = resident mode.
    #[cfg(not(target_arch = "wasm32"))]
    path: Option<std::path::PathBuf>,
}

pub struct DocSearchResult {
    pub doc_id: DocumentId,
    pub score: f64,
}

impl CollectionTextIndex {
    pub fn new(fields: Vec<String>) -> Self {
        Self {
            fields,
            postings: HashMap::new(),
            doc_term_counts: HashMap::new(),
            doc_terms: HashMap::new(),
            total_term_count: 0,
            #[cfg(not(target_arch = "wasm32"))]
            base: None,
            #[cfg(not(target_arch = "wasm32"))]
            dead: std::collections::HashSet::new(),
            #[cfg(not(target_arch = "wasm32"))]
            dead_terms: 0,
            #[cfg(not(target_arch = "wasm32"))]
            path: None,
        }
    }

    /// A disk-backed index that has no base yet: everything accumulates in
    /// the overlay until the first [`persist_disk`](Self::persist_disk)
    /// writes `path`.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_disk(fields: Vec<String>, path: std::path::PathBuf) -> Self {
        let mut idx = Self::new(fields);
        idx.path = Some(path);
        idx
    }

    /// Reopen a persisted index: mmap the base, empty overlay — no
    /// collection scan, which is the point. Fails on a missing/torn file;
    /// the caller falls back to the rebuild it always did.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_disk(path: std::path::PathBuf) -> std::io::Result<Self> {
        let base = crate::mmap_text_index::MmapTextIndex::open(&path)?;
        let mut idx = Self::new(base.fields().to_vec());
        idx.base = Some(base);
        idx.path = Some(path);
        Ok(idx)
    }

    /// Live corpus stats across base and overlay: (doc count, token count).
    /// A doc in the overlay that also existed in the base is in `dead`, so
    /// the two layers never double-count.
    fn corpus_stats(&self) -> (u64, u64) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let (mut docs, mut terms) = (self.doc_term_counts.len() as u64, self.total_term_count);
            if let Some(base) = &self.base {
                docs += base.doc_count() as u64 - self.dead.len() as u64;
                terms += base.total_term_count() - self.dead_terms;
            }
            (docs, terms)
        }
        #[cfg(target_arch = "wasm32")]
        {
            (self.doc_term_counts.len() as u64, self.total_term_count)
        }
    }

    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// Extract text from the specified fields of a document value.
    fn extract_doc_text(&self, data: &serde_json::Value) -> String {
        let mut parts = Vec::new();
        for field in &self.fields {
            if let Some(val) = resolve_field(data, field) {
                match val {
                    serde_json::Value::String(s) => parts.push(s.clone()),
                    serde_json::Value::Array(arr) => {
                        for item in arr {
                            if let serde_json::Value::String(s) = item {
                                parts.push(s.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        parts.join(" ")
    }

    /// Index a document. Removes any existing entry for this doc_id first.
    pub fn index_doc(&mut self, doc_id: DocumentId, data: &serde_json::Value) {
        self.remove_doc(doc_id);

        let text = self.extract_doc_text(data);
        let tokens = tokenize(&text);
        let total_terms = tokens.len() as u32;

        if total_terms == 0 {
            return;
        }

        // Count term frequencies
        let mut term_freq: HashMap<String, u32> = HashMap::new();
        for token in &tokens {
            *term_freq.entry(token.clone()).or_insert(0) += 1;
        }

        // Add postings, interning each term: the doc_terms list shares the
        // postings map's own key so the string exists once per vocabulary
        // entry, not once per (document × term).
        let mut terms: Vec<std::sync::Arc<str>> = Vec::with_capacity(term_freq.len());
        for (term, freq) in term_freq {
            let key: std::sync::Arc<str> = match self.postings.get_key_value(term.as_str()) {
                Some((k, _)) => std::sync::Arc::clone(k),
                None => std::sync::Arc::from(term.as_str()),
            };
            terms.push(std::sync::Arc::clone(&key));
            self.postings.entry(key).or_default().push(DocPosting {
                doc_id,
                frequency: freq,
            });
        }

        self.doc_terms.insert(doc_id, terms);
        self.doc_term_counts.insert(doc_id, total_terms);
        self.total_term_count += total_terms as u64;
    }

    /// Remove a document from the index.
    pub fn remove_doc(&mut self, doc_id: DocumentId) {
        // Base postings cannot be edited — tombstone the doc; search skips
        // it, the next persist drops it. `index_doc` re-indexing a base doc
        // lands here first, so an updated doc is served purely from the
        // overlay afterwards.
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(base) = &self.base
            && !self.dead.contains(&doc_id)
            && let Some(dl) = base.doc_len(doc_id)
        {
            self.dead.insert(doc_id);
            self.dead_terms += dl as u64;
        }
        let removed_terms = match self.doc_term_counts.remove(&doc_id) {
            Some(n) => n,
            None => return, // not indexed in the overlay
        };
        self.total_term_count = self.total_term_count.saturating_sub(removed_terms as u64);
        match self.doc_terms.remove(&doc_id) {
            Some(terms) => {
                // Targeted: only the doc's own posting lists — O(doc terms)
                // instead of a sweep over the whole inverted index.
                for term in &terms {
                    if let Some(postings) = self.postings.get_mut(&**term) {
                        postings.retain(|p| p.doc_id != doc_id);
                        if postings.is_empty() {
                            self.postings.remove(&**term);
                        }
                    }
                }
            }
            None => {
                // Shouldn't happen (both maps are written together), but a
                // full sweep keeps the index consistent if it ever does.
                let mut empty_terms = Vec::new();
                for (term, postings) in self.postings.iter_mut() {
                    postings.retain(|p| p.doc_id != doc_id);
                    if postings.is_empty() {
                        empty_terms.push(term.clone());
                    }
                }
                for term in empty_terms {
                    self.postings.remove(&term);
                }
            }
        }
    }

    /// A term's document frequency across base + overlay.
    ///
    /// The base count includes dead docs until the next persist — counting
    /// them out would cost a walk of the posting list before scoring even
    /// starts. Lucene's docFreq makes the same trade until a segment merge;
    /// the persist tick keeps the window to seconds.
    fn term_df(&self, term: &str) -> usize {
        let overlay = self.postings.get(term).map_or(0, Vec::len);
        #[cfg(not(target_arch = "wasm32"))]
        {
            overlay
                + self
                    .base
                    .as_ref()
                    .and_then(|b| b.postings(term).map(|(_, df)| df as usize))
                    .unwrap_or(0)
        }
        #[cfg(target_arch = "wasm32")]
        overlay
    }

    /// Fold the overlay and the dead set into a fresh `.mtidx` base and drop
    /// them from RAM. No-op in resident mode, and skip-clean like every other
    /// disk index — the periodic tick calls this for free when idle.
    ///
    /// The write streams: base terms (sorted in the file) merge-join the
    /// overlay's sorted vocabulary, each term's postings merging base-minus-
    /// dead with the overlay's list — nothing document-proportional is
    /// materialized.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn persist_disk(&mut self) -> std::io::Result<()> {
        let Some(path) = self.path.clone() else {
            return Ok(()); // resident mode
        };
        if self.base.is_some()
            && self.doc_term_counts.is_empty()
            && self.dead.is_empty()
            && path.exists()
        {
            return Ok(());
        }

        let (doc_count, total_terms) = self.corpus_stats();
        let mut w = crate::mmap_text_index::MtidxWriter::create(
            &path,
            &self.fields,
            total_terms,
            doc_count as u32,
        )?;

        // Doc table: base docs (minus dead) merged with overlay docs, id
        // order. Both sides are already sorted (base by construction, overlay
        // sorted here — doc-count-proportional at 12 bytes an entry, the one
        // transient this write allows itself).
        let mut overlay_docs: Vec<(u64, u32)> = self
            .doc_term_counts
            .iter()
            .map(|(&id, &dl)| (id, dl))
            .collect();
        overlay_docs.sort_unstable_by_key(|&(id, _)| id);
        {
            let mut ov = overlay_docs.iter().peekable();
            let mut push_err: Option<std::io::Error> = None;
            if let Some(base) = &self.base {
                base.for_each_doc(|id, dl| {
                    if push_err.is_some() || self.dead.contains(&id) {
                        return;
                    }
                    while let Some(&&(oid, odl)) = ov.peek() {
                        if oid < id {
                            if let Err(e) = w.push_doc(oid, odl) {
                                push_err = Some(e);
                                return;
                            }
                            ov.next();
                        } else {
                            break;
                        }
                    }
                    if let Err(e) = w.push_doc(id, dl) {
                        push_err = Some(e);
                    }
                });
            }
            if let Some(e) = push_err {
                return Err(e);
            }
            for &(oid, odl) in ov {
                w.push_doc(oid, odl)?;
            }
        }

        // Term merge-join: overlay vocabulary sorted once; the base walks in
        // term order already.
        let mut overlay_terms: Vec<&std::sync::Arc<str>> = self.postings.keys().collect();
        overlay_terms.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        let merged_postings = |term: &str,
                               base_iter: Option<crate::mmap_text_index::PostingsIter<'_>>|
         -> Vec<(u64, u32)> {
            let mut out: Vec<(u64, u32)> = Vec::new();
            if let Some(iter) = base_iter {
                out.extend(iter.filter(|(id, _)| !self.dead.contains(id)));
            }
            if let Some(list) = self.postings.get(term) {
                out.extend(list.iter().map(|p| (p.doc_id, p.frequency)));
            }
            out.sort_unstable_by_key(|&(id, _)| id);
            out
        };

        let mut push_err: Option<std::io::Error> = None;
        let mut ov = overlay_terms.iter().peekable();
        if let Some(base) = &self.base {
            base.for_each_term(|term, base_iter| {
                if push_err.is_some() {
                    return;
                }
                // Overlay-only terms that sort before this base term.
                while let Some(o) = ov.peek() {
                    if o.as_bytes() < term.as_bytes() {
                        let o = ov.next().unwrap();
                        let merged = merged_postings(o, None);
                        if !merged.is_empty()
                            && let Err(e) = w.push_term(o, merged.into_iter())
                        {
                            push_err = Some(e);
                            return;
                        }
                    } else {
                        break;
                    }
                }
                // The base term itself, merged with a same-named overlay list.
                let in_overlay = ov.peek().is_some_and(|o| o.as_bytes() == term.as_bytes());
                let merged = merged_postings(term, Some(base_iter));
                if in_overlay {
                    ov.next();
                }
                if !merged.is_empty()
                    && let Err(e) = w.push_term(term, merged.into_iter())
                {
                    push_err = Some(e);
                }
            });
        }
        if let Some(e) = push_err {
            return Err(e);
        }
        for o in ov {
            let merged = merged_postings(o, None);
            if !merged.is_empty() {
                w.push_term(o, merged.into_iter())?;
            }
        }
        w.finish()?;

        // Reopen the fresh base; the overlay and the dead are inside it now.
        // The maps are REPLACED, not `.clear()`ed: clear keeps bucket
        // capacity, and after folding a 1M-doc build the empty skeletons of
        // these tables alone measured ~180 MB resident.
        self.base = Some(crate::mmap_text_index::MmapTextIndex::open(&path)?);
        self.postings = HashMap::new();
        self.doc_term_counts = HashMap::new();
        self.doc_terms = HashMap::new();
        self.total_term_count = 0;
        self.dead = std::collections::HashSet::new();
        self.dead_terms = 0;
        Ok(())
    }

    /// Documents currently in the RAM overlay — what a bulk builder watches
    /// to fold to disk periodically instead of materializing the corpus.
    pub fn overlay_docs(&self) -> usize {
        self.doc_term_counts.len()
    }

    /// wasm stubs, mirroring `MmapFieldIndex`'s: `is_disk_first()` is always
    /// false there, so these are unreachable — they exist so the call sites
    /// compile on both targets.
    #[cfg(target_arch = "wasm32")]
    pub fn new_disk(_fields: Vec<String>, _path: std::path::PathBuf) -> Self {
        unreachable!("disk-first text indexes are not supported on wasm32")
    }
    #[cfg(target_arch = "wasm32")]
    pub fn open_disk(_path: std::path::PathBuf) -> std::io::Result<Self> {
        unreachable!("disk-first text indexes are not supported on wasm32")
    }
    #[cfg(target_arch = "wasm32")]
    pub fn persist_disk(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    /// Search the index. Returns doc_ids with BM25 scores, sorted by score descending.
    pub fn search(&self, query: &str, limit: usize) -> Vec<DocSearchResult> {
        self.search_capped(query, limit, fts_score_cap())
    }

    /// [`search`](Self::search) with an explicit distinct-doc scoring cap —
    /// see [`fts_score_cap`] for why the cap exists. Split out so tests can
    /// exercise the cap without the process-global env knob.
    fn search_capped(&self, query: &str, limit: usize, cap: usize) -> Vec<DocSearchResult> {
        let mut query_terms = tokenize(query);
        let (total_docs, total_terms_live) = self.corpus_stats();
        if query_terms.is_empty() || total_docs == 0 {
            return Vec::new();
        }
        // Rarest term first. With the cap in play the ORDER is the guarantee:
        // every document matching a rare (high-idf, informative) term is
        // scored before the commonest term can fill the budget, so what the
        // cap cuts is the tail of the least informative posting list.
        query_terms.sort_by_key(|t| self.term_df(t));

        let total_docs = total_docs as f64;
        let avgdl = (total_terms_live as f64 / total_docs).max(1.0);
        let mut scores: HashMap<DocumentId, f64> = HashMap::new();

        for term in &query_terms {
            let docs_with_term = self.term_df(term) as f64;
            if docs_with_term == 0.0 {
                continue;
            }
            let idf = bm25_idf(total_docs, docs_with_term);
            let mut score_one = |doc_id: DocumentId, freq: u32, dl: u32| {
                let contribution = bm25_score(freq as f64, dl as f64, avgdl, idf);
                // At the cap, documents already scored keep accumulating
                // (their ranking stays exact); new ones are not admitted.
                let at_cap = scores.len() >= cap;
                match scores.entry(doc_id) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        *e.get_mut() += contribution;
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        if !at_cap {
                            e.insert(contribution);
                        }
                    }
                }
            };
            // Base layer first (its ids predate the overlay's), dead skipped.
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(base) = &self.base
                && let Some((iter, _df)) = base.postings(term)
            {
                for (doc_id, freq) in iter {
                    if self.dead.contains(&doc_id) {
                        continue;
                    }
                    if let Some(dl) = base.doc_len(doc_id) {
                        score_one(doc_id, freq, dl);
                    }
                }
            }
            if let Some(postings) = self.postings.get(term.as_str()) {
                for posting in postings {
                    if let Some(&dl) = self.doc_term_counts.get(&posting.doc_id) {
                        score_one(posting.doc_id, posting.frequency, dl);
                    }
                }
            }
        }

        let mut results: Vec<DocSearchResult> = scores
            .into_iter()
            .map(|(doc_id, score)| DocSearchResult { doc_id, score })
            .collect();

        // Equal scores tie-break on doc_id: without it the order of ties is
        // the score map's iteration order — nondeterministic run to run,
        // which breaks pagination and made disk and resident mode return
        // different (both "correct") orders.
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        results.truncate(limit);
        results
    }

    /// Clear the entire index (used during compaction rebuild).
    pub fn clear(&mut self) {
        self.postings.clear();
        self.doc_term_counts.clear();
        // Pre-existing leak: `clear` (the compaction rebuild) left every
        // doc's term list resident while the postings it referred to were
        // gone.
        self.doc_terms.clear();
        self.total_term_count = 0;
        #[cfg(not(target_arch = "wasm32"))]
        {
            // The rebuild that follows starts from nothing; a stale base file
            // must not survive a crash between here and the next persist.
            self.base = None;
            self.dead.clear();
            self.dead_terms = 0;
            if let Some(p) = &self.path {
                let _ = std::fs::remove_file(p);
            }
        }
    }
}

/// Single highlighted snippet of source text. Words that match the
/// query (after the same tokenization pipeline) are wrapped in
/// `<open_tag>...</close_tag>`; surrounding text is preserved verbatim.
#[derive(Debug, Clone, PartialEq)]
pub struct HighlightSnippet {
    /// The snippet itself, with matched terms wrapped in the tags.
    pub text: String,
    /// Number of distinct query terms matched in this snippet.
    pub matched_terms: usize,
    /// Byte offset of the snippet within the original text.
    pub offset: usize,
}

/// Generate highlighted snippets from `text` for terms in `query`.
///
/// The query and text are tokenized through the same pipeline used by
/// the index (lowercase → strip accents → stop words → stem), so a
/// query for "kitaplar" will highlight "kitap", "kitaplarda", etc. when
/// the configured stemmer is Turkish.
///
/// Returns up to `max_snippets` snippets, each at most `snippet_chars`
/// long, ordered by descending match density. Tags default to `<mark>`
/// / `</mark>` when called via `highlight()`.
pub fn highlight(
    text: &str,
    query: &str,
    snippet_chars: usize,
    max_snippets: usize,
) -> Vec<HighlightSnippet> {
    highlight_with_tags(
        text,
        query,
        snippet_chars,
        max_snippets,
        "<mark>",
        "</mark>",
    )
}

pub fn highlight_with_tags(
    text: &str,
    query: &str,
    snippet_chars: usize,
    max_snippets: usize,
    open_tag: &str,
    close_tag: &str,
) -> Vec<HighlightSnippet> {
    use std::collections::HashSet;

    let query_stems: HashSet<String> = tokenize(query).into_iter().collect();
    if query_stems.is_empty() || max_snippets == 0 || snippet_chars == 0 {
        return Vec::new();
    }

    // Walk the original text, identify word spans (byte ranges), and
    // mark each one as a hit if its stemmed form is in query_stems.
    // We walk char-by-char to keep byte offsets correct for multibyte
    // text (e.g. Turkish ş, ğ).
    struct Span {
        start: usize,
        end: usize,
        is_hit: bool,
    }
    let mut spans: Vec<Span> = Vec::new();
    let mut cur_start: Option<usize> = None;
    let mut bytes_seen = 0usize;
    for (idx, ch) in text.char_indices() {
        bytes_seen = idx + ch.len_utf8();
        if ch.is_alphanumeric() {
            if cur_start.is_none() {
                cur_start = Some(idx);
            }
        } else if let Some(start) = cur_start.take() {
            let end = idx;
            let word = &text[start..end];
            let is_hit = if word.chars().count() > 1
                && !is_stop_word(&strip_accents(&word.to_lowercase()))
            {
                let stemmed = stem(&strip_accents(&word.to_lowercase()));
                query_stems.contains(&stemmed)
            } else {
                false
            };
            spans.push(Span { start, end, is_hit });
        }
    }
    if let Some(start) = cur_start {
        let end = bytes_seen;
        let word = &text[start..end];
        let is_hit =
            if word.chars().count() > 1 && !is_stop_word(&strip_accents(&word.to_lowercase())) {
                let stemmed = stem(&strip_accents(&word.to_lowercase()));
                query_stems.contains(&stemmed)
            } else {
                false
            };
        spans.push(Span { start, end, is_hit });
    }

    // Find hit positions; bail if no matches.
    let hit_indices: Vec<usize> = spans
        .iter()
        .enumerate()
        .filter(|(_, s)| s.is_hit)
        .map(|(i, _)| i)
        .collect();
    if hit_indices.is_empty() {
        return Vec::new();
    }

    // Build snippets: anchor a window on each hit, expanding by chars
    // before/after, deduping overlapping windows by greedy merge.
    let half = snippet_chars / 2;
    let mut windows: Vec<(usize, usize)> = Vec::new();
    for &hi in &hit_indices {
        let span = &spans[hi];
        let win_start = span.start.saturating_sub(half);
        // Snap to char boundary
        let win_start = snap_left(text, win_start);
        let win_end = (span.end + half).min(text.len());
        let win_end = snap_right(text, win_end);
        windows.push((win_start, win_end));
    }
    windows.sort_by_key(|w| w.0);

    // Merge overlapping windows
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for w in windows {
        match merged.last_mut() {
            Some(last) if w.0 <= last.1 => last.1 = last.1.max(w.1),
            _ => merged.push(w),
        }
    }

    // Render each window with hits wrapped in tags.
    let mut snippets: Vec<HighlightSnippet> = Vec::new();
    for (ws, we) in merged.iter() {
        let mut out = String::with_capacity(we - ws + 16);
        let mut cursor = *ws;
        let mut matched: std::collections::HashSet<String> = std::collections::HashSet::new();
        for span in spans.iter() {
            if span.end <= *ws || span.start >= *we {
                continue;
            }
            // Push gap before this span
            if span.start > cursor {
                out.push_str(&text[cursor..span.start]);
            }
            let span_start = span.start.max(*ws);
            let span_end = span.end.min(*we);
            if span.is_hit {
                out.push_str(open_tag);
                out.push_str(&text[span_start..span_end]);
                out.push_str(close_tag);
                matched.insert(text[span_start..span_end].to_lowercase());
            } else {
                out.push_str(&text[span_start..span_end]);
            }
            cursor = span_end;
        }
        // Push tail gap inside window
        if cursor < *we {
            out.push_str(&text[cursor..*we]);
        }
        snippets.push(HighlightSnippet {
            text: out,
            matched_terms: matched.len(),
            offset: *ws,
        });
    }

    // Order by match density (more matches first), then by position.
    snippets.sort_by(|a, b| {
        b.matched_terms
            .cmp(&a.matched_terms)
            .then(a.offset.cmp(&b.offset))
    });
    snippets.truncate(max_snippets);
    snippets
}

fn snap_left(s: &str, mut i: usize) -> usize {
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

fn snap_right(s: &str, mut i: usize) -> usize {
    while i < s.len() && !s.is_char_boundary(i) {
        i += 1;
    }
    i
}

/// Resolve a dotted field path in a JSON value.
fn resolve_field<'a>(data: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = data;
    for part in path.split('.') {
        current = current.as_object()?.get(part)?;
    }
    Some(current)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn extract_text(data: &[u8], content_type: &str) -> Option<String> {
    let ct = content_type.to_lowercase();

    // HTML and XML are different content types that happen to share a handler;
    // merging the arms would hide that they are separately recognised.
    #[allow(clippy::if_same_then_else)]
    if ct.starts_with("text/html") {
        let text = String::from_utf8_lossy(data);
        Some(strip_html_tags(&text))
    } else if ct == "text/xml" || ct == "application/xml" {
        let text = String::from_utf8_lossy(data);
        Some(strip_html_tags(&text))
    } else if ct.starts_with("text/") {
        String::from_utf8(data.to_vec()).ok()
    } else if ct == "application/json" {
        let val: serde_json::Value = serde_json::from_slice(data).ok()?;
        let mut parts = Vec::new();
        extract_json_strings(&val, &mut parts);
        if parts.is_empty() {
            None
        } else {
            Some(parts.join(" "))
        }
    } else if ct == "application/pdf" {
        // Without `doc-formats` (mobile lite builds) these types simply
        // yield no text — the document is stored fine, just not text-indexed,
        // exactly like any other unrecognized content type.
        #[cfg(feature = "doc-formats")]
        {
            extract_pdf(data)
        }
        #[cfg(not(feature = "doc-formats"))]
        {
            None
        }
    } else if ct == "application/vnd.openxmlformats-officedocument.wordprocessingml.document" {
        #[cfg(feature = "doc-formats")]
        {
            extract_docx(data)
        }
        #[cfg(not(feature = "doc-formats"))]
        {
            None
        }
    } else if ct == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" {
        #[cfg(feature = "doc-formats")]
        {
            extract_xlsx(data)
        }
        #[cfg(not(feature = "doc-formats"))]
        {
            None
        }
    } else {
        #[cfg(feature = "ocr")]
        {
            if ct == "image/png" || ct == "image/jpeg" || ct == "image/tiff" || ct == "image/bmp" {
                return extract_image_ocr(data);
            }
        }
        None
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "doc-formats"))]
fn extract_pdf(data: &[u8]) -> Option<String> {
    pdf_extract::extract_text_from_mem(data).ok().and_then(|s| {
        let trimmed = s.trim().to_string();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    })
}

#[cfg(all(not(target_arch = "wasm32"), feature = "doc-formats"))]
fn extract_docx(data: &[u8]) -> Option<String> {
    let cursor = std::io::Cursor::new(data);
    let mut archive = zip::ZipArchive::new(cursor).ok()?;
    let mut xml = String::new();
    {
        let mut file = archive.by_name("word/document.xml").ok()?;
        std::io::Read::read_to_string(&mut file, &mut xml).ok()?;
    }
    let text = strip_html_tags(&xml);
    let trimmed = text.trim().to_string();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

#[cfg(feature = "ocr")]
fn extract_image_ocr(data: &[u8]) -> Option<String> {
    // Tesseract accepts a "+"-joined list of language codes
    // (e.g. "eng+tur"), each backed by a `*.traineddata` file the
    // image must ship. Env-overridable so operators don't need a
    // recompile to add a language.
    let langs = std::env::var("OXIDB_OCR_LANGS").unwrap_or_else(|_| "eng".to_string());
    // Decode → grayscale → 2× upscale → Otsu binarize is a textbook
    // win for messy phone-camera shots. After this Tesseract sees a
    // clean B&W bitmap, which is exactly what its default PSM
    // (single uniform block) is tuned for — so we don't need to
    // poke `tessedit_pageseg_mode` here. Disable via
    // OXIDB_OCR_PREPROCESS=0 if the raw image happens to OCR
    // better (rare in practice).
    let preprocess = std::env::var("OXIDB_OCR_PREPROCESS")
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    let cooked = if preprocess {
        preprocess_for_ocr(data)
    } else {
        None
    };
    let to_feed: &[u8] = cooked.as_deref().unwrap_or(data);

    let mut lt = leptess::LepTess::new(None, &langs).ok()?;
    lt.set_image_from_mem(to_feed).ok()?;
    let text = lt.get_utf8_text().ok()?;
    let trimmed = text.trim().to_string();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

/// Photo-friendly OCR pre-processing pipeline:
///   decode → grayscale → 2× upscale (Lanczos3) → Otsu binarize → PNG
/// Returns `None` on any decoder/encoder failure so the caller can
/// fall back to feeding the raw bytes to Tesseract.
#[cfg(feature = "ocr")]
fn preprocess_for_ocr(data: &[u8]) -> Option<Vec<u8>> {
    use std::io::Cursor;
    let img = image::load_from_memory(data).ok()?;
    let gray = img.to_luma8();

    // 2× upscale with Lanczos3. Phone-camera business cards often
    // ship at ~600 px wide where text glyphs are 8-12 px tall — too
    // small for Tesseract's LSTM. Upscale before threshold so the
    // binarization operates on smoother input.
    let (w, h) = (
        gray.width().saturating_mul(2),
        gray.height().saturating_mul(2),
    );
    if w == 0 || h == 0 {
        return None;
    }
    let upscaled = image::imageops::resize(&gray, w, h, image::imageops::FilterType::Lanczos3);

    // Otsu's method picks the threshold that maximizes the
    // between-class variance — great for bimodal histograms (dark
    // text on light-ish background or vice versa).
    let threshold = otsu_threshold(&upscaled);
    let mut binary: image::ImageBuffer<image::Luma<u8>, Vec<u8>> = image::ImageBuffer::new(w, h);
    for (x, y, p) in upscaled.enumerate_pixels() {
        let v = if p[0] >= threshold { 255 } else { 0 };
        binary.put_pixel(x, y, image::Luma([v]));
    }

    let mut buf = Vec::new();
    binary
        .write_to(&mut Cursor::new(&mut buf), image::ImageFormat::Png)
        .ok()?;
    Some(buf)
}

#[cfg(feature = "ocr")]
fn otsu_threshold(img: &image::ImageBuffer<image::Luma<u8>, Vec<u8>>) -> u8 {
    let mut hist = [0u64; 256];
    for p in img.pixels() {
        hist[p[0] as usize] += 1;
    }
    let total: u64 = (img.width() as u64) * (img.height() as u64);
    if total == 0 {
        return 127;
    }
    let sum_total: u64 = (0..256).map(|i| (i as u64) * hist[i]).sum();
    let mut best_t: u8 = 127;
    let mut best_var: f64 = 0.0;
    let mut w_b: u64 = 0;
    let mut sum_b: u64 = 0;
    for t in 0..256u32 {
        w_b += hist[t as usize];
        if w_b == 0 {
            continue;
        }
        let w_f = total - w_b;
        if w_f == 0 {
            break;
        }
        sum_b += (t as u64) * hist[t as usize];
        let mean_b = sum_b as f64 / w_b as f64;
        let mean_f = (sum_total - sum_b) as f64 / w_f as f64;
        let var = (w_b as f64) * (w_f as f64) * (mean_b - mean_f).powi(2);
        if var > best_var {
            best_var = var;
            best_t = t as u8;
        }
    }
    best_t
}

#[cfg(all(not(target_arch = "wasm32"), feature = "doc-formats"))]
fn extract_xlsx(data: &[u8]) -> Option<String> {
    let cursor = std::io::Cursor::new(data);
    let mut archive = zip::ZipArchive::new(cursor).ok()?;
    let mut xml = String::new();
    {
        let mut file = archive.by_name("xl/sharedStrings.xml").ok()?;
        std::io::Read::read_to_string(&mut file, &mut xml).ok()?;
    }
    // Extract text between <t> and </t> tags
    let mut parts = Vec::new();
    for segment in xml.split("<t") {
        // handle both <t> and <t ...attributes>
        if let Some(rest) = segment.split_once('>')
            && let Some((text, _)) = rest.1.split_once("</t>")
        {
            let t = text.trim();
            if !t.is_empty() {
                parts.push(t.to_string());
            }
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" "))
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn strip_html_tags(html: &str) -> String {
    let mut result = String::with_capacity(html.len());
    let mut in_tag = false;
    for c in html.chars() {
        match c {
            '<' => in_tag = true,
            '>' => {
                in_tag = false;
                result.push(' ');
            }
            _ if !in_tag => result.push(c),
            _ => {}
        }
    }
    result
}

#[cfg(not(target_arch = "wasm32"))]
fn extract_json_strings(val: &serde_json::Value, out: &mut Vec<String>) {
    match val {
        serde_json::Value::String(s) => out.push(s.clone()),
        serde_json::Value::Array(arr) => {
            for v in arr {
                extract_json_strings(v, out);
            }
        }
        serde_json::Value::Object(map) => {
            for v in map.values() {
                extract_json_strings(v, out);
            }
        }
        _ => {}
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    fn temp_index() -> (tempfile::TempDir, FtsIndex) {
        let dir = tempfile::tempdir().unwrap();
        let idx = FtsIndex::open(dir.path()).unwrap();
        (dir, idx)
    }

    #[test]
    fn tokenize_basic() {
        let tokens = tokenize("Hello, World! This is a test.");
        assert!(tokens.contains(&"hello".to_string()));
        assert!(tokens.contains(&"world".to_string()));
        assert!(tokens.contains(&"test".to_string()));
        // stop words removed
        assert!(!tokens.contains(&"this".to_string()));
        assert!(!tokens.contains(&"is".to_string()));
        assert!(!tokens.contains(&"a".to_string()));
    }

    #[test]
    fn tokenize_removes_single_chars() {
        let tokens = tokenize("I am a b c word");
        assert!(!tokens.contains(&"i".to_string()));
        assert!(!tokens.contains(&"b".to_string()));
        assert!(!tokens.contains(&"c".to_string()));
        assert!(tokens.contains(&"am".to_string()));
        assert!(tokens.contains(&"word".to_string()));
    }

    #[test]
    fn index_and_search_single_doc() {
        let (_dir, mut idx) = temp_index();
        idx.index_document("docs", "hello.txt", "Hello world database engine")
            .unwrap();
        let results = idx.search(None, "database", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, "hello.txt");
        assert!(results[0].score > 0.0);
    }

    #[test]
    fn index_and_search_multiple_docs_ranking() {
        let (_dir, mut idx) = temp_index();
        // Doc with "database" mentioned more should rank higher
        idx.index_document("docs", "a.txt", "database database database performance")
            .unwrap();
        idx.index_document("docs", "b.txt", "the quick brown fox database")
            .unwrap();

        let results = idx.search(None, "database", 10);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].key, "a.txt");
        assert_eq!(results[1].key, "b.txt");
        assert!(results[0].score > results[1].score);
    }

    #[test]
    fn search_with_bucket_filter() {
        let (_dir, mut idx) = temp_index();
        idx.index_document("docs", "a.txt", "database engine")
            .unwrap();
        idx.index_document("images", "b.txt", "database image")
            .unwrap();

        let results = idx.search(Some("docs"), "database", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].bucket, "docs");
    }

    #[test]
    fn remove_document_then_search() {
        let (_dir, mut idx) = temp_index();
        idx.index_document("docs", "a.txt", "hello world").unwrap();
        idx.remove_document("docs", "a.txt").unwrap();
        let results = idx.search(None, "hello", 10);
        assert!(results.is_empty());
    }

    #[test]
    fn reindex_same_doc() {
        let (_dir, mut idx) = temp_index();
        idx.index_document("docs", "a.txt", "old content about cats")
            .unwrap();
        idx.index_document("docs", "a.txt", "new content about dogs")
            .unwrap();

        let results = idx.search(None, "cats", 10);
        assert!(results.is_empty());
        let results = idx.search(None, "dogs", 10);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn extract_text_plain() {
        let text = extract_text(b"Hello World", "text/plain");
        assert_eq!(text, Some("Hello World".to_string()));
    }

    #[test]
    fn extract_text_html() {
        let html = b"<html><body><p>Hello</p><b>World</b></body></html>";
        let text = extract_text(html, "text/html").unwrap();
        assert!(text.contains("Hello"));
        assert!(text.contains("World"));
        assert!(!text.contains("<p>"));
    }

    #[test]
    fn extract_text_json() {
        let json = br#"{"title": "Report", "items": ["alpha", "beta"], "count": 5}"#;
        let text = extract_text(json, "application/json").unwrap();
        assert!(text.contains("Report"));
        assert!(text.contains("alpha"));
        assert!(text.contains("beta"));
    }

    #[test]
    fn extract_text_binary_returns_none() {
        let result = extract_text(b"\x00\x01\x02", "application/octet-stream");
        assert!(result.is_none());
    }

    #[test]
    fn search_empty_index() {
        let (_dir, idx) = temp_index();
        let results = idx.search(None, "anything", 10);
        assert!(results.is_empty());
    }

    #[test]
    fn search_no_matching_terms() {
        let (_dir, mut idx) = temp_index();
        idx.index_document("docs", "a.txt", "hello world").unwrap();
        let results = idx.search(None, "xyznonexistent", 10);
        assert!(results.is_empty());
    }

    #[test]
    fn extract_text_xml() {
        let xml = b"<root><item>Hello</item><item>World</item></root>";
        let text = extract_text(xml, "text/xml").unwrap();
        assert!(text.contains("Hello"));
        assert!(text.contains("World"));
        assert!(!text.contains("<item>"));

        // Also test application/xml
        let text2 = extract_text(xml, "application/xml").unwrap();
        assert!(text2.contains("Hello"));
    }

    #[test]
    fn extract_text_csv() {
        let csv = b"name,age\nAlice,30\nBob,25";
        let text = extract_text(csv, "text/csv").unwrap();
        assert!(text.contains("Alice"));
        assert!(text.contains("Bob"));
    }

    #[test]
    #[cfg(feature = "doc-formats")]
    fn extract_text_docx() {
        // Build a minimal DOCX (ZIP with word/document.xml)
        let mut buf = std::io::Cursor::new(Vec::new());
        {
            let mut zip = zip::ZipWriter::new(&mut buf);
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);
            zip.start_file("word/document.xml", options).unwrap();
            std::io::Write::write_all(
                &mut zip,
                b"<w:document><w:body><w:p><w:r><w:t>Hello DOCX World</w:t></w:r></w:p></w:body></w:document>",
            )
            .unwrap();
            zip.finish().unwrap();
        }
        let data = buf.into_inner();
        let text = extract_text(
            &data,
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
        .unwrap();
        assert!(text.contains("Hello DOCX World"));
    }

    #[test]
    #[cfg(feature = "doc-formats")]
    fn extract_text_xlsx() {
        // Build a minimal XLSX (ZIP with xl/sharedStrings.xml)
        let mut buf = std::io::Cursor::new(Vec::new());
        {
            let mut zip = zip::ZipWriter::new(&mut buf);
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);
            zip.start_file("xl/sharedStrings.xml", options).unwrap();
            std::io::Write::write_all(
                &mut zip,
                b"<sst><si><t>Revenue</t></si><si><t>Expenses</t></si></sst>",
            )
            .unwrap();
            zip.finish().unwrap();
        }
        let data = buf.into_inner();
        let text = extract_text(
            &data,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        .unwrap();
        assert!(text.contains("Revenue"));
        assert!(text.contains("Expenses"));
    }

    #[test]
    #[cfg(feature = "doc-formats")]
    fn extract_text_pdf() {
        // Use a minimal valid PDF
        let pdf_bytes = b"%PDF-1.0
1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj
2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj
3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R/Contents 4 0 R/Resources<</Font<</F1 5 0 R>>>>>>endobj
4 0 obj<</Length 44>>
stream
BT /F1 12 Tf 100 700 Td (Hello PDF) Tj ET
endstream
endobj
5 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj
xref
0 6
0000000000 65535 f
0000000009 00000 n
0000000058 00000 n
0000000115 00000 n
0000000266 00000 n
0000000360 00000 n
trailer<</Size 6/Root 1 0 R>>
startxref
431
%%EOF";
        let result = extract_text(pdf_bytes, "application/pdf");
        // pdf_extract may or may not parse this minimal PDF successfully,
        // so we just verify it doesn't panic and returns Some or None
        if let Some(text) = result {
            assert!(text.contains("Hello PDF"));
        }
    }

    #[test]
    #[cfg(feature = "ocr")]
    fn extract_text_image_png_ocr() {
        // Minimal 1x1 white PNG — no text to extract, should return None
        let png: &[u8] = &[
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, // PNG signature
            0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52, // IHDR chunk
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, // 1x1
            0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x77, 0x53, 0xDE, // 8-bit RGB
            0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41, 0x54, // IDAT chunk
            0x08, 0xD7, 0x63, 0xF8, 0xCF, 0xC0, 0x00, 0x00, // deflated data
            0x00, 0x02, 0x00, 0x01, 0xE2, 0x21, 0xBC, 0x33, // checksum
            0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, // IEND chunk
            0xAE, 0x42, 0x60, 0x82,
        ];
        let result = extract_text(png, "image/png");
        assert!(result.is_none());
    }

    #[test]
    #[cfg(feature = "ocr")]
    fn extract_text_image_unsupported_returns_none() {
        // GIF data should not be processed even with OCR enabled
        let gif = b"GIF89a\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00!\xf9\x04\x00\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;";
        let result = extract_text(gif, "image/gif");
        assert!(result.is_none());
    }

    #[test]
    #[cfg(feature = "ocr")]
    fn extract_text_image_corrupt_returns_none() {
        // Garbage bytes claiming to be image/png should not panic
        let garbage = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09";
        let result = extract_text(garbage, "image/png");
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // CollectionTextIndex tests
    // -----------------------------------------------------------------------

    #[test]
    fn collection_text_index_basic() {
        let mut idx = CollectionTextIndex::new(vec!["title".to_string(), "body".to_string()]);
        let doc = serde_json::json!({
            "_id": 1,
            "title": "Rust programming language",
            "body": "Rust is a systems programming language focused on safety"
        });
        idx.index_doc(1, &doc);

        let results = idx.search("rust programming", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, 1);
        assert!(results[0].score > 0.0);
    }

    #[test]
    fn collection_text_index_ranking() {
        let mut idx = CollectionTextIndex::new(vec!["text".to_string()]);
        let doc1 = serde_json::json!({"_id": 1, "text": "database database database engine"});
        let doc2 = serde_json::json!({"_id": 2, "text": "the quick brown fox database"});
        idx.index_doc(1, &doc1);
        idx.index_doc(2, &doc2);

        let results = idx.search("database", 10);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].doc_id, 1); // higher TF
        assert!(results[0].score > results[1].score);
    }

    #[test]
    fn collection_text_index_remove() {
        let mut idx = CollectionTextIndex::new(vec!["text".to_string()]);
        idx.index_doc(1, &serde_json::json!({"text": "hello world"}));
        idx.remove_doc(1);
        let results = idx.search("hello", 10);
        assert!(results.is_empty());
    }

    #[test]
    fn collection_text_index_update() {
        let mut idx = CollectionTextIndex::new(vec!["text".to_string()]);
        idx.index_doc(1, &serde_json::json!({"text": "old content about cats"}));
        idx.index_doc(1, &serde_json::json!({"text": "new content about dogs"}));

        assert!(idx.search("cats", 10).is_empty());
        assert_eq!(idx.search("dogs", 10).len(), 1);
    }

    #[test]
    fn collection_text_index_multi_field() {
        let mut idx = CollectionTextIndex::new(vec!["title".to_string(), "tags".to_string()]);
        let doc = serde_json::json!({
            "title": "Rust Guide",
            "tags": ["programming", "systems", "safety"]
        });
        idx.index_doc(1, &doc);

        assert_eq!(idx.search("rust", 10).len(), 1);
        assert_eq!(idx.search("programming", 10).len(), 1);
        assert_eq!(idx.search("safety", 10).len(), 1);
    }

    #[test]
    fn collection_text_index_no_match() {
        let mut idx = CollectionTextIndex::new(vec!["text".to_string()]);
        idx.index_doc(1, &serde_json::json!({"text": "hello world"}));
        let results = idx.search("xyznonexistent", 10);
        assert!(results.is_empty());
    }

    #[test]
    fn collection_text_index_limit() {
        let mut idx = CollectionTextIndex::new(vec!["text".to_string()]);
        for i in 1..=10 {
            idx.index_doc(
                i,
                &serde_json::json!({"text": format!("document about databases {}", i)}),
            );
        }
        let results = idx.search("databases", 3);
        assert_eq!(results.len(), 3);
    }

    /// The scoring cap must never cost a document that matches a RARE query
    /// term its place in the results. The fixture is adversarial on purpose:
    /// the rare document is indexed LAST, so its postings sit at the tail of
    /// the common term's list — a search that processed terms in query order
    /// (or didn't order them at all) fills the cap with common-term docs and
    /// never scores it. Rarest-first processing is what this pins.
    #[test]
    fn the_scoring_cap_cannot_evict_a_rare_term_match() {
        let mut idx = CollectionTextIndex::new(vec!["text".to_string()]);
        // 40 docs with only the common term...
        for i in 1..=40 {
            idx.index_doc(i, &serde_json::json!({"text": format!("ortak konu {i}")}));
        }
        // ...then the one doc that also matches the rare term, indexed last.
        idx.index_doc(41, &serde_json::json!({"text": "ortak konu zümrütlü"}));

        // Cap far below the common term's match count.
        let results = idx.search_capped("ortak zümrütlü", 10, 5);
        assert!(
            results.iter().any(|r| r.doc_id == 41),
            "the rare-term document was cut by the cap: {:?}",
            results.iter().map(|r| r.doc_id).collect::<Vec<_>>()
        );
        // And it ranks first — it matched both terms, one of them rare.
        assert_eq!(results[0].doc_id, 41);
    }

    /// At the cap, a common-term-only search still answers — degraded to the
    /// capped candidate set, never empty. This is why the cap is not a term
    /// SKIP: dropping common terms would make a query made only of them
    /// return nothing.
    #[test]
    fn a_common_only_search_at_the_cap_still_answers() {
        let mut idx = CollectionTextIndex::new(vec!["text".to_string()]);
        for i in 1..=40 {
            idx.index_doc(i, &serde_json::json!({"text": format!("ortak konu {i}")}));
        }
        let results = idx.search_capped("ortak", 10, 5);
        assert_eq!(
            results.len(),
            5,
            "cap bounds candidates, limit bounds output"
        );
        let results = idx.search_capped("ortak", 3, 5);
        assert_eq!(results.len(), 3);
    }

    /// Documents scored before the cap was reached keep ACCUMULATING from
    /// later terms — the cap gates admission, not addition. Otherwise a
    /// two-term match inside the cap would rank as a one-term match.
    #[test]
    fn a_doc_admitted_before_the_cap_still_accumulates_later_terms() {
        let mut idx = CollectionTextIndex::new(vec!["text".to_string()]);
        idx.index_doc(1, &serde_json::json!({"text": "zümrütlü ortak"}));
        for i in 2..=20 {
            idx.index_doc(i, &serde_json::json!({"text": format!("ortak konu {i}")}));
        }
        // Cap of 1: only doc 1 (rare term processed first) is admitted; its
        // "ortak" contribution must still land on top.
        let capped = idx.search_capped("zümrütlü ortak", 10, 1);
        assert_eq!(capped.len(), 1);
        let uncapped = idx.search_capped("zümrütlü ortak", 10, usize::MAX);
        let uncapped_doc1 = uncapped.iter().find(|r| r.doc_id == 1).unwrap();
        assert!(
            (capped[0].score - uncapped_doc1.score).abs() < 1e-9,
            "capped score {} != uncapped score {} — the second term's \
             contribution was dropped for an admitted doc",
            capped[0].score,
            uncapped_doc1.score
        );
    }

    /// Disk mode must be indistinguishable from resident mode — same ops,
    /// same searches, same scores — with persists (base rewrites) interleaved
    /// at every stage so base-only, overlay-only and mixed layers are all
    /// exercised. Scores compare exactly right after a persist (dead set
    /// empty ⇒ df exact); in the window between persists only membership is
    /// compared, since base df counting dead docs is a documented
    /// approximation.
    #[test]
    fn disk_mode_matches_resident_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.mtidx");
        let fields = vec!["text".to_string()];
        let mut ram = CollectionTextIndex::new(fields.clone());
        let mut disk = CollectionTextIndex::new_disk(fields, path.clone());

        let doc = |t: &str| serde_json::json!({"text": t});
        let both = |ram: &mut CollectionTextIndex,
                    disk: &mut CollectionTextIndex,
                    f: &dyn Fn(&mut CollectionTextIndex)| {
            f(ram);
            f(disk);
        };

        for i in 1..=30u64 {
            let text = format!(
                "ortak belge {} {}",
                i,
                if i % 3 == 0 { "nadir" } else { "dolgu" }
            );
            both(&mut ram, &mut disk, &|x| x.index_doc(i, &doc(&text)));
        }
        disk.persist_disk().unwrap();

        // Mutations across the persisted base: update, remove, insert.
        both(&mut ram, &mut disk, &|x| {
            x.index_doc(3, &doc("ortak belge tamamen yeni zümrüt"))
        });
        both(&mut ram, &mut disk, &|x| x.remove_doc(6));
        both(&mut ram, &mut disk, &|x| {
            x.index_doc(31, &doc("ortak nadir taze"))
        });

        // Mid-window: membership must agree (scores may differ by the dead-df
        // approximation, which persist erases below).
        for q in ["ortak", "nadir", "zümrüt", "taze", "dolgu"] {
            let r: Vec<u64> = ram.search(q, 100).into_iter().map(|x| x.doc_id).collect();
            let d: Vec<u64> = disk.search(q, 100).into_iter().map(|x| x.doc_id).collect();
            let (mut rs, mut ds) = (r.clone(), d.clone());
            rs.sort_unstable();
            ds.sort_unstable();
            assert_eq!(rs, ds, "membership diverged mid-window for {q:?}");
        }

        disk.persist_disk().unwrap();
        for q in ["ortak", "nadir", "zümrüt", "taze", "dolgu", "yok"] {
            let r = ram.search(q, 100);
            let d = disk.search(q, 100);
            assert_eq!(r.len(), d.len(), "result count diverged for {q:?}");
            for (a, b) in r.iter().zip(d.iter()) {
                assert_eq!(a.doc_id, b.doc_id, "order diverged for {q:?}");
                assert!(
                    (a.score - b.score).abs() < 1e-9,
                    "score diverged for {q:?}: {} vs {}",
                    a.score,
                    b.score
                );
            }
        }
    }

    /// The point of the disk form: reopen mmaps the base instead of scanning
    /// the collection — and the reopened index answers identically, accepts
    /// new writes, and persists again.
    #[test]
    fn a_persisted_index_reopens_without_a_rebuild_and_keeps_working() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.mtidx");
        let doc = |t: &str| serde_json::json!({"text": t});

        let mut idx = CollectionTextIndex::new_disk(vec!["text".into()], path.clone());
        idx.index_doc(1, &doc("eski kayıt hakkında"));
        idx.index_doc(2, &doc("eski ama farklı konu"));
        idx.persist_disk().unwrap();
        drop(idx);

        let mut idx = CollectionTextIndex::open_disk(path.clone()).unwrap();
        assert_eq!(idx.fields(), &["text".to_string()]);
        let r = idx.search("eski", 10);
        assert_eq!(r.len(), 2);

        // Writes after reopen: a new doc, an update, a delete — all served.
        idx.index_doc(3, &doc("yepyeni eski"));
        idx.index_doc(1, &doc("güncellenmiş metin"));
        idx.remove_doc(2);
        let ids: Vec<u64> = idx
            .search("eski", 10)
            .into_iter()
            .map(|r| r.doc_id)
            .collect();
        assert_eq!(ids, vec![3], "update and delete must void base postings");
        assert_eq!(idx.search("güncellenmiş", 10)[0].doc_id, 1);

        // And the folded state survives another persist+reopen.
        idx.persist_disk().unwrap();
        drop(idx);
        let idx = CollectionTextIndex::open_disk(path).unwrap();
        let ids: Vec<u64> = idx
            .search("eski", 10)
            .into_iter()
            .map(|r| r.doc_id)
            .collect();
        assert_eq!(ids, vec![3]);
        assert!(idx.search("farklı", 10).is_empty(), "removed doc came back");
    }

    /// An idle persist must be free — the maintenance tick calls it every
    /// second for every collection with a text index.
    #[test]
    fn a_clean_persist_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.mtidx");
        let mut idx = CollectionTextIndex::new_disk(vec!["text".into()], path.clone());
        idx.index_doc(1, &serde_json::json!({"text": "bir şey"}));
        idx.persist_disk().unwrap();
        let mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        idx.persist_disk().unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().modified().unwrap(),
            mtime,
            "a clean index rewrote its file"
        );
    }

    #[test]
    fn stemming_matches_variants() {
        // "running" and "runs" both stem to "run"
        let t1 = tokenize("running");
        let t2 = tokenize("runs");
        assert_eq!(t1[0], t2[0]);
        assert_eq!(t1[0], "run");

        // "databases" and "database" both stem to "databas"
        let t3 = tokenize("databases");
        let t4 = tokenize("database");
        assert_eq!(t3[0], t4[0]);

        // "connected" and "connecting" stem to same root
        let t5 = tokenize("connected");
        let t6 = tokenize("connecting");
        assert_eq!(t5[0], t6[0]);
    }

    #[test]
    fn stemming_search_finds_variants() {
        let mut idx = CollectionTextIndex::new(vec!["text".to_string()]);
        idx.index_doc(
            1,
            &serde_json::json!({"text": "the server is running smoothly"}),
        );
        idx.index_doc(
            2,
            &serde_json::json!({"text": "database connections are stable"}),
        );

        // Searching for "runs" should find doc containing "running" (both stem to "run")
        let results = idx.search("runs", 10);
        assert_eq!(results.len(), 1);

        // Searching for "connecting" should find "connections"
        let results = idx.search("connecting", 10);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn accent_stripping() {
        let t1 = tokenize("café résumé naïve");
        assert!(t1.contains(&stem("cafe")));
        assert!(t1.contains(&stem("resume")));
        assert!(t1.contains(&stem("naive")));
    }

    #[test]
    fn turkish_stop_words_filtered() {
        let tokens = tokenize("bu bir test ve deneme");
        // "bu", "bir", "ve" are Turkish stop words
        assert!(!tokens.iter().any(|t| t == "bu"));
        assert!(!tokens.iter().any(|t| t == "bir"));
        assert!(!tokens.iter().any(|t| t == "ve"));
        // "test" and "deneme" should remain (possibly stemmed)
        assert!(tokens.len() >= 2);
    }

    #[test]
    fn turkish_characters_normalized() {
        let tokens = tokenize("güneş çiçek şehir");
        // ğ→g, ş→s, ç→c, ü→u
        assert!(tokens.iter().any(|t| t.contains("gune")));
        assert!(tokens.iter().any(|t| t.contains("cicek")));
        assert!(tokens.iter().any(|t| t.contains("sehir")));
    }

    // -----------------------------------------------------------------------
    // BM25-specific behavior
    // -----------------------------------------------------------------------

    #[test]
    fn bm25_length_normalization_short_doc_wins() {
        // Same TF (=1), different doc length → shorter doc ranks higher.
        let mut idx = CollectionTextIndex::new(vec!["text".to_string()]);
        idx.index_doc(1, &serde_json::json!({"text": "rust language"}));
        idx.index_doc(
            2,
            &serde_json::json!({
                "text": "rust language one many programming languages used widely \
                         across many domains applications including systems web \
                         embedded performance reliability"
            }),
        );

        let results = idx.search("rust", 10);
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0].doc_id, 1,
            "shorter doc should rank higher under BM25"
        );
        assert!(results[0].score > results[1].score);
    }

    #[test]
    fn bm25_tf_saturates() {
        // Doubling TF must NOT double the score — BM25 saturates via k1.
        // Both docs have the same length so length normalization is neutral.
        let mut idx = CollectionTextIndex::new(vec!["text".to_string()]);
        idx.index_doc(
            1,
            &serde_json::json!({"text": "rust alpha beta gamma delta epsilon zeta eta theta"}),
        );
        idx.index_doc(
            2,
            &serde_json::json!({"text": "rust rust rust rust rust rust rust rust rust"}),
        );
        // corpus padding so IDF is non-trivial
        for i in 3..=10 {
            idx.index_doc(
                i,
                &serde_json::json!({"text": format!("filler{} content", i)}),
            );
        }

        let r = idx.search("rust", 10);
        let s1 = r.iter().find(|x| x.doc_id == 1).unwrap().score;
        let s2 = r.iter().find(|x| x.doc_id == 2).unwrap().score;

        assert!(s2 > s1, "more TF should still rank higher");
        // 9× TF must yield clearly less than 9× score
        assert!(
            s2 < s1 * 9.0,
            "BM25 should saturate, not scale linearly: s1={s1} s2={s2}"
        );
    }

    #[test]
    fn bm25_avgdl_maintained_across_index_remove_reindex() {
        // Stress the total_term_count cache: index, remove, re-index,
        // then verify search still works and ranks correctly.
        let mut idx = CollectionTextIndex::new(vec!["text".to_string()]);
        idx.index_doc(1, &serde_json::json!({"text": "rust language guide"}));
        idx.index_doc(2, &serde_json::json!({"text": "go language guide"}));
        idx.remove_doc(1);
        idx.index_doc(
            1,
            &serde_json::json!({"text": "rust performance optimization manual"}),
        );

        // Cache must equal the sum of remaining doc lengths.
        let expected: u64 = idx.doc_term_counts.values().map(|&n| n as u64).sum();
        assert_eq!(idx.total_term_count, expected);

        let r = idx.search("rust", 10);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].doc_id, 1);
        assert!(r[0].score > 0.0);
    }

    #[test]
    fn fts_config_parses_k1() {
        assert_eq!(parse_k1(None), BM25_K1_DEFAULT);
        assert_eq!(parse_k1(Some("1.5")), 1.5);
        assert_eq!(parse_k1(Some("2.0")), 2.0);
        // Invalid: non-numeric, zero, negative, infinity → falls back to default
        assert_eq!(parse_k1(Some("nope")), BM25_K1_DEFAULT);
        assert_eq!(parse_k1(Some("0")), BM25_K1_DEFAULT);
        assert_eq!(parse_k1(Some("-1")), BM25_K1_DEFAULT);
        assert_eq!(parse_k1(Some("inf")), BM25_K1_DEFAULT);
    }

    #[test]
    fn fts_config_parses_b() {
        assert_eq!(parse_b(None), BM25_B_DEFAULT);
        assert_eq!(parse_b(Some("0.0")), 0.0);
        assert_eq!(parse_b(Some("0.5")), 0.5);
        assert_eq!(parse_b(Some("1.0")), 1.0);
        // Out of [0, 1] range → falls back
        assert_eq!(parse_b(Some("1.5")), BM25_B_DEFAULT);
        assert_eq!(parse_b(Some("-0.1")), BM25_B_DEFAULT);
        assert_eq!(parse_b(Some("nope")), BM25_B_DEFAULT);
    }

    #[test]
    fn fts_config_parses_lang() {
        use rust_stemmers::Algorithm;
        // Default → English
        assert!(matches!(parse_lang(None), Algorithm::English));
        assert!(matches!(parse_lang(Some("")), Algorithm::English));
        assert!(matches!(parse_lang(Some("nonsense")), Algorithm::English));
        // Turkish aliases
        assert!(matches!(parse_lang(Some("tr")), Algorithm::Turkish));
        assert!(matches!(parse_lang(Some("TR")), Algorithm::Turkish));
        assert!(matches!(parse_lang(Some("turkish")), Algorithm::Turkish));
        assert!(matches!(parse_lang(Some("Turkish")), Algorithm::Turkish));
        assert!(matches!(parse_lang(Some("  TUR  ")), Algorithm::Turkish));
        // A few other languages
        assert!(matches!(parse_lang(Some("de")), Algorithm::German));
        assert!(matches!(parse_lang(Some("fr")), Algorithm::French));
        assert!(matches!(parse_lang(Some("ar")), Algorithm::Arabic));
    }

    #[test]
    fn turkish_stemmer_collapses_inflections() {
        // Verify that the Turkish stemmer is wired up correctly. We bypass
        // the global OnceLock-cached stemmer (which may already be set to
        // English in this test process) and call the algorithm directly.
        use rust_stemmers::{Algorithm, Stemmer};
        let tr = Stemmer::create(Algorithm::Turkish);
        // "kitap" (book) and its inflections should collapse to a common stem.
        let s_kitap = tr.stem("kitap").to_string();
        let s_kitaplar = tr.stem("kitaplar").to_string(); // books
        let s_kitabi = tr.stem("kitabı").to_string(); // his/her book
        let s_kitaplarda = tr.stem("kitaplarda").to_string(); // in the books
        // At minimum, kitap and kitaplar should match.
        assert_eq!(s_kitap, s_kitaplar);
        // The "-da/-de" locative variant should also collapse.
        assert_eq!(s_kitap, s_kitaplarda);
        // Inflected genitive form starts from the root prefix.
        assert!(
            s_kitabi.starts_with("kit"),
            "expected kitabı to stem near 'kit*', got {s_kitabi}"
        );
    }

    // -----------------------------------------------------------------------
    // Highlighting
    // -----------------------------------------------------------------------

    #[test]
    fn highlight_basic_match() {
        let text = "Rust is a systems programming language focused on safety";
        let snippets = highlight(text, "programming", 60, 3);
        assert_eq!(snippets.len(), 1);
        assert!(snippets[0].text.contains("<mark>programming</mark>"));
        assert_eq!(snippets[0].matched_terms, 1);
    }

    #[test]
    fn highlight_no_match_returns_empty() {
        let text = "Rust is a systems programming language";
        let snippets = highlight(text, "javascript", 60, 3);
        assert!(snippets.is_empty());
    }

    #[test]
    fn highlight_uses_same_tokenization_as_index() {
        // Searching for "running" must find "run" / "runs" via stemming.
        let text = "the server runs every night";
        let snippets = highlight(text, "running", 60, 3);
        assert_eq!(snippets.len(), 1);
        assert!(snippets[0].text.contains("<mark>runs</mark>"));
    }

    #[test]
    fn highlight_multiple_distinct_terms() {
        let text = "OxiDB supports rust programming and database engineering";
        let snippets = highlight(text, "rust database", 80, 3);
        assert_eq!(snippets.len(), 1);
        assert!(snippets[0].text.contains("<mark>rust</mark>"));
        assert!(snippets[0].text.contains("<mark>database</mark>"));
        assert_eq!(snippets[0].matched_terms, 2);
    }

    #[test]
    fn highlight_overlapping_windows_merge() {
        // Two hits within the snippet window should produce one merged
        // snippet, not two separate snippets that overlap.
        let text = "alpha beta gamma rust delta epsilon zeta rust eta theta";
        let snippets = highlight(text, "rust", 60, 5);
        assert_eq!(snippets.len(), 1);
        assert_eq!(snippets[0].text.matches("<mark>rust</mark>").count(), 2);
    }

    #[test]
    fn highlight_custom_tags() {
        let text = "Rust is fast";
        let snippets = highlight_with_tags(text, "rust", 30, 1, "[", "]");
        assert_eq!(snippets.len(), 1);
        assert!(snippets[0].text.contains("[Rust]"));
        assert!(!snippets[0].text.contains("<mark>"));
    }

    #[test]
    fn highlight_preserves_original_casing() {
        // Match comes via lowercase+stem tokenization, but the emitted
        // snippet should keep the original characters intact.
        let text = "The Rust Programming Language is great";
        let snippets = highlight(text, "programming", 80, 3);
        assert_eq!(snippets.len(), 1);
        assert!(
            snippets[0].text.contains("<mark>Programming</mark>"),
            "expected original casing 'Programming' to be wrapped, got: {}",
            snippets[0].text
        );
    }

    #[test]
    fn highlight_handles_multibyte_boundaries() {
        // Make sure char-boundary snapping does not panic on Unicode text
        // even when the snippet window edges fall mid-character.
        let text = "Türkçe metin içinde rust kelimesi geçiyor ş ğ ç ü ö";
        let snippets = highlight(text, "rust", 5, 1);
        assert_eq!(snippets.len(), 1);
        assert!(snippets[0].text.contains("<mark>rust</mark>"));
    }

    #[test]
    fn highlight_empty_query_returns_empty() {
        let snippets = highlight("any text", "", 60, 3);
        assert!(snippets.is_empty());
        let snippets = highlight("any text", "  ", 60, 3);
        assert!(snippets.is_empty());
    }

    #[test]
    fn highlight_zero_max_returns_empty() {
        let snippets = highlight("rust is fast", "rust", 60, 0);
        assert!(snippets.is_empty());
    }

    /// The blob index's disk mode, end to end at the unit layer: build,
    /// persist, mutate across the persisted base, search with and without a
    /// bucket filter, quota accounting across both layers, reopen.
    #[test]
    fn blob_disk_mode_folds_and_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = FtsIndex::open(dir.path()).unwrap();
        idx.set_batched(true);
        for i in 1..=20 {
            let bucket = if i % 2 == 0 { "even" } else { "odd" };
            idx.index_document(
                bucket,
                &format!("f{i}.txt"),
                &format!(
                    "ortak metin {} {}",
                    i,
                    if i == 7 { "zümrüt" } else { "dolgu" }
                ),
            )
            .unwrap();
        }
        idx.flush().unwrap();
        assert!(idx.data.docs.is_empty(), "flush must fold the overlay");
        assert!(dir.path().join("_fts/index.mtidx").exists());

        // Search from the base, bucket-filtered and not.
        assert_eq!(idx.search(None, "ortak", 50).len(), 20);
        assert_eq!(idx.search(Some("even"), "ortak", 50).len(), 10);
        let hit = &idx.search(None, "zümrüt", 10);
        assert_eq!(
            (hit[0].bucket.as_str(), hit[0].key.as_str()),
            ("odd", "f7.txt")
        );

        // Mutations across the base: remove one, re-index another.
        idx.remove_document("odd", "f7.txt").unwrap();
        idx.index_document("even", "f2.txt", "artık bambaşka içerik")
            .unwrap();
        assert!(
            idx.search(None, "zümrüt", 10).is_empty(),
            "removed doc answered"
        );
        assert_eq!(
            idx.search(None, "ortak", 50).len(),
            18,
            "one removed, one re-indexed away from 'ortak'"
        );
        assert_eq!(idx.search(None, "bambaşka", 10).len(), 1);

        // Quota accounting spans base + overlay and skips the dead.
        let even = idx.bucket_text_size("even");
        assert!(even > 0);
        idx.flush().unwrap();
        assert_eq!(
            idx.bucket_text_size("even"),
            even,
            "folding must not change what a bucket is charged"
        );

        // Reopen straight from the base.
        drop(idx);
        let idx = FtsIndex::open(dir.path()).unwrap();
        assert_eq!(idx.search(None, "ortak", 50).len(), 18);
        assert_eq!(idx.search(None, "bambaşka", 10).len(), 1);
        assert!(idx.search(None, "zümrüt", 10).is_empty());
    }

    #[test]
    fn a_legacy_json_index_is_migrated_backfilled_and_still_searchable() {
        // A hand-written pre-BM25, pre-mtidx index.json: total_term_count
        // absent (defaults 0 — must be backfilled from the docs at load) and
        // postings still carrying the long-dead `positions` field (must be
        // ignored, not refused). Open migrates it to the mmap'd base and
        // removes the JSON.
        let dir = tempfile::tempdir().unwrap();
        let fts_dir = dir.path().join("_fts");
        std::fs::create_dir_all(&fts_dir).unwrap();
        let legacy = serde_json::json!({
            "postings": {
                "rust": [{"doc_id": "docs\ta.txt", "frequency": 2, "positions": [0, 3]}],
                "go":   [{"doc_id": "docs\tb.txt", "frequency": 1, "positions": [0]}]
            },
            "docs": {
                "docs\ta.txt": {"bucket": "docs", "key": "a.txt", "total_terms": 4},
                "docs\tb.txt": {"bucket": "docs", "key": "b.txt", "total_terms": 3}
            }
        });
        let json_path = fts_dir.join("index.json");
        std::fs::write(&json_path, serde_json::to_vec(&legacy).unwrap()).unwrap();

        let idx = FtsIndex::open(dir.path()).unwrap();
        // Migrated: mmap'd base carries the backfilled corpus stats, the
        // JSON is gone, nothing stays resident.
        let base = idx.base.as_ref().expect("legacy index migrated to mtidx");
        assert_eq!(base.doc_count(), 2);
        assert_eq!(base.total_term_count(), 7, "backfilled from per-doc totals");
        assert!(
            !json_path.exists(),
            "legacy JSON left behind after migration"
        );
        assert!(idx.data.docs.is_empty());

        let r = idx.search(None, "rust", 10);
        assert_eq!(r.len(), 1);
        assert_eq!((r[0].bucket.as_str(), r[0].key.as_str()), ("docs", "a.txt"));
        assert!(r[0].score > 0.0);

        // And a REOPEN comes straight from the base file.
        drop(idx);
        let idx = FtsIndex::open(dir.path()).unwrap();
        assert_eq!(idx.search(None, "go", 10).len(), 1);
    }
}
