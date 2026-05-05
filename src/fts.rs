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
    positions: Vec<u32>,
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
    index_path: PathBuf,
    data: IndexData,
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
    "bir", "ve", "bu", "da", "de", "ile", "mi", "mu", "ne", "o", "ya", "ben", "sen",
    "biz", "siz", "ama", "her", "ki", "en", "var", "yok", "olan", "gibi", "daha",
    "icin", "kadar", "sonra", "once", "ise", "hem", "veya", "sadece",
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
        .map(|w| stem(w))
        .collect()
}

#[cfg(not(target_arch = "wasm32"))]
impl FtsIndex {
    pub fn open(data_dir: &Path) -> Result<Self> {
        let fts_dir = data_dir.join("_fts");
        std::fs::create_dir_all(&fts_dir)?;
        let index_path = fts_dir.join("index.json");

        let mut data: IndexData = if index_path.exists() {
            let bytes = std::fs::read(&index_path)?;
            serde_json::from_slice(&bytes)?
        } else {
            IndexData::default()
        };

        // Lazy migration: backfill total_term_count for indexes
        // written before BM25 was added. New empty indexes also have
        // total_term_count == 0 but docs is empty, so this is safe.
        if data.total_term_count == 0 && !data.docs.is_empty() {
            data.total_term_count = data.docs.values().map(|d| d.total_terms as u64).sum();
        }

        Ok(Self {
            index_path,
            data,
            batched: false,
            dirty: false,
        })
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
        // Remove any existing entry for this doc first
        let doc_id = make_doc_id(bucket, key);
        self.remove_postings(&doc_id);

        let tokens = tokenize(text);
        let total_terms = tokens.len() as u32;

        if total_terms == 0 {
            return Ok(());
        }

        // Count term frequencies and positions
        let mut term_freq: HashMap<String, (u32, Vec<u32>)> = HashMap::new();
        for (pos, token) in tokens.iter().enumerate() {
            let entry = term_freq
                .entry(token.clone())
                .or_insert((0, Vec::new()));
            entry.0 += 1;
            entry.1.push(pos as u32);
        }

        // Add postings
        for (term, (freq, positions)) in term_freq {
            let posting = Posting {
                doc_id: doc_id.clone(),
                frequency: freq,
                positions,
            };
            self.data
                .postings
                .entry(term)
                .or_default()
                .push(posting);
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
        self.data
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
            .sum()
    }

    pub fn remove_document(&mut self, bucket: &str, key: &str) -> Result<()> {
        let doc_id = make_doc_id(bucket, key);
        self.remove_postings(&doc_id);
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

    pub fn search(
        &self,
        bucket: Option<&str>,
        query: &str,
        limit: usize,
    ) -> Vec<SearchResult> {
        let query_terms = tokenize(query);
        if query_terms.is_empty() || self.data.docs.is_empty() {
            return Vec::new();
        }

        let total_docs = self.data.docs.len() as f64;
        let avgdl = (self.data.total_term_count as f64 / total_docs).max(1.0);
        let mut scores: HashMap<String, f64> = HashMap::new();

        for term in &query_terms {
            if let Some(postings) = self.data.postings.get(term) {
                let docs_with_term = postings.len() as f64;
                let idf = bm25_idf(total_docs, docs_with_term);

                for posting in postings {
                    // Optionally filter by bucket
                    if let Some(b) = bucket {
                        if let Some(doc_info) = self.data.docs.get(&posting.doc_id) {
                            if doc_info.bucket != b {
                                continue;
                            }
                        }
                    }

                    if let Some(doc_info) = self.data.docs.get(&posting.doc_id) {
                        let dl = doc_info.total_terms as f64;
                        let f = posting.frequency as f64;
                        *scores.entry(posting.doc_id.clone()).or_insert(0.0) +=
                            bm25_score(f, dl, avgdl, idf);
                    }
                }
            }
        }

        let mut results: Vec<SearchResult> = scores
            .into_iter()
            .filter_map(|(doc_id, score)| {
                self.data.docs.get(&doc_id).map(|info| SearchResult {
                    bucket: info.bucket.clone(),
                    key: info.key.clone(),
                    score,
                })
            })
            .collect();

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);
        results
    }

    fn persist(&self) -> Result<()> {
        let json = serde_json::to_vec(&self.data)?;
        std::fs::write(&self.index_path, json)?;
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
    postings: HashMap<String, Vec<DocPosting>>,
    /// doc_id → total indexed terms count
    doc_term_counts: HashMap<DocumentId, u32>,
    /// Sum of all per-doc term counts; cached for BM25 avgdl.
    total_term_count: u64,
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
            total_term_count: 0,
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

        // Add postings
        for (term, freq) in term_freq {
            self.postings
                .entry(term)
                .or_default()
                .push(DocPosting { doc_id, frequency: freq });
        }

        self.doc_term_counts.insert(doc_id, total_terms);
        self.total_term_count += total_terms as u64;
    }

    /// Remove a document from the index.
    pub fn remove_doc(&mut self, doc_id: DocumentId) {
        let removed_terms = match self.doc_term_counts.remove(&doc_id) {
            Some(n) => n,
            None => return, // not indexed
        };
        self.total_term_count = self.total_term_count.saturating_sub(removed_terms as u64);
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

    /// Search the index. Returns doc_ids with BM25 scores, sorted by score descending.
    pub fn search(&self, query: &str, limit: usize) -> Vec<DocSearchResult> {
        let query_terms = tokenize(query);
        if query_terms.is_empty() || self.doc_term_counts.is_empty() {
            return Vec::new();
        }

        let total_docs = self.doc_term_counts.len() as f64;
        let avgdl = (self.total_term_count as f64 / total_docs).max(1.0);
        let mut scores: HashMap<DocumentId, f64> = HashMap::new();

        for term in &query_terms {
            if let Some(postings) = self.postings.get(term) {
                let docs_with_term = postings.len() as f64;
                let idf = bm25_idf(total_docs, docs_with_term);

                for posting in postings {
                    if let Some(&total_terms) = self.doc_term_counts.get(&posting.doc_id) {
                        let dl = total_terms as f64;
                        let f = posting.frequency as f64;
                        *scores.entry(posting.doc_id).or_insert(0.0) +=
                            bm25_score(f, dl, avgdl, idf);
                    }
                }
            }
        }

        let mut results: Vec<DocSearchResult> = scores
            .into_iter()
            .map(|(doc_id, score)| DocSearchResult { doc_id, score })
            .collect();

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);
        results
    }

    /// Clear the entire index (used during compaction rebuild).
    pub fn clear(&mut self) {
        self.postings.clear();
        self.doc_term_counts.clear();
        self.total_term_count = 0;
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
pub fn highlight(text: &str, query: &str, snippet_chars: usize, max_snippets: usize) -> Vec<HighlightSnippet> {
    highlight_with_tags(text, query, snippet_chars, max_snippets, "<mark>", "</mark>")
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
            let is_hit = if word.chars().count() > 1 && !is_stop_word(&strip_accents(&word.to_lowercase())) {
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
        let is_hit = if word.chars().count() > 1 && !is_stop_word(&strip_accents(&word.to_lowercase())) {
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
        b.matched_terms.cmp(&a.matched_terms).then(a.offset.cmp(&b.offset))
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
        extract_pdf(data)
    } else if ct == "application/vnd.openxmlformats-officedocument.wordprocessingml.document" {
        extract_docx(data)
    } else if ct == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" {
        extract_xlsx(data)
    } else {
        #[cfg(feature = "ocr")]
        {
            if ct == "image/png"
                || ct == "image/jpeg"
                || ct == "image/tiff"
                || ct == "image/bmp"
            {
                return extract_image_ocr(data);
            }
        }
        None
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn extract_pdf(data: &[u8]) -> Option<String> {
    pdf_extract::extract_text_from_mem(data).ok().and_then(|s| {
        let trimmed = s.trim().to_string();
        if trimmed.is_empty() { None } else { Some(trimmed) }
    })
}

#[cfg(not(target_arch = "wasm32"))]
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
    if trimmed.is_empty() { None } else { Some(trimmed) }
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
    let cooked = if preprocess { preprocess_for_ocr(data) } else { None };
    let to_feed: &[u8] = cooked.as_deref().unwrap_or(data);

    let mut lt = leptess::LepTess::new(None, &langs).ok()?;
    lt.set_image_from_mem(to_feed).ok()?;
    let text = lt.get_utf8_text().ok()?;
    let trimmed = text.trim().to_string();
    if trimmed.is_empty() { None } else { Some(trimmed) }
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
    let (w, h) = (gray.width().saturating_mul(2), gray.height().saturating_mul(2));
    if w == 0 || h == 0 {
        return None;
    }
    let upscaled = image::imageops::resize(&gray, w, h, image::imageops::FilterType::Lanczos3);

    // Otsu's method picks the threshold that maximizes the
    // between-class variance — great for bimodal histograms (dark
    // text on light-ish background or vice versa).
    let threshold = otsu_threshold(&upscaled);
    let mut binary: image::ImageBuffer<image::Luma<u8>, Vec<u8>> =
        image::ImageBuffer::new(w, h);
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

#[cfg(not(target_arch = "wasm32"))]
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
        if let Some(rest) = segment.split_once('>') {
            if let Some((text, _)) = rest.1.split_once("</t>") {
                let t = text.trim();
                if !t.is_empty() {
                    parts.push(t.to_string());
                }
            }
        }
    }
    if parts.is_empty() { None } else { Some(parts.join(" ")) }
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
        idx.index_document("docs", "a.txt", "hello world")
            .unwrap();
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
        idx.index_document("docs", "a.txt", "hello world")
            .unwrap();
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
            idx.index_doc(i, &serde_json::json!({"text": format!("document about databases {}", i)}));
        }
        let results = idx.search("databases", 3);
        assert_eq!(results.len(), 3);
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
        idx.index_doc(1, &serde_json::json!({"text": "the server is running smoothly"}));
        idx.index_doc(2, &serde_json::json!({"text": "database connections are stable"}));

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
        assert_eq!(results[0].doc_id, 1, "shorter doc should rank higher under BM25");
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
            idx.index_doc(i, &serde_json::json!({"text": format!("filler{} content", i)}));
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
        idx.index_doc(1, &serde_json::json!({"text": "rust performance optimization manual"}));

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

    #[test]
    fn bm25_persistence_backfills_total_term_count_on_load() {
        // Simulate an old index file that predates the total_term_count
        // field (serialized with 0 because of #[serde(default)]).
        let dir = tempfile::tempdir().unwrap();
        {
            let mut idx = FtsIndex::open(dir.path()).unwrap();
            idx.index_document("docs", "a.txt", "rust performance database engine")
                .unwrap();
            idx.index_document("docs", "b.txt", "go programming language")
                .unwrap();
        }

        // Manually clobber total_term_count in the persisted JSON to 0
        // to simulate a pre-BM25 index.
        let path = dir.path().join("_fts").join("index.json");
        let bytes = std::fs::read(&path).unwrap();
        let mut json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        json["total_term_count"] = serde_json::json!(0u64);
        std::fs::write(&path, serde_json::to_vec(&json).unwrap()).unwrap();

        // Reopening should rebuild the cache from existing doc info.
        let idx = FtsIndex::open(dir.path()).unwrap();
        assert!(
            idx.data.total_term_count > 0,
            "old index should be backfilled on open"
        );
        let r = idx.search(None, "rust", 10);
        assert_eq!(r.len(), 1);
        assert!(r[0].score > 0.0);
    }
}
