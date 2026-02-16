use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::Result;

#[derive(Serialize, Deserialize, Clone)]
struct Posting {
    doc_id: String,
    frequency: u32,
    positions: Vec<u32>,
}

#[derive(Serialize, Deserialize, Clone)]
struct DocInfo {
    bucket: String,
    key: String,
    total_terms: u32,
}

#[derive(Serialize, Deserialize, Default)]
struct IndexData {
    postings: HashMap<String, Vec<Posting>>,
    docs: HashMap<String, DocInfo>,
}

pub struct FtsIndex {
    index_path: PathBuf,
    data: IndexData,
}

pub struct SearchResult {
    pub bucket: String,
    pub key: String,
    pub score: f64,
}

const STOP_WORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "from", "had", "has", "have",
    "he", "her", "his", "if", "in", "into", "is", "it", "its", "no", "not", "of", "on", "or",
    "she", "so", "that", "the", "this", "to", "was", "we", "with", "you",
];

fn make_doc_id(bucket: &str, key: &str) -> String {
    format!("{}\t{}", bucket, key)
}

fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() > 1)
        .filter(|w| !STOP_WORDS.contains(w))
        .map(String::from)
        .collect()
}

impl FtsIndex {
    pub fn open(data_dir: &Path) -> Result<Self> {
        let fts_dir = data_dir.join("_fts");
        std::fs::create_dir_all(&fts_dir)?;
        let index_path = fts_dir.join("index.json");

        let data = if index_path.exists() {
            let bytes = std::fs::read(&index_path)?;
            serde_json::from_slice(&bytes)?
        } else {
            IndexData::default()
        };

        Ok(Self { index_path, data })
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

        // Store doc info
        self.data.docs.insert(
            doc_id,
            DocInfo {
                bucket: bucket.to_string(),
                key: key.to_string(),
                total_terms,
            },
        );

        self.persist()?;
        Ok(())
    }

    pub fn remove_document(&mut self, bucket: &str, key: &str) -> Result<()> {
        let doc_id = make_doc_id(bucket, key);
        self.remove_postings(&doc_id);
        self.data.docs.remove(&doc_id);
        self.persist()?;
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
        let mut scores: HashMap<String, f64> = HashMap::new();

        for term in &query_terms {
            if let Some(postings) = self.data.postings.get(term) {
                let docs_with_term = postings.len() as f64;
                let idf = (total_docs / docs_with_term).ln() + 1.0;

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
                        let tf = posting.frequency as f64 / doc_info.total_terms as f64;
                        *scores.entry(posting.doc_id.clone()).or_insert(0.0) += tf * idf;
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

pub fn extract_text(data: &[u8], content_type: &str) -> Option<String> {
    let ct = content_type.to_lowercase();

    if ct.starts_with("text/html") {
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
    } else {
        None
    }
}

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

#[cfg(test)]
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
}
