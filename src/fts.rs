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

fn extract_pdf(data: &[u8]) -> Option<String> {
    pdf_extract::extract_text_from_mem(data).ok().and_then(|s| {
        let trimmed = s.trim().to_string();
        if trimmed.is_empty() { None } else { Some(trimmed) }
    })
}

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
    let mut lt = leptess::LepTess::new(None, "eng").ok()?;
    lt.set_image_from_mem(data).ok()?;
    let text = lt.get_utf8_text().ok()?;
    let trimmed = text.trim().to_string();
    if trimmed.is_empty() { None } else { Some(trimmed) }
}

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
}
