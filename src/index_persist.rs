use std::fs;
use std::io::{self, Cursor, Write};
use std::path::Path;

use crc32fast::Hasher;

use crate::document::DocumentId;
use crate::index::{CompositeIndex, FieldIndex};
use crate::vector::VectorIndex;

/// Magic bytes identifying an OxiDB index cache file.
const MAGIC: &[u8; 4] = b"OXIX";
/// Current format version.
const VERSION: u32 = 1;
/// Header size: MAGIC(4) + VERSION(4) + DOC_COUNT(8) + NEXT_ID(8) + BODY_CRC(4) + BODY_LEN(8) = 36
const HEADER_SIZE: usize = 36;

// ---------------------------------------------------------------------------
// Field indexes (.fidx)
// ---------------------------------------------------------------------------

/// Save field indexes to a `.fidx` file atomically (write tmp + rename).
pub fn save_field_indexes(
    path: &Path,
    indexes: &[&FieldIndex],
    doc_count: u64,
    next_id: DocumentId,
) -> io::Result<()> {
    if indexes.is_empty() {
        // No field indexes — remove stale cache file if it exists
        let _ = fs::remove_file(path);
        return Ok(());
    }

    // Serialize body
    let mut body = Vec::new();
    body.write_all(&(indexes.len() as u32).to_le_bytes())?;
    for idx in indexes {
        idx.write_to(&mut body)?;
    }

    write_cache_file(path, &body, doc_count, next_id)
}

/// Load field indexes from a `.fidx` file.
/// Returns `None` if the file doesn't exist, is corrupt, or doc_count/next_id don't match.
pub fn load_field_indexes(
    path: &Path,
    expected_doc_count: u64,
    expected_next_id: DocumentId,
) -> Option<Vec<FieldIndex>> {
    let data = fs::read(path).ok()?;
    let body = validate_cache_file(&data, expected_doc_count, expected_next_id)?;

    let mut cursor = Cursor::new(body);
    let mut len_buf = [0u8; 4];
    io::Read::read_exact(&mut cursor, &mut len_buf).ok()?;
    let count = u32::from_le_bytes(len_buf) as usize;

    let mut indexes = Vec::with_capacity(count);
    for _ in 0..count {
        indexes.push(FieldIndex::read_from(&mut cursor).ok()?);
    }
    Some(indexes)
}

// ---------------------------------------------------------------------------
// Composite indexes (.cidx)
// ---------------------------------------------------------------------------

/// Save composite indexes to a `.cidx` file atomically.
pub fn save_composite_indexes(
    path: &Path,
    indexes: &[&CompositeIndex],
    doc_count: u64,
    next_id: DocumentId,
) -> io::Result<()> {
    if indexes.is_empty() {
        let _ = fs::remove_file(path);
        return Ok(());
    }

    let mut body = Vec::new();
    body.write_all(&(indexes.len() as u32).to_le_bytes())?;
    for idx in indexes {
        idx.write_to(&mut body)?;
    }

    write_cache_file(path, &body, doc_count, next_id)
}

/// Load composite indexes from a `.cidx` file.
pub fn load_composite_indexes(
    path: &Path,
    expected_doc_count: u64,
    expected_next_id: DocumentId,
) -> Option<Vec<CompositeIndex>> {
    let data = fs::read(path).ok()?;
    let body = validate_cache_file(&data, expected_doc_count, expected_next_id)?;

    let mut cursor = Cursor::new(body);
    let mut len_buf = [0u8; 4];
    io::Read::read_exact(&mut cursor, &mut len_buf).ok()?;
    let count = u32::from_le_bytes(len_buf) as usize;

    let mut indexes = Vec::with_capacity(count);
    for _ in 0..count {
        indexes.push(CompositeIndex::read_from(&mut cursor).ok()?);
    }
    Some(indexes)
}

// ---------------------------------------------------------------------------
// Vector indexes (.vidx)
// ---------------------------------------------------------------------------

/// Save vector indexes to a `.vidx` file atomically.
pub fn save_vector_indexes(
    path: &Path,
    indexes: &[&VectorIndex],
    doc_count: u64,
    next_id: DocumentId,
) -> io::Result<()> {
    if indexes.is_empty() {
        let _ = fs::remove_file(path);
        return Ok(());
    }

    let mut body = Vec::new();
    body.write_all(&(indexes.len() as u32).to_le_bytes())?;
    for idx in indexes {
        idx.write_to(&mut body)?;
    }

    write_cache_file(path, &body, doc_count, next_id)
}

/// Load vector indexes from a `.vidx` file.
/// Returns `None` if the file doesn't exist, is corrupt, or doc_count/next_id don't match.
pub fn load_vector_indexes(
    path: &Path,
    expected_doc_count: u64,
    expected_next_id: DocumentId,
) -> Option<Vec<VectorIndex>> {
    let data = fs::read(path).ok()?;
    let body = validate_cache_file(&data, expected_doc_count, expected_next_id)?;

    let mut cursor = Cursor::new(body);
    let mut len_buf = [0u8; 4];
    io::Read::read_exact(&mut cursor, &mut len_buf).ok()?;
    let count = u32::from_le_bytes(len_buf) as usize;

    let mut indexes = Vec::with_capacity(count);
    for _ in 0..count {
        indexes.push(VectorIndex::read_from(&mut cursor).ok()?);
    }
    Some(indexes)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Write a cache file atomically: write to `.tmp`, then rename.
fn write_cache_file(
    path: &Path,
    body: &[u8],
    doc_count: u64,
    next_id: DocumentId,
) -> io::Result<()> {
    let body_crc = {
        let mut h = Hasher::new();
        h.update(body);
        h.finalize()
    };

    let mut header = Vec::with_capacity(HEADER_SIZE);
    header.write_all(MAGIC)?;
    header.write_all(&VERSION.to_le_bytes())?;
    header.write_all(&doc_count.to_le_bytes())?;
    header.write_all(&next_id.to_le_bytes())?;
    header.write_all(&body_crc.to_le_bytes())?;
    header.write_all(&(body.len() as u64).to_le_bytes())?;

    let tmp_path = path.with_extension("tmp");
    let mut file = fs::File::create(&tmp_path)?;
    file.write_all(&header)?;
    file.write_all(body)?;
    file.sync_data()?;
    drop(file);

    fs::rename(&tmp_path, path)?;
    Ok(())
}

/// Validate a cache file's header and CRC. Returns the body slice on success.
fn validate_cache_file(
    data: &[u8],
    expected_doc_count: u64,
    expected_next_id: DocumentId,
) -> Option<&[u8]> {
    if data.len() < HEADER_SIZE {
        return None;
    }

    // Magic
    if &data[0..4] != MAGIC {
        return None;
    }

    // Version
    let version = u32::from_le_bytes(data[4..8].try_into().ok()?);
    if version != VERSION {
        return None;
    }

    // Doc count
    let doc_count = u64::from_le_bytes(data[8..16].try_into().ok()?);
    if doc_count != expected_doc_count {
        return None;
    }

    // Next ID
    let next_id = u64::from_le_bytes(data[16..24].try_into().ok()?);
    if next_id != expected_next_id {
        return None;
    }

    // Body CRC
    let stored_crc = u32::from_le_bytes(data[24..28].try_into().ok()?);

    // Body length
    let body_len = u64::from_le_bytes(data[28..36].try_into().ok()?) as usize;
    if data.len() < HEADER_SIZE + body_len {
        return None;
    }

    let body = &data[HEADER_SIZE..HEADER_SIZE + body_len];

    // Verify CRC
    let mut h = Hasher::new();
    h.update(body);
    if h.finalize() != stored_crc {
        return None;
    }

    Some(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::Document;
    use serde_json::json;
    use tempfile::tempdir;

    fn make_doc(id: u64, data: serde_json::Value) -> Document {
        Document::new(id, data).unwrap()
    }

    #[test]
    fn field_index_save_load_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.fidx");

        let mut idx1 = FieldIndex::new("status".into());
        idx1.insert(&make_doc(1, json!({"status": "active"})));
        idx1.insert(&make_doc(2, json!({"status": "inactive"})));
        idx1.insert(&make_doc(3, json!({"status": "active"})));

        let mut idx2 = FieldIndex::new_unique("email".into());
        idx2.insert(&make_doc(1, json!({"email": "a@b.c"})));
        idx2.insert(&make_doc(2, json!({"email": "d@e.f"})));

        save_field_indexes(&path, &[&idx1, &idx2], 3, 4).unwrap();

        let loaded = load_field_indexes(&path, 3, 4).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].field, "status");
        assert!(!loaded[0].unique);
        assert_eq!(loaded[0].count_all(), 3);
        assert_eq!(loaded[1].field, "email");
        assert!(loaded[1].unique);
        assert_eq!(loaded[1].count_all(), 2);
    }

    #[test]
    fn field_index_stale_doc_count_returns_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.fidx");

        let idx = FieldIndex::new("x".into());
        save_field_indexes(&path, &[&idx], 10, 11).unwrap();

        // Wrong doc_count
        assert!(load_field_indexes(&path, 9, 11).is_none());
        // Wrong next_id
        assert!(load_field_indexes(&path, 10, 12).is_none());
        // Correct
        assert!(load_field_indexes(&path, 10, 11).is_some());
    }

    #[test]
    fn composite_index_save_load_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.cidx");

        let mut idx = CompositeIndex::new(vec!["status".into(), "priority".into()]);
        idx.insert(&make_doc(1, json!({"status": "active", "priority": 1})));
        idx.insert(&make_doc(2, json!({"status": "active", "priority": 5})));
        idx.insert(&make_doc(3, json!({"status": "closed", "priority": 1})));

        save_composite_indexes(&path, &[&idx], 3, 4).unwrap();

        let loaded = load_composite_indexes(&path, 3, 4).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].fields, vec!["status", "priority"]);

        use crate::value::IndexValue;
        let result = loaded[0].find_prefix(&[IndexValue::String("active".into())]);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn corrupt_crc_returns_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt.fidx");

        let idx = FieldIndex::new("x".into());
        save_field_indexes(&path, &[&idx], 0, 1).unwrap();

        // Corrupt a byte in the body
        let mut data = fs::read(&path).unwrap();
        if data.len() > HEADER_SIZE {
            data[HEADER_SIZE] ^= 0xFF;
        }
        fs::write(&path, &data).unwrap();

        assert!(load_field_indexes(&path, 0, 1).is_none());
    }

    #[test]
    fn missing_file_returns_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.fidx");
        assert!(load_field_indexes(&path, 0, 1).is_none());
    }

    #[test]
    fn empty_indexes_removes_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.fidx");

        // Create a file first
        let idx = FieldIndex::new("x".into());
        save_field_indexes(&path, &[&idx], 0, 1).unwrap();
        assert!(path.exists());

        // Save with empty list should remove the file
        save_field_indexes(&path, &[], 0, 1).unwrap();
        assert!(!path.exists());
    }
}
