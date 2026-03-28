/// B-tree node operations on raw pages (leaf and internal).
///
/// Cell-pointer array (SQLite-style): sorted `u16` offsets at the start of
/// the page body (after the 32-byte header), cells grow backward from the
/// end of the page.
///
/// **Leaf cell**: `[doc_id: u64][payload_len: u16][payload: bytes]`
/// **Internal cell**: `[child_page: u32][key_doc_id: u64]` = 12 bytes.
///   The rightmost child pointer is stored at a fixed offset right after
///   the header (bytes 32..36).

use super::page::{Page, PageId, HEADER_SIZE};

/// Size of a cell-pointer entry (u16 offset).
const PTR_SIZE: usize = 2;

/// Fixed offset in internal pages where the rightmost-child PageId is stored.
const RIGHTMOST_CHILD_OFFSET: usize = HEADER_SIZE; // bytes 32..36

/// Start of the cell-pointer array in internal pages (after rightmost child).
const INTERNAL_PTRS_START: usize = RIGHTMOST_CHILD_OFFSET + 4; // 36

/// Start of the cell-pointer array in leaf pages.
const LEAF_PTRS_START: usize = HEADER_SIZE; // 32

/// Leaf cell overhead: doc_id(8) + payload_len(2) = 10 bytes.
const LEAF_CELL_OVERHEAD: usize = 10;

/// Internal cell size: child_page(4) + key_doc_id(8) = 12 bytes.
const INTERNAL_CELL_SIZE: usize = 12;

/// Result of a page split.
#[derive(Debug)]
pub struct SplitInfo {
    pub median_key: u64,
    pub new_page_id: PageId,
}

// ============================================================
// Leaf-node helpers
// ============================================================

/// Available free space in a leaf page for new cells + pointers.
pub fn leaf_free_space(page: &Page) -> usize {
    let n = page.num_entries() as usize;
    let ptrs_end = LEAF_PTRS_START + n * PTR_SIZE;
    let content_start = page.cell_content_start() as usize;
    if content_start <= ptrs_end {
        0
    } else {
        content_start - ptrs_end
    }
}

/// Read the cell-pointer at index `i` in a leaf page.
fn leaf_cell_ptr(page: &Page, i: usize) -> u16 {
    let off = LEAF_PTRS_START + i * PTR_SIZE;
    u16::from_le_bytes(page.data[off..off + 2].try_into().unwrap())
}

/// Write the cell-pointer at index `i` in a leaf page.
fn set_leaf_cell_ptr(page: &mut Page, i: usize, val: u16) {
    let off = LEAF_PTRS_START + i * PTR_SIZE;
    page.data[off..off + 2].copy_from_slice(&val.to_le_bytes());
}

/// Read doc_id from a leaf cell at the given byte offset.
fn leaf_cell_doc_id(page: &Page, offset: usize) -> u64 {
    u64::from_le_bytes(page.data[offset..offset + 8].try_into().unwrap())
}

/// Read payload_len from a leaf cell.
fn leaf_cell_payload_len(page: &Page, offset: usize) -> u16 {
    u16::from_le_bytes(page.data[offset + 8..offset + 10].try_into().unwrap())
}

/// Binary-search the cell-pointer array in a leaf page for `doc_id`.
/// Returns `Ok(index)` if found, `Err(index)` for insertion point.
fn leaf_search(page: &Page, doc_id: u64) -> Result<usize, usize> {
    let n = page.num_entries() as usize;
    if n == 0 {
        return Err(0);
    }
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let ptr = leaf_cell_ptr(page, mid) as usize;
        let key = leaf_cell_doc_id(page, ptr);
        match key.cmp(&doc_id) {
            std::cmp::Ordering::Equal => return Ok(mid),
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
        }
    }
    Err(lo)
}

/// Insert a document into a leaf page.
/// Returns `Some(SplitInfo)` if the page must be split — in that case the
/// caller must provide the new page and handle the split. The caller is
/// responsible for creating the new sibling page before calling this if
/// the insert cannot fit.
///
/// Returns `None` if the insert succeeded without splitting.
pub fn leaf_insert(page: &mut Page, doc_id: u64, payload: &[u8]) -> bool {
    let cell_size = LEAF_CELL_OVERHEAD + payload.len();
    let needed = cell_size + PTR_SIZE; // cell + one pointer slot

    if leaf_free_space(page) < needed {
        return false; // not enough room
    }

    // Find sorted position.
    let pos = match leaf_search(page, doc_id) {
        Ok(_existing) => {
            // Overwrite: delete the old cell first (simple approach: leave
            // the old cell as a fragment).
            leaf_delete(page, doc_id);
            // Re-check space after deletion.
            if leaf_free_space(page) < needed {
                return false;
            }
            match leaf_search(page, doc_id) {
                Err(p) => p,
                Ok(_) => unreachable!(),
            }
        }
        Err(pos) => pos,
    };

    // Allocate cell space at the bottom.
    let content_start = page.cell_content_start() as usize;
    let new_start = content_start - cell_size;
    // Write cell.
    page.data[new_start..new_start + 8].copy_from_slice(&doc_id.to_le_bytes());
    page.data[new_start + 8..new_start + 10]
        .copy_from_slice(&(payload.len() as u16).to_le_bytes());
    page.data[new_start + 10..new_start + 10 + payload.len()].copy_from_slice(payload);
    page.set_cell_content_start(new_start as u16);

    // Shift pointers right to make room at `pos`.
    let n = page.num_entries() as usize;
    // Copy pointers [pos..n] one slot to the right.
    for i in (pos..n).rev() {
        let v = leaf_cell_ptr(page, i);
        set_leaf_cell_ptr(page, i + 1, v);
    }
    set_leaf_cell_ptr(page, pos, new_start as u16);
    page.set_num_entries((n + 1) as u16);
    page.dirty = true;
    true
}

/// Look up a document by doc_id. Returns a reference to the payload bytes.
pub fn leaf_find<'a>(page: &'a Page, doc_id: u64) -> Option<&'a [u8]> {
    if let Ok(idx) = leaf_search(page, doc_id) {
        let ptr = leaf_cell_ptr(page, idx) as usize;
        let plen = leaf_cell_payload_len(page, ptr) as usize;
        Some(&page.data[ptr + LEAF_CELL_OVERHEAD..ptr + LEAF_CELL_OVERHEAD + plen])
    } else {
        None
    }
}

/// Delete a document from a leaf page. Returns true if found and deleted.
/// The cell bytes become fragmented space (not immediately reclaimed).
pub fn leaf_delete(page: &mut Page, doc_id: u64) -> bool {
    let idx = match leaf_search(page, doc_id) {
        Ok(i) => i,
        Err(_) => return false,
    };

    let n = page.num_entries() as usize;

    // Account for the freed cell as fragment space.
    let ptr = leaf_cell_ptr(page, idx) as usize;
    let plen = leaf_cell_payload_len(page, ptr) as usize;
    let cell_size = LEAF_CELL_OVERHEAD + plen;
    let frag = page.fragment_bytes() as usize + cell_size;
    page.set_fragment_bytes(frag as u16);

    // Shift pointers left.
    for i in idx..n - 1 {
        let v = leaf_cell_ptr(page, i + 1);
        set_leaf_cell_ptr(page, i, v);
    }
    // Zero the last slot.
    set_leaf_cell_ptr(page, n - 1, 0);
    page.set_num_entries((n - 1) as u16);
    page.dirty = true;
    true
}

/// Collect all (doc_id, payload) pairs from a leaf, sorted by doc_id.
pub fn leaf_entries(page: &Page) -> Vec<(u64, Vec<u8>)> {
    let n = page.num_entries() as usize;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let ptr = leaf_cell_ptr(page, i) as usize;
        let did = leaf_cell_doc_id(page, ptr);
        let plen = leaf_cell_payload_len(page, ptr) as usize;
        let payload = page.data[ptr + LEAF_CELL_OVERHEAD..ptr + LEAF_CELL_OVERHEAD + plen].to_vec();
        out.push((did, payload));
    }
    out
}

/// Return the entry at the given index position within the leaf's sorted
/// cell-pointer array. Returns `None` if `index >= num_entries`.
pub fn leaf_entry_at(page: &Page, index: usize) -> Option<(u64, Vec<u8>)> {
    let n = page.num_entries() as usize;
    if index >= n {
        return None;
    }
    let ptr = leaf_cell_ptr(page, index) as usize;
    let did = leaf_cell_doc_id(page, ptr);
    let plen = leaf_cell_payload_len(page, ptr) as usize;
    let payload = page.data[ptr + LEAF_CELL_OVERHEAD..ptr + LEAF_CELL_OVERHEAD + plen].to_vec();
    Some((did, payload))
}

// ============================================================
// Internal-node helpers
// ============================================================

/// Read the rightmost child pointer from an internal page.
pub fn internal_rightmost_child(page: &Page) -> PageId {
    u32::from_le_bytes(
        page.data[RIGHTMOST_CHILD_OFFSET..RIGHTMOST_CHILD_OFFSET + 4]
            .try_into()
            .unwrap(),
    )
}

/// Set the rightmost child pointer.
pub fn set_internal_rightmost_child(page: &mut Page, child: PageId) {
    page.data[RIGHTMOST_CHILD_OFFSET..RIGHTMOST_CHILD_OFFSET + 4]
        .copy_from_slice(&child.to_le_bytes());
    page.dirty = true;
}

/// Read the cell-pointer at index `i` in an internal page.
fn internal_cell_ptr(page: &Page, i: usize) -> u16 {
    let off = INTERNAL_PTRS_START + i * PTR_SIZE;
    u16::from_le_bytes(page.data[off..off + 2].try_into().unwrap())
}

fn set_internal_cell_ptr(page: &mut Page, i: usize, val: u16) {
    let off = INTERNAL_PTRS_START + i * PTR_SIZE;
    page.data[off..off + 2].copy_from_slice(&val.to_le_bytes());
}

/// Read (child_page, key_doc_id) from an internal cell.
fn internal_cell_read(page: &Page, offset: usize) -> (PageId, u64) {
    let child = u32::from_le_bytes(page.data[offset..offset + 4].try_into().unwrap());
    let key = u64::from_le_bytes(page.data[offset + 4..offset + 12].try_into().unwrap());
    (child, key)
}

/// Available free space in an internal page.
pub fn internal_free_space(page: &Page) -> usize {
    let n = page.num_entries() as usize;
    let ptrs_end = INTERNAL_PTRS_START + n * PTR_SIZE;
    let content_start = page.cell_content_start() as usize;
    if content_start <= ptrs_end {
        0
    } else {
        content_start - ptrs_end
    }
}

/// Binary search on an internal page. Returns the child page to follow.
pub fn internal_find_child(page: &Page, doc_id: u64) -> PageId {
    let n = page.num_entries() as usize;
    if n == 0 {
        return internal_rightmost_child(page);
    }
    for i in 0..n {
        let ptr = internal_cell_ptr(page, i) as usize;
        let (child, key) = internal_cell_read(page, ptr);
        if doc_id < key {
            return child;
        }
    }
    internal_rightmost_child(page)
}

/// Insert a separator key into an internal page.
/// `left_child` goes into the new cell, `right_child` replaces the
/// rightmost-child of the page if inserting at end, or the existing
/// rightmost child is shuffled down.
///
/// Returns true if insert succeeded, false if not enough space.
pub fn internal_insert(page: &mut Page, key: u64, left_child: PageId, right_child: PageId) -> bool {
    let needed = INTERNAL_CELL_SIZE + PTR_SIZE;
    if internal_free_space(page) < needed {
        return false;
    }

    let n = page.num_entries() as usize;

    // Find sorted position among existing keys.
    let mut pos = n;
    for i in 0..n {
        let ptr = internal_cell_ptr(page, i) as usize;
        let (_, k) = internal_cell_read(page, ptr);
        if key < k {
            pos = i;
            break;
        }
    }

    // Allocate cell at bottom.
    let content_start = page.cell_content_start() as usize;
    let new_start = content_start - INTERNAL_CELL_SIZE;
    page.data[new_start..new_start + 4].copy_from_slice(&left_child.to_le_bytes());
    page.data[new_start + 4..new_start + 12].copy_from_slice(&key.to_le_bytes());
    page.set_cell_content_start(new_start as u16);

    // Shift pointers right.
    for i in (pos..n).rev() {
        let v = internal_cell_ptr(page, i);
        set_internal_cell_ptr(page, i + 1, v);
    }
    set_internal_cell_ptr(page, pos, new_start as u16);
    page.set_num_entries((n + 1) as u16);

    // Set rightmost child to right_child.
    set_internal_rightmost_child(page, right_child);

    page.dirty = true;
    true
}

/// Collect all (key, left_child) pairs from an internal page, sorted by key.
/// The rightmost child is NOT included in the returned vec (retrieve it
/// separately via `internal_rightmost_child`).
pub fn internal_entries(page: &Page) -> Vec<(u64, PageId)> {
    let n = page.num_entries() as usize;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let ptr = internal_cell_ptr(page, i) as usize;
        let (child, key) = internal_cell_read(page, ptr);
        out.push((key, child));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::page::{PageType, PAGE_SIZE};

    #[test]
    fn btree_node_leaf_insert_find_delete() {
        let mut page = Page::new(1, PageType::Leaf);
        assert_eq!(leaf_free_space(&page), PAGE_SIZE - LEAF_PTRS_START);

        // Insert three documents.
        assert!(leaf_insert(&mut page, 10, b"hello"));
        assert!(leaf_insert(&mut page, 5, b"world"));
        assert!(leaf_insert(&mut page, 20, b"test!"));

        assert_eq!(page.num_entries(), 3);

        // Find them.
        assert_eq!(leaf_find(&page, 10), Some(b"hello".as_slice()));
        assert_eq!(leaf_find(&page, 5), Some(b"world".as_slice()));
        assert_eq!(leaf_find(&page, 20), Some(b"test!".as_slice()));
        assert_eq!(leaf_find(&page, 99), None);

        // Delete one.
        assert!(leaf_delete(&mut page, 10));
        assert_eq!(page.num_entries(), 2);
        assert_eq!(leaf_find(&page, 10), None);
        assert!(!leaf_delete(&mut page, 10)); // double delete

        // Remaining entries are sorted.
        let entries = leaf_entries(&page);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, 5);
        assert_eq!(entries[1].0, 20);
    }

    #[test]
    fn btree_node_leaf_split() {
        let mut page = Page::new(1, PageType::Leaf);
        let payload = vec![0xABu8; 200];
        let mut count = 0u64;
        // Fill the page until it's full.
        loop {
            count += 1;
            if !leaf_insert(&mut page, count, &payload) {
                break;
            }
        }
        // We should have inserted many entries.
        assert!(page.num_entries() > 10);

        // Split: take the right half into a new page.
        let entries = leaf_entries(&page);
        let mid = entries.len() / 2;
        let left_entries = &entries[..mid];
        let right_entries = &entries[mid..];

        let mut left = Page::new(1, PageType::Leaf);
        let mut right = Page::new(2, PageType::Leaf);
        for (id, data) in left_entries {
            assert!(leaf_insert(&mut left, *id, data));
        }
        for (id, data) in right_entries {
            assert!(leaf_insert(&mut right, *id, data));
        }
        assert_eq!(left.num_entries() + right.num_entries(), entries.len() as u16);
    }

    #[test]
    fn btree_node_internal_insert_find() {
        let mut page = Page::new(10, PageType::Internal);
        set_internal_rightmost_child(&mut page, 100);

        // Insert keys pointing to child pages.
        // key=50, left_child=200, right_child=100 (unchanged rightmost)
        // After: [200|50] -> rightmost=100
        assert!(internal_insert(&mut page, 50, 200, 100));

        // key=25, left_child=300, right_child=200
        // We need to be careful: internal_insert sets rightmost to right_child.
        // Actually the tree layer handles the rightmost propagation. For unit tests
        // we just check the cell structure.
        assert!(internal_insert(&mut page, 25, 300, 100));

        assert_eq!(page.num_entries(), 2);

        // Find child for doc_id < 25 => should be child at cell for key=25 => 300
        assert_eq!(internal_find_child(&page, 10), 300);
        // doc_id = 30 (between 25 and 50) => child at cell for key=50 => 200
        assert_eq!(internal_find_child(&page, 30), 200);
        // doc_id = 999 (> 50) => rightmost child = 100
        assert_eq!(internal_find_child(&page, 999), 100);
    }

    #[test]
    fn btree_node_internal_split() {
        let mut page = Page::new(10, PageType::Internal);
        set_internal_rightmost_child(&mut page, 999);

        // Fill the internal page.
        let mut count = 0u64;
        loop {
            count += 1;
            if !internal_insert(&mut page, count * 10, count as u32, 999) {
                break;
            }
        }
        assert!(page.num_entries() > 10);

        let entries = internal_entries(&page);
        assert_eq!(entries.len(), page.num_entries() as usize);
        // Verify sorted.
        for w in entries.windows(2) {
            assert!(w[0].0 < w[1].0);
        }
    }

    #[test]
    fn btree_node_leaf_overwrite() {
        let mut page = Page::new(1, PageType::Leaf);
        assert!(leaf_insert(&mut page, 42, b"original"));
        assert_eq!(leaf_find(&page, 42), Some(b"original".as_slice()));

        // Insert same key with different payload — should overwrite.
        assert!(leaf_insert(&mut page, 42, b"updated"));
        assert_eq!(leaf_find(&page, 42), Some(b"updated".as_slice()));
        assert_eq!(page.num_entries(), 1);
    }
}
