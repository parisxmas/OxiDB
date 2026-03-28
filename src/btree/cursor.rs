/// Cursor: iterate leaf pages via prev/next sibling pointers.

use std::sync::Arc;

use super::node::{leaf_entries, leaf_entry_at};
use super::page::{PageId, PageType};
use super::pager::Pager;
use super::tree::BTree;

/// A bidirectional cursor over the leaf pages of a BTree.
pub struct Cursor {
    pager: Arc<Pager>,
    page_id: PageId,
    index: usize,
    exhausted: bool,
}

impl Cursor {
    /// Create a cursor that is not yet positioned.
    #[allow(dead_code)]
    fn new(pager: Arc<Pager>) -> Self {
        Self {
            pager,
            page_id: 0,
            index: 0,
            exhausted: true,
        }
    }

    /// Seek to the first (smallest key) entry in the tree.
    pub fn seek_first(tree: &BTree) -> std::io::Result<Self> {
        let leaf_id = tree.leftmost_leaf()?;
        let page = tree.pager.read_page(leaf_id)?;
        let exhausted = page.num_entries() == 0;
        Ok(Self {
            pager: tree.pager.clone(),
            page_id: leaf_id,
            index: 0,
            exhausted,
        })
    }

    /// Seek to the last (largest key) entry in the tree.
    pub fn seek_last(tree: &BTree) -> std::io::Result<Self> {
        let leaf_id = tree.rightmost_leaf()?;
        let page = tree.pager.read_page(leaf_id)?;
        let n = page.num_entries() as usize;
        let exhausted = n == 0;
        Ok(Self {
            pager: tree.pager.clone(),
            page_id: leaf_id,
            index: if n > 0 { n - 1 } else { 0 },
            exhausted,
        })
    }

    /// Seek to the entry with the given doc_id, or the first entry greater
    /// than doc_id if not found.
    pub fn seek(tree: &BTree, doc_id: u64) -> std::io::Result<Self> {
        // Walk to the leaf that would contain doc_id.
        let mut page_id = tree.root_page_id();
        loop {
            let page = tree.pager.read_page(page_id)?;
            match page.page_type() {
                PageType::Leaf => {
                    // Binary search for doc_id or next greater.
                    let entries = leaf_entries(&page);
                    let pos = entries.partition_point(|(k, _)| *k < doc_id);
                    if pos < entries.len() {
                        return Ok(Self {
                            pager: tree.pager.clone(),
                            page_id,
                            index: pos,
                            exhausted: false,
                        });
                    }
                    // doc_id is greater than all entries on this leaf.
                    // Move to the next leaf.
                    let next = page.next_page();
                    if next == 0 {
                        return Ok(Self {
                            pager: tree.pager.clone(),
                            page_id,
                            index: 0,
                            exhausted: true,
                        });
                    }
                    let next_page = tree.pager.read_page(next)?;
                    let exhausted = next_page.num_entries() == 0;
                    return Ok(Self {
                        pager: tree.pager.clone(),
                        page_id: next,
                        index: 0,
                        exhausted,
                    });
                }
                PageType::Internal => {
                    page_id = super::node::internal_find_child(&page, doc_id);
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "unexpected page type in cursor seek",
                    ));
                }
            }
        }
    }

    /// Advance forward and return the current entry, or None if exhausted.
    pub fn next(&mut self) -> std::io::Result<Option<(u64, Vec<u8>)>> {
        if self.exhausted {
            return Ok(None);
        }

        let page = self.pager.read_page(self.page_id)?;
        let entry = leaf_entry_at(&page, self.index);

        // Advance position.
        self.index += 1;
        if self.index >= page.num_entries() as usize {
            // Move to next leaf.
            let next = page.next_page();
            if next == 0 {
                self.exhausted = true;
            } else {
                self.page_id = next;
                self.index = 0;
                // Check if next page is empty.
                let next_page = self.pager.read_page(next)?;
                if next_page.num_entries() == 0 {
                    self.exhausted = true;
                }
            }
        }

        Ok(entry)
    }

    /// Move backward and return the current entry, or None if exhausted.
    pub fn prev(&mut self) -> std::io::Result<Option<(u64, Vec<u8>)>> {
        if self.exhausted {
            return Ok(None);
        }

        let page = self.pager.read_page(self.page_id)?;
        let entry = leaf_entry_at(&page, self.index);

        // Move position backward.
        if self.index == 0 {
            // Move to previous leaf.
            let prev = page.prev_page();
            if prev == 0 {
                self.exhausted = true;
            } else {
                self.page_id = prev;
                let prev_page = self.pager.read_page(prev)?;
                let n = prev_page.num_entries() as usize;
                if n == 0 {
                    self.exhausted = true;
                } else {
                    self.index = n - 1;
                }
            }
        } else {
            self.index -= 1;
        }

        Ok(entry)
    }

    /// Is the cursor exhausted?
    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::page::{Page, PageType};
    use super::super::pager::Pager;
    use super::super::tree::BTree;
    use tempfile::NamedTempFile;

    fn setup_tree(n: u64) -> BTree {
        let tmp = NamedTempFile::new().unwrap();
        // Leak the tempfile to keep it alive.
        let path = tmp.into_temp_path();
        let pager = Arc::new(Pager::open(&path).unwrap());
        let _hdr = pager.allocate_page();
        let root_id = pager.allocate_page();
        pager.write_page(&Page::new(0, PageType::Header)).unwrap();
        pager.write_page(&Page::new(root_id, PageType::Leaf)).unwrap();
        let tree = BTree::new(pager.clone(), root_id, 0);
        for i in 1..=n {
            tree.insert(i, format!("v{}", i).as_bytes()).unwrap();
        }
        tree
    }

    #[test]
    fn btree_cursor_forward_scan() {
        let tree = setup_tree(100);
        let mut cursor = Cursor::seek_first(&tree).unwrap();
        let mut collected = Vec::new();
        while let Some((id, _data)) = cursor.next().unwrap() {
            collected.push(id);
        }
        assert_eq!(collected.len(), 100);
        // Should be sorted.
        for i in 0..100 {
            assert_eq!(collected[i], (i + 1) as u64);
        }
    }

    #[test]
    fn btree_cursor_reverse_scan() {
        let tree = setup_tree(100);
        let mut cursor = Cursor::seek_last(&tree).unwrap();
        let mut collected = Vec::new();
        while let Some((id, _data)) = cursor.prev().unwrap() {
            collected.push(id);
        }
        assert_eq!(collected.len(), 100);
        // Should be reverse sorted.
        for i in 0..100 {
            assert_eq!(collected[i], (100 - i) as u64);
        }
    }

    #[test]
    fn btree_cursor_seek() {
        let tree = setup_tree(100);
        // Seek to key 50.
        let mut cursor = Cursor::seek(&tree, 50).unwrap();
        let entry = cursor.next().unwrap();
        assert!(entry.is_some());
        let (id, _) = entry.unwrap();
        assert_eq!(id, 50);
    }

    #[test]
    fn btree_cursor_seek_nonexistent() {
        let tree = setup_tree(100);
        // Seek to key 150 (beyond range).
        let mut cursor = Cursor::seek(&tree, 150).unwrap();
        assert!(cursor.next().unwrap().is_none());
    }

    #[test]
    fn btree_cursor_empty_tree() {
        let tree = setup_tree(0);
        let mut cursor = Cursor::seek_first(&tree).unwrap();
        assert!(cursor.next().unwrap().is_none());
    }
}
