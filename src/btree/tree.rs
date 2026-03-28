/// B-tree: find / insert / delete with split propagation.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use super::node::{
    SplitInfo, internal_entries, internal_find_child,
    internal_insert, internal_rightmost_child, leaf_delete, leaf_entries, leaf_find,
    leaf_insert, set_internal_rightmost_child,
};
use super::page::{Page, PageId, PageType};
use super::pager::Pager;

pub struct BTree {
    pub(crate) pager: Arc<Pager>,
    pub(crate) root_page: AtomicU32,
    pub(crate) doc_count: AtomicU64,
}

impl BTree {
    /// Create a new BTree around the given pager. `root_page_id` should point
    /// to an existing leaf page (or 1 for a fresh database).
    pub fn new(pager: Arc<Pager>, root_page_id: PageId, doc_count: u64) -> Self {
        Self {
            pager,
            root_page: AtomicU32::new(root_page_id),
            doc_count: AtomicU64::new(doc_count),
        }
    }

    pub fn root_page_id(&self) -> PageId {
        self.root_page.load(Ordering::Acquire)
    }

    pub fn count(&self) -> u64 {
        self.doc_count.load(Ordering::Acquire)
    }

    /// Find a document by doc_id. Traverses from root to leaf.
    pub fn find(&self, doc_id: u64) -> std::io::Result<Option<Vec<u8>>> {
        let mut page_id = self.root_page.load(Ordering::Acquire);
        loop {
            let page = self.pager.read_page(page_id)?;
            match page.page_type() {
                PageType::Leaf => {
                    return Ok(leaf_find(&page, doc_id).map(|s| s.to_vec()));
                }
                PageType::Internal => {
                    page_id = internal_find_child(&page, doc_id);
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unexpected page type during find: {:?}", page.page_type()),
                    ));
                }
            }
        }
    }

    /// Insert a document. Handles leaf splits and propagation up to the root.
    pub fn insert(&self, doc_id: u64, payload: &[u8]) -> std::io::Result<()> {
        // Collect the path from root to the target leaf.
        let mut path: Vec<PageId> = Vec::new();
        let mut page_id = self.root_page.load(Ordering::Acquire);

        loop {
            let page = self.pager.read_page(page_id)?;
            match page.page_type() {
                PageType::Leaf => {
                    path.push(page_id);
                    break;
                }
                PageType::Internal => {
                    path.push(page_id);
                    page_id = internal_find_child(&page, doc_id);
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "unexpected page type during insert",
                    ));
                }
            }
        }

        // Try to insert into the leaf.
        let leaf_id = *path.last().unwrap();
        let mut leaf = self.pager.read_page(leaf_id)?;

        // Check if this is an update (key already exists).
        let is_update = leaf_find(&leaf, doc_id).is_some();

        if leaf_insert(&mut leaf, doc_id, payload) {
            self.pager.write_page(&leaf)?;
            if !is_update {
                self.doc_count.fetch_add(1, Ordering::AcqRel);
            }
            return Ok(());
        }

        // Need to split. Collect all entries + the new one.
        let mut entries = leaf_entries(&leaf);
        // Insert the new entry in sorted position.
        if is_update {
            // Remove old entry first.
            entries.retain(|(id, _)| *id != doc_id);
        }
        let insert_pos = entries.partition_point(|(k, _)| *k < doc_id);
        entries.insert(insert_pos, (doc_id, payload.to_vec()));

        let mid = entries.len() / 2;
        let median_key = entries[mid].0;

        // Left page gets entries[..mid], right page gets entries[mid..].
        let mut left = Page::new(leaf_id, PageType::Leaf);
        let right_id = self.pager.allocate_page();
        let mut right = Page::new(right_id, PageType::Leaf);

        for (id, data) in &entries[..mid] {
            assert!(leaf_insert(&mut left, *id, data));
        }
        for (id, data) in &entries[mid..] {
            assert!(leaf_insert(&mut right, *id, data));
        }

        // Link leaves.
        let old_next = leaf.next_page();
        left.set_next_page(right_id);
        right.set_prev_page(leaf_id);
        right.set_next_page(old_next);
        // If the old leaf had a next sibling, update its prev pointer.
        if old_next != 0 {
            let mut next_page = self.pager.read_page(old_next)?;
            next_page.set_prev_page(right_id);
            self.pager.write_page(&next_page)?;
        }
        // Preserve old left's prev pointer.
        left.set_prev_page(leaf.prev_page());

        self.pager.write_page(&left)?;
        self.pager.write_page(&right)?;

        if !is_update {
            self.doc_count.fetch_add(1, Ordering::AcqRel);
        }

        // Propagate the split up.
        let split = SplitInfo {
            median_key,
            new_page_id: right_id,
        };
        self.propagate_split(&path[..path.len() - 1], leaf_id, split)?;

        Ok(())
    }

    /// Propagate a split upward through ancestor internal pages.
    fn propagate_split(
        &self,
        ancestors: &[PageId],
        left_child: PageId,
        split: SplitInfo,
    ) -> std::io::Result<()> {
        let mut left_child = left_child;
        let mut split = split;

        // Walk ancestors from bottom to top.
        for i in (0..ancestors.len()).rev() {
            let anc_id = ancestors[i];
            let mut anc = self.pager.read_page(anc_id)?;

            if internal_insert(&mut anc, split.median_key, left_child, split.new_page_id) {
                self.pager.write_page(&anc)?;
                return Ok(());
            }

            // Internal page also needs to split. Build full key + child arrays
            // from the original page state plus the new split info.
            let orig_entries = internal_entries(&anc);
            let orig_rm = internal_rightmost_child(&anc);

            // Build the full key list and children list from the original page + the new split.
            let mut all_keys: Vec<u64> = orig_entries.iter().map(|(k, _)| *k).collect();
            let mut all_children: Vec<PageId> = orig_entries.iter().map(|(_, c)| *c).collect();
            all_children.push(orig_rm);
            // Now all_children[i] is the child to the left of all_keys[i] (for i < len(keys)),
            // and all_children[len(keys)] is the rightmost child.

            // Find where to insert the new key.
            let ipos = all_keys.partition_point(|k| *k < split.median_key);
            all_keys.insert(ipos, split.median_key);
            // The child at ipos was the unsplit child. Replace it with left_child,
            // and insert split.new_page_id after it.
            all_children[ipos] = left_child;
            all_children.insert(ipos + 1, split.new_page_id);
            // Now all_children.len() == all_keys.len() + 1. Correct!

            // Split at the median.
            let imid = all_keys.len() / 2;
            let promote_key = all_keys[imid];

            // Left internal page: keys[..imid], children[..=imid]
            let mut new_left = Page::new(anc_id, PageType::Internal);
            let left_keys = &all_keys[..imid];
            let left_children = &all_children[..=imid];
            // The rightmost child of the left page is left_children[imid].
            // But we store entries as (key, left_child), so entries are:
            //   (left_keys[0], left_children[0]), ..., (left_keys[imid-1], left_children[imid-1])
            // with rightmost = left_children[imid].
            set_internal_rightmost_child(&mut new_left, left_children[left_keys.len()]);
            for j in 0..left_keys.len() {
                assert!(internal_insert(
                    &mut new_left,
                    left_keys[j],
                    left_children[j],
                    left_children[left_keys.len()],
                ));
            }
            // Fix: internal_insert sets rightmost each time. We need to set it once at the end.
            set_internal_rightmost_child(&mut new_left, left_children[left_keys.len()]);

            // Right internal page: keys[imid+1..], children[imid+1..]
            let new_right_id = self.pager.allocate_page();
            let mut new_right = Page::new(new_right_id, PageType::Internal);
            let right_keys = &all_keys[imid + 1..];
            let right_children = &all_children[imid + 1..];
            set_internal_rightmost_child(&mut new_right, right_children[right_keys.len()]);
            for j in 0..right_keys.len() {
                assert!(internal_insert(
                    &mut new_right,
                    right_keys[j],
                    right_children[j],
                    right_children[right_keys.len()],
                ));
            }
            set_internal_rightmost_child(&mut new_right, right_children[right_keys.len()]);

            self.pager.write_page(&new_left)?;
            self.pager.write_page(&new_right)?;

            left_child = anc_id;
            split = SplitInfo {
                median_key: promote_key,
                new_page_id: new_right_id,
            };
        }

        // If we get here, we need a new root.
        let new_root_id = self.pager.allocate_page();
        let mut new_root = Page::new(new_root_id, PageType::Internal);
        set_internal_rightmost_child(&mut new_root, split.new_page_id);
        assert!(internal_insert(
            &mut new_root,
            split.median_key,
            left_child,
            split.new_page_id,
        ));
        self.pager.write_page(&new_root)?;
        self.root_page.store(new_root_id, Ordering::Release);

        Ok(())
    }

    /// Delete a document by doc_id. Returns true if found and deleted.
    /// (Does not rebalance — leaves may become underfull.)
    pub fn delete(&self, doc_id: u64) -> std::io::Result<bool> {
        let mut page_id = self.root_page.load(Ordering::Acquire);
        loop {
            let mut page = self.pager.read_page(page_id)?;
            match page.page_type() {
                PageType::Leaf => {
                    if leaf_delete(&mut page, doc_id) {
                        self.pager.write_page(&page)?;
                        self.doc_count.fetch_sub(1, Ordering::AcqRel);
                        return Ok(true);
                    }
                    return Ok(false);
                }
                PageType::Internal => {
                    page_id = internal_find_child(&page, doc_id);
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "unexpected page type during delete",
                    ));
                }
            }
        }
    }

    /// Update a document (delete + insert).
    pub fn update(&self, doc_id: u64, payload: &[u8]) -> std::io::Result<()> {
        // We don't need the delete to succeed (might be a new key).
        let existed = self.delete(doc_id)?;
        self.insert(doc_id, payload)?;
        // If delete removed it and insert added it back, count stays the same.
        // But delete decremented and insert incremented, so net effect is correct
        // ONLY if the key existed before. If it didn't exist, insert already
        // incremented. If it did exist, delete decremented and insert incremented.
        // So it's correct in both cases.
        // Wait — if existed is true, delete did -1 and insert did +1 = net 0. Good.
        // If existed is false, delete did nothing and insert did +1. Good.
        let _ = existed;
        Ok(())
    }

    /// Return the leftmost leaf page id.
    pub fn leftmost_leaf(&self) -> std::io::Result<PageId> {
        let mut page_id = self.root_page.load(Ordering::Acquire);
        loop {
            let page = self.pager.read_page(page_id)?;
            match page.page_type() {
                PageType::Leaf => return Ok(page_id),
                PageType::Internal => {
                    let entries = internal_entries(&page);
                    if entries.is_empty() {
                        page_id = internal_rightmost_child(&page);
                    } else {
                        page_id = entries[0].1;
                    }
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "unexpected page type",
                    ));
                }
            }
        }
    }

    /// Return the rightmost leaf page id.
    pub fn rightmost_leaf(&self) -> std::io::Result<PageId> {
        let mut page_id = self.root_page.load(Ordering::Acquire);
        loop {
            let page = self.pager.read_page(page_id)?;
            match page.page_type() {
                PageType::Leaf => return Ok(page_id),
                PageType::Internal => {
                    page_id = internal_rightmost_child(&page);
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "unexpected page type",
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn setup() -> (Arc<Pager>, BTree) {
        let tmp = NamedTempFile::new().unwrap();
        let pager = Arc::new(Pager::open(tmp.path()).unwrap());
        // Allocate header page (0) and root leaf (1).
        let _hdr = pager.allocate_page(); // 0
        let root_id = pager.allocate_page(); // 1
        let header = Page::new(0, PageType::Header);
        let root = Page::new(root_id, PageType::Leaf);
        pager.write_page(&header).unwrap();
        pager.write_page(&root).unwrap();
        let tree = BTree::new(pager.clone(), root_id, 0);
        (pager, tree)
    }

    #[test]
    fn btree_tree_insert_find() {
        let (_pager, tree) = setup();
        tree.insert(1, b"one").unwrap();
        tree.insert(2, b"two").unwrap();
        tree.insert(3, b"three").unwrap();

        assert_eq!(tree.find(1).unwrap(), Some(b"one".to_vec()));
        assert_eq!(tree.find(2).unwrap(), Some(b"two".to_vec()));
        assert_eq!(tree.find(3).unwrap(), Some(b"three".to_vec()));
        assert_eq!(tree.find(4).unwrap(), None);
        assert_eq!(tree.count(), 3);
    }

    #[test]
    fn btree_tree_delete() {
        let (_pager, tree) = setup();
        tree.insert(10, b"ten").unwrap();
        tree.insert(20, b"twenty").unwrap();

        assert!(tree.delete(10).unwrap());
        assert_eq!(tree.find(10).unwrap(), None);
        assert_eq!(tree.count(), 1);

        assert!(!tree.delete(10).unwrap()); // already deleted
    }

    #[test]
    fn btree_tree_update() {
        let (_pager, tree) = setup();
        tree.insert(1, b"old").unwrap();
        assert_eq!(tree.count(), 1);

        tree.update(1, b"new").unwrap();
        assert_eq!(tree.find(1).unwrap(), Some(b"new".to_vec()));
        assert_eq!(tree.count(), 1);
    }

    #[test]
    fn btree_tree_insert_10k_find_delete_half() {
        let (_pager, tree) = setup();
        let n = 10_000u64;

        // Insert n keys.
        for i in 1..=n {
            let payload = format!("value-{}", i);
            tree.insert(i, payload.as_bytes()).unwrap();
        }
        assert_eq!(tree.count(), n);

        // Find all.
        for i in 1..=n {
            let expected = format!("value-{}", i);
            assert_eq!(
                tree.find(i).unwrap(),
                Some(expected.into_bytes()),
                "failed to find key {}",
                i
            );
        }

        // Delete the odd-numbered half.
        for i in (1..=n).step_by(2) {
            assert!(tree.delete(i).unwrap(), "failed to delete key {}", i);
        }
        assert_eq!(tree.count(), n / 2);

        // Verify: odd keys gone, even keys present.
        for i in 1..=n {
            if i % 2 == 1 {
                assert_eq!(tree.find(i).unwrap(), None);
            } else {
                let expected = format!("value-{}", i);
                assert_eq!(tree.find(i).unwrap(), Some(expected.into_bytes()));
            }
        }
    }

    #[test]
    fn btree_tree_split_propagation() {
        let (_pager, tree) = setup();
        // Insert enough keys to force multiple splits, including internal splits.
        let payload = vec![0u8; 300]; // ~310 bytes per leaf cell
        for i in 1..=2000u64 {
            tree.insert(i, &payload).unwrap();
        }
        assert_eq!(tree.count(), 2000);

        // Verify all keys are findable.
        for i in 1..=2000u64 {
            assert!(
                tree.find(i).unwrap().is_some(),
                "key {} not found after splits",
                i
            );
        }
    }

    #[test]
    fn btree_tree_reverse_order_insert() {
        let (_pager, tree) = setup();
        // Insert in reverse to stress sorted insertion and splits.
        for i in (1..=500u64).rev() {
            tree.insert(i, b"data").unwrap();
        }
        assert_eq!(tree.count(), 500);
        for i in 1..=500u64 {
            assert!(tree.find(i).unwrap().is_some());
        }
    }
}
