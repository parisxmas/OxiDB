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

    /// Find multiple documents in a single traversal.
    ///
    /// Sorts `doc_ids`, then walks the tree once collecting results from
    /// each leaf page visited.  Co-located doc_ids on the same leaf page
    /// are served from a single page read, exploiting spatial locality.
    pub fn find_batch(&self, doc_ids: &[u64]) -> std::io::Result<Vec<(u64, Vec<u8>)>> {
        if doc_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut sorted_ids = doc_ids.to_vec();
        sorted_ids.sort_unstable();
        sorted_ids.dedup();

        let mut results = Vec::with_capacity(sorted_ids.len());
        let mut id_idx = 0;

        // Find the leftmost leaf that could contain sorted_ids[0]
        let mut page_id = self.root_page.load(Ordering::Acquire);
        // Navigate to the leaf for the first doc_id
        loop {
            let page = self.pager.read_page(page_id)?;
            match page.page_type() {
                PageType::Leaf => break,
                PageType::Internal => {
                    page_id = internal_find_child(&page, sorted_ids[0]);
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "unexpected page type during find_batch",
                    ));
                }
            }
        }

        // Now scan leaves left-to-right via next_page links, collecting
        // all matching doc_ids.
        loop {
            if id_idx >= sorted_ids.len() {
                break;
            }

            let page = self.pager.read_page(page_id)?;
            let entries = leaf_entries(&page);

            // For each remaining doc_id that could be on this page, check.
            // Leaf entries are sorted, so we can merge-scan.
            let mut entry_idx = 0;
            while id_idx < sorted_ids.len() && entry_idx < entries.len() {
                let target = sorted_ids[id_idx];
                let (entry_id, ref payload) = entries[entry_idx];

                if target < entry_id {
                    // target is not on this page (would be before entry_idx)
                    // Check if target could be on this page at all by looking
                    // at the first entry. If target < first entry's id,
                    // the doc doesn't exist (tree is sorted), skip it.
                    id_idx += 1;
                } else if target == entry_id {
                    results.push((target, payload.clone()));
                    id_idx += 1;
                    entry_idx += 1;
                } else {
                    // target > entry_id, advance entry_idx
                    entry_idx += 1;
                }
            }

            // If remaining targets are > all entries on this leaf, they might
            // be on the next leaf. But first skip any targets smaller than
            // what the next page would start with (they don't exist).
            let next = page.next_page();
            if next == 0 {
                // No more leaves — remaining ids don't exist
                break;
            }
            page_id = next;
        }

        Ok(results)
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

    #[test]
    fn btree_tree_find_batch_basic() {
        let (_pager, tree) = setup();
        for i in 1..=100u64 {
            let payload = format!("val-{}", i);
            tree.insert(i, payload.as_bytes()).unwrap();
        }

        // Batch find a subset (out of order, with gaps)
        let results = tree.find_batch(&[50, 10, 90, 30, 70]).unwrap();
        let map: std::collections::HashMap<u64, Vec<u8>> = results.into_iter().collect();
        assert_eq!(map.len(), 5);
        assert_eq!(map[&10], b"val-10");
        assert_eq!(map[&30], b"val-30");
        assert_eq!(map[&50], b"val-50");
        assert_eq!(map[&70], b"val-70");
        assert_eq!(map[&90], b"val-90");
    }

    #[test]
    fn btree_tree_find_batch_with_missing() {
        let (_pager, tree) = setup();
        tree.insert(1, b"one").unwrap();
        tree.insert(3, b"three").unwrap();
        tree.insert(5, b"five").unwrap();

        let results = tree.find_batch(&[1, 2, 3, 4, 5, 6]).unwrap();
        let map: std::collections::HashMap<u64, Vec<u8>> = results.into_iter().collect();
        assert_eq!(map.len(), 3);
        assert_eq!(map[&1], b"one");
        assert_eq!(map[&3], b"three");
        assert_eq!(map[&5], b"five");
    }

    #[test]
    fn btree_tree_find_batch_empty() {
        let (_pager, tree) = setup();
        tree.insert(1, b"one").unwrap();

        let results = tree.find_batch(&[]).unwrap();
        assert!(results.is_empty());

        let results = tree.find_batch(&[99, 100]).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn btree_tree_find_batch_after_splits() {
        let (_pager, tree) = setup();
        // Insert enough to force multiple leaf splits
        let payload = vec![0xABu8; 200];
        for i in 1..=500u64 {
            tree.insert(i, &payload).unwrap();
        }

        // Batch find across multiple leaf pages
        let ids: Vec<u64> = (1..=500).step_by(7).collect();
        let results = tree.find_batch(&ids).unwrap();
        assert_eq!(results.len(), ids.len());
        for (doc_id, data) in &results {
            assert!(ids.contains(doc_id));
            assert_eq!(data, &payload);
        }
    }

    #[test]
    fn btree_tree_find_batch_duplicates() {
        let (_pager, tree) = setup();
        tree.insert(1, b"one").unwrap();
        tree.insert(2, b"two").unwrap();

        // Duplicate IDs in request should be deduped
        let results = tree.find_batch(&[1, 1, 2, 2, 1]).unwrap();
        assert_eq!(results.len(), 2);
    }
}
