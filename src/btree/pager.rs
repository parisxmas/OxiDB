/// Page cache (sharded LRU) + file I/O with LZ4 compression.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use lru::LruCache;
use crate::locks::Mutex;

use super::page::{Page, PageId, PAGE_SIZE};

/// Number of cache shards for reducing contention.
const NUM_SHARDS: usize = 16;

/// Default capacity per shard.
const SHARD_CAPACITY: usize = 1024;

/// A cached page held in the LRU cache.
pub struct CachedPage {
    pub data: Box<[u8; PAGE_SIZE]>,
    pub dirty: AtomicBool,
}

impl CachedPage {
    fn from_page(page: &Page) -> Self {
        let mut data = Box::new([0u8; PAGE_SIZE]);
        data.copy_from_slice(&*page.data);
        Self {
            data,
            dirty: AtomicBool::new(page.dirty),
        }
    }

    fn to_page(&self, id: PageId) -> Page {
        Page {
            id,
            data: Box::new(*self.data),
            dirty: self.dirty.load(Ordering::Relaxed),
        }
    }
}

/// Page cache + file I/O layer.
pub struct Pager {
    file: Mutex<File>,
    path: PathBuf,
    cache: Vec<Mutex<LruCache<PageId, Arc<CachedPage>>>>,
    total_pages: AtomicU32,
    freelist: Mutex<Vec<PageId>>,
}

impl Pager {
    /// Open or create a database file. Reads the header page to determine
    /// file size, or initializes a fresh file with a header page + root leaf.
    pub fn open(path: &std::path::Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let meta = file.metadata()?;
        let file_len = meta.len();

        let mut cache = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            cache.push(Mutex::new(LruCache::new(
                NonZeroUsize::new(SHARD_CAPACITY).unwrap(),
            )));
        }

        let total_pages = if file_len == 0 {
            // Fresh file — will be initialized by the caller (BTree::open).
            0
        } else {
            // Each page is stored as: [compressed_len: u32][compressed_data]
            // We need to scan to count pages — but we store total_pages in the
            // header page for O(1) recovery.  For now, count from file.
            // Actually, we store total_pages in the first 4 bytes after the
            // header page's normal content (we embed it in the header page body).
            // Simpler: store page count at a known offset in page 0.
            //
            // On-disk format per page: [u32 compressed_len][compressed bytes]
            // We scan sequentially.
            let mut f = file.try_clone()?;
            f.seek(SeekFrom::Start(0))?;
            let mut count = 0u32;
            loop {
                let mut len_buf = [0u8; 4];
                match f.read_exact(&mut len_buf) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e),
                }
                let clen = u32::from_le_bytes(len_buf) as u64;
                f.seek(SeekFrom::Current(clen as i64))?;
                count += 1;
            }
            count
        };

        Ok(Self {
            file: Mutex::new(file),
            path: path.to_path_buf(),
            cache,
            total_pages: AtomicU32::new(total_pages),
            freelist: Mutex::new(Vec::new()),
        })
    }

    fn shard(&self, id: PageId) -> usize {
        (id as usize) % NUM_SHARDS
    }

    /// Total number of pages in the file.
    pub fn total_pages(&self) -> u32 {
        self.total_pages.load(Ordering::Acquire)
    }

    /// Read a page, using the cache when possible.
    pub fn read_page(&self, id: PageId) -> std::io::Result<Page> {
        let shard_idx = self.shard(id);
        {
            let mut shard = self.cache[shard_idx].lock();
            if let Some(cp) = shard.get(&id) {
                return Ok(cp.to_page(id));
            }
        }
        // Cache miss — read from disk.
        let page = self.read_page_disk(id)?;
        let cp = Arc::new(CachedPage::from_page(&page));
        {
            let mut shard = self.cache[shard_idx].lock();
            shard.put(id, cp);
        }
        Ok(page)
    }

    /// Read a page directly from disk (no cache).
    fn read_page_disk(&self, id: PageId) -> std::io::Result<Page> {
        let mut file = self.file.lock();
        // We need to find the offset of page `id`. Since pages are compressed
        // and variable-length on disk, we scan from the beginning.
        // For better performance we could maintain an offset index, but for now
        // sequential scan is acceptable (pages are cached aggressively).
        file.seek(SeekFrom::Start(0))?;
        for _ in 0..id {
            let mut len_buf = [0u8; 4];
            file.read_exact(&mut len_buf)?;
            let clen = u32::from_le_bytes(len_buf) as u64;
            file.seek(SeekFrom::Current(clen as i64))?;
        }
        let mut len_buf = [0u8; 4];
        file.read_exact(&mut len_buf)?;
        let clen = u32::from_le_bytes(len_buf) as usize;
        let mut compressed = vec![0u8; clen];
        file.read_exact(&mut compressed)?;

        let decompressed = lz4_flex::decompress(&compressed, PAGE_SIZE)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        let mut buf = [0u8; PAGE_SIZE];
        buf.copy_from_slice(&decompressed);
        Ok(Page::from_bytes(id, &buf))
    }

    /// Write a page to disk AND update the cache. The page is compressed.
    pub fn write_page(&self, page: &Page) -> std::io::Result<()> {
        // Update cache.
        let shard_idx = self.shard(page.id);
        {
            let mut shard = self.cache[shard_idx].lock();
            let cp = CachedPage::from_page(page);
            cp.dirty.store(true, Ordering::Relaxed);
            shard.put(page.id, Arc::new(cp));
        }
        Ok(())
    }

    /// Allocate a fresh page id. Reuses from freelist first.
    pub fn allocate_page(&self) -> PageId {
        {
            let mut fl = self.freelist.lock();
            if let Some(id) = fl.pop() {
                return id;
            }
        }
        self.total_pages.fetch_add(1, Ordering::AcqRel)
    }

    /// Return a page to the freelist.
    pub fn free_page(&self, id: PageId) {
        let mut fl = self.freelist.lock();
        fl.push(id);
    }

    /// Flush all dirty pages to disk.
    pub fn flush_dirty(&self) -> std::io::Result<()> {
        // Collect all dirty pages from the cache.
        let mut dirty_pages: Vec<(PageId, Box<[u8; PAGE_SIZE]>)> = Vec::new();
        for shard_idx in 0..NUM_SHARDS {
            let shard = self.cache[shard_idx].lock();
            // Iterate through all cached entries.
            for (&id, cp) in shard.iter() {
                if cp.dirty.load(Ordering::Relaxed) {
                    dirty_pages.push((id, Box::new(*cp.data)));
                    cp.dirty.store(false, Ordering::Relaxed);
                }
            }
        }

        if dirty_pages.is_empty() {
            return Ok(());
        }

        // Sort by page id for sequential write.
        dirty_pages.sort_by_key(|(id, _)| *id);

        // We need to write all pages sequentially (because pages are
        // variable-length compressed). Build the entire file image.
        let total = self.total_pages.load(Ordering::Acquire) as usize;
        let mut all_pages: Vec<Box<[u8; PAGE_SIZE]>> = Vec::with_capacity(total);

        // First read all existing pages from cache or disk.
        for pid in 0..total as u32 {
            let shard_idx = self.shard(pid);
            let cached = {
                let mut shard = self.cache[shard_idx].lock();
                shard.get(&pid).map(|cp| Box::new(*cp.data))
            };
            if let Some(data) = cached {
                all_pages.push(data);
            } else {
                // Read from disk.
                match self.read_page_disk(pid) {
                    Ok(page) => all_pages.push(page.data),
                    Err(_) => {
                        // Page not yet on disk (freshly allocated).
                        all_pages.push(Box::new([0u8; PAGE_SIZE]));
                    }
                }
            }
        }

        // Apply dirty pages.
        for (id, data) in &dirty_pages {
            let idx = *id as usize;
            if idx < all_pages.len() {
                all_pages[idx] = data.clone();
            }
        }

        // Rewrite the file.
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(0))?;
        file.set_len(0)?;

        for page_data in &all_pages {
            let compressed = lz4_flex::compress(&page_data[..]);
            let clen = compressed.len() as u32;
            file.write_all(&clen.to_le_bytes())?;
            file.write_all(&compressed)?;
        }

        Ok(())
    }

    /// Flush dirty pages and fsync.
    pub fn sync(&self) -> std::io::Result<()> {
        self.flush_dirty()?;
        let file = self.file.lock();
        file.sync_all()
    }

    /// Get the file path.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::page::PageType;
    use tempfile::NamedTempFile;

    #[test]
    fn btree_pager_read_write_cache_hit() {
        let tmp = NamedTempFile::new().unwrap();
        let pager = Pager::open(tmp.path()).unwrap();

        // Allocate and write a page.
        let id = pager.allocate_page();
        assert_eq!(id, 0);
        let mut page = Page::new(id, PageType::Leaf);
        page.data[100] = 0xAA;
        pager.write_page(&page).unwrap();

        // Read back — should hit cache.
        let page2 = pager.read_page(id).unwrap();
        assert_eq!(page2.data[100], 0xAA);
        assert_eq!(page2.page_type(), PageType::Leaf);
    }

    #[test]
    fn btree_pager_lz4_roundtrip() {
        let tmp = NamedTempFile::new().unwrap();
        let pager = Pager::open(tmp.path()).unwrap();

        let id = pager.allocate_page();
        let mut page = Page::new(id, PageType::Leaf);
        // Write some data pattern.
        for i in 40..200 {
            page.data[i] = (i % 256) as u8;
        }
        pager.write_page(&page).unwrap();
        pager.flush_dirty().unwrap();

        // Re-open and read.
        let pager2 = Pager::open(tmp.path()).unwrap();
        let page2 = pager2.read_page(id).unwrap();
        assert_eq!(page2.page_type(), PageType::Leaf);
        for i in 40..200 {
            assert_eq!(page2.data[i], (i % 256) as u8);
        }
    }

    #[test]
    fn btree_pager_allocate_free() {
        let tmp = NamedTempFile::new().unwrap();
        let pager = Pager::open(tmp.path()).unwrap();

        let a = pager.allocate_page();
        let b = pager.allocate_page();
        assert_ne!(a, b);

        pager.free_page(a);
        let c = pager.allocate_page();
        assert_eq!(c, a); // should reuse freed page
    }

    #[test]
    fn btree_pager_multiple_pages() {
        let tmp = NamedTempFile::new().unwrap();
        let pager = Pager::open(tmp.path()).unwrap();

        for i in 0..10u32 {
            let id = pager.allocate_page();
            let mut page = Page::new(id, PageType::Leaf);
            page.data[50] = i as u8;
            pager.write_page(&page).unwrap();
        }

        pager.flush_dirty().unwrap();

        // Re-open and verify all pages.
        let pager2 = Pager::open(tmp.path()).unwrap();
        assert_eq!(pager2.total_pages(), 10);

        for i in 0..10u32 {
            let page = pager2.read_page(i).unwrap();
            assert_eq!(page.data[50], i as u8);
        }
    }
}
