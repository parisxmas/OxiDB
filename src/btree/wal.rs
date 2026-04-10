/// Physical page WAL (Write-Ahead Log).
///
/// Frame layout: `[page_id: u32][page_data: PAGE_SIZE bytes][checksum: u32]`
/// Total frame size: 4 + PAGE_SIZE + 4 = 16_392 bytes.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::locks::Mutex;

use super::page::{PageId, PAGE_SIZE};
use super::pager::Pager;

const FRAME_HEADER: usize = 4;  // page_id
const FRAME_FOOTER: usize = 4;  // checksum
const FRAME_SIZE: usize = FRAME_HEADER + PAGE_SIZE + FRAME_FOOTER;

pub struct BTreeWal {
    file: Mutex<File>,
    #[allow(dead_code)]
    path: PathBuf,
}

impl BTreeWal {
    /// Open or create the WAL file.
    pub fn open(path: &std::path::Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        Ok(Self {
            file: Mutex::new(file),
            path: path.to_path_buf(),
        })
    }

    /// Log a page's before-image to the WAL.
    pub fn log_page(&self, page_id: PageId, data: &[u8; PAGE_SIZE]) -> std::io::Result<()> {
        let mut file = self.file.lock();
        file.seek(SeekFrom::End(0))?;

        // Write page_id.
        file.write_all(&page_id.to_le_bytes())?;
        // Write page data.
        file.write_all(data)?;
        // Compute and write checksum.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&page_id.to_le_bytes());
        hasher.update(data);
        let ck = hasher.finalize();
        file.write_all(&ck.to_le_bytes())?;

        file.sync_data()?;
        Ok(())
    }

    /// Truncate the WAL (called after a successful checkpoint).
    pub fn checkpoint(&self) -> std::io::Result<()> {
        let mut file = self.file.lock();
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        file.sync_all()?;
        Ok(())
    }

    /// Recover: replay page images from the WAL into the pager.
    /// Returns the number of pages recovered.
    pub fn recover(&self, pager: &Pager) -> std::io::Result<usize> {
        let mut file = self.file.lock();
        let len = file.metadata()?.len();
        if len == 0 {
            return Ok(0);
        }

        file.seek(SeekFrom::Start(0))?;
        let mut count = 0usize;
        let mut buf = vec![0u8; FRAME_SIZE];

        loop {
            match file.read_exact(&mut buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let page_id = u32::from_le_bytes(buf[0..4].try_into().unwrap());
            let data = &buf[4..4 + PAGE_SIZE];
            let stored_ck = u32::from_le_bytes(
                buf[4 + PAGE_SIZE..4 + PAGE_SIZE + 4].try_into().unwrap(),
            );

            // Verify checksum.
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&buf[0..4]);
            hasher.update(data);
            let computed_ck = hasher.finalize();

            if computed_ck != stored_ck {
                // Corrupted frame — stop recovery here.
                break;
            }

            // Write the page back to the pager.
            let mut page_data = [0u8; PAGE_SIZE];
            page_data.copy_from_slice(data);
            let page = super::page::Page::from_bytes(page_id, &page_data);
            pager.write_page(&page)?;
            count += 1;
        }

        Ok(count)
    }

    /// Check if the WAL has any frames.
    pub fn has_frames(&self) -> std::io::Result<bool> {
        let file = self.file.lock();
        Ok(file.metadata()?.len() > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::page::{Page, PageType};
    use super::super::pager::Pager;
    use tempfile::NamedTempFile;

    #[test]
    fn btree_wal_log_and_recover() {
        let wal_tmp = NamedTempFile::new().unwrap();
        let db_tmp = NamedTempFile::new().unwrap();

        let wal = BTreeWal::open(wal_tmp.path()).unwrap();
        let pager = Pager::open(db_tmp.path()).unwrap();

        // Allocate a page and write it.
        let id = pager.allocate_page();
        let mut page = Page::new(id, PageType::Leaf);
        page.data[50] = 0xCC;
        pager.write_page(&page).unwrap();

        // Log the page to WAL.
        wal.log_page(id, &page.data).unwrap();
        assert!(wal.has_frames().unwrap());

        // Simulate crash: create a fresh pager and recover.
        let pager2 = Pager::open(db_tmp.path()).unwrap();
        let _ = pager2.allocate_page(); // reserve page 0
        let recovered = wal.recover(&pager2).unwrap();
        assert_eq!(recovered, 1);

        // Read the recovered page.
        let page2 = pager2.read_page(id).unwrap();
        assert_eq!(page2.data[50], 0xCC);
    }

    #[test]
    fn btree_wal_checkpoint() {
        let wal_tmp = NamedTempFile::new().unwrap();
        let wal = BTreeWal::open(wal_tmp.path()).unwrap();

        let data = [0xABu8; PAGE_SIZE];
        wal.log_page(0, &data).unwrap();
        assert!(wal.has_frames().unwrap());

        wal.checkpoint().unwrap();
        assert!(!wal.has_frames().unwrap());
    }

    #[test]
    fn btree_wal_corrupted_frame() {
        let wal_tmp = NamedTempFile::new().unwrap();
        let db_tmp = NamedTempFile::new().unwrap();

        let wal = BTreeWal::open(wal_tmp.path()).unwrap();
        let pager = Pager::open(db_tmp.path()).unwrap();
        let _ = pager.allocate_page();

        // Write a valid frame.
        let data = [0x42u8; PAGE_SIZE];
        wal.log_page(0, &data).unwrap();

        // Corrupt the WAL file.
        {
            let mut f = wal.file.lock();
            f.seek(SeekFrom::Start(10)).unwrap();
            f.write_all(&[0xFF; 5]).unwrap();
        }

        // Recovery should skip the corrupted frame.
        let recovered = wal.recover(&pager).unwrap();
        assert_eq!(recovered, 0);
    }
}
