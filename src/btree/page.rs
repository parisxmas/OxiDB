/// 16 KB page abstraction for the B-tree storage engine.
///
/// Every page has a 32-byte header followed by type-specific content.
/// Page types: Header(0), Internal(1), Leaf(2), Overflow(3), Freelist(4).

pub const PAGE_SIZE: usize = 16_384;

pub type PageId = u32;

/// Byte-offset constants inside the 32-byte header.
const OFF_PAGE_TYPE: usize = 0;       // u8
const OFF_FLAGS: usize = 1;           // u8
#[allow(dead_code)]
const OFF_RESERVED: usize = 2;        // u16
const OFF_PAGE_ID: usize = 4;         // u32
const OFF_PARENT: usize = 8;          // u32
const OFF_PREV_PAGE: usize = 12;      // u32
const OFF_NEXT_PAGE: usize = 16;      // u32
const OFF_NUM_ENTRIES: usize = 20;     // u16
const OFF_FREE_SPACE_END: usize = 22; // u16
const OFF_CELL_CONTENT_START: usize = 24; // u16
const OFF_FRAGMENT_BYTES: usize = 26; // u16
const OFF_CHECKSUM: usize = 28;       // u32

pub const HEADER_SIZE: usize = 32;

/// Known page types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    Header   = 0,
    Internal = 1,
    Leaf     = 2,
    Overflow = 3,
    Freelist = 4,
}

impl PageType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Header),
            1 => Some(Self::Internal),
            2 => Some(Self::Leaf),
            3 => Some(Self::Overflow),
            4 => Some(Self::Freelist),
            _ => None,
        }
    }
}

/// A single fixed-size page backed by a heap-allocated buffer.
pub struct Page {
    pub id: PageId,
    pub data: Box<[u8; PAGE_SIZE]>,
    pub dirty: bool,
}

impl Page {
    /// Create a new, zeroed page with the given id and type.
    pub fn new(id: PageId, page_type: PageType) -> Self {
        let mut data = Box::new([0u8; PAGE_SIZE]);
        // Write header fields.
        data[OFF_PAGE_TYPE] = page_type as u8;
        data[OFF_PAGE_ID..OFF_PAGE_ID + 4].copy_from_slice(&id.to_le_bytes());
        // cell_content_start defaults to PAGE_SIZE (no cells yet).
        let ps = PAGE_SIZE as u16;
        data[OFF_CELL_CONTENT_START..OFF_CELL_CONTENT_START + 2]
            .copy_from_slice(&ps.to_le_bytes());
        // free_space_end defaults to PAGE_SIZE.
        data[OFF_FREE_SPACE_END..OFF_FREE_SPACE_END + 2]
            .copy_from_slice(&ps.to_le_bytes());
        Self {
            id,
            data,
            dirty: true,
        }
    }

    /// Reconstruct a page from a raw byte slice (exactly PAGE_SIZE bytes).
    pub fn from_bytes(id: PageId, bytes: &[u8; PAGE_SIZE]) -> Self {
        let mut data = Box::new([0u8; PAGE_SIZE]);
        data.copy_from_slice(bytes);
        Self {
            id,
            data,
            dirty: false,
        }
    }

    // ---- header field accessors ----

    pub fn page_type(&self) -> PageType {
        PageType::from_u8(self.data[OFF_PAGE_TYPE]).unwrap_or(PageType::Header)
    }

    pub fn set_page_type(&mut self, pt: PageType) {
        self.data[OFF_PAGE_TYPE] = pt as u8;
        self.dirty = true;
    }

    pub fn flags(&self) -> u8 {
        self.data[OFF_FLAGS]
    }

    pub fn set_flags(&mut self, f: u8) {
        self.data[OFF_FLAGS] = f;
        self.dirty = true;
    }

    pub fn page_id(&self) -> PageId {
        u32::from_le_bytes(self.data[OFF_PAGE_ID..OFF_PAGE_ID + 4].try_into().unwrap())
    }

    pub fn parent(&self) -> PageId {
        u32::from_le_bytes(self.data[OFF_PARENT..OFF_PARENT + 4].try_into().unwrap())
    }

    pub fn set_parent(&mut self, p: PageId) {
        self.data[OFF_PARENT..OFF_PARENT + 4].copy_from_slice(&p.to_le_bytes());
        self.dirty = true;
    }

    pub fn prev_page(&self) -> PageId {
        u32::from_le_bytes(self.data[OFF_PREV_PAGE..OFF_PREV_PAGE + 4].try_into().unwrap())
    }

    pub fn set_prev_page(&mut self, p: PageId) {
        self.data[OFF_PREV_PAGE..OFF_PREV_PAGE + 4].copy_from_slice(&p.to_le_bytes());
        self.dirty = true;
    }

    pub fn next_page(&self) -> PageId {
        u32::from_le_bytes(self.data[OFF_NEXT_PAGE..OFF_NEXT_PAGE + 4].try_into().unwrap())
    }

    pub fn set_next_page(&mut self, p: PageId) {
        self.data[OFF_NEXT_PAGE..OFF_NEXT_PAGE + 4].copy_from_slice(&p.to_le_bytes());
        self.dirty = true;
    }

    pub fn num_entries(&self) -> u16 {
        u16::from_le_bytes(self.data[OFF_NUM_ENTRIES..OFF_NUM_ENTRIES + 2].try_into().unwrap())
    }

    pub fn set_num_entries(&mut self, n: u16) {
        self.data[OFF_NUM_ENTRIES..OFF_NUM_ENTRIES + 2].copy_from_slice(&n.to_le_bytes());
        self.dirty = true;
    }

    pub fn free_space_end(&self) -> u16 {
        u16::from_le_bytes(self.data[OFF_FREE_SPACE_END..OFF_FREE_SPACE_END + 2].try_into().unwrap())
    }

    pub fn set_free_space_end(&mut self, v: u16) {
        self.data[OFF_FREE_SPACE_END..OFF_FREE_SPACE_END + 2].copy_from_slice(&v.to_le_bytes());
        self.dirty = true;
    }

    pub fn cell_content_start(&self) -> u16 {
        u16::from_le_bytes(
            self.data[OFF_CELL_CONTENT_START..OFF_CELL_CONTENT_START + 2]
                .try_into()
                .unwrap(),
        )
    }

    pub fn set_cell_content_start(&mut self, v: u16) {
        self.data[OFF_CELL_CONTENT_START..OFF_CELL_CONTENT_START + 2]
            .copy_from_slice(&v.to_le_bytes());
        self.dirty = true;
    }

    pub fn fragment_bytes(&self) -> u16 {
        u16::from_le_bytes(
            self.data[OFF_FRAGMENT_BYTES..OFF_FRAGMENT_BYTES + 2]
                .try_into()
                .unwrap(),
        )
    }

    pub fn set_fragment_bytes(&mut self, v: u16) {
        self.data[OFF_FRAGMENT_BYTES..OFF_FRAGMENT_BYTES + 2]
            .copy_from_slice(&v.to_le_bytes());
        self.dirty = true;
    }

    /// Compute CRC32 over bytes 0..OFF_CHECKSUM (everything except the checksum field itself).
    pub fn compute_checksum(&self) -> u32 {
        let mut h = crc32fast::Hasher::new();
        h.update(&self.data[..OFF_CHECKSUM]);
        h.update(&self.data[OFF_CHECKSUM + 4..]);
        h.finalize()
    }

    /// Write the computed checksum into the header.
    pub fn stamp_checksum(&mut self) {
        let ck = self.compute_checksum();
        self.data[OFF_CHECKSUM..OFF_CHECKSUM + 4].copy_from_slice(&ck.to_le_bytes());
    }

    /// Read the stored checksum from the header.
    pub fn stored_checksum(&self) -> u32 {
        u32::from_le_bytes(self.data[OFF_CHECKSUM..OFF_CHECKSUM + 4].try_into().unwrap())
    }

    /// Verify that the stored checksum matches the computed one.
    pub fn verify_checksum(&self) -> bool {
        self.stored_checksum() == self.compute_checksum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn btree_page_create_roundtrip() {
        let page = Page::new(42, PageType::Leaf);
        assert_eq!(page.id, 42);
        assert_eq!(page.page_type(), PageType::Leaf);
        assert_eq!(page.page_id(), 42);
        assert_eq!(page.num_entries(), 0);
        assert_eq!(page.prev_page(), 0);
        assert_eq!(page.next_page(), 0);
        assert_eq!(page.cell_content_start(), PAGE_SIZE as u16);

        // Round-trip via from_bytes.
        let page2 = Page::from_bytes(42, &page.data);
        assert_eq!(page2.page_type(), PageType::Leaf);
        assert_eq!(page2.page_id(), 42);
        assert_eq!(page2.num_entries(), 0);
    }

    #[test]
    fn btree_page_header_fields() {
        let mut page = Page::new(7, PageType::Internal);
        page.set_parent(100);
        page.set_prev_page(5);
        page.set_next_page(9);
        page.set_num_entries(3);
        page.set_flags(0xAB);
        page.set_fragment_bytes(42);

        assert_eq!(page.parent(), 100);
        assert_eq!(page.prev_page(), 5);
        assert_eq!(page.next_page(), 9);
        assert_eq!(page.num_entries(), 3);
        assert_eq!(page.flags(), 0xAB);
        assert_eq!(page.fragment_bytes(), 42);
    }

    #[test]
    fn btree_page_checksum() {
        let mut page = Page::new(1, PageType::Leaf);
        page.data[100] = 0xFF; // modify some content
        page.stamp_checksum();
        assert!(page.verify_checksum());

        // Corrupt a byte — checksum should fail.
        page.data[200] ^= 0x01;
        assert!(!page.verify_checksum());
    }

    #[test]
    fn btree_page_type_from_u8() {
        assert_eq!(PageType::from_u8(0), Some(PageType::Header));
        assert_eq!(PageType::from_u8(1), Some(PageType::Internal));
        assert_eq!(PageType::from_u8(2), Some(PageType::Leaf));
        assert_eq!(PageType::from_u8(3), Some(PageType::Overflow));
        assert_eq!(PageType::from_u8(4), Some(PageType::Freelist));
        assert_eq!(PageType::from_u8(5), None);
    }
}
