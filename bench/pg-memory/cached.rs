//! How much of each file is resident in the OS page cache, via `mincore(2)`.
//!
//! Needed because neither `ps rss` nor `phys_footprint` sees it. An engine that
//! reads through the page cache — PostgreSQL — leaves hundreds of megabytes
//! resident that no process is charged for, and an engine that mmaps its data
//! — OxiDB in disk-first mode — has its pages charged to the kernel too, since
//! `phys_footprint` excludes clean file-backed pages. Comparing the two on
//! process metrics alone flatters whichever one keeps more of its data in files.
//!
//! Mapping a file does not fault its pages in, so this observes without
//! disturbing what it measures.
//!
//! ```text
//! rustc -O -o cached cached.rs
//! ./cached $(find <datadir> -type f)
//! ```
use std::fs::File;
use std::os::unix::io::AsRawFd;

unsafe extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn mincore(addr: *mut u8, len: usize, vec: *mut i8) -> i32;
    fn munmap(addr: *mut u8, len: usize) -> i32;
}
const PROT_READ: i32 = 1;
const MAP_SHARED: i32 = 1;

fn main() {
    let page = 16384usize; // hw.pagesize on Apple silicon
    let mut total = 0u64;
    let mut resident = 0u64;
    for path in std::env::args().skip(1) {
        let Ok(f) = File::open(&path) else { continue };
        let len = f.metadata().map(|m| m.len()).unwrap_or(0) as usize;
        if len == 0 { continue }
        unsafe {
            let p = mmap(std::ptr::null_mut(), len, PROT_READ, MAP_SHARED, f.as_raw_fd(), 0);
            if p as isize == -1 { continue }
            let pages = len.div_ceil(page);
            let mut vec = vec![0i8; pages];
            if mincore(p, len, vec.as_mut_ptr()) == 0 {
                let r = vec.iter().filter(|b| **b & 1 == 1).count();
                total += (pages * page) as u64;
                resident += (r * page) as u64;
            }
            munmap(p, len);
        }
    }
    println!("{:>7} MB of {:>7} MB resident in page cache",
             resident / 1048576, total / 1048576);
}
