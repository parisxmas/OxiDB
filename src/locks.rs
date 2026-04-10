//! Cross-platform lock primitives.
//!
//! Re-exports `Mutex` and `RwLock` from `parking_lot` on native targets
//! and from `spin` on wasm32 (where OS-level parking is unavailable).
//! Both crates share the same API surface (lock/read/write return guards
//! directly, no `Result` wrapper), so downstream code is unchanged.

#[cfg(not(target_arch = "wasm32"))]
pub use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(target_arch = "wasm32")]
pub use spin::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
