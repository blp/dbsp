//! Trace and batch implementations based on files.

pub mod indexed_zset_batch;
pub mod key_batch;
pub mod val_batch;
pub mod zset_batch;

use std::{path::Path, rc::Rc, sync::Arc, sync::OnceLock};

pub use indexed_zset_batch::FileIndexedZSet;
pub use key_batch::FileKeyBatch;
use tempfile::{tempdir, TempDir};
pub use val_batch::FileValBatch;
pub use zset_batch::FileZSet;

use feldera_storage::{
    backend::{monoio_impl::MonoioBackend, AtomicIncrementOnlyI64},
    buffer_cache::{BufferCache, TinyLfuCache},
};

pub type StorageBackend = BufferCache<MonoioBackend>;

/// Returns a singleton file handle factory.
fn next_file_handle() -> Arc<AtomicIncrementOnlyI64> {
    static NEXT_FILE_HANDLE: OnceLock<Arc<AtomicIncrementOnlyI64>> = OnceLock::new();
    NEXT_FILE_HANDLE
        .get_or_init(|| Arc::new(AtomicIncrementOnlyI64::new()))
        .clone()
}

/// Returns a singleton block cache.
fn tiny_lfu_cache() -> Arc<TinyLfuCache> {
    static TINY_LFU_CACHE: OnceLock<Arc<TinyLfuCache>> = OnceLock::new();
    TINY_LFU_CACHE
        .get_or_init(|| Arc::new(TinyLfuCache::with_capacity(1024 * 1024 * 1024)))
        .clone()
}

/// Returns a singleton temporary directory.
fn storage_dir() -> &'static Path {
    static TEMPDIR: OnceLock<TempDir> = OnceLock::new();
    TEMPDIR.get_or_init(|| tempdir().unwrap()).path()
}

/// Returns a per-thread storage backend that uses a per-process cache.
pub fn storage_backend() -> Rc<StorageBackend> {
    thread_local! {
        pub static BACKEND: Rc<StorageBackend> = {
            Rc::new(BufferCache::with_backend_lfu(
                MonoioBackend::new(
                    storage_dir(),
                    next_file_handle(),
                ),
                tiny_lfu_cache(),
            ))
        };
    }
    BACKEND.with(|rc| rc.clone())
}
