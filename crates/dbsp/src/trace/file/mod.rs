//! Trace and batch implementations based on files.

pub mod key_batch;
pub mod indexed_zset_batch;
pub mod zset_batch;

pub use key_batch::FileKeyBatch;
pub use indexed_zset_batch::FileIndexedZSet;
pub use zset_batch::FileZSet;

use crate::trace::Spine;

/// A trace implementation for empty values using a spine of ordered lists.
pub type FileKeySpine<K, T, R> = Spine<FileKeyBatch<K, T, R>>;
pub type FileIndexedZSetSpine<K, V, R> = Spine<FileIndexedZSet<K, V, R>>;
pub type FileZSetSpine<K, R> = Spine<FileZSet<K, R>>;

