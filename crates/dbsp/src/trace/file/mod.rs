//! Trace and batch implementations based on files.

pub mod key_batch;

pub use key_batch::FileKeyBatch;

use crate::trace::Spine;

/// A trace implementation for empty values using a spine of ordered lists.
pub type FileKeySpine<K, T, R> = Spine<FileKeyBatch<K, T, R>>;
