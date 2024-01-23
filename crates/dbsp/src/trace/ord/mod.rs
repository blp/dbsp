//! "Standard" trace and batch implementations.
//!
//! The types and type aliases in this module start with one of:
//!
//! * `OrdVal`: Collections whose data have the form `(key, val)` where `key`
//!   and `val` are ordered.
//! * `OrdKey`: Collections whose data have the form `key` where `key` is
//!   ordered.
//! * `OrdIndexedZSet`:  Collections whose data have the form `(key, val)` where
//!   `key` and `val` are ordered and whose timestamp type is `()`.
//!   Semantically, such collections store `(key, val, weight)` tuples without
//!   timing information, and implement the indexed ZSet abstraction of DBSP.
//! * `OrdZSet`:  Collections whose data have the form `key` where `key` is
//!   ordered and whose timestamp type is `()`.  Semantically, such collections
//!   store `(key, weight)` tuples without timing information, and implement the
//!   ZSet abstraction of DBSP.
//!
//! Although `OrdVal` is more general than `OrdKey`, the latter has a simpler
//! representation and should consume fewer resources (computation and memory)
//! when it applies.
//!
//! Likewise, `OrdIndexedZSet` and `OrdZSet` are less general than `OrdVal` and
//! `OrdKey` respectively, but are more light-weight.

mod merge_batcher;

pub mod file;
pub mod vec;

pub use file::{FileIndexedZSet, FileKeyBatch, FileValBatch, FileZSet};
pub use vec::{VecIndexedZSet, VecKeyBatch, VecValBatch, VecZSet};

pub use FileValBatch as OrdValBatch;
pub use VecIndexedZSet as OrdIndexedZSet;
pub use VecKeyBatch as OrdKeyBatch;
pub use VecZSet as OrdZSet;

use super::{layers::OrdOffset, Batch, BatchReader};
use crate::{DBData, DBWeight};

pub trait AsFileBatch: BatchReader<Time = ()> {
    type FileBatch: Batch<Key = Self::Key, Val = Self::Val, R = Self::R, Time = ()>;
}

impl<K, V, R, O> AsFileBatch for VecIndexedZSet<K, V, R, O>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
    O: OrdOffset + 'static,
{
    type FileBatch = FileIndexedZSet<K, V, R>;
}

impl<K, R> AsFileBatch for VecZSet<K, R>
where
    K: DBData,
    R: DBWeight,
{
    type FileBatch = FileZSet<K, R>;
}
