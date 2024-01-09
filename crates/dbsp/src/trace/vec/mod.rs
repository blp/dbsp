//! Trace and batch implementations based on sorted ranges.
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

pub mod key_batch;
pub mod val_batch;
pub mod zset_batch;
pub mod indexed_zset_batch;

use crate::trace::Spine;

pub use key_batch::VecKeyBatch;
pub use val_batch::VecValBatch;
pub use zset_batch::VecZSet;
pub use indexed_zset_batch::VecIndexedZSet;

/// A trace implementation using a spine of ordered lists.
pub type VecValSpine<K, V, T, R> = Spine<VecValBatch<K, V, T, R>>;

/// A trace implementation for empty values using a spine of ordered lists.
pub type VecKeySpine<K, T, R> = Spine<VecKeyBatch<K, T, R>>;

/// A trace implementation using a [`Spine`] of [`VecZSet`].
pub type VecZSetSpine<K, R> = Spine<VecZSet<K, R>>;

pub type VecIndexedZSetSpine<K, V, R> = Spine<VecIndexedZSet<K, V, R>>;
