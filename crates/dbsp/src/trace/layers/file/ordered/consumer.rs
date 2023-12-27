use crate::{
    trace::{Consumer, ValueConsumer},
    DBData, DBWeight,
};
use ouroboros::self_referencing;

use super::{FileOrderedCursor, FileOrderedLayer};

/// A [`Consumer`] implementation for [`FileOrderedLayer`]s that contain
/// [`ColumnLayer`]s
// TODO: Fuzz testing for correctness and drop safety
#[self_referencing]
pub struct FileOrderedLayerConsumer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    storage: FileOrderedLayer<K, V, R>,
    #[borrows(storage)]
    #[covariant]
    cursor: FileOrderedCursor<'this, K, V, R>,
}

impl<K, V, R> Consumer<K, V, R, ()> for FileOrderedLayerConsumer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type ValueConsumer<'a> = FileOrderedLayerValues<'a, K, V, R>
    where
        Self: 'a;

    fn key_valid(&self) -> bool {
        self.with_cursor(|cursor| cursor.valid)
    }

    fn peek_key(&self) -> &K {
        self.with_cursor(|cursor| cursor.current_key())
    }

    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        todo!()
    }

    fn seek_key(&mut self, key: &K)
    where
        K: Ord,
    {
        todo!()
    }
}

impl<K, V, R> From<FileOrderedLayer<K, V, R>> for FileOrderedLayerConsumer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn from(layer: FileOrderedLayer<K, V, R>) -> Self {
        todo!()
    }
}

/// A [`ValueConsumer`] impl for the values yielded by
/// [`FileOrderedLayerConsumer`]
#[derive(Debug)]
#[self_referencing]
pub struct FileOrderedLayerValues<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{}

/*
impl<'a, K, V, R> FileOrderedLayerValues<'a, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    pub fn new(consumer: &'a mut FileOrderedLayerConsumer<K, V, R>) -> Self {
        todo!()
    }
}
 */

impl<'a, K, V, R> ValueConsumer<'a, K, V, R> for FileOrderedLayerValues<'a, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn value_valid(&self) -> bool {
        todo!()
    }

    fn next_value(&mut self) -> (V, R, ()) {
        todo!()
    }

    fn remaining_values(&self) -> usize {
        todo!()
    }
}
