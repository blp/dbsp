//mod consumer;

//pub use consumer::{FileOrderedLayerConsumer, FileOrderedLayerValues};
use feldera_storage::file::{
    reader::Reader,
    writer::{Parameters, Writer2},
};
use tempfile::tempfile;

use crate::{
    trace::layers::{Builder, Cursor, MergeBuilder, OrdOffset, Trie, TupleBuilder},
    DBData, DBWeight, NumEntries,
};
use std::{
    cmp::min,
    fmt::{Debug, Display, Formatter},
    fs::File,
    marker::PhantomData,
};
use textwrap::indent;

pub struct FileOrderedLayer<K, V, R>
where
    K: 'static,
    V: 'static,
    R: 'static,
{
    file: Reader,
    lower_bound: usize,
    _phantom: std::marker::PhantomData<(K, V, R)>,
}

impl<K, V, R> Debug for FileOrderedLayer<K, V, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileOrderedLayer")
            .field("lower_bound", &self.lower_bound)
            .finish()
    }
}

impl<K, V, R> NumEntries for FileOrderedLayer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.file.rows().len() as usize
    }

    fn num_entries_deep(&self) -> usize {
        self.file.n_rows(1) as usize
    }
}

impl<K, V, R> Trie for FileOrderedLayer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Item = (K, (V, R));
    type Cursor<'s> = FileOrderedCursor<'s, K, V, R> where K: 's, V: 's, R: 's;
    type MergeBuilder = FileOrderedBuilder<K, V, R>;
    type TupleBuilder = FileOrderedBuilder<K, V, R>;

    fn keys(&self) -> usize {
        self.file.rows().len() as usize - self.lower_bound
    }

    fn tuples(&self) -> usize {
        self.file.n_rows(1) as usize
    }

    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor<'_> {
        if lower < upper {
            todo!()
        } else {
            todo!()
        }
    }

    fn lower_bound(&self) -> usize {
        self.lower_bound
    }

    fn truncate_below(&mut self, lower_bound: usize) {
        if lower_bound > self.lower_bound {
            self.lower_bound = min(lower_bound, self.file.rows().len() as usize);
        }
    }
}

/// Assembles a layer of this
pub struct FileOrderedBuilder<K, V, R>(Writer2<File, K, (), V, R>)
where
    K: DBData,
    V: DBData,
    R: DBWeight;

impl<K, V, R> Builder for FileOrderedBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Trie = FileOrderedLayer<K, V, R>;

    fn boundary(&mut self) -> usize {
        todo!()
    }

    fn done(mut self) -> Self::Trie {
        FileOrderedLayer {
            file: Reader::new(self.0.close().unwrap()).unwrap(),
            lower_bound: 0,
            _phantom: PhantomData,
        }
    }
}

impl<K, V, R> MergeBuilder for FileOrderedBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn with_capacity(_other1: &Self::Trie, _other2: &Self::Trie) -> Self {
        Self::with_key_capacity(0)
    }

    fn with_key_capacity(_capacity: usize) -> Self {
        Self(Writer2::new(tempfile().unwrap(), Parameters::default()).unwrap())
    }

    fn reserve(&mut self, _additional: usize) {}

    fn keys(&self) -> usize {
        self.0.n_rows() as usize
    }

    fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
        let mut cursor = other.cursor_from(lower, upper);
        while cursor.valid() {
            let mut value_cursor = cursor.values();
            while value_cursor.valid() {
                self.0.write2(value_cursor.item()).unwrap();
                value_cursor.step();
            }
            self.0.write1(cursor.item()).unwrap();
            cursor.step();
        }
    }

    fn copy_range_retain_keys<'a, F>(
        &mut self,
        other: &'a Self::Trie,
        lower: usize,
        upper: usize,
        filter: &F,
    ) where
        F: Fn(&<<Self::Trie as Trie>::Cursor<'a> as Cursor<'a>>::Key) -> bool,
    {
        let mut cursor = other.cursor_from(lower, upper);
        while cursor.valid() {
            let item = cursor.current_item();
            if filter(item) {
                let mut value_cursor = cursor.values();
                while value_cursor.valid() {
                    self.0.write2(value_cursor.item()).unwrap();
                    value_cursor.step();
                }
                self.0.write1(cursor.current_item()).unwrap();
            }
            cursor.step();
        }
    }

    fn push_merge<'a>(
        &'a mut self,
        cursor1: <Self::Trie as Trie>::Cursor<'a>,
        cursor2: <Self::Trie as Trie>::Cursor<'a>,
    ) {
        todo!()
    }

    fn push_merge_retain_keys<'a, F>(
        &'a mut self,
        cursor1: <Self::Trie as Trie>::Cursor<'a>,
        cursor2: <Self::Trie as Trie>::Cursor<'a>,
        filter: &F,
    ) where
        F: Fn(&<<Self::Trie as Trie>::Cursor<'a> as Cursor<'a>>::Key) -> bool,
    {
        todo!()
    }
}

impl<K, V, R> TupleBuilder for FileOrderedBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Item = (K, (V, R));

    fn new() -> Self {
        todo!()
    }

    fn with_capacity(cap: usize) -> Self {
        todo!()
    }

    fn reserve_tuples(&mut self, additional: usize) {
        todo!()
    }

    fn tuples(&self) -> usize {
        self.vals.tuples()
    }

    fn push_tuple(&mut self, (key, val): (K, (V, R))) {
        todo!()
    }
}

/// A cursor with a child cursor that is updated as we move.
#[derive(Debug)]
pub struct FileOrderedCursor<'s, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    data: PhantomData<&'s (K, V, R)>
}

impl<'s, K, V, R> Cursor<'s> for FileOrderedCursor<'s, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Key = K;

    type Item<'k> = &'k K
    where
        Self: 'k;

    type ValueCursor = L::Cursor<'s>;

    #[inline]
    fn keys(&self) -> usize {
        self.bounds.1 - self.bounds.0
    }

    #[inline]
    fn item(&self) -> Self::Item<'s> {
        &self.storage.keys[self.pos as usize]
    }

    fn values(&self) -> L::Cursor<'s> {
        if self.valid() {
            self.storage.vals.cursor_from(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            )
        } else {
            self.storage.vals.cursor_from(0, 0)
        }
    }

    fn step(&mut self) {
        self.pos += 1;

        if self.pos < self.bounds.1 as isize {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        } else {
            self.pos = self.bounds.1 as isize;
        }
    }

    fn seek(&mut self, key: &Self::Key) {
        todo!()
    }

    fn valid(&self) -> bool {
        self.pos >= self.bounds.0 as isize && self.pos < self.bounds.1 as isize
    }

    fn rewind(&mut self) {
        self.pos = self.bounds.0 as isize;

        if self.valid() {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        }
    }

    fn position(&self) -> usize {
        self.pos as usize
    }

    fn reposition(&mut self, lower: usize, upper: usize) {
        self.pos = lower as isize;
        self.bounds = (lower, upper);

        if self.valid() {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        }
    }

    fn step_reverse(&mut self) {
        self.pos -= 1;

        if self.pos >= self.bounds.0 as isize {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        } else {
            self.pos = self.bounds.0 as isize - 1;
        }
    }

    fn seek_reverse(&mut self, key: &Self::Key) {
        todo!()
    }

    fn fast_forward(&mut self) {
        self.pos = self.bounds.1 as isize - 1;

        if self.valid() {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        }
    }
}

impl<'a, K, V, R> Display for FileOrderedCursor<'a, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let mut cursor: FileOrderedCursor<'_, K, V, R> = self.clone();

        while cursor.valid() {
            let key = cursor.item();
            writeln!(f, "{key:?}:")?;
            let val_str = cursor.values().to_string();

            f.write_str(&indent(&val_str, "    "))?;
            cursor.step();
        }

        Ok(())
    }
}
