//mod consumer;

//pub use consumer::{FileOrderedLayerConsumer, FileOrderedLayerValues};
use feldera_storage::file::{
    reader::{Cursor as FileCursor, Reader},
    writer::{Parameters, Writer2},
};
use tempfile::tempfile;

use crate::{
    trace::layers::{Builder, Cursor, MergeBuilder, Trie, TupleBuilder},
    DBData, DBWeight, NumEntries, Rkyv,
};
use std::{
    cmp::{min, Ordering},
    fmt::Debug,
    fs::File,
    marker::PhantomData,
};

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
    type MergeBuilder = FileOrderedMergeBuilder<K, V, R>;
    type TupleBuilder = FileOrderedTupleBuilder<K, V, R>;

    fn keys(&self) -> usize {
        self.file.rows().len() as usize - self.lower_bound
    }

    fn tuples(&self) -> usize {
        self.file.n_rows(1) as usize
    }

    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor<'_> {
        FileOrderedCursor::new(lower, self, (lower, upper))
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

pub struct FileOrderedMergeBuilder<K, V, R>(Writer2<File, K, (), V, R>)
where
    K: DBData,
    V: DBData,
    R: DBWeight;

impl<K, V, R> FileOrderedMergeBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn copy_key_if<'a, F>(&mut self, cursor: &mut FileOrderedCursor<K, V, R>, filter: &F)
    where
        F: Fn(
            &<<<FileOrderedMergeBuilder<K, V, R> as Builder>::Trie as Trie>::Cursor<'a> as Cursor<
                'a,
            >>::Key,
        ) -> bool,
    {
        let key = cursor.current_key();
        if filter(key) {
            let mut value_cursor = cursor.values();
            while value_cursor.valid() {
                self.0.write2(value_cursor.item()).unwrap();
                value_cursor.step();
            }
            self.0.write1((&key, &())).unwrap();
        }
        cursor.step();
    }

    fn copy_value(&mut self, cursor: &mut FileOrderedValueCursor<V, R>) {
        self.0
            .write2((cursor.current_value(), cursor.current_diff()))
            .unwrap();
        cursor.step();
    }

    fn merge_values<'a>(
        &mut self,
        mut cursor1: FileOrderedValueCursor<'a, V, R>,
        mut cursor2: FileOrderedValueCursor<'a, V, R>,
    ) -> bool {
        let mut n = 0;
        while cursor1.valid() && cursor2.valid() {
            let value1 = cursor1.current_value();
            let value2 = cursor2.current_value();
            match value1.cmp(value2) {
                Ordering::Less => {
                    self.copy_value(&mut cursor1);
                }
                Ordering::Equal => {
                    let mut sum = cursor1.current_diff().clone();
                    sum.add_assign_by_ref(cursor2.current_diff());
                    if !sum.is_zero() {
                        self.0.write2((value1, &sum)).unwrap();
                        n += 1;
                    }
                    cursor1.step();
                    cursor2.step();
                }

                Ordering::Greater => {
                    self.copy_value(&mut cursor2);
                }
            }
        }

        while cursor1.valid() {
            self.copy_value(&mut cursor1);
            n += 1;
        }
        while cursor2.valid() {
            self.copy_value(&mut cursor2);
            n += 1;
        }
        n > 0
    }
}

impl<K, V, R> Builder for FileOrderedMergeBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Trie = FileOrderedLayer<K, V, R>;

    fn boundary(&mut self) -> usize {
        self.keys()
    }

    fn done(self) -> Self::Trie {
        FileOrderedLayer {
            file: Reader::new(self.0.close().unwrap()).unwrap(),
            lower_bound: 0,
            _phantom: PhantomData,
        }
    }
}

impl<K, V, R> MergeBuilder for FileOrderedMergeBuilder<K, V, R>
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
            let key = cursor.current_key();
            if filter(key) {
                let mut value_cursor = cursor.values();
                while value_cursor.valid() {
                    self.0.write2(value_cursor.item()).unwrap();
                    value_cursor.step();
                }
                self.0.write1((&key, &())).unwrap();
            }
            cursor.step();
        }
    }

    fn push_merge_retain_keys<'a, F>(
        &'a mut self,
        mut cursor1: <Self::Trie as Trie>::Cursor<'a>,
        mut cursor2: <Self::Trie as Trie>::Cursor<'a>,
        filter: &F,
    ) where
        F: Fn(&<<Self::Trie as Trie>::Cursor<'a> as Cursor<'a>>::Key) -> bool,
    {
        while cursor1.valid() && cursor2.valid() {
            let key1 = cursor1.current_key();
            let key2 = cursor2.current_key();
            match key1.cmp(key2) {
                Ordering::Less => {
                    self.copy_key_if(&mut cursor1, filter);
                }
                Ordering::Equal => {
                    if filter(key1) && self.merge_values(cursor1.values(), cursor2.values()) {
                        self.0.write1((key1, &())).unwrap();
                    }
                    cursor1.step();
                    cursor2.step();
                }

                Ordering::Greater => {
                    self.copy_key_if(&mut cursor2, filter);
                }
            }
        }

        while cursor1.valid() {
            self.copy_key_if(&mut cursor1, filter);
        }
        while cursor2.valid() {
            self.copy_key_if(&mut cursor2, filter);
        }
    }
}

pub struct FileOrderedTupleBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    writer: Writer2<File, K, (), V, R>,
    key: Option<K>,
}

impl<K, V, R> Builder for FileOrderedTupleBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Trie = FileOrderedLayer<K, V, R>;

    fn boundary(&mut self) -> usize {
        if let Some(key) = self.key.take() {
            self.writer.write1((&key, &())).unwrap()
        }
        self.writer.n_rows() as usize
    }

    fn done(mut self) -> Self::Trie {
        self.boundary();
        FileOrderedLayer {
            file: Reader::new(self.writer.close().unwrap()).unwrap(),
            lower_bound: 0,
            _phantom: PhantomData,
        }
    }
}

impl<K, V, R> TupleBuilder for FileOrderedTupleBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Item = (K, (V, R));

    fn new() -> Self {
        Self {
            writer: Writer2::new(tempfile().unwrap(), Parameters::default()).unwrap(),
            key: None,
        }
    }

    fn with_capacity(_capacity: usize) -> Self {
        Self::new()
    }

    fn reserve_tuples(&mut self, _additional: usize) {}

    fn tuples(&self) -> usize {
        self.writer.n_rows() as usize
    }

    fn push_tuple(&mut self, (key, val): (K, (V, R))) {
        if let Some(ref cur_key) = self.key {
            if *cur_key != key {
                self.writer.write1((cur_key, &())).unwrap();
                self.key = Some(key);
            }
        } else {
            self.key = Some(key);
        }
        self.writer.write2((&val.0, &val.1)).unwrap();
    }
}

#[derive(Debug)]
pub struct FileOrderedCursor<'s, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    storage: &'s FileOrderedLayer<K, V, R>,
    key: Option<K>,
    cursor: FileCursor<'s, K, ()>,
}

impl<'s, K, V, R> FileOrderedCursor<'s, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    pub fn new(pos: usize, storage: &'s FileOrderedLayer<K, V, R>, bounds: (usize, usize)) -> Self {
        let cursor = storage
            .file
            .rows()
            .subset(bounds.0 as u64..bounds.1 as u64)
            .nth((pos - bounds.0) as u64)
            .unwrap();
        let key = unsafe { cursor.key() };
        Self {
            cursor,
            storage,
            key,
        }
    }

    pub fn current_key(&self) -> &K {
        self.key.as_ref().unwrap()
    }

    pub fn take_current_key(&mut self) -> Option<K> {
        let key = self.key.take();
        self.step();
        key
    }

    fn move_cursor(&mut self, f: impl Fn(&mut FileCursor<'s, K, ()>)) {
        f(&mut self.cursor);
        self.key = unsafe { self.cursor.key() };
    }
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

    type ValueCursor = FileOrderedValueCursor<'s, V, R>;

    fn keys(&self) -> usize {
        self.cursor.n_rows() as usize
    }

    fn item<'a>(&'a self) -> Self::Item<'a> {
        self.current_key()
    }

    fn values(&self) -> FileOrderedValueCursor<'s, V, R> {
        if self.valid() {
            todo!()
        } else {
            todo!()
        }
    }

    fn step(&mut self) {
        self.cursor.move_next().unwrap();
        self.key = unsafe { self.cursor.key() };
    }

    fn seek(&mut self, key: &Self::Key) {
        unsafe { self.cursor.advance_to_value_or_larger(key) }.unwrap();
        self.key = unsafe { self.cursor.key() };
    }

    fn valid(&self) -> bool {
        self.cursor.has_value()
    }

    fn rewind(&mut self) {
        self.cursor.move_first().unwrap();
        self.key = unsafe { self.cursor.key() };
    }

    fn position(&self) -> usize {
        self.cursor.position() as usize
    }

    fn reposition(&mut self, lower: usize, upper: usize) {
        self.move_cursor(|cursor| {
            *cursor = self
                .storage
                .file
                .rows()
                .subset(lower as u64..upper as u64)
                .first()
                .unwrap()
        });
    }

    fn step_reverse(&mut self) {
        self.cursor.move_prev().unwrap();
        self.key = unsafe { self.cursor.key() };
    }

    fn seek_reverse(&mut self, key: &Self::Key) {
        unsafe { self.cursor.rewind_to_value_or_smaller(key) }.unwrap();
        self.key = unsafe { self.cursor.key() };
    }

    fn fast_forward(&mut self) {
        self.cursor.move_last().unwrap();
        self.key = unsafe { self.cursor.key() };
    }
}

#[derive(Debug)]
pub struct FileOrderedValueCursor<'s, V, R>
where
    V: DBData,
    R: DBWeight,
{
    item: Option<(V, R)>,
    cursor: FileCursor<'s, V, R>,
}

impl<'s, V, R> FileOrderedValueCursor<'s, V, R>
where
    V: DBData,
    R: DBWeight,
{
    pub fn new<K>(parent_row: &'s FileCursor<K, ()>) -> Self
    where
        K: Rkyv,
    {
        let cursor = parent_row.next_column().unwrap().first().unwrap();
        let item = unsafe { cursor.item() };
        Self { cursor, item }
    }

    pub fn current_value(&self) -> &V {
        &self.item.as_ref().unwrap().0
    }

    pub fn current_diff(&self) -> &R {
        &self.item.as_ref().unwrap().1
    }

    pub fn current_item(&self) -> (&V, &R) {
        let item = self.item.as_ref().unwrap();
        (&item.0, &item.1)
    }

    pub fn take_current_item(&mut self) -> Option<(V, R)> {
        let item = self.item.take();
        self.step();
        item
    }

    fn move_cursor(&mut self, f: impl Fn(&mut FileCursor<'s, V, R>)) {
        f(&mut self.cursor);
        self.item = unsafe { self.cursor.item() };
    }
}

impl<'s, V, R> Cursor<'s> for FileOrderedValueCursor<'s, V, R>
where
    V: DBData,
    R: DBWeight,
{
    type Key = V;

    type Item<'k> = (&'k V, &'k R)
    where
        Self: 'k;

    type ValueCursor = ();

    fn keys(&self) -> usize {
        self.cursor.n_rows() as usize
    }

    fn item<'a>(&'a self) -> Self::Item<'a> {
        self.current_item()
    }

    fn values(&self) {}

    fn step(&mut self) {
        self.move_cursor(|cursor| cursor.move_next().unwrap());
    }

    fn seek(&mut self, key: &Self::Key) {
        self.move_cursor(|cursor| unsafe { cursor.advance_to_value_or_larger(key) }.unwrap());
    }

    fn valid(&self) -> bool {
        self.cursor.has_value()
    }

    fn rewind(&mut self) {
        self.move_cursor(|cursor| cursor.move_first().unwrap());
    }

    fn position(&self) -> usize {
        self.cursor.position() as usize
    }

    fn reposition(&mut self, _lower: usize, _upper: usize) {
        todo!()
    }

    fn step_reverse(&mut self) {
        self.move_cursor(|cursor| cursor.move_prev().unwrap());
    }

    fn seek_reverse(&mut self, key: &Self::Key) {
        self.move_cursor(|cursor| unsafe { cursor.rewind_to_value_or_smaller(key) }.unwrap());
    }

    fn fast_forward(&mut self) {
        self.move_cursor(|cursor| cursor.move_last().unwrap());
    }
}
