use crate::{
    trace::layers::{
        file::column_layer::FileColumnLayer, Builder, Cursor, MergeBuilder, Trie, TupleBuilder,
    },
    DBData, DBWeight,
};
use feldera_storage::file::{
    reader::Reader,
    writer::{Parameters, Writer1},
};
use std::{cmp::Ordering, fs::File, marker::PhantomData};
use tempfile::tempfile;

/// A builder for ordered values
pub struct FileColumnLayerBuilder<K, R>(Writer1<File, K, R>)
where
    K: DBData,
    R: DBWeight;

impl<K, R> Builder for FileColumnLayerBuilder<K, R>
where
    K: DBData,
    R: DBWeight,
{
    type Trie = FileColumnLayer<K, R>;

    fn boundary(&mut self) -> usize {
        self.keys()
    }

    fn done(self) -> Self::Trie {
        FileColumnLayer {
            file: Reader::new(self.0.close().unwrap()).unwrap(),
            lower_bound: 0,
            _phantom: PhantomData,
        }
    }
}

impl<K, R> MergeBuilder for FileColumnLayerBuilder<K, R>
where
    K: DBData,
    R: DBWeight,
{
    fn with_capacity(left: &Self::Trie, right: &Self::Trie) -> Self {
        let capacity = Trie::keys(left) + Trie::keys(right);
        Self::with_key_capacity(capacity)
    }

    fn with_key_capacity(_capacity: usize) -> Self {
        Self(Writer1::new(tempfile().unwrap(), Parameters::default()).unwrap())
    }

    fn reserve(&mut self, _additional: usize) {}

    fn keys(&self) -> usize {
        self.0.n_rows() as usize
    }

    fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
        let mut cursor = other.cursor_from(lower, upper);
        while cursor.valid() {
            self.0.write(cursor.current_item()).unwrap();
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
            if filter(&item.0) {
                self.0.write(item).unwrap();
            }
            cursor.step();
        }
    }

    fn push_merge<'a>(
        &'a mut self,
        mut cursor1: <Self::Trie as Trie>::Cursor<'a>,
        mut cursor2: <Self::Trie as Trie>::Cursor<'a>,
    ) {
        while cursor1.valid() && cursor2.valid() {
            match cursor1.current_key().cmp(cursor2.current_key()) {
                Ordering::Less => {
                    self.0.write(cursor1.current_item()).unwrap();
                    cursor1.step();
                }
                Ordering::Equal => {
                    let mut sum = cursor1.current_diff().clone();
                    sum.add_assign_by_ref(cursor2.current_diff());

                    if !sum.is_zero() {
                        let item = (cursor1.current_key().clone(), sum);
                        self.0.write(&item).unwrap();
                    }
                    cursor1.step();
                    cursor2.step();
                }

                Ordering::Greater => {
                    self.0.write(cursor2.current_item()).unwrap();
                    cursor2.step();
                }
            }
        }

        while cursor1.valid() {
            self.0.write(cursor1.current_item()).unwrap();
            cursor1.step();
        }
        while cursor2.valid() {
            self.0.write(cursor2.current_item()).unwrap();
            cursor2.step();
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
            match cursor1.current_key().cmp(cursor2.current_key()) {
                Ordering::Less => {
                    if filter(cursor1.current_key()) {
                        self.0.write(cursor1.current_item()).unwrap();
                    }
                    cursor1.step();
                }
                Ordering::Equal => {
                    let mut sum = cursor1.current_diff().clone();
                    sum.add_assign_by_ref(cursor2.current_diff());

                    if !sum.is_zero() && filter(cursor1.current_key()) {
                        let item = (cursor1.current_key().clone(), sum);
                        self.0.write(&item).unwrap();
                    }
                    cursor1.step();
                    cursor2.step();
                }

                Ordering::Greater => {
                    if filter(cursor2.current_key()) {
                        self.0.write(cursor2.current_item()).unwrap();
                    }
                    cursor2.step();
                }
            }
        }

        while cursor1.valid() {
            if filter(cursor1.current_key()) {
                self.0.write(cursor1.current_item()).unwrap();
            }
            cursor1.step();
        }
        while cursor2.valid() {
            if filter(cursor2.current_key()) {
                self.0.write(cursor2.current_item()).unwrap();
            }
            cursor2.step();
        }
    }
}

impl<K, R> TupleBuilder for FileColumnLayerBuilder<K, R>
where
    K: DBData,
    R: DBWeight,
{
    type Item = (K, R);

    fn new() -> Self {
        Self(Writer1::new(tempfile().unwrap(), Parameters::default()).unwrap())
    }

    fn with_capacity(_capacity: usize) -> Self {
        Self::new()
    }

    fn reserve_tuples(&mut self, _additional: usize) {}

    fn tuples(&self) -> usize {
        self.0.n_rows() as usize
    }

    fn push_tuple(&mut self, item: (K, R)) {
        self.0.write(&item).unwrap();
    }
}
