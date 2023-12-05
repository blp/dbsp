use crate::column_layer::PersistedColumnLayer;
use crate::Persistence;
use dbsp::algebra::{AddAssignByRef, HasZero, MonoidValue};
use dbsp::trace::Deserializable;
use dbsp::{
    trace::layers::{advance, Builder, Cursor, MergeBuilder, Trie, TupleBuilder},
    DBData, DBWeight,
};
use rkyv::{Archive, Deserialize, Infallible};
use size_of::SizeOf;
use std::cmp::{min, Ordering};

/// A builder for ordered values
#[derive(SizeOf, Debug, Clone)]
pub struct PersistedColumnLayerBuilder<K, R>
where
    K: DBData,
    R: DBWeight,
{
    // Invariant: `keys.len() == diffs.len()`
    keys: Vec<K::Archived>,
    diffs: Vec<R::Archived>,
}

impl<K, R> PersistedColumnLayerBuilder<K, R>
where
    K: DBData,
    R: DBWeight,
{
    /// Assume the invariants of the current builder
    ///
    /// # Safety
    ///
    /// Requires that `keys` and `diffs` have the exact same length
    unsafe fn assume_invariants(&self) {}
}

impl<K, R> Builder for PersistedColumnLayerBuilder<K, R>
where
    K: DBData,
    R: DBWeight,
    <K as Archive>::Archived: Clone,
    <R as Archive>::Archived: Clone,
    <K as Deserializable>::ArchivedDeser: Ord + Clone + 'static,
    <R as Deserializable>::ArchivedDeser: Ord + Clone + MonoidValue + 'static,
    <K as Deserializable>::ArchivedDeser: PartialOrd<K>,
{
    type Trie = PersistedColumnLayer<K, R>;

    fn boundary(&mut self) -> usize {
        unsafe { self.assume_invariants() }
        self.keys.len()
    }

    fn done(self) -> Self::Trie {
        unsafe { self.assume_invariants() }

        // TODO: Should we call `.shrink_to_fit()` here?
        /*PersistedColumnLayer {
            header: FileHeader {},
            metadata: Metadata {},
            block: Default::default(),
            backend: Box::new(()),
            _phantom: Default::default(),
        }*/
        unimplemented!()
    }
}

impl<K, R> MergeBuilder for PersistedColumnLayerBuilder<K, R>
where
    K: DBData,
    R: DBWeight,
    <K as Archive>::Archived: Clone,
    <R as Archive>::Archived: Clone,
    <K as Deserializable>::ArchivedDeser: Ord + Clone,
    <R as Deserializable>::ArchivedDeser: Ord + Clone + MonoidValue,
    <K as Deserializable>::ArchivedDeser: PartialOrd<K>,
{
    fn with_capacity(left: &Self::Trie, right: &Self::Trie) -> Self {
        let capacity = Trie::keys(left) + Trie::keys(right);
        Self::with_key_capacity(capacity)
    }

    fn with_key_capacity(capacity: usize) -> Self {
        Self {
            keys: Vec::with_capacity(capacity),
            diffs: Vec::with_capacity(capacity),
        }
    }

    fn reserve(&mut self, additional: usize) {
        unsafe { self.assume_invariants() }
        self.keys.reserve(additional);
        self.diffs.reserve(additional);
        unsafe { self.assume_invariants() }
    }

    fn keys(&self) -> usize {
        self.keys.len()
    }

    fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
        unsafe {
            self.assume_invariants();
            other.assume_invariants();
        }

        assert!(lower <= other.data().keys.len() && upper <= other.data().keys.len());
        self.keys
            .extend_from_slice(&other.data().keys[lower..upper]);
        self.diffs
            .extend_from_slice(&other.data().diffs[lower..upper]);

        unsafe { self.assume_invariants() }
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
        unsafe {
            self.assume_invariants();
            other.assume_invariants();
        }

        self.keys.reserve(upper - lower);
        self.diffs.reserve(upper - lower);

        assert!(lower <= other.data().keys.len() && upper <= other.data().keys.len());
        for index in lower..upper {
            if filter(
                &other.data().keys[index]
                    .deserialize(&mut Infallible)
                    .unwrap(),
            ) {
                self.keys.push(other.data().keys[index].clone());
                self.diffs.push(other.data().diffs[index].clone());
            }
        }
        unsafe { self.assume_invariants() }
    }

    fn push_merge<'a>(
        &'a mut self,
        cursor1: <Self::Trie as Trie>::Cursor<'a>,
        cursor2: <Self::Trie as Trie>::Cursor<'a>,
    ) {
        unsafe { self.assume_invariants() }

        let (trie1, trie2) = (cursor1.storage(), cursor2.storage());
        trie1.assume_invariants();
        trie2.assume_invariants();

        let (_, upper1) = cursor1.bounds();
        let mut lower1 = cursor1.position();
        let (_, upper2) = cursor2.bounds();
        let mut lower2 = cursor2.position();

        let reserved = (upper1 - lower1) + (upper2 - lower2);
        self.reserve(reserved);

        // while both mergees are still active
        while lower1 < upper1 && lower2 < upper2 {
            let key1 = &trie1.data().keys[lower1];
            let key2 = &trie2.data().keys[lower2];
            match key1.cmp(key2) {
                Ordering::Less => {
                    // determine how far we can advance lower1 until we reach/pass lower2
                    let step = 1 + advance(&trie1.data().keys[(1 + lower1)..upper1], |x| {
                        x < &trie2.data().keys[lower2]
                    });

                    let step = min(step, 1000);
                    self.copy_range(trie1, lower1, lower1 + step);

                    lower1 += step;
                }

                Ordering::Equal => {
                    let mut sum = trie1.data().diffs[lower1].clone();
                    sum.add_assign_by_ref(&trie2.data().diffs[lower2]);

                    if !sum.is_zero() {
                        self.push_tuple((key1.clone(), sum));
                    }

                    lower1 += 1;
                    lower2 += 1;
                }

                Ordering::Greater => {
                    // determine how far we can advance lower2 until we reach/pass lower1
                    let step = 1 + advance(&trie2.data().keys[(1 + lower2)..upper2], |x| {
                        x < &trie1.data().keys[lower1]
                    });

                    let step = min(step, 1000);
                    self.copy_range(trie2, lower2, lower2 + step);

                    lower2 += step;
                }
            }
        }

        if lower1 < upper1 {
            self.copy_range(trie1, lower1, upper1);
        }
        if lower2 < upper2 {
            self.copy_range(trie2, lower2, upper2);
        }

        unsafe { self.assume_invariants() }
    }

    fn push_merge_retain_keys<'a, F>(
        &'a mut self,
        cursor1: <Self::Trie as Trie>::Cursor<'a>,
        cursor2: <Self::Trie as Trie>::Cursor<'a>,
        filter: &F,
    ) where
        F: Fn(&<<Self::Trie as Trie>::Cursor<'a> as Cursor<'a>>::Key) -> bool,
    {
        unsafe { self.assume_invariants() }

        let (trie1, trie2) = (cursor1.storage(), cursor2.storage());
        unsafe {
            trie1.assume_invariants();
            trie2.assume_invariants();
        }

        let (_, upper1) = cursor1.bounds();
        let mut lower1 = cursor1.position();
        let (_, upper2) = cursor2.bounds();
        let mut lower2 = cursor2.position();

        let reserved = (upper1 - lower1) + (upper2 - lower2);
        self.reserve(reserved);

        // while both mergees are still active
        while lower1 < upper1 && lower2 < upper2 {
            match trie1.data().keys[lower1].cmp(&trie2.data().keys[lower2]) {
                Ordering::Less => {
                    // determine how far we can advance lower1 until we reach/pass lower2
                    let step = 1 + advance(&trie1.data().keys[(1 + lower1)..upper1], |x| {
                        x < &trie2.data().keys[lower2]
                    });

                    let step = min(step, 1000);
                    self.copy_range_retain_keys(trie1, lower1, lower1 + step, filter);

                    lower1 += step;
                }

                Ordering::Equal => {
                    let key1 = &trie1.data().keys[lower1]
                        .deserialize(&mut Infallible)
                        .unwrap();
                    if filter(&key1) {
                        let mut sum = trie1.data().diffs[lower1].clone();
                        sum.add_assign_by_ref(&trie2.data().diffs[lower2]);

                        if !sum.is_zero() {
                            self.push_tuple((trie1.data().keys[lower1].clone(), sum));
                        }
                    }

                    lower1 += 1;
                    lower2 += 1;
                }

                Ordering::Greater => {
                    // determine how far we can advance lower2 until we reach/pass lower1
                    let step = 1 + advance(&trie2.data().keys[(1 + lower2)..upper2], |x| {
                        x < &trie1.data().keys[lower1]
                    });

                    let step = min(step, 1000);
                    self.copy_range_retain_keys(trie2, lower2, lower2 + step, filter);

                    lower2 += step;
                }
            }
        }

        if lower1 < upper1 {
            self.copy_range_retain_keys(trie1, lower1, upper1, filter);
        }
        if lower2 < upper2 {
            self.copy_range_retain_keys(trie2, lower2, upper2, filter);
        }

        unsafe { self.assume_invariants() }
    }
}

impl<K, R> TupleBuilder for PersistedColumnLayerBuilder<K, R>
where
    K: DBData,
    R: DBWeight,
    <K as Deserializable>::ArchivedDeser: Ord + Clone,
    <R as Deserializable>::ArchivedDeser: Ord + Clone + MonoidValue,
    <K as Deserializable>::ArchivedDeser: PartialOrd<K>,
{
    type Item = (K::Archived, R::Archived);

    fn new() -> Self {
        Self {
            keys: Vec::new(),
            diffs: Vec::new(),
        }
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            keys: Vec::with_capacity(capacity),
            diffs: Vec::with_capacity(capacity),
        }
    }

    fn reserve_tuples(&mut self, additional: usize) {
        self.keys.reserve(additional);
        self.diffs.reserve(additional);
    }

    fn tuples(&self) -> usize {
        unsafe { self.assume_invariants() }
        self.keys.len()
    }

    fn push_tuple(&mut self, (key, diff): Self::Item) {
        // if cfg!(debug_assertions) && !self.keys.is_empty() {
        //     debug_assert!(
        //         self.keys.last().unwrap() <= &key,
        //         "OrderedSetLeafBuilder expects sorted values to be passed to \
        //          `TupleBuilder::push_tuple()`",
        //      );
        // }

        debug_assert!(!diff.is_zero());
        unsafe { self.assume_invariants() }
        self.keys.push(key);
        self.diffs.push(diff);
        unsafe { self.assume_invariants() }
    }
}
