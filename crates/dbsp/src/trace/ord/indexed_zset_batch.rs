use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef},
    time::AntichainRef,
    trace::{
        layers::{
            file::ordered::{
                FileOrderedCursor, FileOrderedLayer, FileOrderedLayerConsumer,
                FileOrderedLayerValues, FileOrderedTupleBuilder, FileOrderedValueCursor,
            },
            Builder as TrieBuilder, Cursor as TrieCursor, MergeBuilder, Trie, TupleBuilder,
        },
        ord::merge_batcher::MergeBatcher,
        Batch, BatchReader, Builder, Consumer, Cursor, Filter, Merger, ValueConsumer,
    },
    DBData, DBWeight, NumEntries,
};
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::{
    fmt::{self, Debug, Display},
    marker::PhantomData,
    ops::{Add, AddAssign, Neg},
    rc::Rc,
};

/// An immutable collection of update tuples.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OrdIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    /// Where all the data is.
    #[doc(hidden)]
    pub layer: FileOrderedLayer<K, V, R>,
}

impl<K, V, R> Display for OrdIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
    FileOrderedLayer<K, V, R>: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "layer:\n{}",
            textwrap::indent(&self.layer.to_string(), "    ")
        )
    }
}

impl<K, V, R> Default for OrdIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    fn default() -> Self {
        Self::empty(())
    }
}

impl<K, V, R> From<FileOrderedLayer<K, V, R>> for OrdIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    fn from(layer: FileOrderedLayer<K, V, R>) -> Self {
        Self { layer }
    }
}

impl<K, V, R> From<FileOrderedLayer<K, V, R>> for Rc<OrdIndexedZSet<K, V, R>>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    fn from(layer: FileOrderedLayer<K, V, R>) -> Self {
        Rc::new(From::from(layer))
    }
}

impl<K, V, R> NumEntries for OrdIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    const CONST_NUM_ENTRIES: Option<usize> = FileOrderedLayer::<K, V, R>::CONST_NUM_ENTRIES;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.layer.num_entries_shallow()
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        self.layer.num_entries_deep()
    }
}

impl<K, V, R> NegByRef for OrdIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight + NegByRef,
{
    #[inline]
    fn neg_by_ref(&self) -> Self {
        Self {
            layer: self.layer.neg_by_ref(),
        }
    }
}

impl<K, V, R> Neg for OrdIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight + Neg<Output = R>,
{
    type Output = Self;

    #[inline]
    fn neg(self) -> Self {
        Self {
            layer: self.layer.neg(),
        }
    }
}

impl<K, V, R> Add<Self> for OrdIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Output = Self;
    #[inline]

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            layer: self.layer.add(rhs.layer),
        }
    }
}

impl<K, V, R> AddAssign<Self> for OrdIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        self.layer.add_assign(rhs.layer);
    }
}

impl<K, V, R> AddAssignByRef for OrdIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        self.layer.add_assign_by_ref(&rhs.layer);
    }
}

impl<K, V, R> AddByRef for OrdIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    fn add_by_ref(&self, rhs: &Self) -> Self {
        Self {
            layer: self.layer.add_by_ref(&rhs.layer),
        }
    }
}

impl<K, V, R> BatchReader for OrdIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Key = K;
    type Val = V;
    type Time = ();
    type R = R;
    type Cursor<'s> = OrdIndexedZSetCursor<'s, K, V, R>
    where
        V: 's;
    type Consumer = OrdIndexedZSetConsumer<K, V, R>;

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        OrdIndexedZSetCursor::new(self)
    }

    #[inline]
    fn consumer(self) -> Self::Consumer {
        OrdIndexedZSetConsumer {
            consumer: FileOrderedLayerConsumer::from(self.layer),
        }
    }

    #[inline]
    fn key_count(&self) -> usize {
        self.layer.keys()
    }

    #[inline]
    fn len(&self) -> usize {
        self.layer.tuples()
    }

    #[inline]
    fn lower(&self) -> AntichainRef<'_, ()> {
        AntichainRef::new(&[()])
    }

    #[inline]
    fn upper(&self) -> AntichainRef<'_, ()> {
        AntichainRef::empty()
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        self.layer.truncate_keys_below(lower_bound);
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut Vec<Self::Key>)
    where
        RG: Rng,
    {
        self.layer.sample_keys(rng, sample_size, sample);
    }
}

impl<K, V, R> Batch for OrdIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Item = (K, V);
    type Batcher = MergeBatcher<(K, V), (), R, Self>;
    type Builder = OrdIndexedZSetBuilder<K, V, R>;
    type Merger = OrdIndexedZSetMerger<K, V, R>;

    fn item_from(key: K, val: V) -> Self::Item {
        (key, val)
    }

    fn from_keys(time: Self::Time, keys: Vec<(Self::Key, Self::R)>) -> Self
    where
        Self::Val: From<()>,
    {
        Self::from_tuples(
            time,
            keys.into_iter()
                .map(|(k, w)| ((k, From::from(())), w))
                .collect(),
        )
    }

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        OrdIndexedZSetMerger::new_merger(self, other)
    }

    fn recede_to(&mut self, _frontier: &()) {}

    fn empty(_time: Self::Time) -> Self {
        Self {
            layer: FileOrderedLayer::empty(),
        }
    }
}

/// State for an in-progress merge.
pub struct OrdIndexedZSetMerger<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    result: Option<FileOrderedLayer<K, V, R>>,
}

impl<K, V, R> Merger<K, V, (), R, OrdIndexedZSet<K, V, R>> for OrdIndexedZSetMerger<K, V, R>
where
    Self: SizeOf,
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    fn new_merger(_batch1: &OrdIndexedZSet<K, V, R>, _batch2: &OrdIndexedZSet<K, V, R>) -> Self {
        Self { result: None }
    }

    #[inline]
    fn done(mut self) -> OrdIndexedZSet<K, V, R> {
        OrdIndexedZSet {
            layer: self.result.take().unwrap_or_default(),
        }
    }

    fn work(
        &mut self,
        source1: &OrdIndexedZSet<K, V, R>,
        source2: &OrdIndexedZSet<K, V, R>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
        fuel: &mut isize,
    ) {
        if self.result.is_none() {
            let mut builder =
                <<FileOrderedLayer<K, V, R> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(
                    &source1.layer,
                    &source2.layer,
                );
            let cursor1 = source1.layer.cursor();
            let cursor2 = source2.layer.cursor();
            match (key_filter, value_filter) {
                (None, None) => builder.push_merge(cursor1, cursor2),
                (Some(key_filter), None) => {
                    builder.push_merge_retain_keys(cursor1, cursor2, key_filter)
                }
                (Some(key_filter), Some(value_filter)) => {
                    builder.push_merge_retain_values(cursor1, cursor2, key_filter, value_filter)
                }
                (None, Some(value_filter)) => {
                    builder.push_merge_retain_values(cursor1, cursor2, &|_| true, value_filter)
                }
            };
            self.result = Some(builder.done());
        }
        *fuel = 0;
    }
}

impl<K, V, R> SizeOf for OrdIndexedZSetMerger<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct OrdIndexedZSetCursor<'s, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    key_cursor: FileOrderedCursor<'s, K, V, R>,
    val_cursor: FileOrderedValueCursor<'s, K, V, R>,
}

impl<'s, K, V, R> OrdIndexedZSetCursor<'s, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn new(zset: &'s OrdIndexedZSet<K, V, R>) -> Self {
        let key_cursor = zset.layer.cursor();
        let val_cursor = key_cursor.values();
        Self {
            key_cursor,
            val_cursor,
        }
    }
}

impl<'s, K, V, R> Cursor<K, V, (), R> for OrdIndexedZSetCursor<'s, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn key(&self) -> &K {
        self.key_cursor.current_key()
    }

    fn val(&self) -> &V {
        self.val_cursor.current_value()
    }

    fn fold_times<F, U>(&mut self, init: U, mut fold: F) -> U
    where
        F: FnMut(U, &(), &R) -> U,
    {
        if self.val_cursor.valid() {
            fold(init, &(), self.val_cursor.current_diff())
        } else {
            init
        }
    }

    fn fold_times_through<F, U>(&mut self, _upper: &(), init: U, fold: F) -> U
    where
        F: FnMut(U, &(), &R) -> U,
    {
        self.fold_times(init, fold)
    }

    fn weight(&mut self) -> R {
        debug_assert!(self.val_cursor.valid());
        self.val_cursor.current_diff().clone()
    }

    fn key_valid(&self) -> bool {
        self.key_cursor.valid()
    }

    fn val_valid(&self) -> bool {
        self.val_cursor.valid()
    }

    fn step_key(&mut self) {
        self.key_cursor.step();
        self.val_cursor = self.key_cursor.values();
    }

    fn step_key_reverse(&mut self) {
        self.key_cursor.step_reverse();
        self.val_cursor = self.key_cursor.values();
    }

    fn seek_key(&mut self, key: &K) {
        self.key_cursor.seek(key);
        self.val_cursor = self.key_cursor.values();
    }

    fn seek_key_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.key_cursor.seek_with(|k| !predicate(k));
        self.val_cursor = self.key_cursor.values();
    }

    fn seek_key_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.key_cursor.seek_with_reverse(|k| !predicate(k));
        self.val_cursor = self.key_cursor.values();
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.key_cursor.seek_reverse(key);
        self.val_cursor = self.key_cursor.values();
    }

    fn step_val(&mut self) {
        self.val_cursor.step();
    }

    fn seek_val(&mut self, val: &V) {
        self.val_cursor.seek(val);
    }

    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        self.val_cursor.seek_val_with(|v| !predicate(v));
    }

    fn rewind_keys(&mut self) {
        self.key_cursor.rewind();
        self.val_cursor = self.key_cursor.values();
    }

    fn fast_forward_keys(&mut self) {
        self.key_cursor.fast_forward();
        self.val_cursor = self.key_cursor.values();
    }

    fn rewind_vals(&mut self) {
        self.val_cursor.rewind();
    }

    fn step_val_reverse(&mut self) {
        self.val_cursor.step_reverse();
    }

    fn seek_val_reverse(&mut self, val: &V) {
        self.val_cursor.seek_reverse(val);
    }

    fn seek_val_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        self.val_cursor.seek_val_with_reverse(|v| !predicate(v));
    }

    fn fast_forward_vals(&mut self) {
        self.val_cursor.fast_forward();
    }
}

type IndexBuilder<K, V, R> = FileOrderedTupleBuilder<K, V, R>;

/// A builder for creating layers from unsorted update tuples.
pub struct OrdIndexedZSetBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    builder: IndexBuilder<K, V, R>,
}

impl<K, V, R> Builder<(K, V), (), R, OrdIndexedZSet<K, V, R>> for OrdIndexedZSetBuilder<K, V, R>
where
    Self: SizeOf,
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    fn new_builder(_time: ()) -> Self {
        Self {
            builder: IndexBuilder::<K, V, R>::new(),
        }
    }

    #[inline]
    fn with_capacity(_time: (), capacity: usize) -> Self {
        Self {
            builder: <IndexBuilder<K, V, R> as TupleBuilder>::with_capacity(capacity),
        }
    }

    #[inline]
    fn reserve(&mut self, _additional: usize) {}

    #[inline]
    fn push(&mut self, ((key, val), diff): ((K, V), R)) {
        self.builder.push_tuple((key, (val, diff)));
    }

    #[inline(never)]
    fn done(self) -> OrdIndexedZSet<K, V, R> {
        OrdIndexedZSet {
            layer: self.builder.done(),
        }
    }
}

impl<K, V, R> SizeOf for OrdIndexedZSetBuilder<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

pub struct OrdIndexedZSetConsumer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    consumer: FileOrderedLayerConsumer<K, V, R>,
}

impl<K, V, R> Consumer<K, V, R, ()> for OrdIndexedZSetConsumer<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type ValueConsumer<'a> = OrdIndexedZSetValueConsumer<'a, K, V,  R>
    where
        Self: 'a;

    fn key_valid(&self) -> bool {
        self.consumer.key_valid()
    }

    fn peek_key(&self) -> &K {
        self.consumer.peek_key()
    }

    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        let (key, consumer) = self.consumer.next_key();
        (key, OrdIndexedZSetValueConsumer::new(consumer))
    }

    fn seek_key(&mut self, key: &K)
    where
        K: Ord,
    {
        self.consumer.seek_key(key)
    }
}

pub struct OrdIndexedZSetValueConsumer<'a, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    consumer: FileOrderedLayerValues<'a, K, V, R>,
    __type: PhantomData<K>,
}

impl<'a, K, V, R> OrdIndexedZSetValueConsumer<'a, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    #[inline]
    const fn new(consumer: FileOrderedLayerValues<'a, K, V, R>) -> Self {
        Self {
            consumer,
            __type: PhantomData,
        }
    }
}

impl<'a, K, V, R> ValueConsumer<'a, V, R, ()> for OrdIndexedZSetValueConsumer<'a, K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn value_valid(&self) -> bool {
        self.consumer.value_valid()
    }

    fn next_value(&mut self) -> (V, R, ()) {
        self.consumer.next_value()
    }

    fn remaining_values(&self) -> usize {
        self.consumer.remaining_values()
    }
}

impl<K, V, R> SizeOf for OrdIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

impl<K, V, R> Archive for OrdIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, V, R, S> Serialize<S> for OrdIndexedZSet<K, V, R>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
    S: Serializer + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, V, R, D> Deserialize<OrdIndexedZSet<K, V, R>, D> for Archived<OrdIndexedZSet<K, V, R>>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<OrdIndexedZSet<K, V, R>, D::Error> {
        unimplemented!();
    }
}
