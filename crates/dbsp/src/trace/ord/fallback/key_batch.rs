use crate::{
    dynamic::{
        DataTrait, DynDataTyped, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase, Factory,
        WeightTrait,
    },
    storage::{backend::Storage, file::reader::Cursor as FileCursor},
    time::{Antichain, AntichainRef},
    trace::{
        layers::{Cursor as _, LeafCursor, OrdOffset},
        ord::{
            file::key_batch::{FileKeyBuilder, FileKeyCursor, FileKeyMerger},
            merge_batcher::MergeBatcher,
            vec::key_batch::{OrdKeyBuilder, OrdKeyCursor, OrdKeyMerger},
            FileKeyBatch, OrdKeyBatch,
        },
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor,
        FileKeyBatchFactories, Filter, Merger, OrdKeyBatchFactories, TimedBuilder, WeightedItem,
    },
    DBData, DBWeight, NumEntries, Runtime, Timestamp,
};
use dyn_clone::clone_box;
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::{
    cmp::Ordering,
    fmt::{self, Debug},
    path::PathBuf,
};

use super::{cursor::DynamicCursor, utils::Position};

pub struct FallbackKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    file: FileKeyBatchFactories<K, T, R>,
    vec: OrdKeyBatchFactories<K, T, R>,
}

impl<K, T, R> Clone for FallbackKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            file: self.file.clone(),
            vec: self.vec.clone(),
        }
    }
}

impl<K, T, R> BatchReaderFactories<K, DynUnit, T, R> for FallbackKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<DynUnit>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            file: FileKeyBatchFactories::new::<KType, VType, RType>(),
            vec: OrdKeyBatchFactories::new::<KType, VType, RType>(),
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.file.key_factory()
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        self.file.keys_factory()
    }

    fn val_factory(&self) -> &'static dyn Factory<DynUnit> {
        self.file.val_factory()
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.file.weight_factory()
    }
}

impl<K, T, R> BatchFactories<K, DynUnit, T, R> for FallbackKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, DynUnit>> {
        self.file.item_factory()
    }

    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, DynUnit, R>> {
        self.file.weighted_item_factory()
    }

    fn weighted_items_factory(
        &self,
    ) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, DynUnit>, R>> {
        self.file.weighted_items_factory()
    }
}

/// A batch of keys with weights and times.
///
/// Each tuple in `FallbackKeyBatch<K, T, R>` has key type `K`, value type `()`,
/// weight type `R`, and time type `R`.
pub struct FallbackKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories: FallbackKeyBatchFactories<K, T, R>,
    inner: Inner<K, T, R>,
}

enum Inner<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    Vec(OrdKeyBatch<K, T, R>),
    File(FileKeyBatch<K, T, R>),
}

impl<K, T, R> Inner<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn as_file(&self) -> Option<&FileKeyBatch<K, T, R>> {
        match self {
            Inner::Vec(_vec) => None,
            Inner::File(file) => Some(file),
        }
    }
    fn as_vec(&self) -> Option<&OrdKeyBatch<K, T, R>> {
        match self {
            Inner::Vec(vec) => Some(vec),
            Inner::File(_file) => None,
        }
    }
}

impl<K, T, R> Debug for FallbackKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            Inner::Vec(vec) => vec.fmt(f),
            Inner::File(file) => file.fmt(f),
        }
    }
}

impl<K, T, R> Clone for FallbackKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            inner: match &self.inner {
                Inner::Vec(vec) => Inner::Vec(vec.clone()),
                Inner::File(file) => Inner::File(file.clone()),
            },
        }
    }
}

impl<K, T, R> NumEntries for FallbackKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        match &self.inner {
            Inner::Vec(vec) => vec.num_entries_shallow(),
            Inner::File(file) => file.num_entries_shallow(),
        }
    }

    fn num_entries_deep(&self) -> usize {
        match &self.inner {
            Inner::Vec(vec) => vec.num_entries_deep(),
            Inner::File(file) => file.num_entries_deep(),
        }
    }
}

impl<K, T, R> BatchReader for FallbackKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Factories = FallbackKeyBatchFactories<K, T, R>;
    type Key = K;
    type Val = DynUnit;
    type Time = T;
    type R = R;
    type Cursor<'s> = DynamicCursor<'s, K, DynUnit, T, R>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        DynamicCursor(match &self.inner {
            Inner::Vec(vec) => Box::new(vec.cursor()),
            Inner::File(file) => Box::new(file.cursor()),
        })
    }

    #[inline]
    fn key_count(&self) -> usize {
        match &self.inner {
            Inner::Vec(vec) => vec.key_count(),
            Inner::File(file) => file.key_count(),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match &self.inner {
            Inner::Vec(vec) => vec.len(),
            Inner::File(file) => file.len(),
        }
    }

    fn lower(&self) -> AntichainRef<'_, T> {
        match &self.inner {
            Inner::Vec(vec) => vec.lower(),
            Inner::File(file) => file.lower(),
        }
    }

    fn upper(&self) -> AntichainRef<'_, T> {
        match &self.inner {
            Inner::Vec(vec) => vec.upper(),
            Inner::File(file) => file.upper(),
        }
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        match &mut self.inner {
            Inner::Vec(vec) => vec.truncate_keys_below(lower_bound),
            Inner::File(file) => file.truncate_keys_below(lower_bound),
        }
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, output: &mut DynVec<Self::Key>)
    where
        Self::Time: PartialEq<()>,
        RG: Rng,
    {
        match &self.inner {
            Inner::Vec(vec) => vec.sample_keys(rng, sample_size, output),
            Inner::File(file) => file.sample_keys(rng, sample_size, output),
        }
    }
}

impl<K, T, R> Batch for FallbackKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = FallbackKeyBuilder<K, T, R>;
    type Merger = FallbackKeyMerger<K, T, R>;

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        Self::Merger::new_merger(self, other)
    }

    fn recede_to(&mut self, frontier: &T) {
        match &mut self.inner {
            Inner::Vec(vec) => vec.recede_to(frontier),
            Inner::File(file) => file.recede_to(frontier),
        }
    }

    fn persistent_id(&self) -> Option<PathBuf> {
        match &self.inner {
            Inner::Vec(vec) => vec.persistent_id(),
            Inner::File(file) => file.persistent_id(),
        }
    }
}

/// State for an in-progress merge.
pub struct FallbackKeyMerger<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories: FallbackKeyBatchFactories<K, T, R>,
    lower: Antichain<T>,
    upper: Antichain<T>,
    inner: MergerInner<K, T, R>,
}

enum MergerInner<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    AllFile(FileKeyMerger<K, T, R>),
    AllVec(OrdKeyMerger<K, T, R>),
    ToVec(GenericMerger<K, T, R, OrdKeyBatch<K, T, R>>),
    ToFile(GenericMerger<K, T, R, FileKeyBatch<K, T, R>>),
}

impl<K, T, R> Merger<K, DynUnit, T, R, FallbackKeyBatch<K, T, R>> for FallbackKeyMerger<K, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new_merger(batch1: &FallbackKeyBatch<K, T, R>, batch2: &FallbackKeyBatch<K, T, R>) -> Self {
        FallbackKeyMerger {
            factories: batch1.factories.clone(),
            lower: batch1.lower().meet(batch2.lower()),
            upper: batch1.upper().join(batch2.upper()),
            inner: if batch1.len() + batch2.len() < Runtime::min_storage_rows() {
                match (&batch1.inner, &batch2.inner) {
                    (Inner::Vec(vec1), Inner::Vec(vec2)) => {
                        MergerInner::AllVec(OrdKeyMerger::new_merger(vec1, vec2))
                    }
                    _ => MergerInner::ToVec(GenericMerger::new(&batch1.factories.vec)),
                }
            } else {
                match (&batch1.inner, &batch2.inner) {
                    (Inner::File(file1), Inner::File(file2)) => {
                        MergerInner::AllFile(FileKeyMerger::new_merger(file1, file2))
                    }
                    _ => MergerInner::ToFile(GenericMerger::new(&batch1.factories.file)),
                }
            },
        }
    }

    fn done(self) -> FallbackKeyBatch<K, T, R> {
        FallbackKeyBatch {
            factories: self.factories.clone(),
            inner: match self.inner {
                MergerInner::AllFile(merger) => Inner::File(merger.done()),
                MergerInner::AllVec(merger) => Inner::Vec(merger.done()),
                MergerInner::ToVec(merger) => {
                    Inner::Vec(merger.done_with_bounds(self.lower, self.upper))
                }
                MergerInner::ToFile(merger) => {
                    Inner::File(merger.done_with_bounds(self.lower, self.upper))
                }
            },
        }
    }

    fn work(
        &mut self,
        source1: &FallbackKeyBatch<K, T, R>,
        source2: &FallbackKeyBatch<K, T, R>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<DynUnit>>,
        fuel: &mut isize,
    ) {
        match &mut self.inner {
            MergerInner::AllFile(merger) => merger.work(
                source1.inner.as_file().unwrap(),
                source2.inner.as_file().unwrap(),
                key_filter,
                value_filter,
                fuel,
            ),
            MergerInner::AllVec(merger) => merger.work(
                source1.inner.as_vec().unwrap(),
                source2.inner.as_vec().unwrap(),
                key_filter,
                value_filter,
                fuel,
            ),
            MergerInner::ToVec(merger) => match (&source1.inner, &source2.inner) {
                (Inner::File(a), Inner::File(b)) => {
                    merger.work(a, b, key_filter, value_filter, fuel)
                }
                (Inner::Vec(a), Inner::File(b)) => {
                    merger.work(a, b, key_filter, value_filter, fuel)
                }
                (Inner::File(a), Inner::Vec(b)) => {
                    merger.work(a, b, key_filter, value_filter, fuel)
                }
                (Inner::Vec(a), Inner::Vec(b)) => merger.work(a, b, key_filter, value_filter, fuel),
            },
            MergerInner::ToFile(merger) => match (&source1.inner, &source2.inner) {
                (Inner::File(a), Inner::File(b)) => {
                    merger.work(a, b, key_filter, value_filter, fuel)
                }
                (Inner::Vec(a), Inner::File(b)) => {
                    merger.work(a, b, key_filter, value_filter, fuel)
                }
                (Inner::File(a), Inner::Vec(b)) => {
                    merger.work(a, b, key_filter, value_filter, fuel)
                }
                (Inner::Vec(a), Inner::Vec(b)) => merger.work(a, b, key_filter, value_filter, fuel),
            },
        }
    }
}

fn filter<T>(f: &Option<Filter<T>>, t: &T) -> bool
where
    T: ?Sized,
{
    f.as_ref().map_or(true, |f| f(t))
}

impl<K, T, R> SizeOf for FallbackKeyMerger<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, context: &mut size_of::Context) {
        match &self.inner {
            MergerInner::AllFile(file) => file.size_of_children(context),
            MergerInner::AllVec(vec) => vec.size_of_children(context),
            MergerInner::ToFile(merger) => merger.size_of_children(context),
            MergerInner::ToVec(merger) => merger.size_of_children(context),
        }
    }
}

/// A builder for creating layers from unsorted update tuples.
pub struct FallbackKeyBuilder<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories: FallbackKeyBatchFactories<K, T, R>,
    inner: BuilderInner<K, T, R>,
}

enum BuilderInner<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    File(FileKeyBuilder<K, T, R>),
    Vec(OrdKeyBuilder<K, T, R>),
}

impl<K, T, R> Builder<FallbackKeyBatch<K, T, R>> for FallbackKeyBuilder<K, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn new_builder(factories: &FallbackKeyBatchFactories<K, T, R>, time: T) -> Self {
        Self::with_capacity(factories, time, 0)
    }

    #[inline]
    fn with_capacity(
        factories: &FallbackKeyBatchFactories<K, T, R>,
        time: T,
        capacity: usize,
    ) -> Self {
        Self {
            factories: factories.clone(),
            inner: if capacity < Runtime::min_storage_rows() {
                BuilderInner::Vec(OrdKeyBuilder::with_capacity(&factories.vec, time, capacity))
            } else {
                BuilderInner::File(FileKeyBuilder::with_capacity(
                    &factories.file,
                    time,
                    capacity,
                ))
            },
        }
    }

    #[inline]
    fn reserve(&mut self, _additional: usize) {}

    #[inline]
    fn push(&mut self, item: &mut DynPair<DynPair<K, DynUnit>, R>) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push(item),
            BuilderInner::Vec(vec) => vec.push(item),
        }
    }

    #[inline]
    fn push_refs(&mut self, key: &K, val: &DynUnit, weight: &R) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push_refs(key, val, weight),
            BuilderInner::Vec(vec) => vec.push_refs(key, val, weight),
        }
    }

    #[inline]
    fn push_vals(&mut self, key: &mut K, val: &mut DynUnit, weight: &mut R) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push_vals(key, val, weight),
            BuilderInner::Vec(vec) => vec.push_vals(key, val, weight),
        }
    }

    #[inline(never)]
    fn done(self) -> FallbackKeyBatch<K, T, R> {
        FallbackKeyBatch {
            factories: self.factories,
            inner: match self.inner {
                BuilderInner::File(file) => Inner::File(file.done()),
                BuilderInner::Vec(vec) => Inner::Vec(vec.done()),
            },
        }
    }
}

impl<K, T, R> SizeOf for FallbackKeyBuilder<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

impl<K, T, R> SizeOf for FallbackKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

impl<K, T, R> Archive for FallbackKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, T, R, S> Serialize<S> for FallbackKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    S: Serializer + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, T, R, D> Deserialize<FallbackKeyBatch<K, T, R>, D> for Archived<FallbackKeyBatch<K, T, R>>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FallbackKeyBatch<K, T, R>, D::Error> {
        unimplemented!();
    }
}

struct GenericMerger<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: Batch + BatchReader<Key = K, Val = DynUnit, R = R, Time = T>,
    O::Builder: TimedBuilder<O>,
{
    builder: O::Builder,
    pos1: Position<K>,
    pos2: Position<K>,
}

trait HasTimeDiffCursor<K, T, R>: Cursor<K, DynUnit, T, R>
where
    K: ?Sized,
    R: ?Sized,
{
    type TimeDiffCursor<'a>: TimeDiffCursor<'a, T, R>
    where
        Self: 'a;

    fn time_diff_cursor(&self) -> Self::TimeDiffCursor<'_>;
}

trait TimeDiffCursor<'a, T, R>
where
    R: ?Sized,
{
    fn current<'b>(&'b mut self, tmp: &'b mut R) -> Option<(&T, &R)>;
    fn step(&mut self);
}

struct OrdKeyTimeDiffCursor<'a, T, R>(LeafCursor<'a, DynDataTyped<T>, R>)
where
    T: Timestamp,
    R: WeightTrait + ?Sized;

impl<'a, T, R> TimeDiffCursor<'a, T, R> for OrdKeyTimeDiffCursor<'a, T, R>
where
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn current(&mut self, _tmp: &mut R) -> Option<(&T, &R)> {
        if self.0.valid() {
            Some((self.0.current_key(), self.0.current_diff()))
        } else {
            None
        }
    }
    fn step(&mut self) {
        self.0.step()
    }
}

impl<'s, K, T, R, O> HasTimeDiffCursor<K, T, R> for OrdKeyCursor<'s, K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    type TimeDiffCursor<'a> = OrdKeyTimeDiffCursor<'a, T, R> where Self: 'a;

    fn time_diff_cursor(&self) -> Self::TimeDiffCursor<'_> {
        OrdKeyTimeDiffCursor(self.cursor.values())
    }
}

struct FileKeyTimeDiffCursor<'a, K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    cursor: FileCursor<
        'a,
        Box<dyn Storage>,
        DynDataTyped<T>,
        R,
        (),
        (
            &'static K,
            &'static DynUnit,
            (&'static DynDataTyped<T>, &'static R, ()),
        ),
    >,
    time: T,
}

impl<'a, K, T, R> TimeDiffCursor<'a, T, R> for FileKeyTimeDiffCursor<'a, K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn current<'b>(&'b mut self, tmp: &'b mut R) -> Option<(&T, &R)> {
        if unsafe { self.cursor.item((&mut self.time, tmp)) }.is_some() {
            Some((&self.time, tmp))
        } else {
            None
        }
    }

    fn step(&mut self) {
        self.cursor.move_next().unwrap();
    }
}

impl<'s, K, T, R> HasTimeDiffCursor<K, T, R> for FileKeyCursor<'s, K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type TimeDiffCursor<'a> = FileKeyTimeDiffCursor<'a, K, T, R>
    where
        Self: 'a;

    fn time_diff_cursor(&self) -> Self::TimeDiffCursor<'_> {
        FileKeyTimeDiffCursor {
            cursor: self.cursor.next_column().unwrap().first().unwrap(),
            time: T::default(),
        }
    }
}

impl<K, T, R, O> GenericMerger<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: Batch + BatchReader<Key = K, Val = DynUnit, R = R, Time = T>,
    O::Builder: TimedBuilder<O>,
{
    fn new(factories: &O::Factories) -> Self {
        Self {
            builder: O::Builder::new_builder(factories, T::default()),
            pos1: Position::Start,
            pos2: Position::Start,
        }
    }

    fn work<'s, A, B>(
        &mut self,
        source1: &'s A,
        source2: &'s B,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<DynUnit>>,
        fuel: &mut isize,
    ) where
        A: BatchReader<Key = K, Val = DynUnit, R = R, Time = T>,
        B: BatchReader<Key = K, Val = DynUnit, R = R, Time = T>,
        A::Cursor<'s>: HasTimeDiffCursor<K, T, R>,
        B::Cursor<'s>: HasTimeDiffCursor<K, T, R>,
    {
        if !filter(value_filter, &()) {
            return;
        }

        let mut cursor1 = self.pos1.cursor(source1);
        let mut cursor2 = self.pos2.cursor(source2);
        let mut diff1 = source1.factories().weight_factory().default_box();
        let mut diff2 = source1.factories().weight_factory().default_box();
        let mut sum = source1.factories().weight_factory().default_box();
        while cursor1.key_valid() && cursor2.key_valid() && *fuel > 0 {
            match cursor1.key().cmp(cursor2.key()) {
                Ordering::Less => {
                    self.copy_values_if(&mut diff1, &mut cursor1, key_filter, fuel);
                }
                Ordering::Equal => {
                    if filter(key_filter, cursor1.key()) {
                        self.merge_values(
                            &mut cursor1,
                            &mut cursor2,
                            &mut diff1,
                            &mut diff2,
                            &mut sum,
                            fuel,
                        );
                    } else {
                        *fuel -= 1;
                    }
                    cursor1.step_key();
                    cursor2.step_key();
                }

                Ordering::Greater => {
                    self.copy_values_if(&mut diff1, &mut cursor2, key_filter, fuel);
                }
            }
        }

        while cursor1.key_valid() && *fuel > 0 {
            self.copy_values_if(&mut diff1, &mut cursor1, key_filter, fuel);
        }
        while cursor2.key_valid() && *fuel > 0 {
            self.copy_values_if(&mut diff1, &mut cursor2, key_filter, fuel);
        }
        self.pos1 = Position::from_cursor(&cursor1);
        self.pos2 = Position::from_cursor(&cursor2);
    }

    fn done_with_bounds(self, lower: Antichain<T>, upper: Antichain<T>) -> O {
        self.builder.done_with_bounds(lower, upper)
    }

    fn copy_values_if<C>(
        &mut self,
        tmp: &mut R,
        cursor: &mut C,
        key_filter: &Option<Filter<K>>,
        fuel: &mut isize,
    ) where
        C: HasTimeDiffCursor<K, T, R>,
    {
        if filter(key_filter, cursor.key()) {
            let mut tdc = cursor.time_diff_cursor();
            while let Some((time, diff)) = tdc.current(tmp) {
                self.builder.push_time(cursor.key(), &(), time, diff);
                tdc.step();
                *fuel -= 1;
            }
        } else {
            *fuel -= 1;
        }
        cursor.step_key();
    }

    fn merge_values<C1, C2>(
        &mut self,
        cursor1: &mut C1,
        cursor2: &mut C2,
        tmp1: &mut R,
        tmp2: &mut R,
        sum: &mut R,
        fuel: &mut isize,
    ) where
        C1: HasTimeDiffCursor<K, T, R>,
        C2: HasTimeDiffCursor<K, T, R>,
    {
        let mut tdc1 = cursor1.time_diff_cursor();
        let mut tdc2 = cursor2.time_diff_cursor();

        loop {
            let Some((time1, diff1)) = tdc1.current(tmp1) else {
                break;
            };
            let Some((time2, diff2)) = tdc2.current(tmp2) else {
                break;
            };

            match time1.cmp(time2) {
                Ordering::Less => {
                    self.builder.push_time(cursor1.key(), &(), time1, diff1);
                    tdc1.step();
                }
                Ordering::Equal => {
                    diff1.add(diff2, sum);
                    if !sum.is_zero() {
                        self.builder.push_time(cursor1.key(), &(), time1, sum);
                    }
                }
                Ordering::Greater => {
                    self.builder.push_time(cursor2.key(), &(), time2, diff2);
                    tdc2.step();
                }
            }
            *fuel -= 1;
        }
        while let Some((time1, diff1)) = tdc1.current(tmp1) {
            self.builder.push_time(cursor1.key(), &(), time1, diff1);
            tdc1.step();
            *fuel -= 1;
        }
        while let Some((time2, diff2)) = tdc2.current(tmp2) {
            self.builder.push_time(cursor2.key(), &(), time2, diff2);
            tdc2.step();
            *fuel -= 1;
        }
    }
}

impl<K, T, R, O> SizeOf for GenericMerger<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: Batch + BatchReader<Key = K, Val = DynUnit, R = R, Time = T>,
    O::Builder: TimedBuilder<O>,
{
    fn size_of_children(&self, context: &mut size_of::Context) {
        self.builder.size_of_children(context)
    }
}
