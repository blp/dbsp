use dyn_clone::clone_box;
use std::path::PathBuf;
use std::{
    cmp::Ordering,
    fmt,
    fmt::{Debug, Display, Formatter},
    ops::DerefMut,
};

use crate::storage::backend::Backend;
use crate::trace::ord::file::val_batch::FileValBuilder;
use crate::trace::ord::vec::val_batch::OrdValBuilder;
use crate::trace::TimedBuilder;
use crate::{
    dynamic::{
        DataTrait, DynDataTyped, DynOpt, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase,
        Factory, LeanVec, WeightTrait, WithFactory,
    },
    storage::file::{
        reader::{ColumnSpec, Cursor as FileCursor, Reader},
        writer::{Parameters, Writer2},
        Factories as FileFactories,
    },
    time::{Antichain, AntichainRef},
    trace::{
        ord::{
            file::val_batch::FileValMerger, merge_batcher::MergeBatcher,
            vec::val_batch::OrdValMerger,
        },
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor, FileValBatch,
        FileValBatchFactories, Filter, Merger, OrdValBatch, OrdValBatchFactories, WeightedItem,
    },
    utils::Tup2,
    DBData, DBWeight, NumEntries, Runtime, Timestamp,
};
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;

use super::cursor::DynamicCursor;
use super::utils::Position;

pub struct FallbackValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    file: FileValBatchFactories<K, V, T, R>,
    vec: OrdValBatchFactories<K, V, T, R>,
}

impl<K, V, T, R> Clone for FallbackValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
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

impl<K, V, T, R> BatchReaderFactories<K, V, T, R> for FallbackValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<V>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            file: FileValBatchFactories::new::<KType, VType, RType>(),
            vec: OrdValBatchFactories::new::<KType, VType, RType>(),
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.file.key_factory()
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        self.file.keys_factory()
    }

    fn val_factory(&self) -> &'static dyn Factory<V> {
        self.file.val_factory()
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.file.weight_factory()
    }
}

impl<K, V, T, R> BatchFactories<K, V, T, R> for FallbackValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, V>> {
        self.file.item_factory()
    }

    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, V, R>> {
        self.file.weighted_item_factory()
    }

    fn weighted_items_factory(&self) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, V>, R>> {
        self.file.weighted_items_factory()
    }
}

/// An immutable collection of update tuples, from a contiguous interval of
/// logical times.
pub struct FallbackValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories: FallbackValBatchFactories<K, V, T, R>,
    inner: Inner<K, V, T, R>,
}

enum Inner<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    Vec(OrdValBatch<K, V, T, R>),
    File(FileValBatch<K, V, T, R>),
}

impl<K, V, T, R> Inner<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn as_file(&self) -> Option<&FileValBatch<K, V, T, R>> {
        match self {
            Inner::Vec(_vec) => None,
            Inner::File(file) => Some(file),
        }
    }

    fn as_vec(&self) -> Option<&OrdValBatch<K, V, T, R>> {
        match self {
            Inner::Vec(vec) => Some(vec),
            Inner::File(_file) => None,
        }
    }
}

impl<K, V, T, R> Clone for FallbackValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
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

impl<K, V, T, R> NumEntries for FallbackValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        match &self.inner {
            Inner::Vec(vec) => vec.num_entries_shallow(),
            Inner::File(file) => file.num_entries_shallow(),
        }
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        match &self.inner {
            Inner::Vec(vec) => vec.num_entries_deep(),
            Inner::File(file) => file.num_entries_deep(),
        }
    }
}

impl<K, V, T, R> Display for FallbackValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match &self.inner {
            Inner::Vec(vec) => Display::fmt(vec, f),
            Inner::File(file) => Display::fmt(file, f),
        }
    }
}

impl<K, V, T, R> BatchReader for FallbackValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Factories = FallbackValBatchFactories<K, V, T, R>;
    type Key = K;
    type Val = V;
    type Time = T;
    type R = R;

    type Cursor<'s> = DynamicCursor<'s, K, V, T, R>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        DynamicCursor(match &self.inner {
            Inner::Vec(vec) => Box::new(vec.cursor()),
            Inner::File(file) => Box::new(file.cursor()),
        })
    }

    fn key_count(&self) -> usize {
        match &self.inner {
            Inner::Vec(vec) => vec.key_count(),
            Inner::File(file) => file.key_count(),
        }
    }

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
        RG: Rng,
        T: PartialEq<()>,
    {
        match &self.inner {
            Inner::Vec(vec) => vec.sample_keys(rng, sample_size, output),
            Inner::File(file) => file.sample_keys(rng, sample_size, output),
        }
    }
}

impl<K, V, T, R> Batch for FallbackValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = FallbackValBuilder<K, V, T, R>;
    type Merger = FallbackValMerger<K, V, T, R>;

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        FallbackValMerger::new_merger(self, other)
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
pub struct FallbackValMerger<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories: FallbackValBatchFactories<K, V, T, R>,
    lower: Antichain<T>,
    upper: Antichain<T>,
    inner: MergerInner<K, V, T, R>,
}

enum MergerInner<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    AllFile(FileValMerger<K, V, T, R>),
    AllVec(OrdValMerger<K, V, T, R>),
    ToVec(GenericMerger<K, V, T, R, OrdValBatch<K, V, T, R>>),
    ToFile(GenericMerger<K, V, T, R, FileValBatch<K, V, T, R>>),
}

fn include<K: ?Sized>(x: &K, filter: &Option<Filter<K>>) -> bool {
    match filter {
        Some(filter) => filter(x),
        None => true,
    }
}

/*
impl<K, V, T, R> FallbackValMerger<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn copy_values_if(
        &self,
        output: &mut Writer2<Backend, K, DynUnit, V, DynWeightedPairs<DynDataTyped<T>, R>>,
        key: &K,
        key_cursor: &mut RawKeyCursor<'_, K, V, T, R>,
        value_filter: &Option<Filter<V>>,
    ) {
        self.factories.factories1.key_factory.with(&mut |value| {
            self.factories.timediff_factory.with(&mut |aux| {
                let mut value_cursor = key_cursor.next_column().unwrap().first().unwrap();
                let mut n = 0;
                while value_cursor.has_value() {
                    let value = unsafe { value_cursor.key(value) }.unwrap();
                    if include(value, value_filter) {
                        let aux = unsafe { value_cursor.aux(aux) }.unwrap();
                        output.write1((value, aux)).unwrap();
                        n += 1;
                    }
                    value_cursor.move_next().unwrap();
                }
                if n > 0 {
                    output.write0((key, ().erase())).unwrap();
                }
                key_cursor.move_next().unwrap();
            })
        })
    }

    fn copy_value(
        &self,
        output: &mut Writer2<Backend, K, DynUnit, V, DynWeightedPairs<DynDataTyped<T>, R>>,
        cursor: &mut RawValCursor<'_, K, V, T, R>,
        value: &V,
    ) {
        self.factories.timediff_factory.with(&mut |td| {
            let td = unsafe { cursor.aux(td) }.unwrap();
            output.write1((value, td)).unwrap();
            cursor.move_next().unwrap();
        })
    }

    fn merge_values(
        &self,
        output: &mut Writer2<Backend, K, DynUnit, V, DynWeightedPairs<DynDataTyped<T>, R>>,
        cursor1: &mut RawValCursor<'_, K, V, T, R>,
        cursor2: &mut RawValCursor<'_, K, V, T, R>,
        value_filter: &Option<Filter<V>>,
    ) -> bool {
        let mut n = 0;

        self.factories.weight_factory.with(&mut |tmp_w| {
            self.factories.factories1.key_factory.with(&mut |tmp_v1| {
                self.factories.factories1.key_factory.with(&mut |tmp_v2| {
                    self.factories.timediff_factory.with(&mut |td| {
                        self.factories.timediff_factory.with(&mut |td1| {
                            self.factories.timediff_factory.with(&mut |td2| loop {
                                let Some(value1) = read_filtered(cursor1, value_filter, tmp_v1)
                                else {
                                    while let Some(value2) =
                                        read_filtered(cursor2, value_filter, tmp_v2)
                                    {
                                        self.copy_value(output, cursor2, value2);
                                        n += 1;
                                    }
                                    return;
                                };
                                let Some(value2) = read_filtered(cursor2, value_filter, tmp_v2)
                                else {
                                    while let Some(value1) =
                                        read_filtered(cursor1, value_filter, tmp_v1)
                                    {
                                        self.copy_value(output, cursor1, value1);
                                        n += 1;
                                    }
                                    return;
                                };
                                match value1.cmp(value2) {
                                    Ordering::Less => self.copy_value(output, cursor1, value1),
                                    Ordering::Equal => {
                                        let td1 = unsafe { cursor1.aux(td1) }.unwrap();
                                        td1.sort_unstable();
                                        let td2 = unsafe { cursor2.aux(td2) }.unwrap();
                                        td2.sort_unstable();
                                        merge_times(td1, td2, td, tmp_w);
                                        cursor1.move_next().unwrap();
                                        cursor2.move_next().unwrap();
                                        if td.is_empty() {
                                            continue;
                                        }
                                        output.write1((value1, td)).unwrap();
                                    }
                                    Ordering::Greater => self.copy_value(output, cursor2, value2),
                                }
                                n += 1;
                            })
                        })
                    })
                })
            })
        });

        n > 0
    }

    fn merge(
        &self,
        source1: &FallbackValBatch<K, V, T, R>,
        source2: &FallbackValBatch<K, V, T, R>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
    ) -> RawValBatch<K, V, T, R> {
        let mut output = Writer2::new(
            &source1.factories.factories0,
            &source1.factories().factories1,
            &Runtime::storage(),
            Parameters::default(),
        )
        .unwrap();
        let mut cursor1 = source1.file.rows().nth(source1.lower_bound as u64).unwrap();
        let mut cursor2 = source2.file.rows().nth(source2.lower_bound as u64).unwrap();
        self.factories.factories0.key_factory.with(&mut |tmp_key1| {
            self.factories
                .factories0
                .key_factory
                .with(&mut |tmp_key2| loop {
                    let Some(key1) = read_filtered(&mut cursor1, key_filter, tmp_key1) else {
                        while let Some(key2) = read_filtered(&mut cursor2, key_filter, tmp_key2) {
                            self.copy_values_if(&mut output, key2, &mut cursor2, value_filter);
                        }
                        break;
                    };
                    let Some(key2) = read_filtered(&mut cursor2, key_filter, tmp_key2) else {
                        while let Some(key1) = read_filtered(&mut cursor1, key_filter, tmp_key1) {
                            self.copy_values_if(&mut output, key1, &mut cursor1, value_filter);
                        }
                        break;
                    };
                    match key1.cmp(key2) {
                        Ordering::Less => {
                            self.copy_values_if(&mut output, key1, &mut cursor1, value_filter);
                        }
                        Ordering::Equal => {
                            if self.merge_values(
                                &mut output,
                                &mut cursor1.next_column().unwrap().first().unwrap(),
                                &mut cursor2.next_column().unwrap().first().unwrap(),
                                value_filter,
                            ) {
                                output.write0((&key1, &())).unwrap();
                            }
                            cursor1.move_next().unwrap();
                            cursor2.move_next().unwrap();
                        }

                        Ordering::Greater => {
                            self.copy_values_if(&mut output, key2, &mut cursor2, value_filter);
                        }
                    }
                })
        });
        output.into_reader().unwrap()
    }
}*/

impl<K, V, T, R> Merger<K, V, T, R, FallbackValBatch<K, V, T, R>> for FallbackValMerger<K, V, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new_merger(
        batch1: &FallbackValBatch<K, V, T, R>,
        batch2: &FallbackValBatch<K, V, T, R>,
    ) -> Self {
        FallbackValMerger {
            factories: batch1.factories.clone(),
            lower: batch1.lower().meet(batch2.lower()),
            upper: batch1.upper().join(batch2.upper()),
            inner: if batch1.len() + batch2.len() < Runtime::min_storage_rows() {
                match (&batch1.inner, &batch2.inner) {
                    (Inner::Vec(vec1), Inner::Vec(vec2)) => {
                        MergerInner::AllVec(OrdValMerger::new_merger(vec1, vec2))
                    }
                    _ => todo!()
                    //_ => MergerInner::ToVec(GenericMerger::new(&batch1.factories.vec)),
                }
            } else {
                match (&batch1.inner, &batch2.inner) {
                    (Inner::File(file1), Inner::File(file2)) => {
                        MergerInner::AllFile(FileValMerger::new_merger(file1, file2))
                    }
                    _ => todo!()
                    //_ => MergerInner::ToFile(GenericMerger::new(&batch1.factories.file)),
                }
            },
        }
    }

    fn done(mut self) -> FallbackValBatch<K, V, T, R> {
        FallbackValBatch {
            factories: self.factories.clone(),
            inner: match self.inner {
                MergerInner::AllFile(merger) => Inner::File(merger.done()),
                MergerInner::AllVec(merger) => Inner::Vec(merger.done()),
                _ => todo!()
                //MergerInner::ToVec(merger) => Inner::Vec(merger.done()),
                //MergerInner::ToFile(merger) => Inner::File(merger.done()),
            },
        }
    }

    fn work(
        &mut self,
        source1: &FallbackValBatch<K, V, T, R>,
        source2: &FallbackValBatch<K, V, T, R>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
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
            _ => todo!()
                /*
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
            },*/
        }
    }
}

impl<K, V, T, R> SizeOf for FallbackValMerger<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

/// A builder for creating layers from unsorted update tuples.
pub struct FallbackValBuilder<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories: FallbackValBatchFactories<K, V, T, R>,
    inner: BuilderInner<K, V, T, R>,
}

enum BuilderInner<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    File(FileValBuilder<K, V, T, R>),
    Vec(OrdValBuilder<K, V, T, R>),
}

impl<K, V, T, R> Builder<FallbackValBatch<K, V, T, R>> for FallbackValBuilder<K, V, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new_builder(factories: &FallbackValBatchFactories<K, V, T, R>, time: T) -> Self {
        Self::with_capacity(factories, time, 0)
    }

    fn with_capacity(
        factories: &FallbackValBatchFactories<K, V, T, R>,
        time: T,
        capacity: usize,
    ) -> Self {
        Self {
            factories: factories.clone(),
            inner: if capacity < Runtime::min_storage_rows() {
                BuilderInner::Vec(OrdValBuilder::with_capacity(&factories.vec, time, capacity))
            } else {
                BuilderInner::File(FileValBuilder::with_capacity(
                    &factories.file,
                    time,
                    capacity,
                ))
            },
        }
    }

    fn reserve(&mut self, _additional: usize) {}

    fn push(&mut self, item: &mut DynPair<DynPair<K, V>, R>) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push(item),
            BuilderInner::Vec(vec) => vec.push(item),
        }
    }

    fn push_refs(&mut self, key: &K, val: &V, weight: &R) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push_refs(key, val, weight),
            BuilderInner::Vec(vec) => vec.push_refs(key, val, weight),
        }
    }

    fn push_vals(&mut self, key: &mut K, val: &mut V, weight: &mut R) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push_vals(key, val, weight),
            BuilderInner::Vec(vec) => vec.push_vals(key, val, weight),
        }
    }

    fn done(mut self) -> FallbackValBatch<K, V, T, R> {
        FallbackValBatch {
            factories: self.factories,
            inner: match self.inner {
                BuilderInner::File(file) => Inner::File(file.done()),
                BuilderInner::Vec(vec) => Inner::Vec(vec.done()),
            },
        }
    }
}

impl<K, V, T, R> SizeOf for FallbackValBuilder<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

impl<K, V, T, R> SizeOf for FallbackValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

impl<K, V, T, R> Archive for FallbackValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, V, T, R, S> Serialize<S> for FallbackValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    S: Serializer + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, V, T, R, D> Deserialize<FallbackValBatch<K, V, T, R>, D>
    for Archived<FallbackValBatch<K, V, T, R>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FallbackValBatch<K, V, T, R>, D::Error> {
        unimplemented!();
    }
}
struct GenericMerger<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: Batch + BatchReader<Key = K, Val = V, R = R, Time = T>,
    O::Builder: TimedBuilder<O>,
{
    builder: O::Builder,
    pos1: Position<K>,
    pos2: Position<K>,
}
