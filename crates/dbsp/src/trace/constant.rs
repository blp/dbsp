use std::marker::PhantomData;

use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;

use crate::{time::Antichain, DBTimestamp, NumEntries};

use super::{Batch, BatchReader, Consumer, Cursor, ValueConsumer};

/// Attaches a constant time to a batch with unit time `()`.
#[derive(Archive, Serialize, Deserialize, SizeOf)]
pub struct ConstantTimeBatch<B, T>
where
    B: Batch<Time = ()>,
    T: DBTimestamp,
{
    batch: B,
    time: T,
    lower: Antichain<T>,
    upper: Antichain<T>,
}

impl<B, T> ConstantTimeBatch<B, T>
where
    B: Batch<Time = ()>,
    T: DBTimestamp,
{
    pub fn new(batch: B, time: T) -> Self {
        let time_next = time.advance(0);
        let upper = if time_next <= time {
            Antichain::new()
        } else {
            Antichain::from_elem(time_next)
        };
        Self {
            batch,
            time: time.clone(),
            lower: Antichain::from_elem(time),
            upper,
        }
    }
}

impl<B, T> NumEntries for ConstantTimeBatch<B, T>
where
    B: Batch<Time = ()>,
    T: DBTimestamp,
{
    const CONST_NUM_ENTRIES: Option<usize> = B::CONST_NUM_ENTRIES;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.batch.num_entries_shallow()
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        self.batch.num_entries_deep()
    }
}

impl<B, T> BatchReader for ConstantTimeBatch<B, T>
where
    B: Batch<Time = ()>,
    T: DBTimestamp,
{
    type Key = B::Key;
    type Val = B::Val;
    type Time = T;
    type R = B::R;

    type Cursor<'s> = ConstantTimeBatchCursor<'s, B, T>;

    type Consumer = ConstantTimeBatchConsumer<B, T>;

    fn cursor(&self) -> Self::Cursor<'_> {
        ConstantTimeBatchCursor::new(self)
    }

    fn consumer(self) -> Self::Consumer {
        todo!()
    }

    fn key_count(&self) -> usize {
        self.batch.key_count()
    }

    fn len(&self) -> usize {
        self.batch.len()
    }

    fn lower(&self) -> crate::time::AntichainRef<'_, Self::Time> {
        self.lower.as_ref()
    }

    fn upper(&self) -> crate::time::AntichainRef<'_, Self::Time> {
        self.upper.as_ref()
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        self.batch.truncate_keys_below(lower_bound);
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut Vec<Self::Key>)
    where
        Self::Time: PartialEq<()>,
        RG: rand::Rng,
    {
        self.batch.sample_keys(rng, sample_size, sample);
    }
}

#[derive(Debug, SizeOf, Clone)]
pub struct ConstantTimeBatchCursor<'s, B, T>
where
    B: Batch<Time = ()>,
    T: DBTimestamp,
{
    time: &'s T,
    cursor: B::Cursor<'s>,
    _phantom: PhantomData<T>,
}

impl<'s, B, T> ConstantTimeBatchCursor<'s, B, T>
where
    B: Batch<Time = ()>,
    T: DBTimestamp,
{
    fn new(reader: &'s ConstantTimeBatch<B, T>) -> Self {
        Self {
            time: &reader.time,
            cursor: reader.batch.cursor(),
            _phantom: PhantomData,
        }
    }
}

impl<'s, B, T> Cursor<B::Key, B::Val, T, B::R> for ConstantTimeBatchCursor<'s, B, T>
where
    B: Batch<Time = ()>,
    T: DBTimestamp,
{
    fn key_valid(&self) -> bool {
        self.cursor.key_valid()
    }

    fn val_valid(&self) -> bool {
        self.cursor.val_valid()
    }

    fn key(&self) -> &B::Key {
        self.cursor.key()
    }

    fn val(&self) -> &B::Val {
        self.cursor.val()
    }

    fn fold_times<F, U>(&mut self, init: U, mut fold: F) -> U
    where
        F: FnMut(U, &T, &B::R) -> U,
    {
        self.cursor
            .fold_times(init, |value, &(), diff| fold(value, self.time, diff))
    }

    fn fold_times_through<F, U>(&mut self, upper: &T, init: U, fold: F) -> U
    where
        F: FnMut(U, &T, &B::R) -> U,
    {
        if self.time <= upper {
            self.fold_times(init, fold)
        } else {
            init
        }
    }

    fn weight(&mut self) -> B::R
    where
        T: PartialEq<()>,
    {
        self.cursor.weight()
    }

    fn step_key(&mut self) {
        self.cursor.step_key()
    }

    fn step_key_reverse(&mut self) {
        self.cursor.step_key_reverse()
    }

    fn seek_key_with<P>(&mut self, predicate: P)
    where
        P: Fn(&B::Key) -> bool + Clone,
    {
        self.cursor.seek_key_with(predicate)
    }

    fn seek_key_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&B::Key) -> bool + Clone,
    {
        self.cursor.seek_key_with_reverse(predicate)
    }

    fn step_val(&mut self) {
        self.cursor.step_val()
    }

    fn step_val_reverse(&mut self) {
        self.cursor.step_val_reverse()
    }

    fn seek_val(&mut self, val: &B::Val) {
        self.cursor.seek_val(val)
    }

    fn seek_val_reverse(&mut self, val: &B::Val) {
        self.cursor.seek_val_reverse(val)
    }

    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&B::Val) -> bool + Clone,
    {
        self.cursor.seek_val_with(predicate)
    }

    fn seek_val_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&B::Val) -> bool + Clone,
    {
        self.cursor.seek_val_with_reverse(predicate)
    }

    fn rewind_keys(&mut self) {
        self.cursor.rewind_keys()
    }

    fn fast_forward_keys(&mut self) {
        self.cursor.fast_forward_keys()
    }

    fn rewind_vals(&mut self) {
        self.cursor.rewind_vals()
    }

    fn fast_forward_vals(&mut self) {
        self.cursor.fast_forward_vals()
    }
}

pub struct ConstantTimeBatchConsumer<B, T>
where
    B: Batch<Time = ()>,
    T: DBTimestamp,
{
    phantom: PhantomData<(B, T)>,
}

impl<B, T> Consumer<<B as BatchReader>::Key, <B as BatchReader>::Val, <B as BatchReader>::R, T>
    for ConstantTimeBatchConsumer<B, T>
where
    B: Batch<Time = ()>,
    T: DBTimestamp,
{
    type ValueConsumer<'a> = ConstantTimeBatchValueConsumer<'a, B, T>;

    fn key_valid(&self) -> bool {
        todo!()
    }

    fn peek_key(&self) -> &<B as BatchReader>::Key {
        todo!()
    }

    fn next_key(&mut self) -> (<B as BatchReader>::Key, Self::ValueConsumer<'_>) {
        todo!()
    }

    fn seek_key(&mut self, _key: &<B as BatchReader>::Key)
    where
        <B as BatchReader>::Key: Ord,
    {
        todo!()
    }
}

pub struct ConstantTimeBatchValueConsumer<'a, B, T> {
    phantom: PhantomData<&'a (B, T)>,
}

impl<'a, B, T> ValueConsumer<'a, <B as BatchReader>::Val, <B as BatchReader>::R, T>
    for ConstantTimeBatchValueConsumer<'a, B, T>
where
    B: Batch<Time = ()>,
    T: DBTimestamp,
{
    fn value_valid(&self) -> bool {
        todo!()
    }

    fn next_value(&mut self) -> (<B as BatchReader>::Val, <B as BatchReader>::R, T) {
        todo!()
    }

    fn remaining_values(&self) -> usize {
        todo!()
    }
}
