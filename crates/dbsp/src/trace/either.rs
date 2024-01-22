use std::marker::PhantomData;

use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;

use crate::NumEntries;

use super::{Batch, BatchReader, Consumer, Cursor, ValueConsumer};

/// One of two batch types.
#[derive(Archive, Serialize, Deserialize, SizeOf)]
pub enum Either<A, B>
where
    A: Batch,
    B: Batch<Key = A::Key, Val = A::Val, R = A::R, Time = A::Time>,
{
    A(A),
    B(B),
}

impl<A, B> NumEntries for Either<A, B>
where
    A: Batch,
    B: Batch<Key = A::Key, Val = A::Val, R = A::R, Time = A::Time>,
{
    const CONST_NUM_ENTRIES: Option<usize> = B::CONST_NUM_ENTRIES;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        match self {
            Self::A(batch) => batch.num_entries_shallow(),
            Self::B(batch) => batch.num_entries_shallow(),
        }
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        match self {
            Self::A(batch) => batch.num_entries_deep(),
            Self::B(batch) => batch.num_entries_deep(),
        }
    }
}

/*
impl<A, B> Batch for Either<A, B>
where
    A: Batch,
    B: Batch<Key = A::Key, Val = A::Val, R = A::R, Time = A::Time>,
{
    type Item = A::Item;

    type Batcher;

    type Builder;

    type Merger;

    fn item_from(key: Self::Key, val: Self::Val) -> Self::Item {
        Self::A::item_from(key, val)
    }

    fn from_keys(time: Self::Time, keys: Vec<(Self::Key, Self::R)>) -> Self
    where
        Self::Val: From<()>,
    {
        Self::A(Self::A::from_keys(time, keys))
    }

    fn recede_to(&mut self, frontier: &Self::Time) {
        match self {
            Self::A(batch) => batch.recede_to(frontier),
            Self::B(batch) => batch.recede_to(frontier),
        }
    }
}
*/
impl<A, B> BatchReader for Either<A, B>
where
    A: Batch,
    B: Batch<Key = A::Key, Val = A::Val, R = A::R, Time = A::Time>,
{
    type Key = A::Key;
    type Val = A::Val;
    type Time = A::Time;
    type R = A::R;

    type Cursor<'s> = EitherCursor<'s, A, B>;

    type Consumer = EitherConsumer<A, B>;

    fn cursor(&self) -> Self::Cursor<'_> {
        EitherCursor::new(self)
    }

    fn consumer(self) -> Self::Consumer {
        todo!()
    }

    fn key_count(&self) -> usize {
        match self {
            Self::A(batch) => batch.key_count(),
            Self::B(batch) => batch.key_count(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::A(batch) => batch.len(),
            Self::B(batch) => batch.len(),
        }
    }

    fn lower(&self) -> crate::time::AntichainRef<'_, Self::Time> {
        match self {
            Self::A(batch) => batch.lower(),
            Self::B(batch) => batch.lower(),
        }
    }

    fn upper(&self) -> crate::time::AntichainRef<'_, Self::Time> {
        match self {
            Self::A(batch) => batch.upper(),
            Self::B(batch) => batch.upper(),
        }
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        match self {
            Self::A(batch) => batch.truncate_keys_below(lower_bound),
            Self::B(batch) => batch.truncate_keys_below(lower_bound),
        }
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut Vec<Self::Key>)
    where
        Self::Time: PartialEq<()>,
        RG: rand::Rng,
    {
        match self {
            Self::A(batch) => batch.sample_keys(rng, sample_size, sample),
            Self::B(batch) => batch.sample_keys(rng, sample_size, sample),
        }
    }
}

#[derive(Debug, SizeOf, Clone)]
pub enum EitherCursor<'s, A, B>
where
    A: Batch,
    B: Batch<Key = A::Key, Val = A::Val, R = A::R, Time = A::Time>,
{
    A(A::Cursor<'s>),
    B(B::Cursor<'s>),
}

impl<'s, A, B> EitherCursor<'s, A, B>
where
    A: Batch,
    B: Batch<Key = A::Key, Val = A::Val, R = A::R, Time = A::Time>,
{
    fn new(either: &'s Either<A, B>) -> Self {
        match either {
            Either::A(batch) => Self::A(batch.cursor()),
            Either::B(batch) => Self::B(batch.cursor()),
        }
    }
}

impl<'s, A, B> Cursor<A::Key, A::Val, A::Time, A::R> for EitherCursor<'s, A, B>
where
    A: Batch,
    B: Batch<Key = A::Key, Val = A::Val, R = A::R, Time = A::Time>,
{
    fn key_valid(&self) -> bool {
        match self {
            Self::A(cursor) => cursor.key_valid(),
            Self::B(cursor) => cursor.key_valid(),
        }
    }

    fn val_valid(&self) -> bool {
        match self {
            Self::A(cursor) => cursor.val_valid(),
            Self::B(cursor) => cursor.val_valid(),
        }
    }

    fn key(&self) -> &A::Key {
        match self {
            Self::A(cursor) => cursor.key(),
            Self::B(cursor) => cursor.key(),
        }
    }

    fn val(&self) -> &A::Val {
        match self {
            Self::A(cursor) => cursor.val(),
            Self::B(cursor) => cursor.val(),
        }
    }

    fn fold_times<F, U>(&mut self, init: U, fold: F) -> U
    where
        F: FnMut(U, &A::Time, &A::R) -> U,
    {
        match self {
            Self::A(cursor) => cursor.fold_times(init, fold),
            Self::B(cursor) => cursor.fold_times(init, fold),
        }
    }

    fn fold_times_through<F, U>(&mut self, upper: &A::Time, init: U, fold: F) -> U
    where
        F: FnMut(U, &A::Time, &A::R) -> U,
    {
        match self {
            Self::A(cursor) => cursor.fold_times_through(upper, init, fold),
            Self::B(cursor) => cursor.fold_times_through(upper, init, fold),
        }
    }

    fn weight(&mut self) -> A::R
    where
        A::Time: PartialEq<()>,
    {
        match self {
            Self::A(cursor) => cursor.weight(),
            Self::B(cursor) => cursor.weight(),
        }
    }

    fn step_key(&mut self) {
        match self {
            Self::A(cursor) => cursor.step_key(),
            Self::B(cursor) => cursor.step_key(),
        }
    }

    fn step_key_reverse(&mut self) {
        match self {
            Self::A(cursor) => cursor.step_key_reverse(),
            Self::B(cursor) => cursor.step_key_reverse(),
        }
    }

    fn seek_key_with<P>(&mut self, predicate: P)
    where
        P: Fn(&A::Key) -> bool + Clone,
    {
        match self {
            Self::A(cursor) => cursor.seek_key_with(predicate),
            Self::B(cursor) => cursor.seek_key_with(predicate),
        }
    }

    fn seek_key_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&A::Key) -> bool + Clone,
    {
        match self {
            Self::A(cursor) => cursor.seek_key_with_reverse(predicate),
            Self::B(cursor) => cursor.seek_key_with_reverse(predicate),
        }
    }

    fn step_val(&mut self) {
        match self {
            Self::A(cursor) => cursor.step_val(),
            Self::B(cursor) => cursor.step_val(),
        }
    }

    fn step_val_reverse(&mut self) {
        match self {
            Self::A(cursor) => cursor.step_val_reverse(),
            Self::B(cursor) => cursor.step_val_reverse(),
        }
    }

    fn seek_val(&mut self, val: &A::Val) {
        match self {
            Self::A(cursor) => cursor.seek_val(val),
            Self::B(cursor) => cursor.seek_val(val),
        }
    }

    fn seek_val_reverse(&mut self, val: &A::Val) {
        match self {
            Self::A(cursor) => cursor.seek_val_reverse(val),
            Self::B(cursor) => cursor.seek_val_reverse(val),
        }
    }

    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&A::Val) -> bool + Clone,
    {
        match self {
            Self::A(cursor) => cursor.seek_val_with(predicate),
            Self::B(cursor) => cursor.seek_val_with(predicate),
        }
    }

    fn seek_val_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&A::Val) -> bool + Clone,
    {
        match self {
            Self::A(cursor) => cursor.seek_val_with_reverse(predicate),
            Self::B(cursor) => cursor.seek_val_with_reverse(predicate),
        }
    }

    fn rewind_keys(&mut self) {
        match self {
            Self::A(cursor) => cursor.rewind_keys(),
            Self::B(cursor) => cursor.rewind_keys(),
        }
    }

    fn fast_forward_keys(&mut self) {
        match self {
            Self::A(cursor) => cursor.fast_forward_keys(),
            Self::B(cursor) => cursor.fast_forward_keys(),
        }
    }

    fn rewind_vals(&mut self) {
        match self {
            Self::A(cursor) => cursor.rewind_vals(),
            Self::B(cursor) => cursor.rewind_vals(),
        }
    }

    fn fast_forward_vals(&mut self) {
        match self {
            Self::A(cursor) => cursor.fast_forward_vals(),
            Self::B(cursor) => cursor.fast_forward_vals(),
        }
    }
}

pub struct EitherConsumer<A, B>
where
    A: Batch,
    B: Batch<Key = A::Key, Val = A::Val, R = A::R, Time = A::Time>,
{
    phantom: PhantomData<(A, B)>,
}

impl<A, B>
    Consumer<
        <A as BatchReader>::Key,
        <A as BatchReader>::Val,
        <A as BatchReader>::R,
        <A as BatchReader>::Time,
    > for EitherConsumer<A, B>
where
    A: Batch,
    B: Batch<Key = A::Key, Val = A::Val, R = A::R, Time = A::Time>,
{
    type ValueConsumer<'a> = EitherValueConsumer<'a, A, B>;

    fn key_valid(&self) -> bool {
        todo!()
    }

    fn peek_key(&self) -> &<A as BatchReader>::Key {
        todo!()
    }

    fn next_key(&mut self) -> (<A as BatchReader>::Key, Self::ValueConsumer<'_>) {
        todo!()
    }

    fn seek_key(&mut self, _key: &<A as BatchReader>::Key)
    where
        <A as BatchReader>::Key: Ord,
    {
        todo!()
    }
}

pub struct EitherValueConsumer<'a, A, B> {
    phantom: PhantomData<&'a (A, B)>,
}

impl<'a, A, B>
    ValueConsumer<'a, <A as BatchReader>::Val, <A as BatchReader>::R, <A as BatchReader>::Time>
    for EitherValueConsumer<'a, A, B>
where
    A: Batch,
    B: Batch<Key = A::Key, Val = A::Val, R = A::R, Time = A::Time>,
{
    fn value_valid(&self) -> bool {
        todo!()
    }

    fn next_value(
        &mut self,
    ) -> (
        <A as BatchReader>::Val,
        <A as BatchReader>::R,
        <A as BatchReader>::Time,
    ) {
        todo!()
    }

    fn remaining_values(&self) -> usize {
        todo!()
    }
}
