use std::fmt::Debug;

use size_of::SizeOf;

use crate::{
    dynamic::{DataTrait, Factory, WeightTrait},
    trace::Cursor,
};

/// A cursor that can be cloned as a `dyn Cursor` when it is inside a [`Box`].
///
/// Rust doesn't have a built-in way to clone boxed trait objects.  This
/// provides such a way for boxed [`Cursor`]s.
pub trait ClonableCursor<'s, K, V, T, R>: Cursor<K, V, T, R> + Debug
where
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
{
    fn clone_boxed(&self) -> Box<dyn ClonableCursor<'s, K, V, T, R> + 's>;
}

impl<'s, K, V, T, R, C> ClonableCursor<'s, K, V, T, R> for C
where
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
    C: Cursor<K, V, T, R> + Debug + Clone + 's,
{
    fn clone_boxed(&self) -> Box<dyn ClonableCursor<'s, K, V, T, R> + 's> {
        Box::new(self.clone())
    }
}

/// A wrapper around a `dyn Cursor` to allow choice of implementations at runtime.
#[derive(Debug, SizeOf)]
pub struct DynamicCursor<'s, K, V, T, R>(pub Box<dyn ClonableCursor<'s, K, V, T, R> + 's>)
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized;

impl<'s, K, V, T, R> Clone for DynamicCursor<'s, K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self(self.0.clone_boxed())
    }
}

impl<'s, K, V, T, R> Cursor<K, V, T, R> for DynamicCursor<'s, K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.0.weight_factory()
    }

    fn key(&self) -> &K {
        self.0.key()
    }

    fn val(&self) -> &V {
        self.0.val()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &R)) {
        self.0.map_times(logic)
    }

    fn map_times_through(&mut self, upper: &T, logic: &mut dyn FnMut(&T, &R)) {
        self.0.map_times_through(upper, logic)
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R))
    where
        T: PartialEq<()>,
    {
        self.0.map_values(logic)
    }

    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>,
    {
        self.0.weight()
    }

    fn key_valid(&self) -> bool {
        self.0.key_valid()
    }

    fn val_valid(&self) -> bool {
        self.0.val_valid()
    }

    fn step_key(&mut self) {
        self.0.step_key()
    }

    fn step_key_reverse(&mut self) {
        self.0.step_key_reverse()
    }

    fn seek_key(&mut self, key: &K) {
        self.0.seek_key(key)
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.0.seek_key_with(predicate)
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.0.seek_key_with_reverse(predicate)
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.0.seek_key_reverse(key)
    }

    fn step_val(&mut self) {
        self.0.step_val()
    }

    fn seek_val(&mut self, val: &V) {
        self.0.seek_val(val)
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.0.seek_val_with(predicate)
    }

    fn rewind_keys(&mut self) {
        self.0.rewind_keys()
    }

    fn fast_forward_keys(&mut self) {
        self.0.fast_forward_keys()
    }

    fn rewind_vals(&mut self) {
        self.0.rewind_vals()
    }

    fn step_val_reverse(&mut self) {
        self.0.step_val_reverse()
    }

    fn seek_val_reverse(&mut self, val: &V) {
        self.0.seek_val_reverse(val)
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.0.seek_val_with_reverse(predicate)
    }

    fn fast_forward_vals(&mut self) {
        self.0.fast_forward_vals()
    }
}
