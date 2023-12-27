use std::cell::RefCell;
use std::fmt::{self, Debug, Display};

use crate::trace::Deserializable;
use crate::{
    trace::layers::persistent::Persistence,
    trace::layers::{advance, retreat, Cursor},
    DBData, DBWeight,
};

use super::PersistedColumnLayer;

#[derive(Debug, Clone)]
pub struct ScrapSpace<K, R>
where
    K: Clone + Debug,
    R: Clone + Debug,
{
    pub(crate) cur_key: RefCell<Option<K>>,
    pub(crate) cur_r: RefCell<Option<R>>,
}

impl<K, R> ScrapSpace<K, R>
where
    K: Clone + Debug,
    R: Clone + Debug,
{
    pub(crate) fn new() -> Self {
        Self {
            cur_key: RefCell::new(None),
            cur_r: RefCell::new(None),
        }
    }
}

/// A cursor for walking through a [`PersistedColumnLayer`].
#[derive(Debug, Clone)]
pub struct PersistedColumnLayerCursor<'s, K, R>
where
    K: DBData,
    R: DBWeight,
{
    // We represent current position of the cursor as isize, so we can use `-1`
    // to represent invalid cursor that rolled over the left bound of the range.
    pos: isize,
    storage: &'s PersistedColumnLayer<K, R>,
    kv_space: ScrapSpace<K, R>,
    bounds: (usize, usize),
}

impl<'s, K, R> PersistedColumnLayerCursor<'s, K, R>
where
    K: DBData,
    R: DBWeight,
    <K as Deserializable>::ArchivedDeser: PartialOrd<K>,
{
    pub fn new(
        pos: usize,
        storage: &'s PersistedColumnLayer<K, R>,
        bounds: (usize, usize),
    ) -> Self {
        Self {
            pos: pos as isize,
            storage,
            bounds,
            kv_space: ScrapSpace::new(),
        }
    }

    pub(super) const fn storage(&self) -> &'s PersistedColumnLayer<K, R> {
        self.storage
    }

    pub(super) const fn bounds(&self) -> (usize, usize) {
        self.bounds
    }

    pub fn seek_key_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K::Archived) -> bool,
    {
        unsafe { self.storage.assume_invariants() }
        if self.valid() {
            self.pos += advance(
                &self.storage.data().keys[self.pos as usize..self.bounds.1],
                predicate,
            ) as isize;
        }
    }

    pub fn seek_key_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&K::Archived) -> bool,
    {
        unsafe { self.storage.assume_invariants() }
        if self.valid() {
            self.pos -= retreat(
                &self.storage.data().keys[self.bounds.0..=self.pos as usize],
                predicate,
            ) as isize;
        }
    }

    pub fn current_key(&self) -> &K {
        debug_assert!(self.pos >= 0);
        todo!("need to update this on key change");
        &self.kv_space.cur_key.borrow().as_ref().unwrap()
    }

    pub fn current_diff(&self) -> &R {
        debug_assert!(self.pos >= 0);
        todo!("need to update this on key change");
        &self.kv_space.cur_r.borrow().as_ref().unwrap()
    }
}

impl<'s, K, R> Cursor<'s> for PersistedColumnLayerCursor<'s, K, R>
where
    K: DBData,
    R: DBWeight,
    <K as Deserializable>::ArchivedDeser: PartialOrd<K>,
{
    type Item<'k> = (&'k K, &'k R)
        where
            Self: 'k;

    type Key = K;

    type ValueCursor = ();

    fn keys(&self) -> usize {
        self.bounds.1 - self.bounds.0
    }

    fn item(&self) -> Self::Item<'_> {
        // Elide extra bounds checking
        unsafe { self.storage.assume_invariants() }

        if self.pos >= self.storage.data().keys.len() as isize || self.pos < 0 {
            //cursor_position_oob(self.pos, self.storage.keys.len());
            unimplemented!("commented private function")
        }

        todo!("need to update this on key change");
        (
            &self.kv_space.cur_key.borrow().as_ref().unwrap(),
            &self.kv_space.cur_r.borrow().as_ref().unwrap(),
        )
    }

    fn values(&self) {}

    fn step(&mut self) {
        self.pos += 1;

        if self.pos >= self.bounds.1 as isize {
            self.pos = self.bounds.1 as isize;
        }
    }

    fn step_reverse(&mut self) {
        self.pos -= 1;

        if self.pos < self.bounds.0 as isize {
            self.pos = self.bounds.0 as isize - 1;
        }
    }

    fn seek(&mut self, key: &Self::Key) {
        unsafe { self.storage.assume_invariants() }
        if self.valid() {
            self.pos += advance(
                &self.storage.data().keys[self.pos as usize..self.bounds.1],
                |k| k.lt(key),
            ) as isize;
        }
    }

    fn seek_reverse(&mut self, key: &Self::Key) {
        unsafe { self.storage.assume_invariants() }
        if self.valid() {
            self.pos -= retreat(
                &self.storage.data().keys[self.bounds.0..=self.pos as usize],
                |k| k.gt(key),
            ) as isize;
        }
    }

    fn valid(&self) -> bool {
        self.pos >= self.bounds.0 as isize && self.pos < self.bounds.1 as isize
    }

    fn rewind(&mut self) {
        self.pos = self.bounds.0 as isize;
    }

    fn fast_forward(&mut self) {
        self.pos = self.bounds.1 as isize - 1;
    }

    fn position(&self) -> usize {
        self.pos as usize
    }

    fn reposition(&mut self, lower: usize, upper: usize) {
        self.pos = lower as isize;
        self.bounds = (lower, upper);
    }
}

impl<'a, K, R> Display for PersistedColumnLayerCursor<'a, K, R>
where
    K: DBData,
    R: DBWeight,
    <K as Deserializable>::ArchivedDeser: PartialOrd<K>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut cursor: PersistedColumnLayerCursor<K, R> = self.clone();

        while cursor.valid() {
            let (key, val) = cursor.item();
            writeln!(f, "{key:?} -> {val:?}")?;
            cursor.step();
        }

        Ok(())
    }
}
