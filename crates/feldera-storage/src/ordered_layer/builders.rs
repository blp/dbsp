pub use consumer::{OrderedLayerConsumer, OrderedLayerValues};
use rkyv::{Archive, Deserialize, Serialize};

use dbsp::{
    algebra::{AddAssignByRef, AddByRef, NegByRef},
    trace::layers::{
        advance, column_layer::ColumnLayer, retreat, Builder, Cursor, MergeBuilder, OrdOffset,
        Trie, TupleBuilder,
    },
    utils::sample_slice,
    DBData, NumEntries,
};
use rand::Rng;
use size_of::SizeOf;
use std::{
    cmp::{min, Ordering},
    fmt::{Debug, Display, Formatter},
    mem::MaybeUninit,
    ops::{Add, AddAssign, Neg},
};

/// Assembles a layer of this
#[derive(SizeOf, Debug, Clone)]
pub struct OrderedBuilder<K, L, O = usize> {
    /// Keys
    pub keys: Vec<K>,
    /// Offsets
    pub offs: Vec<O>,
    /// The next layer down
    pub vals: L,
}

impl<K, L, O> OrderedBuilder<K, L, O> {
    /// Performs one step of merging.
    pub fn merge_step(
        &mut self,
        (trie1, lower1, upper1): (&<Self as Builder>::Trie, &mut usize, usize),
        (trie2, lower2, upper2): (&<Self as Builder>::Trie, &mut usize, usize),
    ) where
        K: DBData,
        L: MergeBuilder,
        O: OrdOffset,
    {
        match trie1.keys[*lower1].cmp(&trie2.keys[*lower2]) {
            Ordering::Less => {
                // determine how far we can advance lower1 until we reach/pass lower2
                let step = 1 + advance(&trie1.keys[(1 + *lower1)..upper1], |x| {
                    x < &trie2.keys[*lower2]
                });
                let step = min(step, 1_000);
                self.copy_range(trie1, *lower1, *lower1 + step);
                *lower1 += step;
            }

            Ordering::Equal => {
                let lower = self.vals.boundary();
                // record vals_length so we can tell if anything was pushed.
                self.vals.push_merge(
                    trie1.vals.cursor_from(
                        trie1.offs[*lower1].into_usize(),
                        trie1.offs[*lower1 + 1].into_usize(),
                    ),
                    trie2.vals.cursor_from(
                        trie2.offs[*lower2].into_usize(),
                        trie2.offs[*lower2 + 1].into_usize(),
                    ),
                );
                if self.vals.keys() > lower {
                    self.keys.push(trie1.keys[*lower1].clone());
                    self.offs.push(O::from_usize(self.vals.keys()));
                }

                *lower1 += 1;
                *lower2 += 1;
            }

            Ordering::Greater => {
                // determine how far we can advance lower2 until we reach/pass lower1
                let step = 1 + advance(&trie2.keys[(1 + *lower2)..upper2], |x| {
                    x < &trie1.keys[*lower1]
                });
                let step = min(step, 1_000);
                self.copy_range(trie2, *lower2, *lower2 + step);
                *lower2 += step;
            }
        }
    }

    pub fn merge_step_retain_keys<F>(
        &mut self,
        (trie1, lower1, upper1): (&<Self as Builder>::Trie, &mut usize, usize),
        (trie2, lower2, upper2): (&<Self as Builder>::Trie, &mut usize, usize),
        filter: &F,
    ) where
        K: DBData,
        L: MergeBuilder,
        O: OrdOffset,
        F: Fn(&K) -> bool,
    {
        match trie1.keys[*lower1].cmp(&trie2.keys[*lower2]) {
            Ordering::Less => {
                // determine how far we can advance lower1 until we reach/pass lower2
                let step = 1 + advance(&trie1.keys[(1 + *lower1)..upper1], |x| {
                    x < &trie2.keys[*lower2]
                });
                let step = min(step, 1_000);
                self.copy_range_retain_keys(trie1, *lower1, *lower1 + step, filter);
                *lower1 += step;
            }

            Ordering::Equal => {
                let lower = self.vals.boundary();
                if filter(&trie1.keys[*lower1]) {
                    // record vals_length so we can tell if anything was pushed.
                    self.vals.push_merge(
                        trie1.vals.cursor_from(
                            trie1.offs[*lower1].into_usize(),
                            trie1.offs[*lower1 + 1].into_usize(),
                        ),
                        trie2.vals.cursor_from(
                            trie2.offs[*lower2].into_usize(),
                            trie2.offs[*lower2 + 1].into_usize(),
                        ),
                    );
                    if self.vals.keys() > lower {
                        self.keys.push(trie1.keys[*lower1].clone());
                        self.offs.push(O::from_usize(self.vals.keys()));
                    }
                }

                *lower1 += 1;
                *lower2 += 1;
            }

            Ordering::Greater => {
                // determine how far we can advance lower2 until we reach/pass lower1
                let step = 1 + advance(&trie2.keys[(1 + *lower2)..upper2], |x| {
                    x < &trie1.keys[*lower1]
                });
                let step = min(step, 1_000);
                self.copy_range_retain_keys(trie2, *lower2, *lower2 + step, filter);
                *lower2 += step;
            }
        }
    }

    /// Push a key and all of its associated values to the current builder
    ///
    /// Can be more efficient than repeatedly calling `.push_tuple()` because it
    /// doesn't require an owned key for every value+diff pair which can allow
    /// eliding unnecessary clones
    pub fn with_key<F>(&mut self, key: K, with: F)
    where
        K: Eq + 'static,
        L: TupleBuilder,
        O: OrdOffset,
        F: for<'a> FnOnce(OrderedBuilderVals<'a, K, L, O>),
    {
        let mut pushes = 0;
        let vals = OrderedBuilderVals {
            builder: self,
            pushes: &mut pushes,
        };
        with(vals);

        // If the user's closure actually added any elements, push the key
        if pushes != 0
            && (self.keys.is_empty()
                || !self.offs[self.keys.len()].is_zero()
                || self.keys[self.keys.len() - 1] != key)
        {
            if !self.keys.is_empty() && self.offs[self.keys.len()].is_zero() {
                self.offs[self.keys.len()] = O::from_usize(self.vals.boundary());
            }
            self.keys.push(key);
            self.offs.push(O::zero());
        }
    }
}

pub struct OrderedBuilderVals<'a, K, L, O>
where
    K: 'static,
    O: 'static,
{
    builder: &'a mut OrderedBuilder<K, L, O>,
    pushes: &'a mut usize,
}

impl<'a, K, L, O> OrderedBuilderVals<'a, K, L, O>
where
    K: 'static,
    L: TupleBuilder,
{
    pub fn push(&mut self, value: <L as TupleBuilder>::Item) {
        *self.pushes += 1;
        self.builder.vals.push_tuple(value);
    }
}

impl<K, L, O> Builder for OrderedBuilder<K, L, O>
where
    K: DBData,
    L: Builder,
    O: OrdOffset,
{
    type Trie = OrderedLayer<K, L::Trie, O>;

    fn boundary(&mut self) -> usize {
        self.offs[self.keys.len()] = O::from_usize(self.vals.boundary());
        self.keys.len()
    }

    fn done(mut self) -> Self::Trie {
        if !self.keys.is_empty() && self.offs[self.keys.len()].is_zero() {
            self.offs[self.keys.len()] = O::from_usize(self.vals.boundary());
        }

        OrderedLayer {
            keys: self.keys,
            offs: self.offs,
            vals: self.vals.done(),
            lower_bound: 0,
        }
    }
}

impl<K, L, O> OrderedBuilder<K, L, O>
where
    K: DBData,
    L: MergeBuilder,
    O: OrdOffset,
{
    /// Like `push_merge`, but uses `fuel` to bound the amount of work.
    ///
    /// Builds at most `fuel` values plus values for one extra key.
    /// If `fuel > 0` when the method returns, this means that the merge is
    /// complete.
    pub fn push_merge_fueled(
        &mut self,
        (source1, lower1, upper1): (&<Self as Builder>::Trie, &mut usize, usize),
        (source2, lower2, upper2): (&<Self as Builder>::Trie, &mut usize, usize),
        fuel: &mut isize,
    ) {
        let starting_updates = self.vals.keys();
        let mut effort = 0isize;

        // while both mergees are still active
        while *lower1 < upper1 && *lower2 < upper2 && effort < *fuel {
            self.merge_step((source1, lower1, upper1), (source2, lower2, upper2));
            effort = (self.vals.keys() - starting_updates) as isize;
        }

        // Merging is complete; only copying remains.
        if *lower1 == upper1 || *lower2 == upper2 {
            // Limit merging by remaining fuel.
            let mut remaining_fuel = *fuel - effort;
            if remaining_fuel > 0 {
                if *lower1 < upper1 {
                    if remaining_fuel < 1_000 {
                        remaining_fuel = 1_000;
                    }
                    *lower1 =
                        self.copy_range_fueled(source1, *lower1, upper1, remaining_fuel as usize);
                }
                if *lower2 < upper2 {
                    if remaining_fuel < 1_000 {
                        remaining_fuel = 1_000;
                    }
                    *lower2 =
                        self.copy_range_fueled(source2, *lower2, upper2, remaining_fuel as usize);
                }
            }
        }

        effort = (self.vals.keys() - starting_updates) as isize;

        *fuel -= effort;
    }

    pub fn push_merge_retain_keys_fueled<F>(
        &mut self,
        (source1, lower1, upper1): (&<Self as Builder>::Trie, &mut usize, usize),
        (source2, lower2, upper2): (&<Self as Builder>::Trie, &mut usize, usize),
        filter: &F,
        fuel: &mut isize,
    ) where
        F: Fn(&K) -> bool,
    {
        let starting_updates = self.vals.keys();
        let mut effort = 0isize;

        // while both mergees are still active
        while *lower1 < upper1 && *lower2 < upper2 && effort < *fuel {
            self.merge_step_retain_keys(
                (source1, lower1, upper1),
                (source2, lower2, upper2),
                filter,
            );
            effort = (self.vals.keys() - starting_updates) as isize;
        }

        // Merging is complete; only copying remains.
        if *lower1 == upper1 || *lower2 == upper2 {
            // Limit merging by remaining fuel.
            let mut remaining_fuel = *fuel - effort;
            if remaining_fuel > 0 {
                if *lower1 < upper1 {
                    if remaining_fuel < 1_000 {
                        remaining_fuel = 1_000;
                    }
                    *lower1 = self.copy_range_retain_keys_fueled(
                        source1,
                        *lower1,
                        upper1,
                        filter,
                        remaining_fuel as usize,
                    );
                }
                if *lower2 < upper2 {
                    if remaining_fuel < 1_000 {
                        remaining_fuel = 1_000;
                    }
                    *lower2 = self.copy_range_retain_keys_fueled(
                        source2,
                        *lower2,
                        upper2,
                        filter,
                        remaining_fuel as usize,
                    );
                }
            }
        }

        effort = (self.vals.keys() - starting_updates) as isize;

        *fuel -= effort;
    }

    /// Like `copy_range`, but uses `fuel` to bound the amount of work.
    ///
    /// Invariants:
    /// - Copies at most `fuel` values plus values for one extra key.
    /// - If `fuel` is greater than or equal to the number of values, in the
    ///   `lower..upper` key range, copies the entire range.
    pub fn copy_range_fueled(
        &mut self,
        other: &<Self as Builder>::Trie,
        lower: usize,
        mut upper: usize,
        fuel: usize,
    ) -> usize {
        assert!(lower < upper && lower < other.offs.len() && upper < other.offs.len());

        let other_basis = other.offs[lower];
        let self_basis = self.offs.last().copied().unwrap_or_else(|| O::zero());

        // Number of keys in the `[lower..upper]` range that can be copied without
        // exceeding `fuel`.
        let keys = advance(&other.offs[lower..upper], |offset| {
            offset.into_usize() - other_basis.into_usize() <= fuel
        });

        upper = lower + keys;

        self.keys.extend_from_slice(&other.keys[lower..upper]);
        for index in lower..upper {
            self.offs
                .push((other.offs[index + 1] + self_basis) - other_basis);
        }

        self.vals.copy_range(
            &other.vals,
            other_basis.into_usize(),
            other.offs[upper].into_usize(),
        );

        upper
    }

    fn copy_range_retain_keys_fueled<F>(
        &mut self,
        other: &<Self as Builder>::Trie,
        lower: usize,
        mut upper: usize,
        filter: &F,
        fuel: usize,
    ) -> usize
    where
        F: Fn(&K) -> bool,
    {
        assert!(lower < upper && lower < other.offs.len() && upper < other.offs.len());

        let other_basis = other.offs[lower];

        // Number of keys in the `[lower..upper]` range that can be copied without
        // exceeding `fuel`.
        let keys = advance(&other.offs[lower..upper], |offset| {
            offset.into_usize() - other_basis.into_usize() <= fuel
        });

        upper = lower + keys;

        self.copy_range_retain_keys(other, lower, upper, filter);

        upper
    }
}

impl<K, V, L, O> OrderedBuilder<K, L, O>
where
    K: DBData,
    O: OrdOffset,
    L: MergeBuilder,
    <L as Builder>::Trie: 'static,
    for<'a, 'b> <<L as Builder>::Trie as Trie>::Cursor<'a>: Cursor<'a, Key = V>,
{
    /// Like `push_merge_fueled`, but also removes values that don't pass
    /// `filter` in both inputs.
    pub fn push_merge_retain_values_fueled<KF, VF>(
        &mut self,
        (source1, lower1, upper1): (&<Self as Builder>::Trie, &mut usize, usize),
        (source2, lower2, upper2): (&<Self as Builder>::Trie, &mut usize, usize),
        key_filter: &KF,
        value_filter: &VF,
        fuel: &mut isize,
    ) where
        KF: Fn(&K) -> bool,
        VF: Fn(&V) -> bool,
    {
        let starting_updates = self.vals.keys();
        let mut effort = 0isize;

        // while both mergees are still active
        while *lower1 < upper1 && *lower2 < upper2 && effort < *fuel {
            self.merge_step_retain_values_fueled(
                (source1, lower1, upper1),
                (source2, lower2, upper2),
                key_filter,
                value_filter,
                usize::max_value(),
            );
            // TODO: account for filtered out keys.
            effort = (self.vals.keys() - starting_updates) as isize;
        }

        // Merging is complete; only copying remains.
        if *lower1 == upper1 || *lower2 == upper2 {
            // Limit merging by remaining fuel.
            let mut remaining_fuel = *fuel - effort;
            if remaining_fuel > 0 {
                if *lower1 < upper1 {
                    if remaining_fuel < 1_000 {
                        remaining_fuel = 1_000;
                    }
                    *lower1 = self.copy_range_retain_values_fueled(
                        source1,
                        *lower1,
                        upper1,
                        key_filter,
                        value_filter,
                        remaining_fuel as usize,
                    );
                }
                if *lower2 < upper2 {
                    if remaining_fuel < 1_000 {
                        remaining_fuel = 1_000;
                    }
                    *lower2 = self.copy_range_retain_values_fueled(
                        source2,
                        *lower2,
                        upper2,
                        key_filter,
                        value_filter,
                        remaining_fuel as usize,
                    );
                }
            }
        }

        effort = (self.vals.keys() - starting_updates) as isize;
        *fuel -= effort;
    }

    fn merge_step_retain_values_fueled<KF, VF>(
        &mut self,
        (trie1, lower1, upper1): (&<Self as Builder>::Trie, &mut usize, usize),
        (trie2, lower2, upper2): (&<Self as Builder>::Trie, &mut usize, usize),
        key_filter: &KF,
        value_filter: &VF,
        fuel: usize,
    ) where
        KF: Fn(&K) -> bool,
        VF: Fn(&V) -> bool,
    {
        match trie1.keys[*lower1].cmp(&trie2.keys[*lower2]) {
            Ordering::Less => {
                // determine how far we can advance lower1 until we reach/pass lower2
                let step = 1 + advance(&trie1.keys[(1 + *lower1)..upper1], |x| {
                    x < &trie2.keys[*lower2]
                });
                let step = min(step, 1_000);
                *lower1 = self.copy_range_retain_values_fueled(
                    trie1,
                    *lower1,
                    *lower1 + step,
                    key_filter,
                    value_filter,
                    fuel,
                );
            }

            Ordering::Equal => {
                let lower = self.vals.boundary();
                let cursor1 = trie1.vals.cursor_from(
                    trie1.offs[*lower1].into_usize(),
                    trie1.offs[*lower1 + 1].into_usize(),
                );

                let cursor2 = trie2.vals.cursor_from(
                    trie2.offs[*lower2].into_usize(),
                    trie2.offs[*lower2 + 1].into_usize(),
                );

                if key_filter(&trie1.keys[*lower1]) {
                    // record vals_length so we can tell if anything was pushed.
                    self.vals
                        .push_merge_retain_keys(cursor1, cursor2, value_filter);

                    if self.vals.keys() > lower {
                        self.keys.push(trie1.keys[*lower1].clone());
                        self.offs.push(O::from_usize(self.vals.keys()));
                    }
                }
                *lower1 += 1;
                *lower2 += 1;
            }

            Ordering::Greater => {
                // determine how far we can advance lower2 until we reach/pass lower1
                let step = 1 + advance(&trie2.keys[(1 + *lower2)..upper2], |x| {
                    x < &trie1.keys[*lower1]
                });
                let step = min(step, 1_000);
                *lower2 = self.copy_range_retain_values_fueled(
                    trie2,
                    *lower2,
                    *lower2 + step,
                    key_filter,
                    value_filter,
                    fuel,
                );
            }
        }
    }

    fn copy_range_retain_values_fueled<KF, VF>(
        &mut self,
        other: &<Self as Builder>::Trie,
        lower: usize,
        upper: usize,
        key_filter: &KF,
        value_filter: &VF,
        fuel: usize,
    ) -> usize
    where
        KF: Fn(&K) -> bool,
        VF: Fn(&V) -> bool,
    {
        assert!(lower < upper && lower < other.offs.len() && upper < other.offs.len());

        let other_start = other.offs[lower].into_usize();

        for index in lower..upper {
            if key_filter(&other.keys[index]) {
                let self_basis = self.vals.boundary();
                let other_end = other.offs[index + 1].into_usize();

                let cursor = other
                    .vals
                    .cursor_from(other.offs[index].into_usize(), other_end);

                let other_basis = cursor.position();

                if other_end > other_basis {
                    self.vals.copy_range_retain_keys(
                        &other.vals,
                        other_basis,
                        other_end,
                        value_filter,
                    );
                    if self.vals.keys() > self_basis {
                        self.keys.push(other.keys[index].clone());
                        self.offs.push(O::from_usize(self.vals.keys()));
                    }
                }

                if other_end - other_start >= fuel {
                    return index + 1;
                }
            }
        }

        upper
    }
}

impl<K, L, O> MergeBuilder for OrderedBuilder<K, L, O>
where
    K: DBData,
    L: MergeBuilder,
    O: OrdOffset,
{
    fn with_capacity(other1: &Self::Trie, other2: &Self::Trie) -> Self {
        let mut offs = Vec::with_capacity(other1.keys() + other2.keys() + 1);
        offs.push(O::zero());

        Self {
            keys: Vec::with_capacity(other1.keys() + other2.keys()),
            offs,
            vals: L::with_capacity(&other1.vals, &other2.vals),
        }
    }

    fn with_key_capacity(capacity: usize) -> Self {
        let mut offs = Vec::with_capacity(capacity + 1);
        offs.push(O::zero());

        Self {
            keys: Vec::with_capacity(capacity),
            offs,
            vals: L::with_key_capacity(capacity),
        }
    }

    fn reserve(&mut self, additional: usize) {
        self.keys.reserve(additional);
        self.offs.reserve(additional);
        self.vals.reserve(additional);
    }

    fn keys(&self) -> usize {
        self.keys.len()
    }

    fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
        assert!(lower < upper && lower < other.offs.len() && upper < other.offs.len());

        let other_basis = other.offs[lower];
        let self_basis = self.offs.last().copied().unwrap_or_else(|| O::zero());

        self.keys.extend_from_slice(&other.keys[lower..upper]);
        for index in lower..upper {
            self.offs
                .push((other.offs[index + 1] + self_basis) - other_basis);
        }

        self.vals.copy_range(
            &other.vals,
            other_basis.into_usize(),
            other.offs[upper].into_usize(),
        );
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
        assert!(lower < upper && lower < other.offs.len() && upper < other.offs.len());

        for index in lower..upper {
            if filter(&other.keys[index]) {
                self.keys.push(other.keys[index].clone());

                self.vals.copy_range(
                    &other.vals,
                    other.offs[index].into_usize(),
                    other.offs[index + 1].into_usize(),
                );

                self.offs.push(O::from_usize(self.vals.keys()));
            }
        }
    }

    fn push_merge<'a>(
        &'a mut self,
        cursor1: <Self::Trie as Trie>::Cursor<'a>,
        cursor2: <Self::Trie as Trie>::Cursor<'a>,
    ) {
        let (mut lower1, upper1) = cursor1.bounds;
        let (mut lower2, upper2) = cursor2.bounds;

        let capacity = (upper1 - lower1) + (upper2 - lower2);
        self.keys.reserve(capacity);
        self.offs.reserve(capacity);

        // while both mergees are still active
        while lower1 < upper1 && lower2 < upper2 {
            self.merge_step(
                (cursor1.storage, &mut lower1, upper1),
                (cursor2.storage, &mut lower2, upper2),
            );
        }

        if lower1 < upper1 {
            self.copy_range(cursor1.storage, lower1, upper1);
        }
        if lower2 < upper2 {
            self.copy_range(cursor2.storage, lower2, upper2);
        }
    }

    fn push_merge_retain_keys<'a, F>(
        &'a mut self,
        cursor1: <Self::Trie as Trie>::Cursor<'a>,
        cursor2: <Self::Trie as Trie>::Cursor<'a>,
        filter: &F,
    ) where
        F: Fn(&<<Self::Trie as Trie>::Cursor<'a> as Cursor<'a>>::Key) -> bool,
    {
        let (mut lower1, upper1) = cursor1.bounds;
        let (mut lower2, upper2) = cursor2.bounds;

        let capacity = (upper1 - lower1) + (upper2 - lower2);
        self.keys.reserve(capacity);
        self.offs.reserve(capacity);

        // while both mergees are still active
        while lower1 < upper1 && lower2 < upper2 {
            self.merge_step_retain_keys(
                (cursor1.storage, &mut lower1, upper1),
                (cursor2.storage, &mut lower2, upper2),
                filter,
            );
        }

        if lower1 < upper1 {
            self.copy_range_retain_keys(cursor1.storage, lower1, upper1, filter);
        }
        if lower2 < upper2 {
            self.copy_range_retain_keys(cursor2.storage, lower2, upper2, filter);
        }
    }
}

impl<K, L, O> TupleBuilder for OrderedBuilder<K, L, O>
where
    K: DBData,
    L: TupleBuilder,
    O: OrdOffset,
{
    type Item = (K, L::Item);

    fn new() -> Self {
        Self {
            keys: Vec::new(),
            offs: vec![O::zero()],
            vals: L::new(),
        }
    }

    fn with_capacity(cap: usize) -> Self {
        let mut offs = Vec::with_capacity(cap + 1);
        offs.push(O::zero());

        Self {
            keys: Vec::with_capacity(cap),
            offs,
            vals: L::with_capacity(cap),
        }
    }

    fn reserve_tuples(&mut self, additional: usize) {
        self.keys.reserve(additional);
        self.offs.reserve(additional);
        self.vals.reserve_tuples(additional);
    }

    fn tuples(&self) -> usize {
        self.vals.tuples()
    }

    fn push_tuple(&mut self, (key, val): (K, L::Item)) {
        // if first element, prior element finish, or different element, need to push
        // and maybe punctuate.
        if self.keys.is_empty()
            || !self.offs[self.keys.len()].is_zero()
            || self.keys[self.keys.len() - 1] != key
        {
            if !self.keys.is_empty() && self.offs[self.keys.len()].is_zero() {
                self.offs[self.keys.len()] = O::from_usize(self.vals.boundary());
            }
            self.keys.push(key);
            self.offs.push(O::zero()); // <-- indicates "unfinished".
        }
        self.vals.push_tuple(val);
    }
}
