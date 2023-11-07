use crate::Persistence;

/// A cursor with a child cursor that is updated as we move.
#[derive(Debug)]
pub struct OrderedCursor<'s, K, O, L>
where
    K: 'static,
    O: 'static,
    L: Persistence + Trie,
{
    storage: &'s PersistentOrderedLayer<K, L, O>,
    pos: isize,
    bounds: (usize, usize),
    /// The cursor for the trie layer below this one.
    pub child: L::Cursor<'s>,
}

impl<'s, K, O, L> Clone for OrderedCursor<'s, K, O, L>
where
    K: DBData,
    L: Trie,
    O: OrdOffset,
    L::Cursor<'s>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            storage: self.storage,
            pos: self.pos,
            bounds: self.bounds,
            child: self.child.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.storage.clone_from(&source.storage);
        self.pos.clone_from(&source.pos);
        self.bounds.clone_from(&source.bounds);
        self.child.clone_from(&source.child);
    }
}

impl<'s, K, L, O> OrderedCursor<'s, K, O, L>
where
    K: DBData,
    L: Trie,
    O: OrdOffset,
{
    pub fn seek_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool,
    {
        if self.valid() {
            self.pos += advance(
                &self.storage.keys[self.pos as usize..self.bounds.1],
                predicate,
            ) as isize;
        }

        if self.valid() {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        }
    }

    pub fn seek_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool,
    {
        if self.valid() {
            self.pos -= retreat(
                &self.storage.keys[self.bounds.0..=self.pos as usize],
                predicate,
            ) as isize;
        }

        if self.valid() {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        }
    }
}

impl<'s, K, L, O> Cursor<'s> for OrderedCursor<'s, K, O, L>
where
    K: DBData,
    L: Trie,
    O: OrdOffset,
{
    type Key = K;

    type Item<'k> = &'k K
        where
            Self: 'k;

    type ValueStorage = L;

    #[inline]
    fn keys(&self) -> usize {
        self.bounds.1 - self.bounds.0
    }

    #[inline]
    fn item(&self) -> Self::Item<'s> {
        &self.storage.keys[self.pos as usize]
    }

    fn values(&self) -> L::Cursor<'s> {
        if self.valid() {
            self.storage.vals.cursor_from(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            )
        } else {
            self.storage.vals.cursor_from(0, 0)
        }
    }

    fn step(&mut self) {
        self.pos += 1;

        if self.pos < self.bounds.1 as isize {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        } else {
            self.pos = self.bounds.1 as isize;
        }
    }

    fn seek(&mut self, key: &Self::Key) {
        if self.valid() {
            self.pos += advance(&self.storage.keys[self.pos as usize..self.bounds.1], |k| {
                k < key
            }) as isize;
        }

        if self.valid() {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        }
    }

    fn valid(&self) -> bool {
        self.pos >= self.bounds.0 as isize && self.pos < self.bounds.1 as isize
    }

    fn rewind(&mut self) {
        self.pos = self.bounds.0 as isize;

        if self.valid() {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        }
    }

    fn position(&self) -> usize {
        self.pos as usize
    }

    fn reposition(&mut self, lower: usize, upper: usize) {
        self.pos = lower as isize;
        self.bounds = (lower, upper);

        if self.valid() {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        }
    }

    fn step_reverse(&mut self) {
        self.pos -= 1;

        if self.pos >= self.bounds.0 as isize {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        } else {
            self.pos = self.bounds.0 as isize - 1;
        }
    }

    fn seek_reverse(&mut self, key: &Self::Key) {
        if self.valid() {
            self.pos -= retreat(&self.storage.keys[self.bounds.0..=self.pos as usize], |k| {
                k > key
            }) as isize;
        }

        if self.valid() {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        }
    }

    fn fast_forward(&mut self) {
        self.pos = self.bounds.1 as isize - 1;

        if self.valid() {
            self.child.reposition(
                self.storage.offs[self.pos as usize].into_usize(),
                self.storage.offs[self.pos as usize + 1].into_usize(),
            );
        }
    }
}

impl<'a, K, L, O> Display for OrderedCursor<'a, K, O, L>
where
    K: DBData,
    L: Trie,
    L::Cursor<'a>: Clone + Display,
    O: OrdOffset,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let mut cursor: OrderedCursor<'_, K, O, L> = self.clone();

        while cursor.valid() {
            let key = cursor.item();
            writeln!(f, "{key:?}:")?;
            let val_str = cursor.values().to_string();

            f.write_str(&indent(&val_str, "    "))?;
            cursor.step();
        }

        Ok(())
    }
}
