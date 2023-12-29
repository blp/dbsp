mod builders;
mod consumer;
pub(crate) mod cursor;

use feldera_storage::file::reader::Reader;
use rand::{seq::index::sample, Rng};
use rkyv::ser::Serializer;
use rkyv::{Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::ops::AddAssign;
use std::{
    cmp::min,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    marker::PhantomData,
    ops::{Add, Neg},
};

use crate::algebra::{AddAssignByRef, AddByRef, NegByRef};
use crate::trace::layers::{Builder, Cursor, Trie, TupleBuilder};
use crate::{DBData, DBWeight, NumEntries};

pub use self::builders::FileColumnLayerBuilder;
pub use self::cursor::FileColumnLayerCursor;
pub use consumer::{FileColumnLayerConsumer, FileColumnLayerValues};

pub struct FileColumnLayer<K, R> {
    file: Reader,
    lower_bound: usize,
    _phantom: PhantomData<(K, R)>,
}

impl<K, R> FileColumnLayer<K, R> {
    pub fn len(&self) -> u64 {
        self.file.rows().len()
    }

    pub fn is_empty(&self) -> bool {
        self.file.rows().is_empty()
    }

    pub fn empty() -> Self {
        Self {
            file: Reader::empty(1).unwrap(),
            lower_bound: 0,
            _phantom: PhantomData,
        }
    }

    pub fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, output: &mut Vec<K>)
    where
        K: DBData,
        R: DBWeight,
        RG: Rng,
    {
        let size = self.len();

        let mut cursor = self.cursor();
        if sample_size as u64 >= size {
            output.reserve(size as usize);

            while let Some((key, _)) = cursor.take_current_item() {
                output.push(key);
            }
        } else {
            output.reserve(sample_size);

            let mut indexes = sample(rng, size as usize, sample_size).into_vec();
            indexes.sort_unstable();
            for index in indexes.into_iter() {
                cursor.move_to_row(index);
                output.push(cursor.current_key().clone());
            }
        }
    }

    /// Remove keys smaller than `lower_bound` from the batch.
    pub fn truncate_keys_below(&mut self, lower_bound: &K)
    where
        K: DBData,
        R: DBWeight,
    {
        let mut cursor = self.file.rows().before::<K, R>();
        unsafe { cursor.advance_to_value_or_larger(lower_bound) }.unwrap();
        self.truncate_below(cursor.position() as usize);
    }
}

impl<K, R> Clone for FileColumnLayer<K, R> {
    fn clone(&self) -> Self {
        Self {
            file: self.file.clone(),
            lower_bound: self.lower_bound,
            _phantom: PhantomData,
        }
    }
}

impl<K, R> Debug for FileColumnLayer<K, R> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_struct("FileColumnLayer")
            .field("lower_bound", &self.lower_bound)
            .finish()
    }
}

impl<K, R> Display for FileColumnLayer<K, R>
where
    K: DBData,
    R: DBWeight,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        Display::fmt(&self.cursor(), f)
    }
}

impl<K, R> Trie for FileColumnLayer<K, R>
where
    K: DBData,
    R: DBWeight,
{
    type Item = (K, R);
    type Cursor<'s> = FileColumnLayerCursor<'s, K, R> where K: 's, R: 's;
    type MergeBuilder = FileColumnLayerBuilder<K, R>;
    type TupleBuilder = FileColumnLayerBuilder<K, R>;

    fn keys(&self) -> usize {
        self.file.rows().len() as usize - self.lower_bound
    }

    fn tuples(&self) -> usize {
        self.file.rows().len() as usize - self.lower_bound
    }

    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor<'_> {
        FileColumnLayerCursor::new(lower, self, (lower, upper))
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        self.cursor_from(0, self.file.rows().len() as usize) // XXX this cast is
                                                             // risky
    }

    fn truncate_below(&mut self, lower_bound: usize) {
        if lower_bound > self.lower_bound {
            self.lower_bound = min(lower_bound, self.file.rows().len() as usize);
        }
    }

    fn lower_bound(&self) -> usize {
        self.lower_bound
    }
}

impl<K, R> Archive for FileColumnLayer<K, R>
where
    K: DBData,
    R: DBWeight,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, R, S> Serialize<S> for FileColumnLayer<K, R>
where
    K: DBData,
    R: DBWeight,
    S: Serializer + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, R, D> Deserialize<FileColumnLayer<K, R>, D> for Archived<FileColumnLayer<K, R>>
where
    K: DBData,
    R: DBWeight,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FileColumnLayer<K, R>, D::Error> {
        unimplemented!();
    }
}

impl<K, R> SizeOf for FileColumnLayer<K, R>
where
    K: DBData,
    R: DBWeight,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

impl<K, R> PartialEq for FileColumnLayer<K, R>
where
    K: DBData,
    R: DBWeight,
{
    fn eq(&self, other: &Self) -> bool {
        if self.lower_bound != other.lower_bound {
            false
        } else if let Some(equality) = self.file.equal(&other.file) {
            equality
        } else if self.keys() != other.keys() {
            false
        } else {
            let mut cursor1 = self.cursor();
            let mut cursor2 = other.cursor();
            while cursor1.valid() && cursor2.valid() {
                if cursor1.take_current_item() != cursor2.take_current_item() {
                    return false;
                }
            }
            true
        }
    }
}

impl<K, R> Eq for FileColumnLayer<K, R>
where
    K: DBData,
    R: DBWeight,
{
}

impl<K, R> NumEntries for FileColumnLayer<K, R> {
    const CONST_NUM_ENTRIES: Option<usize> = None;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.len() as usize
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        // FIXME: Doesn't take element sizes into account
        self.len() as usize
    }
}

impl<K, R> Add<Self> for FileColumnLayer<K, R>
where
    K: DBData,
    R: DBWeight,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        if self.is_empty() {
            rhs
        } else if rhs.is_empty() {
            self
        } else {
            self.merge(&rhs)
        }
    }
}

impl<K, R> AddAssign<Self> for FileColumnLayer<K, R>
where
    K: DBData,
    R: DBWeight,
{
    fn add_assign(&mut self, rhs: Self) {
        if !rhs.is_empty() {
            *self = self.merge(&rhs);
        }
    }
}

impl<K, R> AddAssignByRef for FileColumnLayer<K, R>
where
    K: DBData,
    R: DBWeight,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        if !other.is_empty() {
            *self = self.merge(other);
        }
    }
}

impl<K, R> AddByRef for FileColumnLayer<K, R>
where
    K: DBData,
    R: DBWeight,
{
    fn add_by_ref(&self, rhs: &Self) -> Self {
        self.merge(rhs)
    }
}

impl<K, R> NegByRef for FileColumnLayer<K, R>
where
    K: DBData,
    R: DBWeight + NegByRef,
{
    fn neg_by_ref(&self) -> Self {
        let mut tuple_builder = <Self as Trie>::TupleBuilder::new();
        let mut cursor = self.cursor();
        while let Some((key, diff)) = cursor.take_current_item() {
            let diff = diff.neg_by_ref();
            tuple_builder.push_tuple((key, diff));
        }
        tuple_builder.done()
    }
}

impl<K, R> Neg for FileColumnLayer<K, R>
where
    K: DBData,
    R: DBWeight + Neg<Output = R>,
{
    type Output = Self;

    fn neg(self) -> Self {
        let mut tuple_builder = <Self as Trie>::TupleBuilder::new();
        let mut cursor = self.cursor();
        while let Some((key, diff)) = cursor.take_current_item() {
            tuple_builder.push_tuple((key, -diff));
        }
        tuple_builder.done()
    }
}
