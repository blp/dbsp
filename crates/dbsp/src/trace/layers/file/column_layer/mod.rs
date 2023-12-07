mod builders;
mod consumer;
pub(crate) mod cursor;

use feldera_storage::file::reader::Reader;
use std::cmp::min;
use std::fmt::Debug;

use crate::trace::layers::Trie;
use crate::{DBData, DBWeight};

pub use self::builders::FileColumnLayerBuilder;
pub use consumer::{FileColumnLayerConsumer, FileColumnLayerValues};
pub use self::cursor::FileColumnLayerCursor;

pub struct FileColumnLayer<K, R> {
    file: Reader,
    lower_bound: usize,
    _phantom: std::marker::PhantomData<(K, R)>,
}

impl<K, R> Debug for FileColumnLayer<K, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileColumnLayer")
            .field("lower_bound", &self.lower_bound)
            .finish()
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
        self.cursor_from(0, self.file.rows().len() as usize) // XXX this cast is risky
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
