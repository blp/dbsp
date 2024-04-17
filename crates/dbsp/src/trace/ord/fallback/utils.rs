use dyn_clone::clone_box;

use crate::{
    dynamic::DataTrait,
    trace::{BatchReader, Cursor},
    Timestamp,
};

pub(super) enum Position<K>
where
    K: DataTrait + ?Sized,
{
    Start,
    At(Box<K>),
    End,
}

impl<K> Position<K>
where
    K: DataTrait + ?Sized,
{
    pub fn cursor<'s, B>(&self, source: &'s B) -> B::Cursor<'s>
    where
        B: BatchReader<Key = K>,
    {
        let mut cursor = source.cursor();
        match self {
            Position::Start => (),
            Position::At(key) => cursor.seek_key(key.as_ref()),
            Position::End => {
                cursor.fast_forward_keys();
                cursor.step_key()
            }
        }
        cursor
    }

    pub fn from_cursor<C, V, T, R>(cursor: &C) -> Position<K>
    where
        C: Cursor<K, V, T, R>,
        V: DataTrait + ?Sized,
        T: Timestamp,
        R: ?Sized,
    {
        if cursor.key_valid() {
            Self::At(clone_box(cursor.key()))
        } else {
            Self::End
        }
    }
}
