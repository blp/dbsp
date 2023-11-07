use rkyv::util::to_bytes;
use rkyv::{AlignedVec, Archive, Deserialize};
use std::cmp::min;
use std::fmt::{Display, Formatter};
use std::fs::OpenOptions;
use std::io;
use std::marker::PhantomData;
use std::path::Path;

use dbsp::trace::layers::ordered::{ArchivedOrderedLayer, OrderedLayer};
use dbsp::trace::layers::{advance, OrdOffset, Trie};
use dbsp::trace::Deserializable;
use dbsp::{DBData, DBWeight, NumEntries};
use rand::Rng;

use crate::backend::metadata::{FileHeader, Metadata, PageSection};
use crate::backend::{Backend, StorageBackend, FILE_VERSION_FORMAT};
use crate::{Infallible, Persistence};

//mod builders;
//mod cursor;

struct PersistedOrderedLayer<K, L>
where
    K: DBData,
    L: Persistence + 'static,
{
    header: FileHeader,
    metadata: Metadata<K>,
    block: AlignedVec,
    _layer: PhantomData<L>,
    backend: Box<dyn StorageBackend>,
    lower_bound: usize,
}

impl<K, L> Persistence for PersistedOrderedLayer<K, L>
where
    K: DBData,
    L: Persistence + 'static,
{
    type Persist = OrderedLayer<K, L::Persist>;

    fn data(&self) -> &ArchivedOrderedLayer<K, L::Persist> {
        unsafe { rkyv::archived_root::<OrderedLayer<K, L::Persist>>(&self.block) }
    }

    fn read<P>(path: P, backend_typ: Backend) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let mut backend = backend_typ.create();
        let fd = OpenOptions::new().read(true).open(path).unwrap();
        let to_read = 24;
        let header_buf = AlignedVec::with_capacity(to_read);
        backend.submit_read(&fd, 0x0, header_buf, 0x1)?;
        let header_buf = backend.await_results(1)?.pop().unwrap();

        let header = rkyv::check_archived_root::<FileHeader>(&header_buf[..]).unwrap();
        assert_eq!(header.magic_number, 0xdeadbeef);
        assert_eq!(header.version, FILE_VERSION_FORMAT);

        let metadata_buf = AlignedVec::with_capacity(header.metadata_size as usize);
        backend.submit_read(&fd, header.metadata_start_offset, metadata_buf, 0x2)?;
        let metadata_buf = backend.await_results(1)?.pop().unwrap();

        let archived_metadata = unsafe { rkyv::archived_root::<Metadata<K>>(&metadata_buf[..]) };
        assert_eq!(header.magic_number, 0xdeadbeef);
        assert_eq!(header.version, FILE_VERSION_FORMAT);
        let first_section = archived_metadata.page_index.first().unwrap();
        let page_buf = AlignedVec::with_capacity(first_section.page_size as usize);

        backend.submit_read(&fd, first_section.file_offset, page_buf, 0x3)?;
        let page_buf = backend.await_results(1)?.pop().unwrap();

        Ok(Self {
            header: header.deserialize(&mut Infallible).unwrap(),
            metadata: archived_metadata.deserialize(&mut Infallible).unwrap(),
            block: page_buf,
            backend,
            _layer: PhantomData,
            lower_bound: 0,
        })
    }

    fn write<P>(
        mut backend: Box<dyn StorageBackend>,
        path: P,
        cl: &OrderedLayer<K, L::Persist>,
    ) -> io::Result<()>
    where
        P: AsRef<Path>,
    {
        let fd = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .unwrap();

        let page_buf = to_bytes(cl).expect("Can't serialize OrderedLayer");
        let page_buf_len = page_buf.len();
        let meta_offset = 0x40 + page_buf.len().next_multiple_of(16) as u64; // TODO a unchecked cast
        backend.submit_write(&fd, 0x40, page_buf, 0x1)?;

        let keys = cl.as_parts().0;
        let range = keys
            .first()
            .map(|first| (first.clone(), keys.last().unwrap().clone()));

        let meta_buf = to_bytes(&Metadata {
            page_index: vec![PageSection {
                file_offset: 0x40,
                page_size: page_buf_len as u64, // TODO another unchecked cast
                range,
            }],
            lower_bound: 0,
        })
        .expect("Can't serialize metadata");
        let meta_buf_len = meta_buf.len();

        backend.submit_write(&fd, meta_offset, meta_buf, 0x2)?;

        let header_buf = to_bytes::<FileHeader, 0x40>(&FileHeader {
            magic_number: 0xdeadbeef,
            version: FILE_VERSION_FORMAT,
            metadata_start_offset: meta_offset,
            metadata_size: meta_buf_len as u64,
        })
        .expect("Can't serialize header");

        backend.submit_write(&fd, 0x0, header_buf, 0x3).unwrap();
        backend.await_results(0x3).expect("execute all writes");
        Ok(())
    }
}

impl<K, L> PersistedOrderedLayer<K, L>
where
    K: DBData,
    L: Persistence + 'static,
{
    /// Assume the invariants of the current builder
    ///
    /// # Safety
    ///
    /// Requires that `offs` has a length of `keys + 1`
    unsafe fn assume_invariants(&self) {
        //assume(self.data().offs.len() == self.data().keys.len() + 1);
        //assume(self.data().lower_bound <= self.data().keys.len());
    }

    /// Compute a random sample of size `sample_size` of keys in `self.keys`.
    ///
    /// Pushes the random sample of keys to the `output` vector in ascending
    /// order.
    pub fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, output: &mut Vec<K>)
    where
        K: Clone,
        RG: Rng,
    {
        unimplemented!("can we sample just from the index?")
    }
}

impl<K, L> PersistedOrderedLayer<K, L>
where
    K: DBData,
    L: Trie + Persistence,
{
    /// Truncate layer at the first key greater than or equal to `lower_bound`.
    pub fn truncate_keys_below(&mut self, lower_bound: &K) {
        unimplemented!()

        //let index = advance(&self.data().keys, |k| k < lower_bound);
        //self.truncate_below(index);
    }
}

impl<K, L> NumEntries for PersistedOrderedLayer<K, L>
where
    K: DBData,
    L: Persistence + Trie + NumEntries,
    <L::Persist as Deserializable>::ArchivedDeser: NumEntries,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.data().keys.len()
    }

    fn num_entries_deep(&self) -> usize {
        self.data().vals.num_entries_deep()
    }
}

/*
impl<K, L> Trie for PersistedOrderedLayer<K, L>
where
    K: DBData,
    L: Persistence + Trie,
    <K as Archive>::Archived: Clone,
    <K as Deserializable>::ArchivedDeser: Ord + Clone,
    <K as Deserializable>::ArchivedDeser: PartialOrd<K>,
{
    type Item = (K, L::Item);
    type Cursor<'s> = PersistedOrderedCursor<'s, K, L> where K: 's, L: 's;
    type MergeBuilder = PersistedOrderedBuilder<K, L::MergeBuilder>;
    type TupleBuilder = PersistedOrderedBuilder<K, L::TupleBuilder>;

    fn keys(&self) -> usize {
        unsafe { self.assume_invariants() }
        self.data().keys.len() - self.lower_bound
    }

    fn tuples(&self) -> usize {
        unsafe { self.assume_invariants() }
        self.data().vals.tuples()
    }

    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor<'_> {
        unsafe { self.assume_invariants() }

        if lower < upper {
            let _child_lower = self.data().offs[lower];
            let _child_upper = self.data().offs[lower + 1];

            PersistedOrderedCursor {
                bounds: (lower, upper),
                storage: self,
                child: self._layer,
                pos: lower as isize,
            }
        } else {
            PersistedOrderedCursor {
                bounds: (0, 0),
                storage: self,
                child: self._layer,
                pos: 0,
            }
        }
    }

    fn lower_bound(&self) -> usize {
        self.lower_bound
    }

    fn truncate_below(&mut self, lower_bound: usize) {
        if lower_bound > self.lower_bound {
            self.lower_bound = min(lower_bound, self.data().keys.len());
        }

        let vals_bound = self.data().offs[self.lower_bound];
        // self.data().vals.truncate_below(vals_bound.into_usize());
        unimplemented!("the truncate_below stuff will likely moved in spine anyways")
    }
}
*/

impl<K, L> Default for PersistedOrderedLayer<K, L>
where
    K: DBData,
    L: Default + Persistence,
{
    fn default() -> Self {
        unimplemented!()
    }
}

impl<K, L> Display for PersistedOrderedLayer<K, L>
where
    K: DBData,
    L: Trie + Persistence,
    <K as Deserializable>::ArchivedDeser: Clone + Ord + PartialOrd<K>,
    for<'a> L::Cursor<'a>: Clone + Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        todo!()
        //self.cursor().fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_layer::PersistedColumnLayer;
    use crate::test::{mkcl, mkol};
    use dbsp::trace::layers::column_layer::ColumnLayer;
    use proptest::proptest;

    fn write_cl_to_file<K: DBData, L: Persistence + 'static>(
        cl: &OrderedLayer<K, L::Persist>,
        backend: Box<dyn StorageBackend>,
        path: &str,
    ) {
        std::fs::remove_file(path).unwrap_or_default();
        PersistedOrderedLayer::<K, L>::write(backend, path, cl).unwrap()
    }

    #[test]
    fn test_write() {
        let backend = Backend::Posix;
        let cl = mkol(
            vec![1, 2, 3, 4, 5],
            vec![0, 1, 2, 3, 4, 5],
            mkcl(vec![1, 2, 3, 4, 5], vec![1, 2, 3, 4, 5]),
            0,
        );
        write_cl_to_file::<u64, PersistedColumnLayer<u64, u64>>(
            &cl,
            backend.create(),
            "/tmp/test_ol_write.db",
        )
    }

    #[test]
    fn read_write() {
        let backend = Backend::Posix;
        let cl = mkol(
            vec![1, 2, 3, 4, 5],
            vec![0, 1, 2, 3, 4, 5],
            mkcl(vec![1, 2, 3, 4, 5], vec![1, 2, 3, 4, 5]),
            0,
        );
        write_cl_to_file::<u64, PersistedColumnLayer<u64, u64>>(
            &cl,
            backend.create(),
            "/tmp/test_ol_read_write.db",
        );

        let cl2 = PersistedOrderedLayer::<u64, PersistedColumnLayer<u64, u64>>::read(
            "/tmp/test_ol_read_write.db",
            backend,
        )
        .unwrap();

        let (keys, diffs, layer, bound) = cl.as_parts();
        //assert_eq!(keys, &cl2.data().keys);
        //assert_eq!(cl2.data(), &cl);
    }

    proptest! {
        #[test]
        fn read_write_proptest(cl: OrderedLayer<u64, ColumnLayer<u64, u64>>) {
            write_cl_to_file::<u64, PersistedColumnLayer<u64, u64>>(&cl, Backend::Posix.create(), "/tmp/test_read_write_proptest.db");
            let cl2 = PersistedOrderedLayer::<u64, PersistedColumnLayer<u64, u64>>::read("/tmp/test_read_write_proptest.db", Backend::Posix).unwrap();

            //assert_eq!(&cl, cl2.data());
            let (keys, diffs, layer, bound) = cl.as_parts();

            //assert_eq!(keys, &cl2.data().keys);
            //assert_eq!(bound, &cl2.data().lower_bound);
            //assert_eq!(&cl.offs, &cl2.data().offs);
            //assert_eq!(&cl.vals, &cl2.data().vals);
        }
    }
}
