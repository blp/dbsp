mod builders;
pub(crate) mod cursor;

use rkyv::util::to_bytes;
use rkyv::{AlignedVec, Archive, Deserialize};
use std::cmp::min;
use std::fmt::Debug;
use std::fs::OpenOptions;
use std::io;

use dbsp::algebra::MonoidValue;
use dbsp::trace::layers::column_layer::{ArchivedColumnLayer, ColumnLayer};
use dbsp::trace::layers::Trie;
use dbsp::trace::Deserializable;
use dbsp::{DBData, DBWeight};
use std::path::Path;

use crate::backend::metadata::{FileHeader, Metadata, PageSection};
use crate::backend::{Backend, StorageBackend, FILE_VERSION_FORMAT};
use crate::column_layer::builders::PersistedColumnLayerBuilder;
use crate::column_layer::cursor::{PersistedColumnLayerCursor, ScrapSpace};
use crate::{Infallible, Persistence};

pub struct PersistedColumnLayer<K, R> {
    header: FileHeader,
    metadata: Metadata<K>,
    block: AlignedVec,
    backend: Box<dyn StorageBackend>,
    lower_bound: usize,
    _phantom: std::marker::PhantomData<R>,
}

impl<K, R> Debug for PersistedColumnLayer<K, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistedColumnLayer")
            .field("header", &self.header)
            .field("lower_bound", &self.lower_bound)
            .finish()
    }
}

impl<K, R> PersistedColumnLayer<K, R> {
    pub(crate) fn assume_invariants(&self) {}
}

impl<K, R> Persistence for PersistedColumnLayer<K, R>
where
    K: DBData,
    R: DBWeight,
{
    type Persist = ColumnLayer<K, R>;
    fn data(&self) -> &ArchivedColumnLayer<K, R> {
        unsafe { rkyv::archived_root::<ColumnLayer<K, R>>(&self.block) }
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
            _phantom: std::marker::PhantomData,
            lower_bound: 0,
        })
    }

    fn write<P>(
        mut backend: Box<dyn StorageBackend>,
        path: P,
        cl: &ColumnLayer<K, R>,
    ) -> io::Result<()>
    where
        P: AsRef<Path>,
    {
        let fd = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .unwrap();

        let page_buf = to_bytes(cl).expect("Can't serialize ColumnLayer");
        let page_buf_len = page_buf.len();
        let meta_offset = 0x40 + page_buf.len().next_multiple_of(16) as u64; // TODO a unchecked cast
        backend.submit_write(&fd, 0x40, page_buf, 0x1)?;

        let range = cl
            .keys()
            .first()
            .and_then(|first| Some((first.clone(), cl.keys().last().unwrap().clone())));

        let meta_buf = to_bytes(&Metadata {
            page_index: vec![PageSection {
                file_offset: 0x40,
                page_size: page_buf_len as u64, // TODO another unchecked cast
                range,
            }],
            lower_bound: cl.lower_bound(),
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

impl<K, R> Trie for PersistedColumnLayer<K, R>
where
    K: DBData,
    R: DBWeight,
    <K as Archive>::Archived: Clone,
    <R as Archive>::Archived: Clone,
    <K as Deserializable>::ArchivedDeser: Ord + Clone,
    <R as Deserializable>::ArchivedDeser: Ord + Clone + MonoidValue,
    <K as Deserializable>::ArchivedDeser: PartialOrd<K>,
{
    type Item = (K::Archived, R::Archived);
    type Cursor<'s> = PersistedColumnLayerCursor<'s, K, R> where K: 's, R: 's;
    type MergeBuilder = PersistedColumnLayerBuilder<K, R>;
    type TupleBuilder = PersistedColumnLayerBuilder<K, R>;

    fn keys(&self) -> usize {
        self.data().len() - self.lower_bound
    }

    fn tuples(&self) -> usize {
        self.data().len() - self.lower_bound
    }

    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor<'_> {
        unsafe { self.assume_invariants() }
        PersistedColumnLayerCursor::new(lower, self, (lower, upper))
    }

    fn truncate_below(&mut self, lower_bound: usize) {
        if lower_bound > self.lower_bound {
            self.lower_bound = min(lower_bound, self.data().keys.len());
        }
    }

    fn lower_bound(&self) -> usize {
        self.lower_bound
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_layer::ColumnLayer;
    use crate::test::mkcl;
    use proptest::proptest;

    fn write_cl_to_file<K: DBData, V: DBWeight>(
        cl: &ColumnLayer<K, V>,
        backend: Box<dyn StorageBackend>,
        path: &str,
    ) {
        std::fs::remove_file(path).unwrap_or_default();
        PersistedColumnLayer::write(backend, path, cl).unwrap()
    }

    #[test]
    fn test_write() {
        let backend = Backend::Posix;
        let cl = mkcl(vec![1, 2, 3, 4, 5], vec![1, 2, 3, 4, 5]);
        write_cl_to_file(&cl, backend.create(), "/tmp/test_write.db")
    }

    #[test]
    fn read_write() {
        let backend = Backend::Posix;
        let cl = mkcl::<usize, usize>(vec![11, 22, 33, 44, 55], vec![66, 77, 88, 99, 100]);
        write_cl_to_file(&cl, backend.create(), "/tmp/test_cl_read_write.db");

        let cl2 = PersistedColumnLayer::<usize, usize>::read("/tmp/test_cl_read_write.db", backend)
            .unwrap();
        assert_eq!(cl, cl2.data().deserialize(&mut Infallible).unwrap());
    }

    proptest! {
        #[test]
        fn read_write_proptest(cl: ColumnLayer<usize, usize>) {
            write_cl_to_file(&cl, Backend::Posix.create(), "/tmp/test_cl_read_write_proptest.db");

            let cl2 = PersistedColumnLayer::<usize, usize>::read("/tmp/test_cl_read_write_proptest.db", Backend::Posix).unwrap();
            assert_eq!(cl, cl2.data().deserialize(&mut Infallible).unwrap());
        }
    }
}
