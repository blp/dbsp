use std::{
    cmp::Ordering,
    fmt::{Debug, Formatter, Result as FmtResult},
    fs::File,
    io::{Result as IoResult, Write},
    marker::PhantomData,
    ops::Range,
    os::unix::fs::FileExt,
    rc::Rc,
    sync::Arc,
};

use binrw::{
    io::{self, Seek, SeekFrom},
    BinRead,
};
use crc32c::crc32c;
use rkyv::{archived_value, AlignedVec, Deserialize, Infallible};
use tempfile::tempfile;

use crate::file::Item;

use super::{
    ArchivedItem, BlockLocation, CorruptionError, DataBlockHeader, Error, FileHeader, FileTrailer,
    FileTrailerColumn, IndexBlockHeader, NodeType, Rkyv, Varint, VERSION_NUMBER,
};

#[derive(Clone)]
struct VarintReader {
    varint: Varint,
    start: usize,
    count: usize,
}
impl VarintReader {
    fn new(buf: &AlignedVec, varint: Varint, start: usize, count: usize) -> Result<Self, Error> {
        let block_size = buf.len();
        match varint
            .len()
            .checked_mul(count)
            .map(|len| len.checked_add(start))
            .flatten()
        {
            Some(end) if end <= block_size => Ok(Self {
                varint,
                start,
                count,
            }),
            _ => Err(CorruptionError::InvalidArray {
                block_size,
                offset: start,
                count,
                each: varint.len(),
            }
            .into()),
        }
    }
    fn new_opt(
        buf: &AlignedVec,
        varint: Option<Varint>,
        start: usize,
        count: usize,
    ) -> Result<Option<Self>, Error> {
        varint
            .map(|varint| VarintReader::new(buf, varint, start, count))
            .transpose()
    }
    fn get(&self, src: &AlignedVec, index: usize) -> u64 {
        debug_assert!(index < self.count);
        self.varint.get(src, self.start + self.varint.len() * index)
    }
}

#[derive(Clone)]
struct StrideReader {
    start: usize,
    stride: usize,
    count: usize,
}

impl StrideReader {
    fn new(raw: &AlignedVec, start: usize, stride: usize, count: usize) -> Result<Self, Error> {
        let block_size = raw.len();
        if count > 0 {
            if let Some(last) = stride
                .checked_mul(count - 1)
                .map(|len| len.checked_add(start))
                .flatten()
            {
                if last < block_size {
                    return Ok(Self {
                        start,
                        stride,
                        count,
                    });
                }
            }
        }
        Err(CorruptionError::InvalidStride {
            block_size,
            start,
            stride,
            count,
        }
        .into())
    }
    fn get(&self, index: usize) -> usize {
        debug_assert!(index < self.count);
        self.start + index * self.stride
    }
}

#[derive(Clone)]
enum ValueMapReader {
    VarintMap(VarintReader),
    StrideMap(StrideReader),
}

impl ValueMapReader {
    fn new(
        raw: &AlignedVec,
        varint: Option<Varint>,
        offset: u32,
        n_values: u32,
    ) -> Result<Self, Error> {
        let offset = offset as usize;
        let n_values = n_values as usize;
        if let Some(varint) = varint {
            Ok(Self::VarintMap(VarintReader::new(
                raw, varint, offset, n_values,
            )?))
        } else {
            let stride_map = VarintReader::new(raw, Varint::B32, offset, 2)?;
            let start = stride_map.get(raw, 0) as usize;
            let stride = stride_map.get(raw, 1) as usize;
            Ok(Self::StrideMap(StrideReader::new(
                raw, start, stride, n_values,
            )?))
        }
    }
    fn len(&self) -> usize {
        match self {
            ValueMapReader::VarintMap(ref varint_reader) => varint_reader.count,
            ValueMapReader::StrideMap(ref stride_reader) => stride_reader.count,
        }
    }
    fn get(&self, raw: &AlignedVec, index: usize) -> usize {
        match self {
            ValueMapReader::VarintMap(ref varint_reader) => varint_reader.get(raw, index) as usize,
            ValueMapReader::StrideMap(ref stride_reader) => stride_reader.get(index),
        }
    }
}

struct DataBlock<K, A> {
    raw: Rc<AlignedVec>,
    value_map: ValueMapReader,
    row_groups: Option<VarintReader>,
    first_row: u64,
    _phantom: PhantomData<(K, A)>,
}

impl<K, A> Clone for DataBlock<K, A> {
    fn clone(&self) -> Self {
        Self {
            raw: self.raw.clone(),
            value_map: self.value_map.clone(),
            row_groups: self.row_groups.clone(),
            first_row: self.first_row,
            _phantom: PhantomData,
        }
    }
}

impl<K, A> DataBlock<K, A>
where
    K: Rkyv,
    A: Rkyv,
{
    fn new(file: &File, node: &TreeNode) -> Result<Self, Error> {
        // XXX return error if there's no row_groups and this isn't the last column
        let raw = read_block(file, node.location)?;
        let header = DataBlockHeader::read_le(&mut io::Cursor::new(&raw))?;
        if header.n_values == 0 {
            return Err(CorruptionError::EmptyData(node.location).into());
        }
        Ok(Self {
            value_map: ValueMapReader::new(
                &raw,
                header.value_map_varint,
                header.value_map_ofs,
                header.n_values,
            )?,
            row_groups: VarintReader::new_opt(
                &raw,
                header.row_group_varint,
                header.row_groups_ofs as usize,
                header.n_values as usize + 1,
            )?,
            raw: Rc::new(raw),
            first_row: node.first_row,
            _phantom: PhantomData,
        })
    }
    fn n_values(&self) -> usize {
        self.value_map.len()
    }
    fn rows(&self) -> Range<u64> {
        self.first_row..(self.first_row + self.n_values() as u64)
    }
    fn get_row_group(&self, row: u64) -> Option<Range<u64>> {
        self.row_groups.as_ref().map(|row_groups| {
            let index = (row - self.first_row) as usize;
            row_groups.get(&self.raw, index)..row_groups.get(&self.raw, index + 1)
        })
    }
    unsafe fn archived_item(&self, index: usize) -> &ArchivedItem<K, A> {
        archived_value::<Item<K, A>>(&self.raw, self.value_map.get(&self.raw, index))
    }
    unsafe fn item(&self, index: usize) -> (K, A) {
        let item = self.archived_item(index);
        let key = item.0.deserialize(&mut Infallible).unwrap();
        let aux = item.1.deserialize(&mut Infallible).unwrap();
        (key, aux)
    }
    unsafe fn item_for_row(&self, row: u64) -> (K, A) {
        let index = (row - self.first_row) as usize;
        self.item(index)
    }
    unsafe fn key(&self, index: usize) -> K {
        let item = self.archived_item(index);
        let key = item.0.deserialize(&mut Infallible).unwrap();
        key
    }
    unsafe fn key_for_row(&self, row: u64) -> K {
        let index = (row - self.first_row) as usize;
        self.key(index)
    }

    unsafe fn first_ge<C>(&self, target_rows: &Range<u64>, compare: &C) -> Option<usize>
    where
        C: Fn(&K) -> Ordering,
    {
        let mut start = 0;
        let mut end = self.n_values();
        let mut best = None;
        while start < end {
            let mid = (start + end) / 2;
            let row = self.first_row + mid as u64;
            let cmp = range_compare(target_rows, row);
            match cmp {
                Ordering::Equal => {
                    let key = self.key(mid);
                    match compare(&key) {
                        Ordering::Less => {
                            best = Some(mid);
                            end = mid;
                        }
                        Ordering::Equal => return Some(mid),
                        Ordering::Greater => start = mid + 1,
                    }
                }
                Ordering::Less => end = mid,
                Ordering::Greater => start = mid + 1,
            };
        }
        best
    }

    unsafe fn last_le<C>(&self, target_rows: &Range<u64>, compare: &C) -> Option<usize>
    where
        C: Fn(&K) -> Ordering,
    {
        let mut start = 0;
        let mut end = self.n_values();
        let mut best = None;
        while start < end {
            let mid = (start + end) / 2;
            let row = self.first_row + mid as u64;
            let cmp = range_compare(target_rows, row);
            match cmp {
                Ordering::Equal => {
                    let key = self.key(mid);
                    match compare(&key) {
                        Ordering::Greater => {
                            best = Some(mid);
                            start = mid + 1;
                        }
                        Ordering::Equal => return Some(mid),
                        Ordering::Less => end = mid,
                    }
                }
                Ordering::Less => end = mid,
                Ordering::Greater => start = mid + 1,
            };
        }
        best
    }
}

fn range_compare<T>(range: &Range<T>, target: T) -> Ordering
where
    T: Ord,
{
    if target < range.start {
        Ordering::Greater
    } else if target >= range.end {
        Ordering::Less
    } else {
        Ordering::Equal
    }
}

#[derive(Copy, Clone)]
struct TreeNode {
    location: BlockLocation,
    node_type: NodeType,
    depth: usize,
    first_row: u64,
}

impl TreeNode {
    fn read<K, A>(self, file: &File) -> Result<TreeBlock<K, A>, Error>
    where
        K: Rkyv,
        A: Rkyv,
    {
        match self.node_type {
            NodeType::Data => Ok(TreeBlock::Data(DataBlock::new(file, &self)?)),
            NodeType::Index => Ok(TreeBlock::Index(IndexBlock::new(file, &self)?)),
        }
    }
}

enum TreeBlock<K, A> {
    Data(DataBlock<K, A>),
    Index(IndexBlock<K>),
}

impl<K, A> TreeBlock<K, A>
where
    K: Rkyv,
    A: Rkyv,
{
    fn lookup_row(&self, row: u64) -> Result<Option<TreeNode>, Error> {
        match self {
            Self::Data(data_block) => {
                if data_block.rows().contains(&row) {
                    return Ok(None);
                }
            }
            Self::Index(index_block) => {
                if let Some(child_node) = index_block.get_child_by_row(row) {
                    return Ok(Some(child_node));
                }
            }
        }
        Err(CorruptionError::MissingRow(row).into())
    }
}

struct IndexBlock<K> {
    raw: Rc<AlignedVec>,
    child_type: NodeType,
    bounds: VarintReader,
    row_totals: VarintReader,
    child_pointers: VarintReader,
    depth: usize,
    first_row: u64,
    _phantom: PhantomData<fn() -> K>,
}

impl<K> Clone for IndexBlock<K> {
    fn clone(&self) -> Self {
        Self {
            raw: self.raw.clone(),
            child_type: self.child_type,
            bounds: self.bounds.clone(),
            row_totals: self.row_totals.clone(),
            child_pointers: self.child_pointers.clone(),
            depth: self.depth,
            first_row: self.first_row,
            _phantom: PhantomData,
        }
    }
}

impl<K> IndexBlock<K>
where
    K: Rkyv,
{
    fn new(file: &File, node: &TreeNode) -> Result<Self, Error> {
        if node.depth > 64 {
            // A depth of 64 (very deep) with a branching factor of 2 (very
            // small) would allow for over `2**64` items.  A deeper file is a
            // bug or a memory exhaustion attack.
            return Err(CorruptionError::TooDeep(node.depth).into());
        }

        let raw = read_block(file, node.location)?;
        let header = IndexBlockHeader::read_le(&mut io::Cursor::new(&raw))?;
        if header.n_children == 0 {
            return Err(CorruptionError::EmptyIndex(node.location).into());
        }

        Ok(Self {
            child_type: header.child_type,
            bounds: VarintReader::new(
                &raw,
                header.bound_map_varint,
                header.bound_map_offset as usize,
                header.n_children as usize * 2,
            )?,
            row_totals: VarintReader::new(
                &raw,
                header.row_total_varint,
                header.row_totals_offset as usize,
                header.n_children as usize,
            )?,
            child_pointers: VarintReader::new(
                &raw,
                header.child_pointer_varint,
                header.child_pointers_offset as usize,
                header.n_children as usize,
            )?,
            raw: Rc::new(raw),
            depth: node.depth,
            first_row: node.first_row,
            _phantom: PhantomData,
        })
    }

    fn get_child(&self, index: usize) -> TreeNode {
        TreeNode {
            location: self.child_pointers.get(&self.raw, index).into(),
            node_type: self.child_type,
            depth: self.depth + 1,
            first_row: self.get_rows(index).start,
        }
    }

    fn get_child_by_row(&self, row: u64) -> Option<TreeNode> {
        self.find_row(row)
            .map(|child_idx| self.get_child(child_idx))
    }

    fn get_rows(&self, index: usize) -> Range<u64> {
        let low = if index == 0 {
            0
        } else {
            self.row_totals.get(&self.raw, index - 1)
        };
        let high = self.row_totals.get(&self.raw, index);
        (self.first_row + low)..(self.first_row + high)
    }

    fn get_row_bound(&self, index: usize) -> u64 {
        if index == 0 {
            0
        } else if index % 2 == 1 {
            self.row_totals.get(&self.raw, index / 2) - 1
        } else {
            self.row_totals.get(&self.raw, index / 2 - 1)
        }
    }

    fn find_row(&self, row: u64) -> Option<usize> {
        let mut indexes = 0..self.n_children();
        while !indexes.is_empty() {
            let mid = (indexes.start + indexes.end) / 2;
            let rows = self.get_rows(mid);
            if row < rows.start {
                indexes.end = mid;
            } else if row >= rows.end {
                indexes.start = mid + 1;
            } else {
                return Some(mid);
            }
        }
        None
    }

    unsafe fn get_bound(&self, index: usize) -> K {
        let offset = self.bounds.get(&self.raw, index) as usize;
        let archived = archived_value::<K>(&self.raw, offset);
        archived.deserialize(&mut Infallible).unwrap()
    }

    unsafe fn first_ge<C>(&self, target_rows: &Range<u64>, compare: &C) -> Option<usize>
    where
        C: Fn(&K) -> Ordering,
    {
        let mut start = 0;
        let mut end = self.n_children() * 2;
        let mut best = None;
        while start < end {
            let mid = (start + end) / 2;
            let row = self.get_row_bound(mid);
            let cmp = range_compare(target_rows, row);
            match cmp {
                Ordering::Equal => {
                    let bound = self.get_bound(mid);
                    match compare(&bound) {
                        Ordering::Less => {
                            best = Some(mid / 2);
                            end = mid;
                        }
                        Ordering::Equal => return Some(mid / 2),
                        Ordering::Greater => start = mid + 1,
                    };
                }
                Ordering::Less => {
                    best = Some(mid / 2);
                    end = mid;
                }
                Ordering::Greater => start = mid + 1,
            };
        }
        best
    }

    unsafe fn last_le<C>(&self, target_rows: &Range<u64>, compare: &C) -> Option<usize>
    where
        C: Fn(&K) -> Ordering,
    {
        let mut start = 0;
        let mut end = self.n_children() * 2;
        let mut best = None;
        while start < end {
            let mid = (start + end) / 2;
            let row = self.get_row_bound(mid);
            let cmp = range_compare(target_rows, row);
            match cmp {
                Ordering::Equal => {
                    let bound = self.get_bound(mid);
                    match compare(&bound) {
                        Ordering::Less => end = mid,
                        Ordering::Equal => return Some(mid / 2),
                        Ordering::Greater => {
                            best = Some(mid / 2);
                            start = mid + 1
                        }
                    };
                }
                Ordering::Less => {
                    end = mid;
                }
                Ordering::Greater => {                    best = Some(mid / 2);
start = mid + 1},
            };
        }
        best
    }

    fn n_children(&self) -> usize {
        self.child_pointers.count
    }

    #[allow(unused)]
    fn dbg<W>(&self, mut f: W) -> IoResult<()>
    where
        K: Debug,
        W: Write,
    {
        write!(
            f,
            "IndexBlock {{ depth: {}, first_row: {}, child_type: {:?}, children = {{",
            self.depth, self.first_row, self.child_type
        )?;
        for i in 0..self.n_children() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(
                f,
                " [{i}] = {{ rows: {:?}, bounds: {:?} at {}..={:?} at {} }}",
                self.get_rows(i),
                unsafe { self.get_bound(i * 2) },
                self.bounds.get(&self.raw, i * 2),
                unsafe { self.get_bound(i * 2 + 1) },
                self.bounds.get(&self.raw, i * 2 + 1),
            )?;
        }
        write!(f, " }}")
    }
}

struct Column {
    root: Option<TreeNode>,
    n_rows: u64,
}

impl Column {
    fn new(info: &FileTrailerColumn) -> Result<Self, Error> {
        let root = if info.n_rows != 0 {
            Some(TreeNode {
                location: BlockLocation::new(info.index_offset, info.index_size as usize)?,
                node_type: NodeType::Index,
                depth: 0,
                first_row: 0,
            })
        } else {
            None
        };
        Ok(Self {
            root,
            n_rows: info.n_rows,
        })
    }
    fn empty() -> Self {
        Self {
            root: None,
            n_rows: 0,
        }
    }
}

struct ReaderInner<T> {
    file: File,
    columns: Vec<Column>,

    /// `fn() -> T` is `Send` and `Sync` regardless of `T`.  See
    /// <https://doc.rust-lang.org/nomicon/phantom-data.html>.
    _phantom: PhantomData<fn() -> T>,
}

fn read_block(file: &File, location: BlockLocation) -> Result<AlignedVec, Error> {
    let mut block = AlignedVec::with_capacity(location.size);
    // XXX This zeros the whole buffer before reading into it.
    block.resize(location.size, 0);
    file.read_exact_at(block.as_mut_slice(), location.offset)?;
    let computed_checksum = crc32c(&block[4..]);
    let checksum = u32::from_le_bytes(block[..4].try_into().unwrap());
    if checksum != computed_checksum {
        return Err(CorruptionError::InvalidChecksum {
            location,
            checksum,
            computed_checksum,
        }
        .into());
    }
    Ok(block)
}

fn check_version_number(version: u32) -> Result<(), Error> {
    if version != VERSION_NUMBER {
        return Err(CorruptionError::InvalidVersion {
            version,
            expected_version: VERSION_NUMBER,
        }
        .into());
    }
    Ok(())
}

/// Layer file column specification.
///
/// A column specification must take the form `K1, A1, N1`, where `(K1, A1)` is
/// the first column's key and auxiliary data types.  If there is only one
/// column, `N1` is `()`; otherwise, it is `(K2, A2, N2)`, where `(K2, A2)` is
/// the second column's key and auxiliary data types.  If there are only two
/// columns, `N2` is `()`, otherwise it is `(K3, A3, N3)`; and so on.  Thus:
///
/// * For one column, `T` is `(K1, A1, ())`.
///
/// * For two columns, `T` is `(K1, A1, (K2, A2, ()))`.
///
/// * For three columns, `T` is `(K1, A1, (K2, A2, (K3, A3, ())))`.
pub trait ColumnSpec {
    fn count() -> usize;
}
impl ColumnSpec for () {
    fn count() -> usize {
        0
    }
}
impl<K, A, N> ColumnSpec for (K, A, N)
where
    K: Rkyv,
    A: Rkyv,
    N: ColumnSpec,
{
    fn count() -> usize {
        1 + N::count()
    }
}

/// Layer file reader.
#[derive(Clone)]
pub struct Reader<T>(Arc<ReaderInner<T>>);

impl<T> Reader<T>
where
    T: ColumnSpec,
{
    pub fn new(mut file: File) -> Result<Self, Error> {
        let file_size = file.seek(SeekFrom::End(0))?;
        if file_size == 0 || file_size % 4096 != 0 {
            return Err(CorruptionError::InvalidFileSize(file_size).into());
        }

        let file_header_block = read_block(&mut file, BlockLocation::new(0, 4096).unwrap())?;
        let file_header = FileHeader::read_le(&mut io::Cursor::new(&file_header_block))?;
        check_version_number(file_header.version)?;

        let file_trailer_block = read_block(
            &mut file,
            BlockLocation::new(file_size - 4096, 4096).unwrap(),
        )?;
        let file_trailer = FileTrailer::read_le(&mut io::Cursor::new(&file_trailer_block))?;
        check_version_number(file_trailer.version)?;

        let columns: Vec<_> = file_trailer
            .columns
            .iter()
            .map(Column::new)
            .collect::<Result<_, _>>()?;
        if columns.is_empty() {
            return Err(CorruptionError::NoColumns.into());
        }
        if columns.len() != T::count() {
            return Err(Error::WrongNumberOfColumns {
                actual: columns.len(),
                expected: T::count(),
            });
        }
        if columns[0].n_rows > 0 {
            for (column_index, column) in columns.iter().enumerate().skip(1) {
                if column.n_rows == 0 {
                    return Err(CorruptionError::UnexpectedlyEmptyColumn(column_index).into());
                }
            }
        }

        Ok(Self(Arc::new(ReaderInner {
            file,
            columns,
            _phantom: PhantomData,
        })))
    }

    pub fn empty(n_columns: usize) -> Result<Self, Error> {
        Ok(Self(Arc::new(ReaderInner {
            file: tempfile()?,
            columns: (0..n_columns).map(|_| Column::empty()).collect(),
            _phantom: PhantomData,
        })))
    }

    pub fn equal(&self, other: &Self) -> Option<bool> {
        if Arc::ptr_eq(&self.0, &other.0) {
            // Definitely the same.
            Some(true)
        } else if self.0.columns.len() != other.0.columns.len() {
            // Definitely different.
            Some(false)
        } else if let Some(true) = is_same_file(&self.0.file, &other.0.file) {
            // Definitely the same.
            Some(true)
        } else {
            // Who knows?
            None
        }
    }

    pub fn n_columns(&self) -> usize {
        self.0.columns.len()
    }

    pub fn n_rows(&self, column: usize) -> u64 {
        self.0.columns[column].n_rows
    }
}

impl<K, A, N> Reader<(K, A, N)>
where
    K: Rkyv,
    A: Rkyv,
    (K, A, N): ColumnSpec,
{
    pub fn rows(&self) -> RowGroup<K, A, N, (K, A, N)> {
        RowGroup::new(self, 0, 0..self.0.columns[0].n_rows)
    }
}

fn is_same_file(file1: &File, file2: &File) -> Option<bool> {
    #[cfg(unix)]
    if let (Ok(md1), Ok(md2)) = (file1.metadata(), file2.metadata()) {
        use std::os::unix::fs::MetadataExt;
        Some(md1.dev() == md2.dev() && md1.ino() == md2.ino())
    } else {
        None
    }
}

#[derive(Clone)]
pub struct RowGroup<'a, K, A, N, T>
where
    K: Rkyv,
    A: Rkyv,
    T: ColumnSpec,
{
    reader: &'a Reader<T>,
    column: usize,
    rows: Range<u64>,
    _phantom: PhantomData<(K, A, N)>,
}

impl<'a, K, A, N, T> Debug for RowGroup<'a, K, A, N, T>
where
    K: Rkyv,
    A: Rkyv,
    T: ColumnSpec,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "RowGroup(column={}, rows={:?})", self.column, self.rows)
    }
}

impl<'a, K, A, N, T> RowGroup<'a, K, A, N, T>
where
    K: Rkyv,
    A: Rkyv,
    T: ColumnSpec,
{
    fn new(reader: &'a Reader<T>, column: usize, rows: Range<u64>) -> Self {
        Self {
            reader,
            column,
            rows,
            _phantom: PhantomData,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn len(&self) -> u64 {
        self.rows.end - self.rows.start
    }

    pub fn before(self) -> Cursor<'a, K, A, N, T> {
        Cursor::<K, A, N, T>::new(self, Position::Before)
    }

    pub fn after(self) -> Cursor<'a, K, A, N, T> {
        Cursor::<K, A, N, T>::new(self, Position::After)
    }

    pub fn first(self) -> Result<Cursor<'a, K, A, N, T>, Error> {
        let position = if self.is_empty() {
            Position::After
        } else {
            Position::for_row(&self, self.rows.start)?
        };
        Ok(Cursor::<K, A, N, T>::new(self, position))
    }

    pub fn last(self) -> Result<Cursor<'a, K, A, N, T>, Error> {
        let position = if self.is_empty() {
            Position::After
        } else {
            Position::for_row(&self, self.rows.end - 1)?
        };
        Ok(Cursor::<K, A, N, T>::new(self, position))
    }

    pub fn nth(self, row: u64) -> Result<Cursor<'a, K, A, N, T>, Error> {
        let position = if row < self.len() {
            Position::for_row(&self, self.rows.start + row)?
        } else {
            Position::After
        };
        Ok(Cursor::<K, A, N, T>::new(self, position))
    }

    pub fn subset(mut self, subset: Range<u64>) -> Self {
        assert!(subset.end <= self.len());
        self.rows.start += subset.start;
        self.rows.end = self.rows.start + (subset.end - subset.start);
        self
    }
}

/// A cursor for a layer file.
///
/// A cursor can be positioned on a particular value or before or after all the
/// values.
#[derive(Clone)]
pub struct Cursor<'a, K, A, N, T>
where
    K: Rkyv,
    A: Rkyv,
    T: ColumnSpec,
{
    row_group: RowGroup<'a, K, A, N, T>,
    position: Position<K, A>,
}

impl<'a, K, A, N, T> Debug for Cursor<'a, K, A, N, T>
where
    K: Rkyv + Debug,
    A: Rkyv + Debug,
    T: ColumnSpec,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "Cursor({:?}, {:?})", self.row_group, self.position)
    }
}

impl<'a, K, A, N, T> Cursor<'a, K, A, N, T>
where
    K: Rkyv,
    A: Rkyv,
    T: ColumnSpec,
{
    fn new(row_group: RowGroup<'a, K, A, N, T>, position: Position<K, A>) -> Self {
        Self {
            row_group,
            position,
        }
    }

    pub fn move_next(&mut self) -> Result<(), Error> {
        self.position.next(&self.row_group)?;
        Ok(())
    }

    pub fn move_prev(&mut self) -> Result<(), Error> {
        self.position.prev(&self.row_group)?;
        Ok(())
    }

    pub fn move_first(&mut self) -> Result<(), Error> {
        self.position
            .move_to_row(&self.row_group, self.row_group.rows.start)
    }

    pub fn move_last(&mut self) -> Result<(), Error> {
        if !self.row_group.is_empty() {
            self.position
                .move_to_row(&self.row_group, self.row_group.rows.end - 1)
        } else {
            self.position = Position::After;
            Ok(())
        }
    }

    pub fn move_to_row(&mut self, row: u64) -> Result<(), Error> {
        if row < self.row_group.rows.end - self.row_group.rows.start {
            self.position
                .move_to_row(&self.row_group, self.row_group.rows.start + row)
        } else {
            self.position = Position::After;
            Ok(())
        }
    }

    pub unsafe fn key(&self) -> Option<K> {
        self.position.key()
    }

    pub unsafe fn item(&self) -> Option<(K, A)> {
        self.position.item()
    }

    pub fn has_value(&self) -> bool {
        self.position.has_value()
    }

    // Only meaningful in column 0.
    pub const fn bounds(&self) -> &Range<u64> {
        &self.row_group.rows
    }

    pub fn n_rows(&self) -> u64 {
        self.row_group.len()
    }

    pub fn position(&self) -> u64 {
        self.position.position(&self.row_group)
    }

    pub fn remaining_rows(&self) -> u64 {
        self.position.remaining_rows(&self.row_group)
    }

    pub unsafe fn seek_forward_until<P>(&mut self, predicate: P) -> Result<(), Error>
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.advance_to_first_ge(&|key| {
            if predicate(key) {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        })
    }
    pub unsafe fn advance_to_value_or_larger(&mut self, target: &K) -> Result<(), Error>
    where
        K: Ord,
    {
        self.advance_to_first_ge(&|key| target.cmp(key))
    }
    pub unsafe fn advance_to_first_ge<C>(&mut self, compare: &C) -> Result<(), Error>
    where
        C: Fn(&K) -> Ordering,
    {
        // XXX optimization possibilities here
        let position = Position::first_ge::<N, T, _>(&self.row_group, compare)?;
        if position > self.position {
            self.position = position;
        }
        Ok(())
    }

    pub unsafe fn seek_backward_until<P>(&mut self, predicate: P) -> Result<(), Error>
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.rewind_to_last_le(&|key| {
            if !predicate(key) {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        })
    }
    pub unsafe fn rewind_to_value_or_smaller(&mut self, target: &K) -> Result<(), Error>
    where
        K: Ord,
    {
        self.rewind_to_last_le(&|key| target.cmp(key))
    }
    pub unsafe fn rewind_to_last_le<C>(&mut self, compare: &C) -> Result<(), Error>
    where
        C: Fn(&K) -> Ordering,
    {
        // XXX optimization possibilities here
        let position = Position::last_le::<N, T, _>(&self.row_group, compare)?;
        if position < self.position {
            self.position = position;
        }
        Ok(())
    }
}

impl<'a, K, A, NK, NA, NN, T> Cursor<'a, K, A, (NK, NA, NN), T>
where
    K: Rkyv,
    A: Rkyv,
    NK: Rkyv,
    NA: Rkyv,
    T: ColumnSpec,
{
    pub fn next_column<'b>(&'b self) -> RowGroup<'a, NK, NA, NN, T> {
        RowGroup::new(
            self.row_group.reader,
            self.row_group.column + 1,
            self.position.row_group(),
        )
    }
}

struct Path<K, A> {
    row: u64,
    indexes: Vec<IndexBlock<K>>,
    data: DataBlock<K, A>,
}

impl<K, A> PartialEq for Path<K, A> {
    fn eq(&self, other: &Self) -> bool {
        self.row == other.row
    }
}

impl<K, A> Eq for Path<K, A> {}

impl<K, A> PartialOrd for Path<K, A> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, A> Ord for Path<K, A> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.row.cmp(&other.row)
    }
}

impl<K, A> Clone for Path<K, A> {
    fn clone(&self) -> Self {
        Self {
            row: self.row,
            indexes: self.indexes.clone(),
            data: self.data.clone(),
        }
    }
}

impl<K, A> Path<K, A>
where
    K: Rkyv,
    A: Rkyv,
{
    fn for_row<N, T>(row_group: &RowGroup<K, A, N, T>, row: u64) -> Result<Self, Error>
    where
        T: ColumnSpec,
    {
        Self::for_row_from_ancestor(
            row_group.reader,
            Vec::new(),
            row_group.reader.0.columns[row_group.column].root.unwrap(),
            row,
        )
    }
    fn for_row_from_ancestor<T>(
        reader: &Reader<T>,
        mut indexes: Vec<IndexBlock<K>>,
        mut node: TreeNode,
        row: u64,
    ) -> Result<Self, Error> {
        loop {
            let block = node.read(&reader.0.file)?;
            let next = block.lookup_row(row)?;
            match block {
                TreeBlock::Data(data) => return Ok(Self { row, indexes, data }),
                TreeBlock::Index(index) => indexes.push(index),
            };
            node = next.unwrap();
        }
    }
    fn for_row_from_hint<T>(reader: &Reader<T>, hint: &Self, row: u64) -> Result<Self, Error> {
        if hint.data.rows().contains(&row) {
            return Ok(Self {
                row,
                ..hint.clone()
            });
        }
        for (idx, index_block) in hint.indexes.iter().enumerate().rev() {
            if let Some(node) = index_block.get_child_by_row(row) {
                return Self::for_row_from_ancestor(
                    reader,
                    hint.indexes[0..=idx].iter().cloned().collect(),
                    node,
                    row,
                );
            }
        }
        Err(CorruptionError::MissingRow(row).into())
    }
    unsafe fn key<'a>(&'a self) -> K {
        self.data.key_for_row(self.row)
    }
    unsafe fn item<'a>(&'a self) -> (K, A) {
        self.data.item_for_row(self.row)
    }
    fn get_row_group(&self) -> Range<u64> {
        self.data.get_row_group(self.row).unwrap()
    }
    fn move_to_row<T>(&mut self, reader: &Reader<T>, row: u64) -> Result<(), Error> {
        if self.data.rows().contains(&row) {
            self.row = row;
        } else {
            *self = Self::for_row_from_hint(reader, self, row)?;
        }
        Ok(())
    }
    unsafe fn last_le<N, T, C>(
        row_group: &RowGroup<K, A, N, T>,
        compare: &C,
    ) -> Result<Option<Self>, Error>
    where
        T: ColumnSpec,
        C: Fn(&K) -> Ordering,
    {
        let mut indexes = Vec::new();
        let Some(mut node) = row_group.reader.0.columns[row_group.column].root else {
            return Ok(None);
        };
        loop {
            match node.read(&row_group.reader.0.file)? {
                TreeBlock::Index(index_block) => {
                    let Some(child_idx) = index_block.last_le(&row_group.rows, compare) else {
                        return Ok(None);
                    };
                    node = index_block.get_child(child_idx);
                    indexes.push(index_block);
                }
                TreeBlock::Data(data_block) => {
                    let Some(child_idx) = data_block.last_le(&row_group.rows, compare) else {
                        return Ok(None);
                    };
                    return Ok(Some(Self {
                        row: data_block.first_row + child_idx as u64,
                        indexes,
                        data: data_block,
                    }));
                }
            }
        }
    }
    unsafe fn first_ge<N, T, C>(
        row_group: &RowGroup<K, A, N, T>,
        compare: &C,
    ) -> Result<Option<Self>, Error>
    where
        T: ColumnSpec,
        C: Fn(&K) -> Ordering,
    {
        let mut indexes = Vec::new();
        let Some(mut node) = row_group.reader.0.columns[row_group.column].root else {
            return Ok(None);
        };
        loop {
            match node.read(&row_group.reader.0.file)? {
                TreeBlock::Index(index_block) => {
                    let Some(child_idx) = index_block.first_ge(&row_group.rows, compare) else {
                        return Ok(None);
                    };
                    node = index_block.get_child(child_idx);
                    indexes.push(index_block);
                }
                TreeBlock::Data(data_block) => {
                    let Some(child_idx) = data_block.first_ge(&row_group.rows, compare) else {
                        return Ok(None);
                    };
                    return Ok(Some(Self {
                        row: data_block.first_row + child_idx as u64,
                        indexes,
                        data: data_block,
                    }));
                }
            }
        }
    }
}

impl<K, A> Debug for Path<K, A>
where
    K: Rkyv,
    A: Rkyv,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "Path {{ row: {}, indexes:", self.row)?;
        for index in &self.indexes {
            write!(
                f,
                " [child {:?} of {}]",
                index.find_row(self.row),
                index.n_children()
            )?;
        }
        write!(
            f,
            ", data: [row {} of {}] }}",
            self.row - self.data.first_row,
            self.data.n_values()
        )
    }
}

#[derive(Clone, Debug)]
enum Position<K, A>
where
    K: Rkyv,
    A: Rkyv,
{
    Before,
    Row(Path<K, A>),
    After,
}

impl<K, A> PartialEq for Position<K, A>
where
    K: Rkyv,
    A: Rkyv,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Before, Self::Before) => true,
            (Self::Row(a), Self::Row(b)) => a.eq(b),
            (Self::After, Self::After) => true,
            _ => false,
        }
    }
}

impl<K, A> Eq for Position<K, A>
where
    K: Rkyv,
    A: Rkyv,
{
}

impl<K, A> PartialOrd for Position<K, A>
where
    K: Rkyv,
    A: Rkyv,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, A> Ord for Position<K, A>
where
    K: Rkyv,
    A: Rkyv,
{
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Before, Self::Before) => Ordering::Equal,
            (Self::Before, Self::Row(_)) => Ordering::Less,
            (Self::Before, Self::After) => Ordering::Less,

            (Self::Row(_), Self::Before) => Ordering::Greater,
            (Self::Row(a), Self::Row(b)) => a.cmp(b),
            (Self::Row(_), Self::After) => Ordering::Less,

            (Self::After, Self::Before) => Ordering::Greater,
            (Self::After, Self::Row(_)) => Ordering::Greater,
            (Self::After, Self::After) => Ordering::Equal,
        }
    }
}

impl<K, A> Position<K, A>
where
    K: Rkyv,
    A: Rkyv,
{
    fn for_row<N, T>(row_group: &RowGroup<K, A, N, T>, row: u64) -> Result<Self, Error>
    where
        T: ColumnSpec,
    {
        Ok(Self::Row(Path::for_row(row_group, row)?))
    }
    fn next<N, T>(&mut self, row_group: &RowGroup<K, A, N, T>) -> Result<(), Error>
    where
        T: ColumnSpec,
    {
        match self {
            Self::Before => {
                *self = Self::Row(Path::for_row(row_group, row_group.rows.start)?);
            }
            Self::Row(path) => {
                let row = path.row + 1;
                if row < row_group.rows.end {
                    path.move_to_row(row_group.reader, row)?;
                } else {
                    *self = Self::After;
                }
            }
            Self::After => (),
        }
        Ok(())
    }
    fn prev<N, T>(&mut self, row_group: &RowGroup<K, A, N, T>) -> Result<(), Error>
    where
        T: ColumnSpec,
    {
        match self {
            Self::Before => (),
            Self::Row(path) => {
                if path.row > row_group.rows.start {
                    path.move_to_row(row_group.reader, path.row - 1)?;
                } else {
                    *self = Self::Before;
                }
            }
            Self::After => {
                *self = Self::Row(Path::for_row(row_group, row_group.rows.end - 1)?);
            }
        }
        Ok(())
    }
    fn move_to_row<N, T>(&mut self, row_group: &RowGroup<K, A, N, T>, row: u64) -> Result<(), Error>
    where
        T: ColumnSpec,
    {
        if !row_group.rows.is_empty() {
            match self {
                Position::Before | Position::After => {
                    *self = Self::Row(Path::for_row(row_group, row)?)
                }
                Position::Row(path) => path.move_to_row(row_group.reader, row)?,
            }
        }
        Ok(())
    }
    fn path(&self) -> Option<&Path<K, A>> {
        match self {
            Position::Before => None,
            Position::Row(path) => Some(path),
            Position::After => None,
        }
    }
    pub unsafe fn key(&self) -> Option<K> {
        self.path().map(|path| path.key())
    }
    pub unsafe fn item(&self) -> Option<(K, A)> {
        self.path().map(|path| path.item())
    }
    pub fn row_group(&self) -> Range<u64> {
        match self.path() {
            Some(path) => path.get_row_group(),
            None => 0..0,
        }
    }
    fn has_value(&self) -> bool {
        self.path().is_some()
    }
    unsafe fn first_ge<N, T, C>(
        row_group: &RowGroup<K, A, N, T>,
        compare: &C,
    ) -> Result<Self, Error>
    where
        T: ColumnSpec,
        C: Fn(&K) -> Ordering,
    {
        match Path::first_ge(row_group, compare)? {
            Some(path) => Ok(Position::Row(path)),
            None => Ok(Position::After),
        }
    }
    unsafe fn last_le<N, T, C>(row_group: &RowGroup<K, A, N, T>, compare: &C) -> Result<Self, Error>
    where
        T: ColumnSpec,
        C: Fn(&K) -> Ordering,
    {
        match Path::last_le(row_group, compare)? {
            Some(path) => Ok(Position::Row(path)),
            None => Ok(Position::After),
        }
    }
    fn position<N, T>(&self, row_group: &RowGroup<K, A, N, T>) -> u64
    where
        T: ColumnSpec,
    {
        match self {
            Position::Before => 0,
            Position::Row(path) => path.row,
            Position::After => row_group.rows.end,
        }
    }
    fn remaining_rows<N, T>(&self, row_group: &RowGroup<K, A, N, T>) -> u64
    where
        T: ColumnSpec,
    {
        match self {
            Position::Before => row_group.len(),
            Position::Row(path) => path.row - row_group.rows.start,
            Position::After => 0,
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;

    use super::Reader;

    #[test]
    fn read_file() {
        let reader = Reader::<(i64, (), ())>::new(File::open("file.layer").unwrap()).unwrap();
        let mut cursor = reader.rows().first().unwrap();
        let mut count = 0;
        unsafe { cursor.advance_to_value_or_larger(&997).unwrap() };
        while let Some(value) = unsafe { cursor.key() } {
            println!("{value}");
            count += 1;
            cursor.move_next().unwrap();
        }
        println!("count={count}");
    }

    #[test]
    fn read_tuple() {
        let reader =
            Reader::<((i32, char), i32, ())>::new(File::open("file.layer").unwrap()).unwrap();
        let mut cursor = reader.rows().first().unwrap();
        let mut count = 0;
        while let Some(value) = unsafe { cursor.key() } {
            let number = value.0;
            let c = value.1;
            println!("({number}, {c:?})");
            count += 1;
            cursor.move_next().unwrap();
        }
        println!("count={count}");
    }

    #[test]
    fn read_string() {
        let reader = Reader::<(String, (), ())>::new(File::open("file.layer").unwrap()).unwrap();
        let mut cursor = reader.rows().first().unwrap();
        let mut count = 0;
        unsafe { cursor.advance_to_value_or_larger(&format!("01 1")).unwrap() };
        while let Some(value) = unsafe { cursor.key() } {
            println!("'{value}'");
            count += 1;
            cursor.move_next().unwrap();
        }
        println!("count={count}");
    }

    #[test]
    fn read_2_layers() {
        let reader =
            Reader::<(u32, (), (u32, (), ()))>::new(File::open("file.layer").unwrap()).unwrap();
        let mut keys = reader.rows().first().unwrap();
        let mut count = 0;
        let mut count2 = 0;
        while let Some(key) = unsafe { keys.key() } {
            println!("{key}");
            count += 1;
            let mut values = keys.next_column().last().unwrap();
            unsafe { values.rewind_to_value_or_smaller(&3).unwrap() };
            while let Some(value) = unsafe { values.key() } {
                count2 += 1;
                println!("\t{value}");
                values.move_prev().unwrap();
            }
            keys.move_next().unwrap();
            //break;
        }
        println!("count={count} count2={count2}");
    }
}
