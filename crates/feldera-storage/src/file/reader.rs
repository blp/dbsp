//! Layer file reader.
//!
//! [`Reader`] is the top-level interface for reading layer files.

use std::{
    cmp::Ordering::{self, *},
    fmt::{Debug, Formatter, Result as FmtResult},
    fs::File,
    marker::PhantomData,
    ops::Range,
    os::unix::fs::FileExt,
    rc::Rc,
    sync::Arc,
};

use binrw::{
    io::{self, Error as IoError, Seek, SeekFrom},
    BinRead, Error as BinError,
};
use crc32c::crc32c;
use rkyv::{archived_value, AlignedVec, Deserialize, Infallible};
use tempfile::tempfile;
use thiserror::Error as ThisError;

use crate::file::Item;

use super::{
    ArchivedItem, BlockLocation, DataBlockHeader, FileHeader, FileTrailer, FileTrailerColumn,
    IndexBlockHeader, NodeType, Rkyv, Varint, VERSION_NUMBER,
};

/// Any kind of error encountered reading a layer file.
#[derive(ThisError, Debug)]
pub enum Error {
    /// Errors that indicate a problem with the layer file contents.
    #[error("Corrupt layer file: {0}")]
    Corruption(#[from] CorruptionError),

    /// Errors reading the layer file.
    #[error("I/O error: {0}")]
    Io(#[from] IoError),

    #[allow(missing_docs)]
    #[error("File has {actual} column(s) but should have {expected}.")]
    WrongNumberOfColumns { actual: usize, expected: usize },
}

impl From<BinError> for Error {
    fn from(source: BinError) -> Self {
        Error::Corruption(source.into())
    }
}

/// Errors that indicate a problem with the layer file contents.
#[allow(missing_docs)]
#[derive(ThisError, Debug)]
pub enum CorruptionError {
    #[error("File size {0} must be a positive multiple of 4096")]
    InvalidFileSize(u64),

    #[error("{size}-byte block at offset {offset} has invalid checksum {checksum:#x} (expected {computed_checksum:#})")]
    InvalidChecksum {
        offset: u64,
        size: usize,
        checksum: u32,
        computed_checksum: u32,
    },

    #[error("File has invalid version {version} (expected {expected_version})")]
    InvalidVersion { version: u32, expected_version: u32 },

    #[error("Binary read/write error: {0}")]
    Binrw(#[from] BinError),

    #[error("{count}-element array of {each}-byte elements starting at offset {offset} within block overflows {block_size}-byte block")]
    InvalidArray {
        block_size: usize,
        offset: usize,
        count: usize,
        each: usize,
    },

    #[error("{count} strides of {stride} bytes each starting at offset {start} overflows {block_size}-byte block")]
    InvalidStride {
        block_size: usize,
        start: usize,
        stride: usize,
        count: usize,
    },

    #[error("Index nesting depth {0} exceeds maximum.")]
    TooDeep(usize),

    #[error("File has no columns.")]
    NoColumns,

    #[error("{size}-byte index block at offset {offset} is empty")]
    EmptyIndex { offset: u64, size: usize },

    #[error("{size}-byte index or data block at offset {offset} contains {n_rows} rows but {expected_rows} were expected.")]
    WrongNumberOfRows {
        offset: u64,
        size: usize,
        n_rows: u64,
        expected_rows: u64,
    },

    #[error("{size}-byte index block at offset {offset} has nonmonotonic row totals ({prev} then {next}).")]
    NonmonotonicIndex {
        offset: u64,
        size: usize,
        prev: u64,
        next: u64,
    },

    #[error("Column {0} is empty even though column 0 was not empty")]
    UnexpectedlyEmptyColumn(usize),

    #[error("Unexpectedly missing row {0} in column 1 (or later)")]
    MissingRow(u64),

    #[error("{index_size}-byte index block at offset {index_offset} has child pointer {index} with invalid offset {child_offset} or size {child_size}.")]
    InvalidChildPointer {
        index_offset: u64,
        index_size: usize,
        index: usize,
        child_offset: u64,
        child_size: usize,
    },

    #[error("File trailer column specification has invalid index offset {index_offset} or size {index_size}.")]
    InvalidIndexRoot { index_offset: u64, index_size: u32 },

    #[error("Row group {index} in {size}-byte data block at offset {offset} has invalid row range {start}..{end}.")]
    InvalidRowGroup {
        offset: u64,
        size: usize,
        index: usize,
        start: u64,
        end: u64,
    },
}

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
            .and_then(|len| len.checked_add(start))
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
                .and_then(|len| len.checked_add(start))
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
    location: BlockLocation,
    raw: Rc<AlignedVec>,
    value_map: ValueMapReader,
    row_groups: Option<VarintReader>,
    first_row: u64,
    _phantom: PhantomData<(K, A)>,
}

impl<K, A> Clone for DataBlock<K, A> {
    fn clone(&self) -> Self {
        Self {
            location: self.location,
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
        let expected_rows = node.rows.end - node.rows.start;
        if header.n_values as u64 != expected_rows {
            let BlockLocation { size, offset } = node.location;
            return Err(CorruptionError::WrongNumberOfRows {
                offset,
                size,
                n_rows: header.n_values as u64,
                expected_rows,
            }
            .into());
        }
        Ok(Self {
            location: node.location,
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
            first_row: node.rows.start,
            _phantom: PhantomData,
        })
    }
    fn n_values(&self) -> usize {
        self.value_map.len()
    }
    fn rows(&self) -> Range<u64> {
        self.first_row..(self.first_row + self.n_values() as u64)
    }
    fn row_group(&self, row: u64) -> Result<Range<u64>, Error> {
        let row_groups = self.row_groups.as_ref().unwrap();
        let index = (row - self.first_row) as usize;
        let start = row_groups.get(&self.raw, index);
        let end = row_groups.get(&self.raw, index + 1);
        if start < end {
            Ok(start..end)
        } else {
            Err(CorruptionError::InvalidRowGroup {
                offset: self.location.offset,
                size: self.location.size,
                index,
                start,
                end,
            }
            .into())
        }
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
        item.0.deserialize(&mut Infallible).unwrap()
    }
    unsafe fn key_for_row(&self, row: u64) -> K {
        let index = (row - self.first_row) as usize;
        self.key(index)
    }

    unsafe fn find_best_match<C>(
        &self,
        target_rows: &Range<u64>,
        compare: &C,
        bias: Ordering,
    ) -> Option<usize>
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
                Equal => {
                    let key = self.key(mid);
                    let cmp = compare(&key);
                    match cmp {
                        Less => end = mid,
                        Equal => return Some(mid),
                        Greater => start = mid + 1,
                    };
                    if cmp == bias {
                        best = Some(mid);
                    }
                }
                Less => end = mid,
                Greater => start = mid + 1,
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
        Greater
    } else if target >= range.end {
        Less
    } else {
        Equal
    }
}

#[derive(Clone)]
struct TreeNode {
    location: BlockLocation,
    node_type: NodeType,
    depth: usize,
    rows: Range<u64>,
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
                if let Some(child_node) = index_block.get_child_by_row(row)? {
                    return Ok(Some(child_node));
                }
            }
        }
        Err(CorruptionError::MissingRow(row).into())
    }
}

struct IndexBlock<K> {
    location: BlockLocation,
    raw: Rc<AlignedVec>,
    child_type: NodeType,
    bounds: VarintReader,
    row_totals: VarintReader,
    child_pointers: VarintReader,
    depth: usize,
    first_row: u64,
    _phantom: PhantomData<K>,
}

impl<K> Clone for IndexBlock<K> {
    fn clone(&self) -> Self {
        Self {
            location: self.location,
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
        let BlockLocation { size, offset } = node.location;
        if header.n_children == 0 {
            return Err(CorruptionError::EmptyIndex { size, offset }.into());
        }

        let row_totals = VarintReader::new(
            &raw,
            header.row_total_varint,
            header.row_totals_offset as usize,
            header.n_children as usize,
        )?;
        for i in 1..header.n_children as usize {
            let prev = row_totals.get(&raw, i - 1);
            let next = row_totals.get(&raw, i);
            if prev >= next {
                return Err(CorruptionError::NonmonotonicIndex {
                    size,
                    offset,
                    prev,
                    next,
                }
                .into());
            }
        }

        let expected_rows = node.rows.end - node.rows.start;
        let n_rows = row_totals.get(&raw, header.n_children as usize - 1);
        if n_rows != expected_rows {
            return Err(CorruptionError::WrongNumberOfRows {
                offset,
                size,
                n_rows,
                expected_rows,
            }
            .into());
        }

        Ok(Self {
            location: node.location,
            child_type: header.child_type,
            bounds: VarintReader::new(
                &raw,
                header.bound_map_varint,
                header.bound_map_offset as usize,
                header.n_children as usize * 2,
            )?,
            row_totals,
            child_pointers: VarintReader::new(
                &raw,
                header.child_pointer_varint,
                header.child_pointers_offset as usize,
                header.n_children as usize,
            )?,
            raw: Rc::new(raw),
            depth: node.depth,
            first_row: node.rows.start,
            _phantom: PhantomData,
        })
    }

    fn get_child(&self, index: usize) -> Result<TreeNode, Error> {
        Ok(TreeNode {
            location: match self.child_pointers.get(&self.raw, index).try_into() {
                Ok(location) => location,
                Err(error) => {
                    return Err(Error::Corruption(CorruptionError::InvalidChildPointer {
                        index_offset: self.location.offset,
                        index_size: self.location.size,
                        index,
                        child_offset: error.offset,
                        child_size: error.size,
                    }))
                }
            },
            node_type: self.child_type,
            depth: self.depth + 1,
            rows: self.get_rows(index),
        })
    }

    fn get_child_by_row(&self, row: u64) -> Result<Option<TreeNode>, Error> {
        self.find_row(row)
            .map(|child_idx| self.get_child(child_idx))
            .transpose()
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

    unsafe fn find_best_match<C>(
        &self,
        target_rows: &Range<u64>,
        compare: &C,
        bias: Ordering,
    ) -> Option<usize>
    where
        C: Fn(&K) -> Ordering,
    {
        let mut start = 0;
        let mut end = self.n_children() * 2;
        let mut best = None;
        while start < end {
            let mid = (start + end) / 2;
            let row = self.get_row_bound(mid);
            let cmp = match range_compare(target_rows, row) {
                Equal => {
                    let bound = self.get_bound(mid);
                    let cmp = compare(&bound);
                    if cmp == Equal {
                        return Some(mid / 2);
                    }
                    cmp
                }
                cmp => cmp,
            };
            if cmp == Less {
                end = mid
            } else {
                start = mid + 1
            };
            if bias == cmp {
                best = Some(mid / 2);
            }
        }
        best
    }

    fn n_children(&self) -> usize {
        self.child_pointers.count
    }
}

impl<K> Debug for IndexBlock<K>
where
    K: Rkyv + Debug,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
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
        let FileTrailerColumn {
            index_offset,
            index_size,
            n_rows,
        } = *info;
        let root = if n_rows != 0 {
            let location = match BlockLocation::new(index_offset, index_size as usize) {
                Ok(location) => location,
                Err(_) => {
                    return Err(Error::Corruption(CorruptionError::InvalidIndexRoot {
                        index_offset,
                        index_size,
                    }))
                }
            };
            Some(TreeNode {
                location,
                node_type: NodeType::Index,
                depth: 0,
                rows: 0..n_rows,
            })
        } else {
            None
        };
        Ok(Self { root, n_rows })
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
        let BlockLocation { size, offset } = location;
        return Err(CorruptionError::InvalidChecksum {
            size,
            offset,
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
    /// Returns the number of columns in this `ColumnSpec`.
    fn n_columns() -> usize;
}
impl ColumnSpec for () {
    fn n_columns() -> usize {
        0
    }
}
impl<K, A, N> ColumnSpec for (K, A, N)
where
    K: Rkyv,
    A: Rkyv,
    N: ColumnSpec,
{
    fn n_columns() -> usize {
        1 + N::n_columns()
    }
}

/// Layer file reader.
///
/// `T` in `Reader<T>` must be a [`ColumnSpec`] that specifies the key and
/// auxiliary data types for all of the columns in the file to be read.
#[derive(Clone)]
pub struct Reader<T>(Arc<ReaderInner<T>>);

impl<T> Reader<T>
where
    T: ColumnSpec,
{
    /// Creates and returns a new `Reader` for `file`.
    pub fn new(mut file: File) -> Result<Self, Error> {
        let file_size = file.seek(SeekFrom::End(0))?;
        if file_size == 0 || file_size % 4096 != 0 {
            return Err(CorruptionError::InvalidFileSize(file_size).into());
        }

        let file_header_block = read_block(&file, BlockLocation::new(0, 4096).unwrap())?;
        let file_header = FileHeader::read_le(&mut io::Cursor::new(&file_header_block))?;
        check_version_number(file_header.version)?;

        let file_trailer_block =
            read_block(&file, BlockLocation::new(file_size - 4096, 4096).unwrap())?;
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
        if columns.len() != T::n_columns() {
            return Err(Error::WrongNumberOfColumns {
                actual: columns.len(),
                expected: T::n_columns(),
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

    /// Create and returns a new `Reader` that has no rows.
    ///
    /// This internally creates an empty temporary file, which means that it can
    /// fail with an I/O error.
    pub fn empty() -> Result<Self, Error> {
        Ok(Self(Arc::new(ReaderInner {
            file: tempfile()?,
            columns: (0..T::n_columns()).map(|_| Column::empty()).collect(),
            _phantom: PhantomData,
        })))
    }

    /// The number of columns in the layer file.
    ///
    /// This is a fixed value for any given `Reader`.
    pub fn n_columns(&self) -> usize {
        T::n_columns()
    }

    /// The number of rows in the given `column`.
    ///
    /// For column 0, this is the number of rows that may be visited with
    /// [`rows`](Self::rows).  In other columns, it is the number of rows that
    /// may be visited in total by calling `next_column()` on each of the rows
    /// in the previous column.
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
    /// Returns a [`RowGroup`] for all of the rows in column 0.
    pub fn rows(&self) -> RowGroup<K, A, N, (K, A, N)> {
        RowGroup::new(self, 0, 0..self.0.columns[0].n_rows)
    }
}

/// A sorted, indexed group of unique rows in a [`Reader`].
///
/// Column 0 in a layer file has a single [`RowGroup`] that includes all of the
/// rows in column 0.  This row group, obtained with [`Reader::rows`], is empty
/// if the layer file is empty.
///
/// Row groups for other columns are obtained by first obtaining a [`Cursor`]
/// for a row in column 0 and calling [`Cursor::next_column`] to get its row
/// group in column 1, and then repeating as many times as necessary to get to
/// the desired column.
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

impl<'a, K, A, N, T> Clone for RowGroup<'a, K, A, N, T>
where
    K: Rkyv,
    A: Rkyv,
    T: ColumnSpec,
{
    fn clone(&self) -> Self {
        Self {
            reader: self.reader,
            column: self.column,
            rows: self.rows.clone(),
            _phantom: PhantomData,
        }
    }
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

    /// Returns `true` if the row group contains no rows.
    ///
    /// The row group for column 0 is empty if and only if the layer file is
    /// empty.  A row group obtained from [`Cursor::next_column`] is never
    /// empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Returns the number of rows in the row group.
    pub fn len(&self) -> u64 {
        self.rows.end - self.rows.start
    }

    /// Returns a cursor for just before the row group.
    pub fn before(self) -> Cursor<'a, K, A, N, T> {
        Cursor::<K, A, N, T>::new(self, Position::Before)
    }

    /// Return a cursor for just after the row group.
    pub fn after(self) -> Cursor<'a, K, A, N, T> {
        Cursor::<K, A, N, T>::new(self, Position::After)
    }

    /// Return a cursor for the first row in the row group, or just after the
    /// row group if it is empty.
    pub fn first(self) -> Result<Cursor<'a, K, A, N, T>, Error> {
        let position = if self.is_empty() {
            Position::After
        } else {
            Position::for_row(&self, self.rows.start)?
        };
        Ok(Cursor::<K, A, N, T>::new(self, position))
    }

    /// Return a cursor for the last row in the row group, or just after the
    /// row group if it is empty.
    pub fn last(self) -> Result<Cursor<'a, K, A, N, T>, Error> {
        let position = if self.is_empty() {
            Position::After
        } else {
            Position::for_row(&self, self.rows.end - 1)?
        };
        Ok(Cursor::<K, A, N, T>::new(self, position))
    }

    /// If `row` is less than the number of rows in the row group, returns a
    /// cursor for that row; otherwise, returns a cursor for just after the row
    /// group.
    pub fn nth(self, row: u64) -> Result<Cursor<'a, K, A, N, T>, Error> {
        let position = if row < self.len() {
            Position::for_row(&self, self.rows.start + row)?
        } else {
            Position::After
        };
        Ok(Cursor::<K, A, N, T>::new(self, position))
    }

    /// Returns a row group for a subset of the rows in this one.
    pub fn subset(mut self, subset: Range<u64>) -> Self {
        assert!(subset.end <= self.len());
        self.rows.start += subset.start;
        self.rows.end = self.rows.start + (subset.end - subset.start);
        self
    }
}

/// Trait for equality comparisons that might fail due to an I/O error.
pub trait FallibleEq {
    /// Compares `self` to `other` and returns whether they are equal, with
    /// the possibility of failure due to an I/O error.
    fn equals(&self, other: &Self) -> Result<bool, Error>;
}

impl<K, A, N> FallibleEq for Reader<(K, A, N)>
where
    K: Rkyv,
    A: Rkyv,
    (K, A, N): ColumnSpec,
    for<'a> RowGroup<'a, K, A, N, (K, A, N)>: FallibleEq,
{
    fn equals(&self, other: &Self) -> Result<bool, Error> {
        self.rows().equals(&other.rows())
    }
}

impl<'a, K, A, T> FallibleEq for RowGroup<'a, K, A, (), T>
where
    K: Rkyv + Eq,
    A: Rkyv + Eq,
    T: ColumnSpec,
{
    fn equals(&self, other: &Self) -> Result<bool, Error> {
        if self.len() != other.len() {
            return Ok(false);
        }
        let mut sc = self.clone().first()?;
        let mut oc = other.clone().first()?;
        while sc.has_value() {
            if unsafe { sc.item() != oc.item() } {
                return Ok(false);
            }
            sc.move_next()?;
            oc.move_next()?;
        }
        Ok(true)
    }
}

impl<'a, K, A, NK, NA, NN, T> FallibleEq for RowGroup<'a, K, A, (NK, NA, NN), T>
where
    K: Rkyv + Eq,
    A: Rkyv + Eq,
    NK: Rkyv + Eq,
    NA: Rkyv + Eq,
    T: ColumnSpec,
    RowGroup<'a, NK, NA, NN, T>: FallibleEq,
{
    fn equals(&self, other: &Self) -> Result<bool, Error> {
        if self.len() != other.len() {
            return Ok(false);
        }
        let mut sc = self.clone().first()?;
        let mut oc = other.clone().first()?;
        while sc.has_value() {
            if unsafe { sc.item() != oc.item() } {
                return Ok(false);
            }
            if !sc.next_column()?.equals(&oc.next_column()?)? {
                return Ok(false);
            }
            sc.move_next()?;
            oc.move_next()?;
        }
        Ok(true)
    }
}

/// A cursor for a layer file.
///
/// A cursor traverses a [`RowGroup`].  It can be positioned on a particular row
/// or before or after the row group.  (If the row group is empty, then the
/// cursor can only be before or after the row group.)
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

    /// Moves to the next row in the row group.  If the cursor was previously
    /// before the row group, it moves to the first row; if it was on the last
    /// row, it moves after the row group.
    pub fn move_next(&mut self) -> Result<(), Error> {
        self.position.next(&self.row_group)?;
        Ok(())
    }

    /// Moves to the previous row in the row group.  If the cursor was
    /// previously after the row group, it moves to the last row; if it was
    /// on the first row, it moves before the row group.
    pub fn move_prev(&mut self) -> Result<(), Error> {
        self.position.prev(&self.row_group)?;
        Ok(())
    }

    /// Moves to the first row in the row group.  If the row group is empty,
    /// this has no effect.
    pub fn move_first(&mut self) -> Result<(), Error> {
        self.position
            .move_to_row(&self.row_group, self.row_group.rows.start)
    }

    /// Moves to the last row in the row group.  If the row group is empty,
    /// this has no effect.
    pub fn move_last(&mut self) -> Result<(), Error> {
        if !self.row_group.is_empty() {
            self.position
                .move_to_row(&self.row_group, self.row_group.rows.end - 1)
        } else {
            self.position = Position::After;
            Ok(())
        }
    }

    /// Moves to row `row`.  If `row >= self.len()`, this has no effect.
    pub fn move_to_row(&mut self, row: u64) -> Result<(), Error> {
        if row < self.row_group.rows.end - self.row_group.rows.start {
            self.position
                .move_to_row(&self.row_group, self.row_group.rows.start + row)
        } else {
            self.position = Position::After;
            Ok(())
        }
    }

    /// Returns the key in the current row, or `None` if the cursor is before or
    /// after the row group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn key(&self) -> Option<K> {
        self.position.key()
    }

    /// Returns the key and auxiliary data in the current row, or `None` if the
    /// cursor is before or after the row group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn item(&self) -> Option<(K, A)> {
        self.position.item()
    }

    /// Returns `true` if the cursor is on a row.
    pub fn has_value(&self) -> bool {
        self.position.has_value()
    }

    /// Returns the number of rows in the cursor's row group.
    pub fn len(&self) -> u64 {
        self.row_group.len()
    }

    /// Returns true if this cursor's row group has no rows.
    pub fn is_empty(&self) -> bool {
        self.row_group.is_empty()
    }

    /// Returns the current row's offset from the start of the row group.  If
    /// the cursor is before the row group or on the first row, returns 0; if
    /// the cursor is on the last row, returns `self.len() - 1`; if the cursor
    /// is after the row group, returns `self.len()`.
    pub fn position(&self) -> u64 {
        self.position.position(&self.row_group)
    }

    /// Returns `self.len() - self.position()`.
    pub fn remaining_rows(&self) -> u64 {
        self.position.remaining_rows(&self.row_group)
    }

    /// Moves the cursor forward past rows for which `predicate` returns false,
    /// where `predicate` is a function such that if it is true for a given key,
    /// it is also true for all larger keys.
    ///
    /// This function does not move the cursor if `predicate` is true for the
    /// current row or a previous row.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn seek_forward_until<P>(&mut self, predicate: P) -> Result<(), Error>
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.advance_to_first_ge(&|key| {
            if predicate(key) {
                Less
            } else {
                Greater
            }
        })
    }

    /// Moves the cursor forward past rows whose keys are less than `target`.
    /// This function does not move the cursor if the current row's key is
    /// greater than or equal to `target`.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn advance_to_value_or_larger(&mut self, target: &K) -> Result<(), Error>
    where
        K: Ord,
    {
        self.advance_to_first_ge(&|key| target.cmp(key))
    }

    /// Moves the cursor forward past rows for which `compare` returns [`Less`],
    /// where `compare` is a function such that if it returns [`Equal`] or
    /// [`Greater`] for a given key, it returns [`Greater`] for all larger keys.
    ///
    /// This function does not move the cursor if `compare` returns [`Equal`] or
    /// [`Greater`] for the current row or a previous row.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn advance_to_first_ge<C>(&mut self, compare: &C) -> Result<(), Error>
    where
        C: Fn(&K) -> Ordering,
    {
        let position = Position::best_match::<N, T, _>(&self.row_group, compare, Less)?;
        if position > self.position {
            self.position = position;
        }
        Ok(())
    }

    /// Moves the cursor backward past rows for which `predicate` returns false,
    /// where `predicate` is a function such that if it is true for a given key,
    /// it is also true for all lesser keys.
    ///
    /// This function does not move the cursor if `predicate` is true for the
    /// current row or a previous row.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn seek_backward_until<P>(&mut self, predicate: P) -> Result<(), Error>
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.rewind_to_last_le(&|key| {
            if !predicate(key) {
                Less
            } else {
                Greater
            }
        })
    }

    /// Moves the cursor backward past rows whose keys are greater than
    /// `target`.  This function does not move the cursor if the current row's
    /// key is less than or equal to `target`.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn rewind_to_value_or_smaller(&mut self, target: &K) -> Result<(), Error>
    where
        K: Ord,
    {
        self.rewind_to_last_le(&|key| target.cmp(key))
    }

    /// Moves the cursor backward past rows for which `compare` returns
    /// [`Greater`], where `compare` is a function such that if it returns
    /// [`Equal`] or [`Less`] for a given key, it returns [`Less`] for all
    /// lesser keys.
    ///
    /// This function does not move the cursor if `compare` returns [`Equal`] or
    /// [`Less`] for the current row or a previous row.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn rewind_to_last_le<C>(&mut self, compare: &C) -> Result<(), Error>
    where
        C: Fn(&K) -> Ordering,
    {
        let position = Position::best_match::<N, T, _>(&self.row_group, compare, Greater)?;
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
    /// Obtains the row group in the next column associated with the current
    /// row.  If the cursor is on a row, the returned row group will contain at
    /// least one row.  If the cursor is before or after the row group, the
    /// returned row group will be empty.
    pub fn next_column<'b>(&'b self) -> Result<RowGroup<'a, NK, NA, NN, T>, Error> {
        Ok(RowGroup::new(
            self.row_group.reader,
            self.row_group.column + 1,
            self.position.row_group()?,
        ))
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
            row_group.reader.0.columns[row_group.column]
                .root
                .clone()
                .unwrap(),
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
            if let Some(node) = index_block.get_child_by_row(row)? {
                return Self::for_row_from_ancestor(
                    reader,
                    hint.indexes[0..=idx].to_vec(),
                    node,
                    row,
                );
            }
        }
        Err(CorruptionError::MissingRow(row).into())
    }
    unsafe fn key(&self) -> K {
        self.data.key_for_row(self.row)
    }
    unsafe fn item(&self) -> (K, A) {
        self.data.item_for_row(self.row)
    }
    fn row_group(&self) -> Result<Range<u64>, Error> {
        self.data.row_group(self.row)
    }
    fn move_to_row<T>(&mut self, reader: &Reader<T>, row: u64) -> Result<(), Error> {
        if self.data.rows().contains(&row) {
            self.row = row;
        } else {
            *self = Self::for_row_from_hint(reader, self, row)?;
        }
        Ok(())
    }
    unsafe fn best_match<N, T, C>(
        row_group: &RowGroup<K, A, N, T>,
        compare: &C,
        bias: Ordering,
    ) -> Result<Option<Self>, Error>
    where
        T: ColumnSpec,
        C: Fn(&K) -> Ordering,
    {
        let mut indexes = Vec::new();
        let Some(mut node) = row_group.reader.0.columns[row_group.column].root.clone() else {
            return Ok(None);
        };
        loop {
            match node.read(&row_group.reader.0.file)? {
                TreeBlock::Index(index_block) => {
                    let Some(child_idx) =
                        index_block.find_best_match(&row_group.rows, compare, bias)
                    else {
                        return Ok(None);
                    };
                    node = index_block.get_child(child_idx)?;
                    indexes.push(index_block);
                }
                TreeBlock::Data(data_block) => {
                    return Ok(data_block
                        .find_best_match(&row_group.rows, compare, bias)
                        .map(|child_idx| Self {
                            row: data_block.first_row + child_idx as u64,
                            indexes,
                            data: data_block,
                        }))
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
            (Self::Before, Self::Before) => Equal,
            (Self::Before, Self::Row(_)) => Less,
            (Self::Before, Self::After) => Less,

            (Self::Row(_), Self::Before) => Greater,
            (Self::Row(a), Self::Row(b)) => a.cmp(b),
            (Self::Row(_), Self::After) => Less,

            (Self::After, Self::Before) => Greater,
            (Self::After, Self::Row(_)) => Greater,
            (Self::After, Self::After) => Equal,
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
    pub fn row_group(&self) -> Result<Range<u64>, Error> {
        match self.path() {
            Some(path) => path.row_group(),
            None => Ok(0..0),
        }
    }
    fn has_value(&self) -> bool {
        self.path().is_some()
    }
    unsafe fn best_match<N, T, C>(
        row_group: &RowGroup<K, A, N, T>,
        compare: &C,
        bias: Ordering,
    ) -> Result<Self, Error>
    where
        T: ColumnSpec,
        C: Fn(&K) -> Ordering,
    {
        match Path::best_match(row_group, compare, bias)? {
            Some(path) => Ok(Position::Row(path)),
            None => Ok(if bias == Less {
                Position::After
            } else {
                Position::Before
            }),
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
        unsafe { cursor.advance_to_value_or_larger(&String::from("01 1")).unwrap() };
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
            let mut values = keys.next_column().unwrap().last().unwrap();
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
