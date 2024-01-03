use std::{
    cmp::Ordering,
    fmt::{Debug, Formatter, Result as FmtResult},
    fs::File,
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
    BlockLocation, CorruptionError, DataBlockHeader, Error, FileHeader, FileTrailer,
    FileTrailerColumn, IndexBlockHeader, NodeType, Rkyv, Varint, VERSION_NUMBER, ArchivedItem,
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

#[derive(Clone)]
struct DataBlock {
    raw: Rc<AlignedVec>,
    value_map: ValueMapReader,
    row_groups: Option<VarintReader>,
    first_row: u64,
}

impl DataBlock {
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
    unsafe fn archived_item<K, A>(&self, index: usize) -> &ArchivedItem<K, A>
    where
        K: Rkyv,
        A: Rkyv,
    {
        archived_value::<Item<K, A>>(&self.raw, self.value_map.get(&self.raw, index))
    }
    unsafe fn item<K, A>(&self, index: usize) -> (K, A)
    where
        K: Rkyv,
        A: Rkyv,
    {
        let item = self.archived_item::<K, A>(index);
        let key = item.0.deserialize(&mut Infallible).unwrap();
        let aux = item.1.deserialize(&mut Infallible).unwrap();
        (key, aux)
    }
    unsafe fn item_for_row<K, A>(&self, row: u64) -> (K, A)
    where
        K: Rkyv,
        A: Rkyv,
    {
        let index = (row - self.first_row) as usize;
        self.item(index)
    }
    unsafe fn key<K, A>(&self, index: usize) -> K
    where
        K: Rkyv,
        A: Rkyv,
    {
        let item = self.archived_item::<K, A>(index);
        let key = item.0.deserialize(&mut Infallible).unwrap();
        key
    }
    unsafe fn key_for_row<K, A>(&self, row: u64) -> K
    where
        K: Rkyv,
        A: Rkyv,
    {
        let index = (row - self.first_row) as usize;
        self.key::<K, A>(index)
    }

    unsafe fn find_first<K, A, P>(&self, target_rows: &Range<u64>, predicate: P) -> Option<usize>
    where
        K: Rkyv + Ord,
        A: Rkyv,
        P: Fn(&K) -> bool + Clone,
    {
        let mut start = 0;
        let mut end = self.n_values();
        let mut best = None;
        while start < end {
            let mid = (start + end) / 2;
            let key = self.key::<K, A>(mid);
            let row = self.first_row + mid as u64;
            let cmp = range_compare(target_rows, row);
            match cmp {
                Ordering::Equal => {
                    if predicate(&key) {
                        best = Some(mid);
                        end = mid;
                    } else {
                        start = mid + 1;
                    }
                }
                Ordering::Less => end = mid,
                Ordering::Greater => start = mid + 1,
            };
        }
        best
    }

    unsafe fn find_last<K, A, P>(&self, target_rows: &Range<u64>, predicate: P) -> Option<usize>
    where
        K: Rkyv + Ord,
        A: Rkyv,
        P: Fn(&K) -> bool + Clone,
    {
        let mut start = 0;
        let mut end = self.n_values();
        let mut best = None;
        while start < end {
            let mid = (start + end) / 2;
            let key = self.key::<K, A>(mid);
            let row = self.first_row + mid as u64;
            let cmp = range_compare(target_rows, row);
            match cmp {
                Ordering::Equal => {
                    if predicate(&key) {
                        best = Some(mid);
                        start = mid + 1;
                    } else {
                        end = mid;
                    }
                }
                Ordering::Less => end = mid,
                Ordering::Greater => start = mid + 1,
            };
        }
        best
    }

    unsafe fn min_ge<K, A>(&self, target_rows: &Range<u64>, target_key: &K) -> Option<usize>
    where
        K: Rkyv + Ord,
        A: Rkyv,
    {
        let mut start = 0;
        let mut end = self.n_values();
        let mut best = None;
        while start < end {
            let mid = (start + end) / 2;
            let key = self.key::<K, A>(mid);
            let row = self.first_row + mid as u64;
            let cmp = range_compare(target_rows, row);
            match cmp {
                Ordering::Equal => match target_key.cmp(&key) {
                    Ordering::Less => {
                        best = Some(mid);
                        end = mid;
                    }
                    Ordering::Equal => return Some(mid),
                    Ordering::Greater => start = mid + 1,
                },
                Ordering::Less => end = mid,
                Ordering::Greater => start = mid + 1,
            };
        }
        best
    }

    unsafe fn max_le<K, A>(&self, target_rows: &Range<u64>, target_key: &K) -> Option<usize>
    where
        K: Rkyv + Ord,
        A: Rkyv,
    {
        let mut start = 0;
        let mut end = self.n_values();
        let mut best = None;
        while start < end {
            let mid = (start + end) / 2;
            let key = self.key::<K, A>(mid);
            let row = self.first_row + mid as u64;
            let cmp = range_compare(target_rows, row);
            match cmp {
                Ordering::Equal => match target_key.cmp(&key) {
                    Ordering::Less => end = mid,

                    Ordering::Equal => return Some(mid),
                    Ordering::Greater => {
                        best = Some(mid);
                        start = mid + 1
                    }
                },
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
    fn read(self, file: &File) -> Result<TreeBlock, Error> {
        match self.node_type {
            NodeType::Data => Ok(TreeBlock::Data(DataBlock::new(file, &self)?)),
            NodeType::Index => Ok(TreeBlock::Index(IndexBlock::new(file, &self)?)),
        }
    }
}

enum TreeBlock {
    Data(DataBlock),
    Index(IndexBlock),
}

impl TreeBlock {
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

#[derive(Clone)]
struct IndexBlock {
    raw: Rc<AlignedVec>,
    child_type: NodeType,
    bounds: VarintReader,
    row_totals: VarintReader,
    child_pointers: VarintReader,
    depth: usize,
    first_row: u64,
}

impl IndexBlock {
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

    fn get_row2(&self, index: usize) -> u64 {
        if index == 0 {
            0
        } else if index % 2 == 1 {
            self.row_totals.get(&self.raw, index / 2)
        } else {
            self.row_totals.get(&self.raw, index / 2 - 1) + 1
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

    unsafe fn get_bound<K>(&self, index: usize) -> K
    where
        K: Rkyv,
    {
        let offset = self.bounds.get(&self.raw, index) as usize;
        let archived = archived_value::<K>(&self.raw, offset);
        archived.deserialize(&mut Infallible).unwrap()
    }

    unsafe fn find_first<K, P>(&self, target_rows: &Range<u64>, predicate: P) -> Option<usize>
    where
        K: Rkyv + Ord,
        P: Fn(&K) -> bool + Clone,
    {
        let mut start = 0;
        let mut end = self.n_children() * 2;
        let mut best = None;
        while start < end {
            let mid = (start + end) / 2;
            let bound = self.get_bound::<K>(mid);
            let row = self.get_row2(mid);
            let cmp = range_compare(target_rows, row);
            match cmp {
                Ordering::Equal => {
                    if predicate(&bound) {
                        best = Some(mid / 2);
                        end = mid;
                    } else {
                        start = mid + 1;
                    }
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

    unsafe fn find_last<K, P>(&self, target_rows: &Range<u64>, predicate: P) -> Option<usize>
    where
        K: Rkyv + Ord,
        P: Fn(&K) -> bool + Clone,
    {
        let mut start = 0;
        let mut end = self.n_children() * 2;
        let mut best = None;
        while start < end {
            let mid = (start + end) / 2;
            let bound = self.get_bound::<K>(mid);
            let row = self.get_row2(mid);
            let cmp = range_compare(target_rows, row);
            match cmp {
                Ordering::Equal => {
                    if predicate(&bound) {
                        best = Some(mid / 2);
                        start = mid + 1;
                    } else {
                        end = mid;
                    }
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

    unsafe fn min_ge<K>(&self, target_rows: &Range<u64>, target: &K) -> Option<usize>
    where
        K: Rkyv + Ord,
    {
        let mut start = 0;
        let mut end = self.n_children() * 2;
        let mut best = None;
        while start < end {
            let mid = (start + end) / 2;
            let bound = self.get_bound::<K>(mid);
            let row = self.get_row2(mid);
            let cmp = range_compare(target_rows, row);
            match cmp {
                Ordering::Equal => {
                    let cmp2 = target.cmp(&bound);
                    match cmp2 {
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

    unsafe fn max_le<K>(&self, target_rows: &Range<u64>, target: &K) -> Option<usize>
    where
        K: Rkyv + Ord,
    {
        let mut start = 0;
        let mut end = self.n_children() * 2;
        let mut best = None;
        while start < end {
            let mid = (start + end) / 2;
            let bound = self.get_bound::<K>(mid);
            let row = self.get_row2(mid);
            let cmp = range_compare(target_rows, row);
            match cmp {
                Ordering::Equal => {
                    let cmp2 = target.cmp(&bound);
                    match cmp2 {
                        Ordering::Less => end = mid,
                        Ordering::Equal => return Some(mid / 2),
                        Ordering::Greater => {
                            best = Some(mid / 2);
                            start = mid + 1
                        }
                    };
                }
                Ordering::Less => end = mid,
                Ordering::Greater => {
                    best = Some(mid / 2);
                    start = mid + 1
                }
            };
        }
        best
    }

    fn n_children(&self) -> usize {
        self.child_pointers.count
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

#[derive(Clone)]
pub struct Reader(Arc<ReaderInner>);

struct ReaderInner {
    file: File,
    columns: Vec<Column>,
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

impl Reader {
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
        if columns[0].n_rows > 0 {
            for (column_index, column) in columns.iter().enumerate().skip(1) {
                if column.n_rows == 0 {
                    return Err(CorruptionError::UnexpectedlyEmptyColumn(column_index).into());
                }
            }
        }

        Ok(Self(Arc::new(ReaderInner { file, columns })))
    }

    pub fn empty(n_columns: usize) -> Result<Self, Error> {
        Ok(Self(Arc::new(ReaderInner {
            file: tempfile()?,
            columns: (0..n_columns).map(|_| Column::empty()).collect(),
        })))
    }

    pub fn equal(&self, other: &Reader) -> Option<bool> {
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

    pub fn rows(&self) -> RowGroup {
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
pub struct RowGroup<'a> {
    reader: &'a Reader,
    column: usize,
    rows: Range<u64>,
}

impl<'a> Debug for RowGroup<'a> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "RowGroup(column={}, rows={:?})", self.column, self.rows)
    }
}

impl<'a> RowGroup<'a> {
    fn new(reader: &'a Reader, column: usize, rows: Range<u64>) -> Self {
        Self {
            reader,
            column,
            rows,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn len(&self) -> u64 {
        self.rows.end - self.rows.start
    }

    pub fn before<K, A>(self) -> Cursor<'a, K, A>
    where
        K: Rkyv,
        A: Rkyv,
    {
        Cursor::<K, A>::new(self, Position::Before)
    }

    pub fn after<K, A>(self) -> Cursor<'a, K, A>
    where
        K: Rkyv,
        A: Rkyv,
    {
        Cursor::<K, A>::new(self, Position::After)
    }

    pub fn first<K, A>(self) -> Result<Cursor<'a, K, A>, Error>
    where
        K: Rkyv,
        A: Rkyv,
    {
        let position = if self.is_empty() {
            Position::After
        } else {
            Position::for_row(&self, self.rows.start)?
        };
        Ok(Cursor::<K, A>::new(self, position))
    }

    pub fn last<K, A>(self) -> Result<Cursor<'a, K, A>, Error>
    where
        K: Rkyv,
        A: Rkyv,
    {
        let position = if self.is_empty() {
            Position::After
        } else {
            Position::for_row(&self, self.rows.end - 1)?
        };
        Ok(Cursor::<K, A>::new(self, position))
    }

    pub fn nth<K, A>(self, row: u64) -> Result<Cursor<'a, K, A>, Error>
    where
        K: Rkyv,
        A: Rkyv,
    {
        let position = if row < self.len() {
            Position::for_row(&self, self.rows.start + row)?
        } else {
            Position::After
        };
        Ok(Cursor::<K, A>::new(self, position))
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
pub struct Cursor<'a, K, A>
where
    K: Rkyv,
    A: Rkyv,
{
    row_group: RowGroup<'a>,
    position: Position,
    _phantom: PhantomData<(K, A)>,
}

impl<'a, K, A> Debug for Cursor<'a, K, A>
where
    K: Rkyv + Debug,
    A: Rkyv + Debug,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "Cursor({:?}, {:?})", self.row_group, self.position)
    }
}

impl<'a, K, A> Cursor<'a, K, A>
where
    K: Rkyv,
    A: Rkyv,
{
    fn new(row_group: RowGroup<'a>, position: Position) -> Self {
        Self {
            row_group,
            position,
            _phantom: PhantomData,
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
        self.position.key::<K, A>()
    }

    pub unsafe fn item(&self) -> Option<(K, A)> {
        self.position.item::<K, A>()
    }

    pub fn next_column(&self) -> Option<RowGroup> {
        self.position
            .row_group()
            .map(|rows| RowGroup::new(self.row_group.reader, self.row_group.column + 1, rows))
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
}

impl<'a, K, A> Cursor<'a, K, A>
where
    K: Rkyv + ?Sized + Ord,
    A: Rkyv,
{
    pub unsafe fn seek_forward_until<P>(&mut self, predicate: P) -> Result<(), Error>
    where
        P: Fn(&K) -> bool + Clone,
    {
        // XXX optimization possibilities here
        let position = Position::find_first::<K, A, P>(&self.row_group, predicate)?;
        if let Position::Row(ref old_path) = self.position {
            if let Position::Row(ref new_path) = position {
                if new_path.row < old_path.row {
                    return Ok(());
                }
            }
        }
        self.position = position;
        Ok(())
    }
    pub unsafe fn seek_backward_until<P>(&mut self, predicate: P) -> Result<(), Error>
    where
        P: Fn(&K) -> bool + Clone,
    {
        // XXX optimization possibilities here
        let position = Position::find_last::<K, A, P>(&self.row_group, predicate)?;
        if let Position::Row(ref old_path) = self.position {
            if let Position::Row(ref new_path) = position {
                if new_path.row < old_path.row {
                    return Ok(());
                }
            }
        }
        self.position = position;
        Ok(())
    }
    pub unsafe fn advance_to_value_or_larger(&mut self, target: &K) -> Result<(), Error> {
        // XXX optimization possibilities here
        let position = Position::for_value_or_larger::<K, A>(&self.row_group, target)?;
        if let Position::Row(ref old_path) = self.position {
            if let Position::Row(ref new_path) = position {
                if new_path.row < old_path.row {
                    return Ok(());
                }
            }
        }
        self.position = position;
        Ok(())
    }
    pub unsafe fn rewind_to_value_or_smaller(&mut self, target: &K) -> Result<(), Error> {
        // XXX optimization possibilities here
        let position = Position::for_value_or_smaller::<K, A>(&self.row_group, target)?;
        if let Position::Row(ref old_path) = self.position {
            if let Position::Row(ref new_path) = position {
                if new_path.row > old_path.row {
                    return Ok(());
                }
            }
        }
        self.position = position;
        Ok(())
    }
}

#[derive(Clone)]
struct Path {
    row: u64,
    indexes: Vec<IndexBlock>,
    data: DataBlock,
}

impl Path {
    fn for_row(row_group: &RowGroup, row: u64) -> Result<Path, Error> {
        Self::for_row_from_ancestor(
            row_group.reader,
            Vec::new(),
            row_group.reader.0.columns[row_group.column].root.unwrap(),
            row,
        )
    }
    fn for_row_from_ancestor(
        reader: &Reader,
        mut indexes: Vec<IndexBlock>,
        mut node: TreeNode,
        row: u64,
    ) -> Result<Path, Error> {
        loop {
            let block = node.read(&reader.0.file)?;
            let next = block.lookup_row(row)?;
            match block {
                TreeBlock::Data(data) => return Ok(Path { row, indexes, data }),
                TreeBlock::Index(index) => indexes.push(index),
            };
            node = next.unwrap();
        }
    }
    fn for_row_from_hint(reader: &Reader, hint: &Path, row: u64) -> Result<Path, Error> {
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
    unsafe fn key<'a, K, A>(&'a self) -> K
    where
        K: Rkyv,
        A: Rkyv,
    {
        self.data.key_for_row::<K, A>(self.row)
    }
    unsafe fn item<'a, K, A>(&'a self) -> (K, A)
    where
        K: Rkyv,
        A: Rkyv,
    {
        self.data.item_for_row::<K, A>(self.row)
    }
    fn get_row_group(&self) -> Range<u64> {
        self.data.get_row_group(self.row).unwrap()
    }
    fn move_to_row(&mut self, reader: &Reader, row: u64) -> Result<(), Error> {
        if self.data.rows().contains(&row) {
            self.row = row;
        } else {
            *self = Self::for_row_from_hint(reader, self, row)?;
        }
        Ok(())
    }
    unsafe fn find_first<K, A, P>(row_group: &RowGroup, predicate: P) -> Result<Option<Path>, Error>
    where
        K: Rkyv + Ord,
        A: Rkyv,
        P: Fn(&K) -> bool + Clone,
    {
        let mut indexes = Vec::new();
        let Some(mut node) = row_group.reader.0.columns[row_group.column].root else {
            return Ok(None);
        };
        loop {
            match node.read(&row_group.reader.0.file)? {
                TreeBlock::Index(index_block) => {
                    let Some(child_idx) = index_block.find_first(&row_group.rows, &predicate)
                    else {
                        return Ok(None);
                    };
                    node = index_block.get_child(child_idx);
                    indexes.push(index_block);
                }
                TreeBlock::Data(data_block) => {
                    let Some(child_idx) =
                        data_block.find_first::<K, A, _>(&row_group.rows, &predicate)
                    else {
                        return Ok(None);
                    };
                    return Ok(Some(Path {
                        row: data_block.first_row + child_idx as u64,
                        indexes,
                        data: data_block,
                    }));
                }
            }
        }
    }
    unsafe fn find_last<K, A, P>(row_group: &RowGroup, predicate: P) -> Result<Option<Path>, Error>
    where
        K: Rkyv + Ord,
        A: Rkyv,
        P: Fn(&K) -> bool + Clone,
    {
        let mut indexes = Vec::new();
        let Some(mut node) = row_group.reader.0.columns[row_group.column].root else {
            return Ok(None);
        };
        loop {
            match node.read(&row_group.reader.0.file)? {
                TreeBlock::Index(index_block) => {
                    let Some(child_idx) = index_block.find_last(&row_group.rows, &predicate) else {
                        return Ok(None);
                    };
                    node = index_block.get_child(child_idx);
                    indexes.push(index_block);
                }
                TreeBlock::Data(data_block) => {
                    let Some(child_idx) =
                        data_block.find_last::<K, A, _>(&row_group.rows, &predicate)
                    else {
                        return Ok(None);
                    };
                    return Ok(Some(Path {
                        row: data_block.first_row + child_idx as u64,
                        indexes,
                        data: data_block,
                    }));
                }
            }
        }
    }
    unsafe fn for_value_or_larger<K, A>(
        row_group: &RowGroup,
        value: &K,
    ) -> Result<Option<Path>, Error>
    where
        K: Rkyv + Ord,
        A: Rkyv,
    {
        let mut indexes = Vec::new();
        let Some(mut node) = row_group.reader.0.columns[row_group.column].root else {
            return Ok(None);
        };
        loop {
            match node.read(&row_group.reader.0.file)? {
                TreeBlock::Index(index_block) => {
                    let Some(child_idx) = index_block.min_ge(&row_group.rows, value) else {
                        return Ok(None);
                    };
                    node = index_block.get_child(child_idx);
                    indexes.push(index_block);
                }
                TreeBlock::Data(data_block) => {
                    let Some(child_idx) = data_block.min_ge::<K, A>(&row_group.rows, value) else {
                        return Ok(None);
                    };
                    return Ok(Some(Path {
                        row: data_block.first_row + child_idx as u64,
                        indexes,
                        data: data_block,
                    }));
                }
            }
        }
    }
    unsafe fn for_value_or_smaller<K, A>(
        row_group: &RowGroup,
        value: &K,
    ) -> Result<Option<Path>, Error>
    where
        K: Rkyv + Ord,
        A: Rkyv,
    {
        let mut indexes = Vec::new();
        let Some(mut node) = row_group.reader.0.columns[row_group.column].root else {
            return Ok(None);
        };
        loop {
            match node.read(&row_group.reader.0.file)? {
                TreeBlock::Index(index_block) => {
                    let Some(child_idx) = index_block.max_le(&row_group.rows, value) else {
                        return Ok(None);
                    };
                    node = index_block.get_child(child_idx);
                    indexes.push(index_block);
                }
                TreeBlock::Data(data_block) => {
                    let Some(child_idx) = data_block.max_le::<K, A>(&row_group.rows, value) else {
                        return Ok(None);
                    };
                    return Ok(Some(Path {
                        row: data_block.first_row + child_idx as u64,
                        indexes,
                        data: data_block,
                    }));
                }
            }
        }
    }
}

#[derive(Clone)]
enum Position {
    Before,
    Row(Path),
    After,
}

impl Debug for Position {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Position::Before => write!(f, "before"),
            Position::Row(path) => write!(f, "row {}", path.row),
            Position::After => write!(f, "after"),
        }
    }
}

impl Position {
    fn for_row(row_group: &RowGroup, row: u64) -> Result<Self, Error> {
        Ok(Self::Row(Path::for_row(row_group, row)?))
    }
    fn next(&mut self, row_group: &RowGroup) -> Result<(), Error> {
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
    fn prev(&mut self, row_group: &RowGroup) -> Result<(), Error> {
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
    fn move_to_row(&mut self, row_group: &RowGroup, row: u64) -> Result<(), Error> {
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
    fn path(&self) -> Option<&Path> {
        match self {
            Position::Before => None,
            Position::Row(path) => Some(path),
            Position::After => None,
        }
    }
    pub unsafe fn key<K, A>(&self) -> Option<K>
    where
        K: Rkyv,
        A: Rkyv,
    {
        self.path().map(|path| path.key::<K, A>())
    }
    pub unsafe fn item<K, A>(&self) -> Option<(K, A)>
    where
        K: Rkyv,
        A: Rkyv,
    {
        self.path().map(|path| path.item::<K, A>())
    }
    pub fn row_group(&self) -> Option<Range<u64>> {
        self.path().map(|path| path.get_row_group())
    }
    fn has_value(&self) -> bool {
        self.path().is_some()
    }
    unsafe fn find_first<K, A, P>(row_group: &RowGroup, predicate: P) -> Result<Self, Error>
    where
        K: Rkyv + Ord,
        A: Rkyv,
        P: Fn(&K) -> bool + Clone,
    {
        match Path::find_first::<K, A, P>(row_group, predicate)? {
            Some(path) => Ok(Position::Row(path)),
            None => Ok(Position::After),
        }
    }
    unsafe fn find_last<K, A, P>(row_group: &RowGroup, predicate: P) -> Result<Self, Error>
    where
        K: Rkyv + Ord,
        A: Rkyv,
        P: Fn(&K) -> bool + Clone,
    {
        match Path::find_last::<K, A, P>(row_group, predicate)? {
            Some(path) => Ok(Position::Row(path)),
            None => Ok(Position::Before),
        }
    }
    unsafe fn for_value_or_larger<K, A>(row_group: &RowGroup, target: &K) -> Result<Self, Error>
    where
        K: Rkyv + Ord,
        A: Rkyv,
    {
        match Path::for_value_or_larger::<K, A>(row_group, target)? {
            Some(path) => Ok(Position::Row(path)),
            None => Ok(Position::After),
        }
    }
    unsafe fn for_value_or_smaller<K, A>(row_group: &RowGroup, target: &K) -> Result<Self, Error>
    where
        K: Rkyv + Ord,
        A: Rkyv,
    {
        match Path::for_value_or_smaller::<K, A>(row_group, target)? {
            Some(path) => Ok(Position::Row(path)),
            None => Ok(Position::Before),
        }
    }
    fn position(&self, row_group: &RowGroup) -> u64 {
        match self {
            Position::Before => 0,
            Position::Row(path) => path.row,
            Position::After => row_group.rows.end,
        }
    }
    fn remaining_rows(&self, row_group: &RowGroup) -> u64 {
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
        let reader = Reader::new(File::open("file.layer").unwrap()).unwrap();
        let mut cursor = reader.rows().first::<i64, ()>().unwrap();
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
        let reader = Reader::new(File::open("file.layer").unwrap()).unwrap();
        let mut cursor = reader.rows().first::<(i32, char), i32>().unwrap();
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
        let reader = Reader::new(File::open("file.layer").unwrap()).unwrap();
        let mut cursor = reader.rows().first::<String, ()>().unwrap();
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
        let reader = Reader::new(File::open("file.layer").unwrap()).unwrap();
        let mut keys = reader.rows().first::<u32, ()>().unwrap();
        let mut count = 0;
        let mut count2 = 0;
        while let Some(key) = unsafe { keys.key() } {
            println!("{key}");
            count += 1;
            let mut values = keys.next_column().unwrap().last::<u32, ()>().unwrap();
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
