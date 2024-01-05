use std::{
    marker::PhantomData,
    mem::{replace, take},
    ops::Range,
    rc::Rc,
};

use binrw::{
    io::{Cursor, NoSeek, Result as IoResult, Seek, Write},
    BinWrite,
};
use crc32c::crc32c;
use rkyv::{
    archived_value, ser::serializers::AlignedSerializer, AlignedVec, Archive, Archived,
    Deserialize, Infallible, Serialize,
};

use crate::file::BlockLocation;

use rkyv::ser::Serializer as RkyvSerializer;

use super::{
    DataBlockHeader, FileHeader, FileTrailer, FileTrailerColumn, FixedLen, IndexBlockHeader, Item,
    NodeType, Rkyv, Serializer, Varint, VERSION_NUMBER,
};

struct VarintWriter {
    varint: Varint,
    start: usize,
    count: usize,
}
impl VarintWriter {
    fn new(varint: Varint, start: usize, count: usize) -> Self {
        Self {
            start: varint.align(start),
            varint,
            count,
        }
    }
    fn offset_after(&self) -> usize {
        self.start + self.varint.len() * self.count
    }
    fn offset_after_or(opt_array: &Option<VarintWriter>, otherwise: usize) -> usize {
        match opt_array {
            Some(array) => array.offset_after(),
            None => otherwise,
        }
    }
    fn put<V>(&self, dst: &mut AlignedVec, values: V)
    where
        V: Iterator<Item = u64>,
    {
        dst.resize(self.start, 0);
        let mut count = 0;
        for value in values {
            self.varint.put(dst, value);
            count += 1;
        }
        debug_assert_eq!(count, self.count);
    }
}

pub struct Parameters {
    pub min_data_block: usize,
    pub min_index_block: usize,
    pub min_branch: usize,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            min_data_block: 8192,
            min_index_block: 8192,
            min_branch: 32,
        }
    }
}

trait IntoBlock {
    fn into_block(&self) -> AlignedVec;
    fn overwrite_head(&self, dst: &mut AlignedVec)
    where
        Self: FixedLen;
}

impl<B> IntoBlock for B
where
    B: for<'a> BinWrite<Args<'a> = ()>,
{
    fn into_block(&self) -> AlignedVec {
        let mut block = NoSeek::new(AlignedVec::with_capacity(4096));
        self.write_le(&mut block).unwrap();
        block.into_inner()
    }

    fn overwrite_head(&self, dst: &mut AlignedVec)
    where
        Self: FixedLen,
    {
        let mut writer = Cursor::new(dst.as_mut_slice());
        self.write_le(&mut writer).unwrap();
        debug_assert_eq!(writer.position(), <Self as FixedLen>::LEN as u64);
    }
}

struct Writer<W>
where
    W: Write + Seek,
{
    writer: W,
    cws: Vec<ColumnWriter>,
    finished_columns: Vec<FileTrailerColumn>,
}

struct ColumnWriter {
    parameters: Rc<Parameters>,
    column_index: usize,
    rows: Range<u64>,
    data_block: DataBlockBuilder,
    index_blocks: Vec<IndexBlockBuilder>,
}

impl ColumnWriter {
    fn new(parameters: &Rc<Parameters>, column_index: usize) -> Self {
        ColumnWriter {
            parameters: parameters.clone(),
            column_index,
            rows: 0..0,
            data_block: DataBlockBuilder::new(parameters),
            index_blocks: Vec::new(),
        }
    }

    fn take_rows(&mut self) -> Range<u64> {
        let end = self.rows.end;
        replace(&mut self.rows, end..end)
    }

    fn finish<W, K, A>(&mut self, writer: &mut W) -> IoResult<FileTrailerColumn>
    where
        W: Write + Seek,
        K: Rkyv,
        A: Rkyv,
    {
        // Flush data.
        if !self.data_block.is_empty() {
            let data_block = self.data_block.take().build();
            self.write_data_block::<W, K, A>(writer, data_block)?;
        }

        // Flush index.
        let mut retval = None;
        for level in 0..self.index_blocks.len() {
            if !self.index_blocks[level].is_empty() {
                let index_block = self.index_blocks[level].take().build();
                retval = Some(self.write_index_block::<W, K>(writer, index_block, level)?);
            }
        }
        if let Some((location, n_rows)) = retval {
            Ok(FileTrailerColumn {
                index_offset: location.offset,
                index_size: location.size as u32,
                n_rows,
            })
        } else {
            Ok(FileTrailerColumn {
                index_offset: 0,
                index_size: 0,
                n_rows: 0,
            })
        }
    }

    fn get_index_block(&mut self, level: usize) -> &mut IndexBlockBuilder {
        if level >= self.index_blocks.len() {
            debug_assert_eq!(level, self.index_blocks.len());
            self.index_blocks.push(IndexBlockBuilder::new(
                &self.parameters,
                self.column_index,
                if level == 0 {
                    NodeType::Data
                } else {
                    NodeType::Index
                },
            ));
        }
        &mut self.index_blocks[level]
    }

    fn write_data_block<W, K, A>(
        &mut self,
        writer: &mut W,
        mut data_block: DataBlock,
    ) -> IoResult<()>
    where
        W: Write + Seek,
        K: Rkyv,
        A: Rkyv,
    {
        let location = write_block(writer, &mut data_block.raw)?;

        let serialize_minmax = |dst: &mut AlignedVec| {
            let min_offset = rkyv_copy_key::<K, A>(dst, &data_block.raw, data_block.min_offset);
            let max_offset = rkyv_copy_key::<K, A>(dst, &data_block.raw, data_block.max_offset);
            (min_offset, max_offset)
        };
        if let Some(index_block) = self.get_index_block(0).add_entry(
            location,
            serialize_minmax,
            data_block.n_values as u64,
        ) {
            self.write_index_block::<W, K>(writer, index_block, 0)
                .map(|_| ())
        } else {
            Ok(())
        }
    }

    fn write_index_block<W, K>(
        &mut self,
        writer: &mut W,
        mut index_block: IndexBlock,
        mut level: usize,
    ) -> IoResult<(BlockLocation, u64)>
    where
        W: Write + Seek,
        K: Rkyv,
    {
        loop {
            let location = write_block(writer, &mut index_block.raw)?;

            level += 1;
            let serialize_minmax = |dst: &mut AlignedVec| {
                let min_offset = rkyv_copy::<K>(dst, &index_block.raw, index_block.min_offset);
                let max_offset = rkyv_copy::<K>(dst, &index_block.raw, index_block.max_offset);
                (min_offset, max_offset)
            };
            let opt_index_block = self.get_index_block(level).add_entry(
                location,
                serialize_minmax,
                index_block.n_rows,
            );
            index_block = match opt_index_block {
                None => return Ok((location, index_block.n_rows)),
                Some(index_block) => index_block,
            };
        }
    }

    fn add_item<W, K, A>(
        &mut self,
        writer: &mut W,
        item: (&K, &A),
        row_group: &Option<Range<u64>>,
    ) -> IoResult<()>
    where
        W: Write + Seek,
        K: Rkyv,
        A: Rkyv,
    {
        if let Some(data_block) = self.data_block.add_item(item, row_group) {
            self.write_data_block::<W, K, A>(writer, data_block)?;
        }
        Ok(())
    }
}

#[derive(Copy, Clone)]
enum StrideBuilder {
    NoValues,
    OneValue { first: usize },
    Constant { delta: usize, prev: usize },
    Variable,
}

impl StrideBuilder {
    fn new() -> Self {
        Self::NoValues
    }
    fn push(&mut self, value: usize) {
        *self = match *self {
            StrideBuilder::NoValues => StrideBuilder::OneValue { first: value },
            StrideBuilder::OneValue { first } => StrideBuilder::Constant {
                delta: value - first,
                prev: value,
            },
            StrideBuilder::Constant { delta, prev } => {
                if value - prev == delta {
                    StrideBuilder::Constant { delta, prev: value }
                } else {
                    StrideBuilder::Variable
                }
            }
            StrideBuilder::Variable => StrideBuilder::Variable,
        };
    }
    fn get_stride(&self) -> Option<usize> {
        if let StrideBuilder::Constant { delta, .. } = self {
            Some(*delta)
        } else {
            None
        }
    }
}

struct DataBlockBuilder {
    parameters: Rc<Parameters>,
    raw: AlignedVec,
    value_offsets: Vec<usize>,
    value_offset_stride: StrideBuilder,
    row_groups: ContiguousRanges,
    size_target: Option<usize>,
}

struct DataBuildSpecs {
    value_map: VarintWriter,
    row_groups: Option<VarintWriter>,
    len: usize,
}

struct DataBlock {
    raw: AlignedVec,
    min_offset: usize,
    max_offset: usize,
    n_values: usize,
}

impl DataBlockBuilder {
    fn new(parameters: &Rc<Parameters>) -> Self {
        let mut raw = AlignedVec::with_capacity(parameters.min_data_block);
        raw.resize(DataBlockHeader::LEN, 0);
        Self {
            parameters: parameters.clone(),
            raw,
            row_groups: ContiguousRanges::with_capacity(parameters.min_branch),
            value_offsets: Vec::with_capacity(parameters.min_branch),
            value_offset_stride: StrideBuilder::new(),
            size_target: None,
        }
    }
    fn is_empty(&self) -> bool {
        self.value_offsets.is_empty()
    }
    fn take(&mut self) -> DataBlockBuilder {
        replace(self, Self::new(&self.parameters))
    }
    fn try_add_item<K, A>(&mut self, item: (&K, &A), row_group: &Option<Range<u64>>) -> bool
    where
        K: Rkyv,
        A: Rkyv,
    {
        let old_len = self.raw.len();
        let old_stride = self.value_offset_stride;

        let offset = rkyv_serialize(&mut self.raw, &Item(item.0, item.1));
        self.value_offsets.push(offset);
        self.value_offset_stride.push(offset);
        if let Some(row_group) = row_group.as_ref() {
            self.row_groups.push(row_group);
        }

        if let Some(size_target) = self.size_target {
            if self.specs().len > size_target {
                self.raw.resize(old_len, 0);
                self.value_offsets.pop();
                self.value_offset_stride = old_stride;
                if row_group.is_some() {
                    self.row_groups.pop();
                }
                return false;
            }
        } else if self.value_offsets.len() >= self.parameters.min_branch {
            self.size_target = Some(
                self.specs()
                    .len
                    .next_power_of_two()
                    .max(self.parameters.min_data_block),
            );
        }

        true
    }
    fn add_item<K, A>(
        &mut self,
        item: (&K, &A),
        row_group: &Option<Range<u64>>,
    ) -> Option<DataBlock>
    where
        K: Rkyv,
        A: Rkyv,
    {
        if self.try_add_item(item, row_group) {
            None
        } else {
            let retval = self.take().build();
            assert!(self.try_add_item(item, row_group));
            Some(retval)
        }
    }

    fn specs(&self) -> DataBuildSpecs {
        debug_assert!(!self.is_empty());
        let len = self.raw.len();

        let value_map = match self.value_offset_stride.get_stride() {
            // General case.
            None => VarintWriter::new(
                Varint::from_len(self.raw.len()),
                len,
                self.value_offsets.len(),
            ),

            // Optimization for constant stride.  We need a starting offset and
            // a stride, both 32 bits.
            Some(_) => VarintWriter::new(Varint::B32, len, 2),
        };
        let len = value_map.offset_after();

        let row_groups = self.row_groups.max().map(|max| {
            debug_assert_eq!(self.row_groups.0.len(), self.value_offsets.len() + 1);
            VarintWriter::new(Varint::from_max_value(max), len, self.row_groups.0.len())
        });
        let len = VarintWriter::offset_after_or(&row_groups, len);

        DataBuildSpecs {
            value_map,
            row_groups,
            len,
        }
    }
    fn build(mut self) -> DataBlock {
        let specs = self.specs();

        self.raw
            .reserve(specs.len.saturating_sub(self.raw.capacity()));

        let value_map_varint = if let Some(stride) = self.value_offset_stride.get_stride() {
            specs.value_map.put(
                &mut self.raw,
                [self.value_offsets[0] as u64, stride as u64].into_iter(),
            );
            None
        } else {
            specs.value_map.put(
                &mut self.raw,
                self.value_offsets.iter().map(|offset| *offset as u64),
            );
            Some(specs.value_map.varint)
        };

        let (row_group_varint, row_groups_ofs) = if let Some(row_groups) = specs.row_groups.as_ref()
        {
            row_groups.put(&mut self.raw, self.row_groups.0.iter().copied());
            (Some(row_groups.varint), row_groups.start as u32)
        } else {
            (None, 0)
        };

        let n_values = self.value_offsets.len();
        let header = DataBlockHeader {
            checksum: 0,
            n_values: n_values as u32,
            value_map_varint,
            row_group_varint,
            value_map_ofs: specs.value_map.start as u32,
            row_groups_ofs,
        };
        header.overwrite_head(&mut self.raw);

        DataBlock {
            raw: self.raw,
            min_offset: *self.value_offsets.first().unwrap(),
            max_offset: *self.value_offsets.last().unwrap(),
            n_values,
        }
    }
}

struct IndexEntry {
    child: BlockLocation,
    min_offset: usize,
    max_offset: usize,
    row_total: u64,
}

struct ContiguousRanges(Vec<u64>);

impl ContiguousRanges {
    fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity.saturating_add(1)))
    }
    fn push(&mut self, range: &Range<u64>) {
        match self.0.last() {
            Some(&last) => {
                debug_assert_eq!(last, range.start);
            }
            None => self.0.push(range.start),
        };
        self.0.push(range.end);
    }
    fn pop(&mut self) {
        match self.0.len() {
            0 | 1 => unreachable!(),
            2 => self.0.clear(),
            _ => {
                self.0.pop();
            }
        }
    }
    fn max(&self) -> Option<u64> {
        self.0.last().copied()
    }
}

struct IndexBlockBuilder {
    parameters: Rc<Parameters>,
    column_index: usize,
    raw: AlignedVec,
    entries: Vec<IndexEntry>,
    child_type: NodeType,
    size_target: Option<usize>,
}

struct IndexBuildSpecs {
    bound_map: VarintWriter,
    row_totals: VarintWriter,
    child_pointers: VarintWriter,
    len: usize,
}

struct IndexBlock {
    raw: AlignedVec,
    min_offset: usize,
    max_offset: usize,
    n_rows: u64,
}

fn rkyv_copy_key<K, A>(dst: &mut AlignedVec, src: &AlignedVec, offset: usize) -> usize
where
    K: Rkyv,
    A: Rkyv,
{
    let archived: &Archived<(K, A)> = unsafe { archived_value::<(K, A)>(src.as_slice(), offset) };
    let value: K = archived.0.deserialize(&mut Infallible).unwrap();
    rkyv_serialize(dst, &value)
}

fn rkyv_copy<T>(dst: &mut AlignedVec, src: &AlignedVec, offset: usize) -> usize
where
    T: Rkyv,
{
    let archived: &T::Archived = unsafe { archived_value::<T>(src, offset) };
    let value: T = archived.deserialize(&mut Infallible).unwrap();
    rkyv_serialize(dst, &value)
}

fn rkyv_serialize<T>(dst: &mut AlignedVec, value: &T) -> usize
where
    T: Archive + for<'a> Serialize<Serializer<'a>>,
{
    let old_len = dst.len();

    let mut serializer = Serializer::new(
        AlignedSerializer::new(take(dst)),
        Default::default(),
        Default::default(),
    );
    let offset = serializer.serialize_value(value).unwrap();
    *dst = serializer.into_components().0.into_inner();

    if dst.len() == old_len {
        // Ensure that a value takes up at least one byte.  Otherwise, we'll
        // have to think hard about how fitting an unbounded number of values in
        // a block works with our other assumptions.
        dst.push(0);
    }
    offset
}

impl IndexBlockBuilder {
    fn new(parameters: &Rc<Parameters>, column_index: usize, child_type: NodeType) -> Self {
        let mut raw = AlignedVec::with_capacity(parameters.min_index_block);
        raw.resize(IndexBlockHeader::LEN, 0);

        Self {
            parameters: parameters.clone(),
            column_index,
            raw,
            entries: Vec::with_capacity(parameters.min_branch),
            child_type,
            size_target: None,
        }
    }
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    fn take(&mut self) -> IndexBlockBuilder {
        replace(
            self,
            Self::new(&self.parameters, self.column_index, self.child_type),
        )
    }
    fn try_add_entry<F>(&mut self, child: BlockLocation, serialize_minmax: F, n_rows: u64) -> bool
    where
        F: Fn(&mut AlignedVec) -> (usize, usize),
    {
        let saved_len = self.raw.len();

        let (min_offset, max_offset) = serialize_minmax(&mut self.raw);
        self.entries.push(IndexEntry {
            child,
            min_offset,
            max_offset,
            row_total: self.entries.last().map_or(0, |entry| entry.row_total) + n_rows,
        });

        if let Some(size_target) = self.size_target {
            if self.specs().len > size_target {
                self.raw.resize(saved_len, 0);
                self.entries.pop();
                return false;
            }
        } else if self.entries.len() >= self.parameters.min_branch {
            self.size_target = Some(
                self.specs()
                    .len
                    .next_power_of_two()
                    .max(self.parameters.min_index_block),
            );
        }

        true
    }
    fn add_entry<F>(
        &mut self,
        child: BlockLocation,
        serialize_minmax: F,
        n_rows: u64,
    ) -> Option<IndexBlock>
    where
        F: Fn(&mut AlignedVec) -> (usize, usize),
    {
        let f = |t: &mut Self| t.try_add_entry(child, &serialize_minmax, n_rows);
        if f(self) {
            None
        } else {
            let retval = self.take().build();
            assert!(f(self));
            Some(retval)
        }
    }
    fn specs(&self) -> IndexBuildSpecs {
        debug_assert!(!self.entries.is_empty());
        let len = self.raw.len();

        let bound_map = VarintWriter::new(
            Varint::from_len(self.raw.len()),
            len,
            self.entries.len() * 2,
        );
        let len = bound_map.offset_after();

        let row_totals = {
            let max_row_total = self.entries.last().unwrap().row_total;
            VarintWriter::new(
                Varint::from_max_value(max_row_total),
                len,
                self.entries.len(),
            )
        };
        let len = row_totals.offset_after();

        let child_pointers = VarintWriter::new(
            Varint::from_max_value(self.entries.last().unwrap().child.offset),
            len,
            self.entries.len(),
        );
        let len = child_pointers.offset_after();

        IndexBuildSpecs {
            bound_map,
            row_totals,
            child_pointers,
            len,
        }
    }
    fn build(mut self) -> IndexBlock {
        /*
        Format:
        - Header
        - Bounds (1 per child, only if enabled)
        - Bound map (1 per child, only if enabled and not fixed strides)
        - Row totals (1 per child, only if enabled)
        - Child pointer offsets and sizes (1 per child)
         */
        let specs = self.specs();

        self.raw
            .reserve(specs.len.saturating_sub(self.raw.capacity()));

        specs.bound_map.put(
            &mut self.raw,
            self.entries
                .iter()
                .flat_map(|entry| [entry.min_offset as u64, entry.max_offset as u64]),
        );

        specs.row_totals.put(
            &mut self.raw,
            self.entries.iter().map(|entry| entry.row_total),
        );

        specs.child_pointers.put(
            &mut self.raw,
            self.entries.iter().map(|entry| entry.child.into()),
        );

        let header = IndexBlockHeader {
            checksum: 0,
            bound_map_offset: specs.bound_map.start as u32,
            row_totals_offset: specs.row_totals.start as u32,
            child_pointers_offset: specs.child_pointers.start as u32,
            n_children: self.entries.len() as u16,
            child_type: self.child_type,
            bound_map_varint: specs.bound_map.varint,
            row_total_varint: specs.row_totals.varint,
            child_pointer_varint: specs.child_pointers.varint,
        };
        header.overwrite_head(&mut self.raw);

        let entry_0 = self.entries.first().unwrap();
        let entry_n = self.entries.last().unwrap();

        IndexBlock {
            raw: self.raw,
            min_offset: entry_0.min_offset,
            max_offset: entry_n.max_offset,
            n_rows: entry_n.row_total,
        }
    }
}

fn write_block<W>(writer: &mut W, block: &mut AlignedVec) -> IoResult<BlockLocation>
where
    W: Write + Seek,
{
    block.resize(block.len().max(4096).next_power_of_two(), 0);
    let checksum = crc32c(&block[4..]).to_le_bytes();
    block[..4].copy_from_slice(checksum.as_slice());

    let offset = writer.stream_position()?;
    writer.write_all(block.as_slice())?;
    Ok(BlockLocation::new(offset, block.len()).unwrap())
}

impl<W> Writer<W>
where
    W: Write + Seek,
{
    fn new(mut writer: W, parameters: Parameters, n_columns: usize) -> IoResult<Self> {
        let file_header = FileHeader {
            checksum: 0,
            version: VERSION_NUMBER,
            n_columns: n_columns as u32,
        };
        write_block(&mut writer, &mut file_header.into_block())?;

        let parameters = Rc::new(parameters);
        let cws = (0..n_columns)
            .map(|column| ColumnWriter::new(&parameters, column))
            .collect();
        let finished_columns = Vec::with_capacity(n_columns);
        let writer = Self {
            writer,
            cws,
            finished_columns,
        };
        Ok(writer)
    }

    fn write<K, A>(&mut self, column: usize, item: (&K, &A)) -> IoResult<()>
    where
        K: Rkyv,
        A: Rkyv,
    {
        let row_group = if column + 1 < self.n_columns() {
            let row_group = self.cws[column + 1].take_rows();
            debug_assert!(!row_group.is_empty());
            Some(row_group)
        } else {
            None
        };

        // Add `value` to row group for column.
        self.cws[column].rows.end += 1;
        self.cws[column].add_item(&mut self.writer, item, &row_group)
    }

    fn finish_column<K, A>(&mut self, column: usize) -> IoResult<()>
    where
        K: Rkyv,
        A: Rkyv,
    {
        debug_assert_eq!(column, self.finished_columns.len());
        for cw in self.cws.iter().skip(1) {
            debug_assert!(cw.rows.is_empty());
        }

        self.finished_columns
            .push(self.cws[column].finish::<W, K, A>(&mut self.writer)?);
        Ok(())
    }

    fn close(mut self) -> IoResult<W> {
        debug_assert_eq!(self.cws.len(), self.finished_columns.len());

        // Write the file trailer block.
        let file_trailer = FileTrailer {
            checksum: 0,
            version: VERSION_NUMBER,
            columns: take(&mut self.finished_columns),
        };
        write_block(&mut self.writer, &mut file_trailer.into_block())?;

        Ok(self.writer)
    }

    fn n_columns(&self) -> usize {
        self.cws.len()
    }

    fn n_rows(&self) -> u64 {
        self.cws[0].rows.end
    }
}

pub struct Writer1<W, K1, A1>
where
    W: Write + Seek,
    K1: Rkyv,
    A1: Rkyv,
{
    inner: Writer<W>,
    _phantom: PhantomData<(K1, A1)>,
}

impl<W, K1, A1> Writer1<W, K1, A1>
where
    W: Write + Seek,
    K1: Rkyv,
    A1: Rkyv,
{
    pub fn new(writer: W, parameters: Parameters) -> IoResult<Self> {
        Ok(Self {
            inner: Writer::new(writer, parameters, 1)?,
            _phantom: PhantomData,
        })
    }
    pub fn write(&mut self, item: (&K1, &A1)) -> IoResult<()> {
        self.inner.write(0, item)
    }
    pub fn n_rows(&self) -> u64 {
        self.inner.n_rows()
    }
    pub fn close(mut self) -> IoResult<W> {
        self.inner.finish_column::<K1, A1>(0)?;
        self.inner.close()
    }
}

pub struct Writer2<W, K1, A1, K2, A2>
where
    W: Write + Seek,
    K1: Rkyv,
    A1: Rkyv,
    K2: Rkyv,
    A2: Rkyv,
{
    inner: Writer<W>,
    _phantom: PhantomData<(K1, A1, K2, A2)>,
}

impl<W, K1, A1, K2, A2> Writer2<W, K1, A1, K2, A2>
where
    W: Write + Seek,
    K1: Rkyv,
    A1: Rkyv,
    K2: Rkyv,
    A2: Rkyv,
{
    pub fn new(writer: W, parameters: Parameters) -> IoResult<Self> {
        Ok(Self {
            inner: Writer::new(writer, parameters, 2)?,
            _phantom: PhantomData,
        })
    }
    pub fn write1(&mut self, item: (&K1, &A1)) -> IoResult<()> {
        self.inner.write(0, item)
    }
    pub fn write2(&mut self, item: (&K2, &A2)) -> IoResult<()> {
        self.inner.write(1, item)
    }
    pub fn n_rows(&self) -> u64 {
        self.inner.n_rows()
    }
    pub fn close(mut self) -> IoResult<W> {
        self.inner.finish_column::<K1, A1>(0)?;
        self.inner.finish_column::<K2, A2>(1)?;
        self.inner.close()
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;

    use super::{Parameters, Writer1, Writer2};

    #[test]
    fn write_file() {
        let mut layer_file =
            Writer1::new(File::create("file.layer").unwrap(), Parameters::default()).unwrap();
        for i in 0..1000i64 {
            layer_file.write((&i, &())).unwrap();
        }
        layer_file.close().unwrap();
    }

    #[test]
    fn write_tuple() {
        let mut layer_file =
            Writer1::new(File::create("file.layer").unwrap(), Parameters::default()).unwrap();
        layer_file.write((&(1i32, 'a'), &2i32)).unwrap();
        layer_file.write((&(3, 'b'), &4)).unwrap();
        layer_file.write((&(5, 'a'), &6)).unwrap();
        layer_file.write((&(7, 'c'), &8)).unwrap();
        layer_file.write((&(9, 'a'), &10)).unwrap();
        layer_file.write((&(11, 'b'), &12)).unwrap();
        layer_file.close().unwrap();
    }

    #[test]
    fn write_string() {
        let mut layer_file =
            Writer1::new(File::create("file.layer").unwrap(), Parameters::default()).unwrap();
        for i in 0..1000 {
            layer_file.write((&format!("{i:04}"), &())).unwrap();
        }
        layer_file.close().unwrap();
    }

    #[test]
    fn write_2_layers() {
        let mut layer_file =
            Writer2::new(File::create("file.layer").unwrap(), Parameters::default()).unwrap();
        for i in 0..1000_u32 {
            for j in 0..10_u32 {
                layer_file.write2((&j, &())).unwrap();
            }
            layer_file.write1((&i, &())).unwrap();
        }
        layer_file.close().unwrap();
    }
}
