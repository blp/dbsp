use binrw::{binrw, io::Error as IoError, BinRead, BinResult, BinWrite, Error as BinError};
use num::FromPrimitive;
use num_derive::FromPrimitive;
use rkyv::{
    ser::serializers::AllocSerializer, with::Inline, AlignedVec, Archive, Archived, Deserialize,
    Infallible, Serialize,
};
use thiserror::Error as ThisError;

pub mod reader;
pub mod writer;

/// Increment this on each incompatible change.
const VERSION_NUMBER: u32 = 1;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("Corrupt layer file: {0}")]
    Corruption(#[from] CorruptionError),

    #[error("I/O error: {0}")]
    Io(#[from] IoError),

    #[error("Last column does not have values.")]
    NoValuesInLastColumn,
}

impl From<BinError> for Error {
    fn from(source: BinError) -> Self {
        Error::Corruption(source.into())
    }
}

#[derive(ThisError, Debug)]
pub enum CorruptionError {
    #[error("File size {0} must be a positive multiple of 4096")]
    InvalidFileSize(u64),

    #[error("{}-byte block at offset {} has invalid checksum {checksum:#x} (expected {computed_checksum:#})", .location.size, .location.offset)]
    InvalidChecksum {
        location: BlockLocation,
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

    #[error("{}-byte index block at offset {} is empty", .0.size, .0.offset)]
    EmptyIndex(BlockLocation),

    #[error("{}-byte data block at offset {} is empty", .0.size, .0.offset)]
    EmptyData(BlockLocation),

    #[error("Cannot have {size:#x}-byte block at offset {offset:#x}.")]
    InvalidBlockLocation { offset: u64, size: usize },

    #[error("Missing reference to values in next column")]
    MissingRowGroup,

    #[error("Column {0} is empty even though column 0 was not empty")]
    UnexpectedlyEmptyColumn(usize),

    #[error("Unexpectedly missing row {0} in column 1 (or later)")]
    MissingRow(u64),
}

#[binrw]
#[derive(Debug)]
struct FileHeader {
    checksum: u32,

    #[brw(magic(b"LFFH"))]
    version: u32,

    n_columns: u32,
}

#[binrw]
#[derive(Debug)]
struct FileTrailer {
    checksum: u32,

    #[brw(magic(b"LFFT"))]
    version: u32,

    #[bw(calc(columns.len() as u32))]
    n_columns: u32,

    #[br(count = n_columns)]
    columns: Vec<FileTrailerColumn>,
}

#[binrw]
#[derive(Debug)]
struct FileTrailerColumn {
    index_offset: u64,
    index_size: u32,
    n_rows: u64,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
#[binrw]
#[brw(repr(u8))]
enum NodeType {
    Data = 0,
    Index = 1,
}

trait FixedLen {
    const LEN: usize;
}

#[binrw]
struct IndexBlockHeader {
    checksum: u32,
    #[brw(magic(b"LFIB"))]
    bound_map_offset: u32,
    row_totals_offset: u32,
    child_pointers_offset: u32,
    n_children: u16,
    child_type: NodeType,
    bound_map_varint: Varint,
    row_total_varint: Varint,
    #[brw(align_after = 16)]
    child_pointer_varint: Varint,
}

impl FixedLen for IndexBlockHeader {
    const LEN: usize = 32;
}

#[binrw]
struct DataBlockHeader {
    checksum: u32,
    #[brw(magic(b"LFDB"))]
    n_values: u32,
    value_map_ofs: u32,
    row_groups_ofs: u32,
    #[bw(write_with = Varint::write_opt)]
    #[br(parse_with = Varint::parse_opt)]
    value_map_varint: Option<Varint>,
    #[bw(write_with = Varint::write_opt)]
    #[br(parse_with = Varint::parse_opt)]
    #[brw(align_after = 16)]
    row_group_varint: Option<Varint>,
}

impl FixedLen for DataBlockHeader {
    const LEN: usize = 32;
}

#[derive(Copy, Clone, PartialEq, Eq, FromPrimitive)]
#[binrw]
#[brw(repr(u8))]
enum Varint {
    B8 = 1,
    B16 = 2,
    B24 = 3,
    B32 = 4,
    B48 = 6,
    B64 = 8,
}
impl Varint {
    fn from_max_value(max_value: u64) -> Varint {
        match max_value {
            ..=0xff => Varint::B8,
            ..=0xffff => Varint::B16,
            ..=0xffff_ff => Varint::B24,
            ..=0xffff_ffff => Varint::B32,
            ..=0xffff_ffff_ffff => Varint::B48,
            _ => Varint::B64,
        }
    }
    fn from_len(len: usize) -> Varint {
        Self::from_max_value(len as u64 - 1)
    }
    fn alignment(&self) -> usize {
        match self {
            Self::B24 => 1,
            Self::B48 => 2,
            _ => *self as usize,
        }
    }
    fn align(&self, offset: usize) -> usize {
        next_multiple_of_pow2(offset, self.alignment())
    }
    fn len(&self) -> usize {
        *self as usize
    }
    fn put(&self, dst: &mut AlignedVec, value: u64) {
        match *self {
            Self::B8 => dst.push(value as u8),
            Self::B16 => dst.extend_from_slice(&(value as u16).to_le_bytes()),
            Self::B24 => dst.extend_from_slice(&(value as u32).to_le_bytes()[..3]),
            Self::B32 => dst.extend_from_slice(&(value as u32).to_le_bytes()),
            Self::B48 => dst.extend_from_slice(&(value as u64).to_le_bytes()[..6]),
            Self::B64 => dst.extend_from_slice(&(value as u64).to_le_bytes()),
        }
    }
    fn get(&self, src: &AlignedVec, offset: usize) -> u64 {
        let mut raw = [0u8; 8];
        raw[..self.len()].copy_from_slice(&src[offset..offset + self.len()]);
        u64::from_le_bytes(raw)
    }
    #[binrw::parser(reader, endian)]
    fn parse_opt() -> BinResult<Option<Varint>> {
        let byte: u8 = <_>::read_options(reader, endian, ())?;
        match byte {
            0 => Ok(None),
            _ => match FromPrimitive::from_u8(byte) {
                Some(varint) => Ok(Some(varint)),
                None => Err(BinError::NoVariantMatch {
                    pos: reader.stream_position()? - 1,
                }),
            },
        }
    }
    #[binrw::writer(writer, endian)]
    fn write_opt(value: &Option<Varint>) -> BinResult<()> {
        value
            .map_or(0, |varint| varint as u8)
            .write_options(writer, endian, ())
    }
}

// Rounds up `offset` to the next multiple of `alignment`, which must be a power
// of 2.  This is equivalent to `offset.next_multiple(alignment)` except for the
// assumption about `alignment` being a power of 2, which allows it to be faster
// and smaller in the case where the compiler can't see the power-of-2 property.
fn next_multiple_of_pow2(offset: usize, alignment: usize) -> usize {
    let mask = alignment - 1;
    (offset + mask) & !mask
}

#[derive(Copy, Clone, Debug)]
pub struct BlockLocation {
    pub offset: u64,
    pub size: usize,
}

impl BlockLocation {
    fn new(offset: u64, size: usize) -> Result<Self, Error> {
        if (offset & 0xfff) != 0 || size < 4096 || size > 1 << 31 || !size.is_power_of_two() {
            Err(CorruptionError::InvalidBlockLocation { offset, size }.into())
        } else {
            Ok(Self { offset, size })
        }
    }
}

impl From<u64> for BlockLocation {
    fn from(source: u64) -> Self {
        Self {
            offset: ((source & !0x1f) << 7),
            size: 1 << (source & 0x1f),
        }
    }
}

impl From<BlockLocation> for u64 {
    fn from(source: BlockLocation) -> Self {
        let shift = source.size.trailing_zeros() as u64;
        (source.offset >> 7) | shift
    }
}

/// Trait for data that can be serialized and deserialized with [`rkyv`].
pub trait Rkyv: Archive + for<'a> Serialize<Serializer<'a>> + Deserializable {}
impl<T> Rkyv for T where T: Archive + for<'a> Serialize<Serializer<'a>> + Deserializable {}

/// Trait for data that can be deserialized with [`rkyv`].
pub trait Deserializable: Archive<Archived = Self::ArchivedDeser> + Sized {
    type ArchivedDeser: Deserialize<Self, Deserializer>;
}
impl<T: Archive> Deserializable for T
where
    Archived<T>: Deserialize<T, Deserializer>,
{
    type ArchivedDeser = Archived<T>;
}

/// The particular [`rkyv::ser::Serializer`] that we use.
pub type Serializer<'a> = AllocSerializer<1024>;

/// The particular [`rkyv`] deserializer that we use.
pub type Deserializer = Infallible;

#[derive(Archive, Serialize)]
struct Item<'a, K, A>(#[with(Inline)] &'a K, #[with(Inline)] &'a A)
where
    K: Rkyv,
    A: Rkyv;

#[cfg(test)]
mod test {
    use std::fs::File;

    use super::{
        reader::Reader,
        writer::{Parameters, Writer2},
    };

    #[test]
    fn test() {
        let mut layer_file =
            Writer2::new(File::create("file.layer").unwrap(), Parameters::default()).unwrap();
        let end = 1000_i32;
        let range = (0..end).step_by(2);
        println!("start");
        let c2range = (0..14_i32).step_by(2);
        let a1 = 0x1111_u64;
        let a2 = 0x2222_u64;
        for i in range.clone() {
            for j in c2range.clone() {
                layer_file.write2((&j, &a2)).unwrap();
            }
            layer_file.write1((&i, &a1)).unwrap();
        }
        println!("written");
        layer_file.close().unwrap();

        let reader = Reader::new(File::open("file.layer").unwrap()).unwrap();
        for i in range.clone() {
            if i % (end / 16) == 0 {
                println!("{i}");
            }
            let mut cursor = reader.rows().first::<i32, u64>().unwrap();
            unsafe { cursor.advance_to_value_or_larger(&i) }.unwrap();
            assert_eq!(unsafe { cursor.item() }, Some((i, a1)));

            let mut cursor = reader.rows().first::<i32, u64>().unwrap();
            unsafe { cursor.advance_to_value_or_larger(&(i - 1)) }.unwrap();
            assert_eq!(unsafe { cursor.item() }, Some((i, a1)));

            for j in c2range.clone() {
                let mut c2cursor = cursor.next_column().first::<i32, u64>().unwrap();
                unsafe { c2cursor.seek_forward_until(|key| key >= &j) }.unwrap();
                assert_eq!(unsafe { c2cursor.item() }, Some((j, a2)));

                let mut c2cursor = cursor.next_column().first::<i32, u64>().unwrap();
                unsafe { c2cursor.advance_to_value_or_larger(&(j - 1)) }.unwrap();
                assert_eq!(unsafe { c2cursor.item() }, Some((j, a2)));
            }
        }

        let mut cursor = reader.rows().first::<i32, u64>().unwrap();
        unsafe { cursor.advance_to_value_or_larger(&end) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, None);

        for i in range.clone() {
            if i % (end / 16) == 0 {
                println!("{i}");
            }
            let mut cursor = reader.rows().first::<i32, u64>().unwrap();
            unsafe { cursor.seek_forward_until(|key| key >= &i) }.unwrap();
            assert_eq!(unsafe { cursor.item() }, Some((i, a1)));

            let mut cursor = reader.rows().first::<i32, u64>().unwrap();
            unsafe { cursor.seek_forward_until(|key| key >= &(i - 1)) }.unwrap();
            assert_eq!(unsafe { cursor.item() }, Some((i, a1)));

            for j in c2range.clone() {
                let mut c2cursor = cursor.next_column().first::<i32, u64>().unwrap();
                unsafe { c2cursor.seek_forward_until(|key| key >= &j) }.unwrap();
                assert_eq!(unsafe { c2cursor.item() }, Some((j, a2)));

                let mut c2cursor = cursor.next_column().first::<i32, u64>().unwrap();
                unsafe { c2cursor.seek_forward_until(|key| key >= &(j - 1)) }.unwrap();
                assert_eq!(unsafe { c2cursor.item() }, Some((j, a2)));
            }
        }
        let mut cursor = reader.rows().first::<i32, u64>().unwrap();
        unsafe { cursor.seek_forward_until(|key| key >= &end) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, None);
    }
}
