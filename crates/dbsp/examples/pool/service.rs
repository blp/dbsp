use std::ops::Deref;

use bincode::{Decode, BorrowDecode, Encode, error::{DecodeError, EncodeError}, de::BorrowDecoder, enc::Encoder};
use dbsp::circuit::Layout;
use serde::{Deserialize, Serialize};
use size_of::SizeOf;
use time::Date;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, SizeOf)]
pub struct WrappedDate(Date);

impl Deref for WrappedDate {
    type Target = Date;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Decode for WrappedDate {
    fn decode<D: bincode::de::Decoder>(decoder: &mut D) -> Result<Self, DecodeError> {
        let year = Decode::decode(decoder)?;
        let month: u8 = Decode::decode(decoder)?;
        let day = Decode::decode(decoder)?;
        Ok(WrappedDate(
            Date::from_calendar_date(year, month.try_into().unwrap(), day).unwrap(),
        ))
    }
}
impl<'de> BorrowDecode<'de> for WrappedDate {
    fn borrow_decode<D: BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> core::result::Result<Self, DecodeError> {
        let year = Decode::decode(decoder)?;
        let month: u8 = Decode::decode(decoder)?;
        let day = Decode::decode(decoder)?;
        Ok(WrappedDate(
            Date::from_calendar_date(year, month.try_into().unwrap(), day).unwrap(),
        ))
    }
}

impl Encode for WrappedDate {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        let (year, month, day) = self.to_calendar_date();
        Encode::encode(&year, encoder)?;
        let month = month as u8;
        Encode::encode(&month, encoder)?;
        Encode::encode(&day, encoder)?;
        Ok(())
    }
}

#[derive(
    Clone,
    Debug,
    Decode,
    Deserialize,
    Encode,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    SizeOf,
)]
pub struct Record {
    pub location: String,
    pub date: WrappedDate,
    pub daily_vaccinations: Option<u64>,
}

#[derive(
    Clone,
    Debug,
    Decode,
    Deserialize,
    Encode,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Serialize,
    SizeOf,
)]
pub struct VaxMonthly {
    pub count: u64,
    pub year: i32,
    pub month: u8,
}

#[tarpc::service]
pub trait Circuit {
    async fn init(layout: Layout);
    async fn append(records: Vec<(Record, isize)>);
    async fn output() -> Vec<(String, isize, isize)>;
    async fn step();
}
