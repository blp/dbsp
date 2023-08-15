use anyhow::Result;
use chrono::NaiveDate;
use csv::Reader;
use dbsp::{CollectionHandle, RootCircuit, ZSet};
use rkyv::{Archive, Serialize};
use size_of::SizeOf;

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    SizeOf,
    Archive,
    Serialize,
    rkyv::Deserialize,
    serde::Deserialize,
)]
struct Record {
    location: String,
    date: NaiveDate,
    daily_vaccinations: Option<u64>,
}
fn build_circuit(circuit: &mut RootCircuit) -> Result<CollectionHandle<Record, isize>> {
    let (input_stream, input_handle) = circuit.add_input_zset::<Record, isize>();
    input_stream.inspect(|records| {
        println!("{}", records.weighted_count());
    });
    // ...populate `circuit` with more operators...
    Ok(input_handle)
}

fn main() -> Result<()> {
    // Build circuit.
    let (circuit, input_handle) = RootCircuit::build(build_circuit)?;

    // Feed data into circuit.
    let path = format!(
        "{}/examples/tutorial/vaccinations.csv",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut input_records = Reader::from_path(path)?
        .deserialize()
        .map(|result| result.map(|record| (record, 1)))
        .collect::<Result<Vec<(Record, isize)>, _>>()?;
    input_handle.append(&mut input_records);

    // Execute circuit.
    circuit.step()?;

    // ...read output from circuit...
    Ok(())
}
/*
struct AsCalendarDate;

impl ArchiveWith<Date> for AsCalendarDate {
    type Archived = Archived<(i32, u16)>;
    type Resolver = Resolver<(i32, u16)>;

    unsafe fn resolve_with(field: &Date, pos: usize, _: ((), ()), out: *mut Self::Archived) {
        let ord = field.to_ordinal_date();
        ord.resolve(pos, ((), ()), out);
    }
}

impl<S: Fallible + ?Sized> SerializeWith<Date, S> for AsCalendarDate
where
    i32: Serialize<S>,
{
    fn serialize_with(field: &Date, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let ord = field.to_ordinal_date();
        ord.serialize(serializer)
    }
}

impl<D: Fallible + ?Sized> DeserializeWith<Archived<(i32, u16)>, Date, D> for AsCalendarDate
where
    Archived<i32>: rkyv::Deserialize<i32, D>,
{
    fn deserialize_with(
        field: &Archived<(i32, u16)>,
        deserializer: &mut D,
    ) -> Result<Date, D::Error> {
        let (year, ordinal) = field.deserialize(deserializer)?;
        Ok(Date::from_ordinal_date(year, ordinal).unwrap())
    }
}
*/
