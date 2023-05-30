use anyhow::Result;
use csv;
use dbsp::{
    operator::{time_series::RelOffset, time_series::RelRange, CsvSource, FilterMap},
    trace::BatchReader,
    Circuit, IndexedZSet, OrdZSet, RootCircuit, ZSet,
};
use ordered_float::OrderedFloat;
use serde::Deserialize;
use size_of::SizeOf;
use std::fs::File;
use time::Date;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash, SizeOf)]
struct Record {
    location: String,
    iso_code: String,
    date: Date,
    total_vaccinations: Option<u64>,
    people_vaccinated: Option<u64>,
    people_fully_vaccinated: Option<u64>,
    total_boosters: Option<u64>,
    daily_vaccinations_raw: Option<u64>,
    daily_vaccinations: Option<u64>,
    total_vaccinations_per_hundred: Option<OrderedFloat<f64>>,
    people_vaccinated_per_hundred: Option<OrderedFloat<f64>>,
    people_fully_vaccinated_per_hundred: Option<OrderedFloat<f64>>,
    total_boosters_per_hundred: Option<OrderedFloat<f64>>,
    daily_vaccinations_per_million: Option<OrderedFloat<f64>>,
    daily_people_vaccinated: Option<OrderedFloat<f64>>,
    daily_people_vaccinated_per_hundred: Option<OrderedFloat<f64>>,
}

fn reader() -> Result<csv::Reader<File>> {
    Ok(csv::Reader::from_path(std::env::args().nth(1).unwrap())?)
}

fn build_circuit(circuit: &mut RootCircuit) -> Result<()> {
    circuit
        .add_source(CsvSource::<_, Record, isize, OrdZSet<_, _>>::from_csv_reader(reader()?))
        .filter(|r| {
            r.location == "England"
                || r.location == "Northern Ireland"
                || r.location == "Scotland"
                || r.location == "Wales"
        })
        .index_with(|r| {
            (
                (r.location.clone(), r.date.year(), r.date.month() as u8),
                r.daily_vaccinations.unwrap_or(0),
            )
        })
        .aggregate_linear(|_k, &v| v as isize)
        .map_index(|(k, &v)| (k.0.clone(), (k.1 * 12 + (k.2 as i32 - 1), v)))
        .inspect(|data| {
            data.iter()
                .for_each(|(k, (date, v), w)| println!("monthly {k:16} {}-{:02} {v}: {w:+}", date / 12, date % 12 + 1))
        })
        .partitioned_rolling_aggregate_linear(
            |&v| v,
            |x| x,
            RelRange::new(RelOffset::Before(0), RelOffset::Before(0)),
        )
        .inspect(|data| {
            data.iter()
                .for_each(|(k, (date, v), w)| println!("moving  {k:16} {}-{:02} {}: {w:+}", date / 12, date % 12 + 1, v.unwrap()))
        });
    Ok(())
}

fn main() -> Result<()> {
    let (circuit, ()) = RootCircuit::build(build_circuit).unwrap();
    circuit.step().unwrap();
    Ok(())
}
