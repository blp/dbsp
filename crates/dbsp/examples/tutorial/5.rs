use anyhow::Result;
use csv;
use dbsp::{
    operator::{CsvSource, FilterMap},
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
        .inspect(|data| {
            data.iter()
                .for_each(|(k, v, w)| println!("{:16} {}-{:02} {v:10}: {w:+}", k.0, k.1, k.2))
        });
    Ok(())
}

fn main() -> Result<()> {
    let (circuit, ()) = RootCircuit::build(build_circuit).unwrap();
    circuit.step().unwrap();
    Ok(())
}
