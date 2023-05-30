use anyhow::Result;
use csv;
use dbsp::{operator::CsvSource, Circuit, OrdZSet, RootCircuit, ZSet};
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
    let stream = circuit.add_source(CsvSource::from_csv_reader(reader()?));
    stream.inspect(move |record: &OrdZSet<Record, isize>| {
        println!("{}", record.weighted_count());
    });
    Ok(())
}

fn main() -> Result<()> {
    let (_circuit, ()) = RootCircuit::build(build_circuit).unwrap();
    _circuit.step().unwrap();
    Ok(())
}
