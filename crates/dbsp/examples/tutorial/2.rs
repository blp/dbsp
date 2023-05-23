use anyhow::Result;
use csv;
use dbsp::RootCircuit;
use serde::Deserialize;
use std::fs::File;
use time::Date;

#[derive(Debug, Deserialize)]
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
    total_vaccinations_per_hundred: Option<f64>,
    people_vaccinated_per_hundred: Option<f64>,
    people_fully_vaccinated_per_hundred: Option<f64>,
    total_boosters_per_hundred: Option<f64>,
    daily_vaccinations_per_million: Option<f64>,
    daily_people_vaccinated: Option<f64>,
    daily_people_vaccinated_per_hundred: Option<f64>,
}

fn reader() -> Result<csv::Reader<File>> {
    Ok(csv::Reader::from_path(std::env::args().nth(1).unwrap())?)
}

fn main() -> Result<()> {
    for result in reader()?.deserialize() {
        let record: Record = result?;
        println!("{:?}", record);
        break;
    }
    Ok(())
}
