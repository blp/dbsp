# Developer Tutorial

This tutorial aims to be a start for teaching Rust developers how to
use DBSP in their projects.

DBSP computations work in terms of "circuits", so in the tutorial
we'll build a circuit.  There are three basic steps to using a
circuit: getting input in, processing the data, and getting the output
out.  Let's look at each of them in order.

## Input

To process data in DBSP, we need to get data from somewhere.  DBSP has
input adapters for a number of formats and transports.  In a real
application, we'd probably want to use a transport that can continue
to pull data over time from some source.  For an application that
wants to allow a user to configure the source, there's the
`dbsp_adapters` crate in `crates/adapters`.  This crate is overkill
for just a tutorial.

Instead, let's just parse some data from a CSV file.  We'll first
parse it on its own, then bring it into a circuit.

### CSV parsing

Let's work with the [Our World in Data](https://ourworldindata.org/)
public-domain dataset on COVID-19 vaccinations, which is available on
Github.  Its main data file on vaccinations is `vaccinations.csv`,
which contains about 168,000 rows of data with 16 columns per row.
Download a snapshot from a fixed point in the history so that your
results can exactly match the ones shown here:

```
$ wget https://github.com/owid/covid-19-data/blob/88ab53d1081ef7651b16212658ea43bd175d572a/public/data/vaccinations/vaccinations.csv
```

If you look at the first few lines, you can see the basic structure:

```csv
location,iso_code,date,total_vaccinations,people_vaccinated,people_fully_vaccinated,total_boosters,daily_vaccinations_raw,daily_vaccinations,total_vaccinations_per_hundred,people_vaccinated_per_hundred,people_fully_vaccinated_per_hundred,total_boosters_per_hundred,daily_vaccinations_per_million,daily_people_vaccinated,daily_people_vaccinated_per_hundred
Afghanistan,AFG,2021-02-22,0,0,,,,,0.0,0.0,,,,,
...
```

Afghanistan, the first country in the file, has a lot of missing data,
so let's look at some other data to better understand the format (the
use of `shuf` below means you won't see exactly the same output):

```sh
$ shuf vaccinations.csv | head -2
Saudi Arabia,SAU,2021-08-04,28583042,19516714,9066328,,323861,387149,78.51,53.6,24.9,,10633,152180,0.418
Italy,ITA,2023-03-14,143822055,50894762,47967218,46962487,1578,1182,243.61,86.21,81.25,79.55,20,32,0.0
```

A little extra use of shell utilities helps more.  I reran the
following a few times until I got a record that had every field filled
in (`<(...)` is a `bash` extension):

```sh
$ paste <(head -1 vaccinations.csv | sed 's/,/\n/g') <(shuf vaccinations.csv | head -1 | sed 's/,/\n/g')
location        Dominican Republic
iso_code        DOM
date    2021-09-26
total_vaccinations      11798933
people_vaccinated       6032542
people_fully_vaccinated 4867470
total_boosters  898921
daily_vaccinations_raw  5440
daily_vaccinations      23075
total_vaccinations_per_hundred  105.08
people_vaccinated_per_hundred   53.72
people_fully_vaccinated_per_hundred     43.35
total_boosters_per_hundred      8.01
daily_vaccinations_per_million  2055
daily_people_vaccinated 8024
daily_people_vaccinated_per_hundred     0.071
```

This is good enough to read the file.  We can combine Rust's `csv`
crate to read CSV files with `serde` for deserializing into a `struct`
and `time` for parsing the date field like this:

```rust
use anyhow::Result;
use csv;
use serde::Deserialize;
use std::fs::File;
use time::Date;

#[derive(Debug,Deserialize)]
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
```

If we run this, passing the location of `vaccinations.csv` as the
first command line argument, then it prints the first record in
`Debug` format.  Here it is, reflowed to 80 columns to make it easier
to read:

```text
Record { location: "Afghanistan", iso_code: "AFG", date: 2021-02-22,
total_vaccinations: Some(0), people_vaccinated: Some(0),
people_fully_vaccinated: None, total_boosters: None,
daily_vaccinations_raw: None, daily_vaccinations: None,
total_vaccinations_per_hundred: Some(0.0),
people_vaccinated_per_hundred: Some(0.0),
people_fully_vaccinated_per_hundred: None, total_boosters_per_hundred:
None, daily_vaccinations_per_million: None, daily_people_vaccinated:
None, daily_people_vaccinated_per_hundred: None }
```

### Reading CSV into a DBSP circuit

`RootCircuit::build()` is the basic way to create a circuit in DBSP.
A no-op use of it looks like this:

```rust
use dbsp::RootCircuit;

fn main() {
    let (_circuit, ()) = RootCircuit::build::<(), _, _>(|_circuit| {
        Ok(())
    }).unwrap();
}
```

Our circuit can read a CSV file using the `CsvSource` operator, like
shown below, reusing the definition of `Record` and `reader()` from
before.  The type annotation here will not be necessary later:

```rust
use dbsp::{operator::CsvSource, Circuit, OrdZSet, RootCircuit};

fn build_circuit(circuit: &mut RootCircuit) -> Result<()> {
    circuit.add_source(CsvSource::<_, Record, isize, OrdZSet<_, _>>::from_csv_reader(reader()?));
    Ok(())
}

fn main() -> Result<()> {
    let (_circuit, ()) = RootCircuit::build(build_circuit).unwrap();
    Ok(())
}
```

The compiler will point out a problem: `Record` lacks a pile of traits
required for the record type of the "Z-sets" (see
`dbsp::algebra::zset::ZSet`).

We can add them:
```rust
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash, SizeOf)]
```
but this yields several warnings similar to the following:
```text
error[E0277]: the trait bound `f64: Eq` is not satisfied
   --> crates/dbsp/examples/tutorial/main.rs:21:5
    |
10  | #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash, SizeOf)]
    |                                     -- in this derive macro expansion
...
21  |     total_vaccinations_per_hundred: Option<f64>,
    |     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ the trait `Eq` is not implemented for `f64`
```

This is because `f64` (and `f32`) doesn't implement `Eq` (or `Ord`)
due to the peculiarities of floating-point arithmetic.  We can fix it
with `OrderedFloat`, which implements ordering on top of
floating-point types.  The full program is now:

```rust
use anyhow::Result;
use csv;
use dbsp::{operator::CsvSource, Circuit, OrdZSet, RootCircuit};
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
    circuit.add_source(CsvSource::<_, Record, isize, OrdZSet<_, _>>::from_csv_reader(reader()?));
    Ok(())
}

fn main() -> Result<()> {
    let (_circuit, ()) = RootCircuit::build(build_circuit).unwrap();
    Ok(())
}
```

You can run it, passing the path to `vaccinations.csv`, but it won't
do anything, for two reasons.  First, although our circuit now has a
source, which would read our input file if it runs, we're not adding
anything to process the data, so it would get read and then discarded.
Second, although we build a circuit, we don't run it, so even reading
the data doesn't happen.

Let's change both of those.  To something with the data, we need to
work with the `Stream` that `Circuit::add_source` returns.  `Stream`
methods are the main way to do computation in DBSP.  Let's call
`Stream::inspect` on the stream.  This method takes a closure that it
calls, on each clock tick, with the data that passes through.  Let's
just have it print the number of rows in the Z-set, like this:

```rust
fn build_circuit(circuit: &mut RootCircuit) -> Result<()> {
    circuit
        .add_source(CsvSource::<_, Record, isize, OrdZSet<_, _>>::from_csv_reader(reader()?))
        .inspect(|data| println!("{}", data.weighted_count()));
    Ok(())
}
```

There's still nothing that happens if you run it now, because we're
still not running the circuit.  To run it a single step, we insert a
call to `CircuitHandle::step()`:

```rust
fn main() -> Result<()> {
    let (circuit, ()) = RootCircuit::build(build_circuit).unwrap();
    circuit.step().unwrap();
    Ok(())
}
```

Now, if we run it, it prints `168662`, the number of records in
`vaccinations.csv`.  That's because `CsvSource` read an entire CSV
file and feeds it as input in a single step.  That means that running
for more steps wouldn't make a difference.  That's not a normal use
case for DBSP but it does make a reasonable setup for a tutorial.

## Processing

We've got the data now.  Let's write a few queries on it to see how
that would work.

### Selecting a subset

This is a lot of data.  Suppose we want to pick out a subset of the
records.  We can use the `FilterMap` trait implemented for `Stream` to
do that.  For example, we can take just the records for locations in
the United Kingdom:

```
fn build_circuit(circuit: &mut RootCircuit) -> Result<()> {
    circuit
        .add_source(CsvSource::<_, Record, isize, OrdZSet<_, _>>::from_csv_reader(reader()?))
        .filter(|r| {
            r.location == "England"
                || r.location == "Northern Ireland"
                || r.location == "Scotland"
                || r.location == "Wales"
        })
        .inspect(|data| println!("{}", data.weighted_count()));
    Ok(())
}
```

This prints 3083, showing that we did select a subset of the 168662
total records.

### Counting

