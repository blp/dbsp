use std::net::SocketAddr;

use anyhow::Result as AnyResult;
use clap::Parser;
use csv::Reader;
use serde::{Deserialize, Serialize};
use size_of::SizeOf;
use tarpc::{client, context, tokio_serde::formats::Bincode};
use time::Date;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, SizeOf)]
struct Record {
    location: String,
    date: Date,
    daily_vaccinations: Option<u64>,
}

#[tarpc::service]
trait Circuit {
    async fn append(records: Vec<(Record, isize)>);
    async fn output() -> Vec<(String, VaxMonthly, isize)>;
    async fn step();
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, SizeOf)]
struct VaxMonthly {
    count: u64,
    year: i32,
    month: u8,
}

#[derive(Debug, Clone, Parser)]
struct Args {
    /// IP addresses and TCP ports of the pool nodes.
    #[clap(long, required(true))]
    pool: Vec<SocketAddr>,
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    let Args { pool } = Args::parse();

    let mut clients: Vec<_> = Vec::new();
    for server_addr in pool {
        let mut transport = tarpc::serde_transport::tcp::connect(server_addr, Bincode::default);
        transport.config_mut().max_frame_length(usize::MAX);

        clients.push(CircuitClient::new(client::Config::default(), transport.await?).spawn());
    }

    let path = format!(
        "{}/examples/tutorial/vaccinations.csv",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut reader = Reader::from_path(path)?;
    let mut input_records = reader.deserialize();
    let mut i = 0;
    loop {
        let client = &clients[i];
        i = (i + 1) % clients.len();

        let mut batch = Vec::new();
        while batch.len() < 500 {
            let Some(record) = input_records.next() else { break };
            batch.push((record?, 1));
        }
        if batch.is_empty() {
            break;
        }
        println!("Input {} records to {i}:", batch.len());
        client.append(context::current(), batch).await?;

        client.step(context::current()).await.unwrap();

        let output = client.output(context::current()).await.unwrap();
        output
            .iter()
            .for_each(|(l, VaxMonthly { count, year, month }, w)| {
                println!("   {l:16} {year}-{month:02} {count:10}: {w:+}")
            });
        println!();
    }

    Ok(())
}
