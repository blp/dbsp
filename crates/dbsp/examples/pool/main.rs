use anyhow::Result as AnyResult;
use bincode::{
    de::BorrowDecoder,
    enc::Encoder,
    error::{DecodeError, EncodeError},
    BorrowDecode, Decode, Encode,
};
use clap::Parser;
use dbsp::{
    operator::FilterMap, CollectionHandle, DBSPHandle, IndexedZSet, OrdIndexedZSet, OutputHandle,
    RootCircuit, Runtime,
};
use futures::{
    future::{self, Ready},
    prelude::*,
};
use serde::{Deserialize, Serialize};
use size_of::SizeOf;
use std::{
    net::SocketAddr,
    ops::Deref,
    sync::{Arc, Mutex, MutexGuard},
};
use tarpc::{
    context,
    serde_transport::tcp::listen,
    server::{self, incoming::Incoming, Channel},
    tokio_serde::formats::Bincode,
};
use time::Date;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, SizeOf)]
struct WrappedDate(Date);

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
struct Record {
    location: String,
    date: WrappedDate,
    daily_vaccinations: Option<u64>,
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
struct VaxMonthly {
    count: u64,
    year: i32,
    month: u8,
}

#[tarpc::service]
trait Circuit {
    async fn append(records: Vec<(Record, isize)>);
    async fn output() -> Vec<(String, VaxMonthly, isize)>;
    async fn step();
}

#[derive(Debug, Clone, Parser)]
struct Args {
    /// IP address and TCP port to listen, in the form `<ip>:<port>`.  If
    /// `<port>` is `0`, the kernel will pick a free port.
    #[clap(long, default_value = "127.0.0.1:0")]
    address: SocketAddr,

    /// Number of worker threads.
    #[clap(long, default_value = "4")]
    workers: usize,
}

fn build_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<(
    CollectionHandle<Record, isize>,
    OutputHandle<OrdIndexedZSet<String, VaxMonthly, isize>>,
)> {
    let (input_stream, input_handle) = circuit.add_input_zset::<Record, isize>();
    let subset = input_stream.filter(|r| {
        r.location == "England"
            || r.location == "Northern Ireland"
            || r.location == "Scotland"
            || r.location == "Wales"
    });
    let monthly_totals = subset
        .index_with(|r| {
            (
                (r.location.clone(), r.date.year(), r.date.month() as u8),
                r.daily_vaccinations.unwrap_or(0),
            )
        })
        .aggregate_linear(|(_l, _y, _m), v| *v as isize);
    let most_vax = monthly_totals
        .map_index(|((l, y, m), sum)| {
            (
                l.clone(),
                VaxMonthly {
                    count: *sum as u64,
                    year: *y,
                    month: *m,
                },
            )
        })
        .topk_desc(3);
    Ok((input_handle, most_vax.output()))
}

struct Inner {
    circuit: DBSPHandle,
    input_handle: CollectionHandle<Record, isize>,
    output_handle: OutputHandle<OrdIndexedZSet<String, VaxMonthly, isize>>,
}

impl Inner {
    fn new(n_workers: usize) -> AnyResult<Inner> {
        let (circuit, (input_handle, output_handle)) =
            Runtime::init_circuit(n_workers, build_circuit)?;
        Ok(Inner {
            circuit,
            input_handle,
            output_handle,
        })
    }
}

#[derive(Clone)]
struct Server(Arc<Mutex<Inner>>);

impl Server {
    fn new(n_workers: usize) -> AnyResult<Server> {
        Ok(Server(Arc::new(Mutex::new(Inner::new(n_workers)?))))
    }
    fn inner(&self) -> MutexGuard<'_, Inner> {
        self.0.lock().unwrap()
    }
}
impl Circuit for Server {
    type AppendFut = Ready<()>;
    fn append(self, _: context::Context, mut records: Vec<(Record, isize)>) -> Self::AppendFut {
        println!("add {} records", records.len());
        self.inner().input_handle.append(&mut records);
        future::ready(())
    }
    type StepFut = Ready<()>;
    fn step(self, _: context::Context) -> Self::StepFut {
        self.inner().circuit.step().unwrap();
        future::ready(())
    }
    type OutputFut = Ready<Vec<(String, VaxMonthly, isize)>>;
    fn output(self, _: context::Context) -> Self::OutputFut {
        future::ready(self.inner().output_handle.consolidate().iter().collect())
    }
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    let Args { address, workers } = Args::parse();

    let mut listener = listen(address, Bincode::default).await?;
    println!("Listening on port {}", listener.local_addr().port());
    listener.config_mut().max_frame_length(usize::MAX);
    let server = Server::new(workers)?;
    listener
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
        .map(|channel| channel.execute(server.clone().serve()))
        .buffer_unordered(10)
        .for_each(|_| async {})
        .await;

    Ok(())
}
