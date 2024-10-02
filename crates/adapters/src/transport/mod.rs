#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unreachable_code)]
#![allow(dead_code)]
//! Data transports.
//!
//! Data transport adapters implement support for a specific streaming
//! technology like Kafka.  A transport adapter carries data without
//! interpreting it (data interpretation is the job of **data format** adapters
//! found in [dbsp_adapters::format](crate::format)).
//!
//! Both input and output data transport adapters exist.  Some transports
//! have both input and output variants, and others only have one.
//!
//! Data transports are created and configured through Yaml, with a string name
//! that designates a transport and a transport-specific Yaml object to
//! configure it.  Transport configuration is encapsulated in
//! [`dbsp_adapters::TransportConfig`](crate::TransportConfig).
//!
//! To obtain a transport, create an endpoint with it, and then start reading it
//! from the beginning:
//!
//! ```ignore
//! let endpoint = input_transport_config_to_endpoint(config.clone());
//! let reader = endpoint.open(consumer, 0);
//! ```
use crate::{ParseError, Parser};
use anyhow::{Error as AnyError, Result as AnyResult};
use dyn_clone::DynClone;
#[cfg(feature = "with-pubsub")]
use pubsub::PubSubInputEndpoint;
use serde_json::Value as JsonValue;
use std::sync::atomic::AtomicU64;

mod file;
pub mod http;

pub mod url;

mod datagen;

mod s3;
mod secret_resolver;

#[cfg(feature = "with-kafka")]
pub(crate) mod kafka;

#[cfg(feature = "with-nexmark")]
mod nexmark;

#[cfg(feature = "with-pubsub")]
mod pubsub;

use crate::catalog::InputCollectionHandle;
use feldera_types::config::TransportConfig;
use feldera_types::program_schema::Relation;

use crate::transport::datagen::GeneratorEndpoint;
use crate::transport::file::{FileInputEndpoint, FileOutputEndpoint};
#[cfg(feature = "with-kafka")]
use crate::transport::kafka::{KafkaInputEndpoint, KafkaOutputEndpoint};
#[cfg(feature = "with-nexmark")]
use crate::transport::nexmark::NexmarkEndpoint;
use crate::transport::s3::S3InputEndpoint;
use crate::transport::url::UrlInputEndpoint;

/// Step number for fault-tolerant input and output.
///
/// A [fault-tolerant](crate#fault-tolerance) data transport divides input into
/// steps numbered sequentially.  The first step is numbered zero.  If a given
/// step is read multiple times, it will have the same content as the first
/// time.
///
/// The step number increases by 1 each time the circuit runs; that is, it
/// tracks the global clock for the outermost circuit.
pub type Step = u64;

/// Atomic version of [`Step`].
pub type AtomicStep = AtomicU64;

/// Creates an input transport endpoint instance using an input transport configuration.
///
/// Returns an error if there is a invalid configuration for the endpoint.
/// Returns `None` if the transport configuration variant is incompatible with an input endpoint.
pub fn input_transport_config_to_endpoint(
    config: TransportConfig,
) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {
    match config {
        TransportConfig::FileInput(config) => Ok(Some(Box::new(FileInputEndpoint::new(config)))),
        #[cfg(feature = "with-kafka")]
        TransportConfig::KafkaInput(config) => match config.fault_tolerance {
            None => Ok(Some(Box::new(KafkaInputEndpoint::new(config)?))),
            Some(_) => todo!(),
        },
        #[cfg(not(feature = "with-kafka"))]
        TransportConfig::KafkaInput(_) => Ok(None),
        #[cfg(feature = "with-pubsub")]
        TransportConfig::PubSubInput(config) => {
            Ok(Some(Box::new(PubSubInputEndpoint::new(config.clone())?)))
        }
        #[cfg(not(feature = "with-pubsub"))]
        TransportConfig::PubSubInput(_) => Ok(None),
        TransportConfig::UrlInput(config) => Ok(Some(Box::new(UrlInputEndpoint::new(config)))),
        TransportConfig::S3Input(config) => Ok(Some(Box::new(S3InputEndpoint::new(config)))),
        TransportConfig::Datagen(config) => {
            Ok(Some(Box::new(GeneratorEndpoint::new(config.clone()))))
        }
        #[cfg(feature = "with-nexmark")]
        TransportConfig::Nexmark(config) => {
            Ok(Some(Box::new(NexmarkEndpoint::new(config.clone()))))
        }
        #[cfg(not(feature = "with-nexmark"))]
        TransportConfig::Nexmark(_) => Ok(None),
        TransportConfig::FileOutput(_)
        | TransportConfig::KafkaOutput(_)
        | TransportConfig::DeltaTableInput(_)
        | TransportConfig::DeltaTableOutput(_)
        | TransportConfig::HttpInput
        | TransportConfig::HttpOutput => Ok(None),
    }
}

/// Creates an output transport endpoint instance using an output transport configuration.
///
/// Returns an error if there is a invalid configuration for the endpoint.
/// Returns `None` if the transport configuration variant is incompatible with an output endpoint.
pub fn output_transport_config_to_endpoint(
    config: TransportConfig,
) -> AnyResult<Option<Box<dyn OutputEndpoint>>> {
    match config {
        TransportConfig::FileOutput(config) => Ok(Some(Box::new(FileOutputEndpoint::new(config)?))),
        #[cfg(feature = "with-kafka")]
        TransportConfig::KafkaOutput(config) => match config.fault_tolerance {
            None => Ok(Some(Box::new(KafkaOutputEndpoint::new(config)?))),
            Some(_) => todo!(),
        },
        _ => Ok(None),
    }
}

/// A configured input endpoint.
///
/// Input endpoints come in two flavors:
///
/// * A [fault-tolerant](crate#fault-tolerance) endpoint divides its input into
///   numbered steps.  A given step always contains the same data if it is read
///   more than once.
///
/// * A non-fault-tolerant endpoint does not have a concept of steps and need
///   not yield the same data each time it is read.
pub trait InputEndpoint: Send {
    /// Whether this endpoint is [fault tolerant](crate#fault-tolerance).
    fn is_fault_tolerant(&self) -> bool;

    /// For a fault-tolerant endpoint, notifies the endpoint that steps less
    /// than `step` aren't needed anymore.  It may optionally discard them.
    ///
    /// This is a no-op for non-fault-tolerant endpoints.
    fn expire(&self, _step: Step) {}
}

pub trait TransportInputEndpoint: InputEndpoint {
    /// Returns an [`InputReader`] for reading the endpoint's data.  For a
    /// fault-tolerant endpoint, `step` indicates the first step to be read; for
    /// a non-fault-tolerant endpoint, it is ignored.
    ///
    /// Data and status will be passed to `consumer`.
    ///
    /// The reader is initially paused.  The caller may call
    /// [`InputReader::start`] to start reading.
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        start_step: Option<InputStep>,
        schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>>;
}

pub trait IntegratedInputEndpoint: InputEndpoint {
    fn open(
        &self,
        input_handle: &InputCollectionHandle,
        start_step: Step,
    ) -> AnyResult<Box<dyn InputReader>>;
}

pub enum InputStep {}

#[derive(Debug)]
pub enum InputReaderCommand {
    Seek(JsonValue),

    /// Tells the input reader to replay the step described in `metadata` by
    /// calling [InputConsumer:;queue] to report data and errors, and then
    /// [InputConsumer::replay_complete] to signal completion.
    ///
    /// The input reader doesn't have to process other calls while it does the
    /// replay.
    ///
    /// # State
    ///
    /// The controller will not call this function on a given reader after any
    /// calling any of the other functions.
    Replay(JsonValue),

    /// Tells the input reader to accept further input, starting from:
    ///
    /// - If [InputReader::replay] was previously called, then just beyond the
    ///   end of the data from the last call.
    ///
    /// - Otherwise, from the beginning of the input.
    ///
    /// The input reader should pass new data to [InputConsumer::queue] as it
    /// becomes available.
    ///
    /// # State
    ///
    /// The controller will not call this function:
    ///
    /// - Twice on a given reader without an intervening call to
    ///   [InputReader::finish_extend].
    ///
    /// - If it requested a replay (with [InputReader::replay]) and the reader
    ///   hasn't yet reported that the replay is complete.
    ///
    /// - If it requested the reader to finish extending (with
    ///   [InputReader::finish_extend]) and the reader hasn't yet reported
    ///   that extension is complete (with [InputConsumer::extend_complete]).
    Extend,

    Pause,

    /// Tells the reader to finish up accepting further input for now. The
    /// reader may call [InputConsumer::queue] some more times if necessary,
    /// and then call [InputConsumer::extend_complete] to indicate that it
    /// finished.
    ///
    /// # State
    ///
    /// The controller will call this at most once sometime after each call to
    /// [InputReader::start_extend].
    Queue,

    /// Tells the reader it's going to be dropped soon and should clean up.
    ///
    /// The reader can continue to queue some data buffers afterward if that's
    /// the easiest implementation.
    ///
    /// # State
    ///
    /// The controller calls this only once and won't call any other functions
    /// for a given reader after it calls this one.
    Disconnect,
}

/// Reads data from an endpoint.
///
/// Use [`TransportInputEndpoint::open`] to obtain an [`InputReader`].
pub trait InputReader: Send {
    fn request(&self, command: InputReaderCommand);

    fn seek(&self, metadata: JsonValue) {
        self.request(InputReaderCommand::Seek(metadata));
    }

    fn replay(&self, metadata: JsonValue) {
        self.request(InputReaderCommand::Replay(metadata));
    }

    fn extend(&self) {
        self.request(InputReaderCommand::Extend);
    }

    fn pause(&self) {
        self.request(InputReaderCommand::Pause);
    }

    fn queue(&self) {
        self.request(InputReaderCommand::Queue);
    }

    fn disconnect(&self) {
        self.request(InputReaderCommand::Disconnect);
    }
}

/// Input stream consumer.
///
/// A transport endpoint pushes binary data downstream via an instance of this
/// trait.
pub trait InputConsumer: Send + Sync + DynClone {
    fn max_batch_size(&self) -> usize;
    fn parse_errors(&self, errors: Vec<ParseError>);
    fn buffered(&self, num_records: usize, num_bytes: usize);
    fn replayed(&self, num_records: usize);
    fn extended(&self, num_records: usize, metadata: JsonValue);

    /// Reports that the endpoint has reached end of input and that no more data
    /// will be received from the endpoint.
    ///
    /// If the endpoint has already indicated that it has buffered records then
    /// the controller will request them in future [InputReaderCommand::Queue]
    /// messages. The endpoint must not make further calls to
    /// [InputConsumer::buffered] or [InputConsumer::parse_errors].
    fn eoi(&self);

    /// Endpoint failed.
    ///
    /// Reports that the endpoint failed and that it will not queue any more
    /// data.
    fn error(&self, fatal: bool, error: AnyError);
}

dyn_clone::clone_trait_object!(InputConsumer);

pub type AsyncErrorCallback = Box<dyn Fn(bool, AnyError) + Send + Sync>;

/// A configured output transport endpoint.
///
/// Output endpoints come in two flavors:
///
/// * A [fault-tolerant](crate#fault-tolerance) endpoint accepts output that has
///   been divided into numbered steps.  If it is given output associated with a
///   step number that has already been output, then it discards the duplicate.
///   It must also keep data written to the output transport from becoming
///   visible to downstream readers until `batch_end` is called.  (This works
///   for output to Kafka, which supports transactional output.  If it is
///   difficult for some future fault-tolerant output endpoint, then the API
///   could be adjusted to support writing output only after it can become
///   immediately visible.)
///
/// * A non-fault-tolerant endpoint does not have a concept of steps and ignores
///   them.
pub trait OutputEndpoint: Send {
    /// Finishes establishing the connection to the output endpoint.
    ///
    /// If the endpoint encounters any errors during output, now or later, it
    /// invokes `async_error_callback` to notify the client about asynchronous
    /// errors, i.e., errors that happen outside the context of the
    /// [`OutputEndpoint::push_buffer`] method. For instance, a reliable message
    /// bus like Kafka may notify the endpoint about a failure to deliver a
    /// previously sent message via an async callback. If the endpoint is unable
    /// to handle this error, it must forward it to the client via the
    /// `async_error_callback`.  The first argument of the callback is a flag
    /// that indicates a fatal error that the endpoint cannot recover from.
    fn connect(&mut self, async_error_callback: AsyncErrorCallback) -> AnyResult<()>;

    /// Maximum buffer size that this transport can transmit.
    /// The encoder should not generate buffers exceeding this size.
    fn max_buffer_size_bytes(&self) -> usize;

    /// Notifies the output endpoint that data subsequently written by
    /// `push_buffer` belong to the given `step`.
    ///
    /// A [fault-tolerant](crate#fault-tolerance) endpoint has additional
    /// requirements:
    ///
    /// 1. If data for the given step has been written before, the endpoint
    ///    should discard it.
    ///
    /// 2. The output batch must not be made visible to downstream readers
    ///    before the next call to `batch_end`.
    fn batch_start(&mut self, _step: Step) -> AnyResult<()> {
        Ok(())
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()>;

    /// Output a message consisting of a key/value pair, with optional value.
    ///
    /// This API is implemented by Kafka and other transports that transmit
    /// messages consisting of key and value fields and in invoked by
    /// Kafka-specific data formats that rely on this message structure,
    /// e.g., Debezium. If a given transport does not implement this API, it
    /// should return an error.
    fn push_key(&mut self, key: &[u8], val: Option<&[u8]>) -> AnyResult<()>;

    /// Notifies the output endpoint that output for the current step is
    /// complete.
    ///
    /// A fault-tolerant output endpoint may now make the output batch visible
    /// to readers.
    fn batch_end(&mut self) -> AnyResult<()> {
        Ok(())
    }

    /// Whether this endpoint is [fault tolerant](crate#fault-tolerance).
    fn is_fault_tolerant(&self) -> bool;
}
