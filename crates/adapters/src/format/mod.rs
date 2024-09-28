use crate::catalog::{InputCollectionHandle, SerBatchReader};
use crate::format::parquet::{ParquetInputFormat, ParquetOutputFormat};
use crate::{transport::Step, ControllerError};
use actix_web::HttpRequest;
use anyhow::Result as AnyResult;
#[cfg(feature = "with-avro")]
use avro::input::AvroInputFormat;
use erased_serde::Serialize as ErasedSerialize;
use feldera_types::config::ConnectorConfig;
use feldera_types::program_schema::Relation;
use feldera_types::serde_with_context::FieldParseError;
use once_cell::sync::Lazy;
use serde::Serialize;
use serde_yaml::Value as YamlValue;
use std::ops::Range;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    error::Error as StdError,
    fmt::{Display, Error as FmtError, Formatter},
    fs::File,
    io::{Error as IoError, Read},
};

#[cfg(feature = "with-avro")]
pub(crate) mod avro;
pub(crate) mod csv;
mod json;
pub mod parquet;

#[cfg(feature = "with-avro")]
use crate::format::avro::output::AvroOutputFormat;
pub use parquet::relation_to_parquet_schema;

pub use self::csv::{byte_record_deserializer, string_record_deserializer};
use self::{
    csv::{CsvInputFormat, CsvOutputFormat},
    json::{JsonInputFormat, JsonOutputFormat},
};

/// The largest weight of a record that can be output using
/// a format without explicit weights. Such formats require
/// duplicating the record `w` times, which is expensive
/// for large weights (and is most likely not what the user
/// intends).
pub const MAX_DUPLICATES: i64 = 1_000_000;

/// When including a long JSON record in an error message,
/// truncate it to `MAX_RECORD_LEN_IN_ERRMSG` bytes.
const MAX_RECORD_LEN_IN_ERRMSG: usize = 4096;

/// Error parsing input data.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(transparent)]
// Box the internals of `ParseError` to avoid
// "Error variant to large" clippy warnings".
pub struct ParseError(Box<ParseErrorInner>);
impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        self.0.fmt(f)
    }
}

impl StdError for ParseError {}

impl ParseError {
    pub fn new(
        description: String,
        event_number: Option<u64>,
        field: Option<String>,
        invalid_text: Option<&str>,
        invalid_bytes: Option<&[u8]>,
        suggestion: Option<Cow<'static, str>>,
    ) -> Self {
        Self(Box::new(ParseErrorInner::new(
            description,
            event_number,
            field,
            invalid_text,
            invalid_bytes,
            suggestion,
        )))
    }

    pub fn text_event_error<E>(
        msg: &str,
        error: E,
        event_number: u64,
        invalid_text: Option<&str>,
        suggestion: Option<Cow<'static, str>>,
    ) -> Self
    where
        E: ToString,
    {
        Self(Box::new(ParseErrorInner::text_event_error(
            msg,
            error,
            event_number,
            invalid_text,
            suggestion,
        )))
    }

    pub fn text_envelope_error(
        description: String,
        invalid_text: &str,
        suggestion: Option<Cow<'static, str>>,
    ) -> Self {
        Self(Box::new(ParseErrorInner::text_envelope_error(
            description,
            invalid_text,
            suggestion,
        )))
    }

    pub fn bin_event_error(
        description: String,
        event_number: u64,
        invalid_bytes: &[u8],
        suggestion: Option<Cow<'static, str>>,
    ) -> Self {
        Self(Box::new(ParseErrorInner::bin_event_error(
            description,
            event_number,
            invalid_bytes,
            suggestion,
        )))
    }

    pub fn bin_envelope_error(
        description: String,
        invalid_bytes: &[u8],
        suggestion: Option<Cow<'static, str>>,
    ) -> Self {
        Self(Box::new(ParseErrorInner::bin_envelope_error(
            description,
            invalid_bytes,
            suggestion,
        )))
    }
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ParseErrorInner {
    /// Error description.
    description: String,

    /// Event number relative to the start of the stream.
    ///
    /// An input stream is a series data change events (row insertions,
    /// deletions, and updates).  This field specifies the index (starting
    /// from 1) of the event that caused the error, relative to the start of
    /// the stream.  In some cases this index cannot be identified, e.g., if
    /// the error makes an entire block of events unparseable.
    event_number: Option<u64>,

    /// Field that failed to parse.
    ///
    /// Only set when the parsing error can be attributed to a
    /// specific field.
    field: Option<String>,

    /// Invalid fragment of input data.
    ///
    /// Used for binary data formats and for text-based formats when the input
    /// is not valid UTF-8 string.
    invalid_bytes: Option<Vec<u8>>,

    /// Invalid fragment of the input text.
    ///
    /// Only used for text-based formats and in cases when input is valid UTF-8.
    invalid_text: Option<String>,

    /// Any additional information that may help fix the problem, e.g., example
    /// of a valid input.
    suggestion: Option<Cow<'static, str>>,
}

impl Display for ParseErrorInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        let event = if let Some(event_number) = self.event_number {
            format!(" (event #{})", event_number)
        } else {
            String::new()
        };

        let invalid_fragment = if let Some(invalid_bytes) = &self.invalid_bytes {
            format!("\nInvalid bytes: {invalid_bytes:?}")
        } else if let Some(invalid_text) = &self.invalid_text {
            format!("\nInvalid fragment: '{invalid_text}'")
        } else {
            String::new()
        };

        let suggestion = if let Some(suggestion) = &self.suggestion {
            format!("\n{suggestion}")
        } else {
            String::new()
        };

        write!(
            f,
            "Parse error{event}: {}{invalid_fragment}{suggestion}",
            self.description
        )
    }
}

impl ParseErrorInner {
    pub fn new(
        description: String,
        event_number: Option<u64>,
        field: Option<String>,
        invalid_text: Option<&str>,
        invalid_bytes: Option<&[u8]>,
        suggestion: Option<Cow<'static, str>>,
    ) -> Self {
        Self {
            description,
            event_number,
            field,
            invalid_text: invalid_text.map(str::to_string),
            invalid_bytes: invalid_bytes.map(ToOwned::to_owned),
            suggestion,
        }
    }

    /// Error parsing an individual event in a text-based input format (e.g.,
    /// JSON, CSV).
    pub fn text_event_error<E>(
        msg: &str,
        error: E,
        event_number: u64,
        invalid_text: Option<&str>,
        suggestion: Option<Cow<'static, str>>,
    ) -> Self
    where
        E: ToString,
    {
        let err_str = error.to_string();
        // Try to parse the error as `FieldParseError`.  If this is not a field-specific
        // error or the error was not returned by the `deserialize_table_record`
        // macro, this will fail and we'll store the error as is.
        let (descr, field) = if let Some(offset) = err_str.find("{\"field\":") {
            if let Some(Ok(err)) = serde_json::Deserializer::from_str(&err_str[offset..])
                .into_iter::<FieldParseError>()
                .next()
            {
                (err.description, Some(err.field))
            } else {
                (err_str, None)
            }
        } else {
            (err_str, None)
        };
        let column_name = if let Some(field) = &field {
            format!(": error parsing field '{field}'")
        } else {
            String::new()
        };

        Self::new(
            format!("{msg}{column_name}: {descr}",),
            Some(event_number),
            field,
            invalid_text,
            None,
            suggestion,
        )
    }

    /// Error parsing a container, e.g., a JSON array, with multiple events.
    ///
    /// Such errors cannot be attributed to an individual event numbers.
    pub fn text_envelope_error(
        description: String,
        invalid_text: &str,
        suggestion: Option<Cow<'static, str>>,
    ) -> Self {
        Self::new(
            description,
            None,
            None,
            Some(invalid_text),
            None,
            suggestion,
        )
    }

    /// Error parsing an individual event in a binary input format (e.g.,
    /// bincode).
    pub fn bin_event_error(
        description: String,
        event_number: u64,
        invalid_bytes: &[u8],
        suggestion: Option<Cow<'static, str>>,
    ) -> Self {
        Self::new(
            description,
            Some(event_number),
            None,
            None,
            Some(invalid_bytes),
            suggestion,
        )
    }

    /// Error parsing a container with multiple events.
    ///
    /// Such errors cannot be attributed to an individual event numbers.
    pub fn bin_envelope_error(
        description: String,
        invalid_bytes: &[u8],
        suggestion: Option<Cow<'static, str>>,
    ) -> Self {
        Self::new(
            description,
            None,
            None,
            None,
            Some(invalid_bytes),
            suggestion,
        )
    }
}

/// Static map of supported input formats.
// TODO: support for registering new formats at runtime in order to allow
// external crates to implement new formats.
static INPUT_FORMATS: Lazy<BTreeMap<&'static str, Box<dyn InputFormat>>> = Lazy::new(|| {
    BTreeMap::from([
        ("csv", Box::new(CsvInputFormat) as Box<dyn InputFormat>),
        ("json", Box::new(JsonInputFormat) as Box<dyn InputFormat>),
        (
            "parquet",
            Box::new(ParquetInputFormat) as Box<dyn InputFormat>,
        ),
        #[cfg(feature = "with-avro")]
        ("avro", Box::new(AvroInputFormat) as Box<dyn InputFormat>),
    ])
});

/// Static map of supported output formats.
static OUTPUT_FORMATS: Lazy<BTreeMap<&'static str, Box<dyn OutputFormat>>> = Lazy::new(|| {
    BTreeMap::from([
        ("csv", Box::new(CsvOutputFormat) as Box<dyn OutputFormat>),
        ("json", Box::new(JsonOutputFormat) as Box<dyn OutputFormat>),
        (
            "parquet",
            Box::new(ParquetOutputFormat) as Box<dyn OutputFormat>,
        ),
        #[cfg(feature = "with-avro")]
        ("avro", Box::new(AvroOutputFormat) as Box<dyn OutputFormat>),
    ])
});

/// Trait that represents a specific data format.
///
/// This is a factory trait that creates parsers for a specific data format.
pub trait InputFormat: Send + Sync {
    /// Unique name of the data format.
    fn name(&self) -> Cow<'static, str>;

    /// Extract parser configuration from an HTTP request.
    ///
    /// Returns the extracted configuration cast to the `ErasedSerialize` trait
    /// object (to keep this trait object-safe).
    ///
    /// # Discussion
    ///
    /// We could rely on the `serde_urlencoded` crate to deserialize the config
    /// from the HTTP request, which is what most implementations will do
    /// internally; however allowing the implementation to override this
    /// method enables additional flexibility. For example, an
    /// implementation may use `Content-Type` and other request headers, set
    /// HTTP-specific defaults for config fields, etc.
    fn config_from_http_request(
        &self,
        endpoint_name: &str,
        request: &HttpRequest,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError>;

    /// Create a new parser for the format.
    ///
    /// # Arguments
    ///
    /// * `input_stream` - Input stream of the circuit to push parsed data to.
    ///
    /// * `config` - Format-specific configuration.
    fn new_parser(
        &self,
        endpoint_name: &str,
        input_stream: &InputCollectionHandle,
        config: &YamlValue,
    ) -> Result<Box<dyn Parser>, ControllerError>;
}

impl dyn InputFormat {
    /// Lookup input format by name.
    pub fn get_format(name: &str) -> Option<&'static dyn InputFormat> {
        INPUT_FORMATS.get(name).map(|f| &**f)
    }
}

/// A collection of records associated with an input handle.
///
/// A [Parser] holds and adds records to an [InputBuffer].  The client, which is
/// typically an [InputReader](crate::transport::InputReader), gathers one or
/// more [InputBuffer]s and pushes them to the circuit when the controller
/// requests it.
pub trait InputBuffer: Send {
    /// Pushes the `n` earliest buffered records into the circuit input
    /// handle. If fewer than `n` are available, pushes all of them.  Discards
    /// the records that are sent.  Returns the number sent.
    fn flush(&mut self, n: usize) -> usize;

    fn flush_all(&mut self) -> usize {
        self.flush(usize::MAX)
    }

    /// Returns the number of buffered records.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Removes all of the records from this input buffer and returns a new
    /// [InputBuffer] that holds them. Returns `None` if this input buffer is
    /// empty.
    ///
    /// This is useful for extracting the records from one of several parser
    /// threads to send to a single common thread to be pushed later.
    fn take(&mut self) -> Option<Box<dyn InputBuffer>>;

    fn take_some(&mut self, _n: usize) -> Option<Box<dyn InputBuffer>> {
        self.take()
    }
}

impl InputBuffer for Option<Box<dyn InputBuffer>> {
    fn len(&self) -> usize {
        self.as_ref().map_or(0, |buffer| buffer.len())
    }

    fn flush(&mut self, n: usize) -> usize {
        self.as_mut().map_or(0, |buffer| buffer.flush(n))
    }

    fn take(&mut self) -> Option<Box<dyn InputBuffer>> {
        self.as_mut().and_then(|buffer| buffer.take())
    }
}

/// An empty [InputBuffer].
pub struct EmptyInputBuffer;

impl InputBuffer for EmptyInputBuffer {
    fn flush(&mut self, _n: usize) -> usize {
        0
    }

    fn len(&self) -> usize {
        0
    }

    fn take(&mut self) -> Option<Box<dyn InputBuffer>> {
        None
    }
}

/// Parses raw bytes into database records.
pub trait Parser: Send + Sync {
    /// Parses `data` into records and returns the records and any parse errors
    /// that occurred.
    fn parse(&mut self, data: &[u8]) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>);

    /// Returns an object that can be used to break a stream of incoming data
    /// into complete records to pass to [Parser::parse].
    fn splitter(&self) -> Box<dyn Splitter>;

    /// Create a new parser with the same configuration as `self`.
    ///
    /// Used by multithreaded transport endpoints to create multiple parallel
    /// input pipelines.
    fn fork(&self) -> Box<dyn Parser>;
}

/// Splits a data stream into individual records.
///
/// [Parser::parse] can only parse complete records. For a byte stream source, a
/// format-specific [Splitter] allows a transport to find record boundaries.
pub trait Splitter: Send {
    /// Looks for a record boundary in `data`. Returns:
    ///
    /// - `None`, if `data` does not necessarily complete a record.
    ///
    ///- `Some(n)`, if the first `n` bytes of data, plus any data previously
    ///   presented for which `None` was returned, forms a record. If `n <
    ///   data.len()`, then the caller should re-present `data[n..]`, which
    ///   might include one or more complete records or (otherwise) the
    ///   beginning of a new record.
    fn input(&mut self, data: &[u8]) -> Option<usize>;

    /// Clears any state in this splitter and prepares it to start splitting new
    /// data.
    fn clear(&mut self);
}

/// A [Splitter] that never breaks data into records.
///
/// This supports [Parser]s that need all of a streaming data source to be read
/// in full before parsing.
pub struct Sponge;

impl Splitter for Sponge {
    fn input(&mut self, _data: &[u8]) -> Option<usize> {
        None
    }
    fn clear(&mut self) {}
}

/// A [Splitter] that breaks data into records at ASCII new-lines.
pub struct LineSplitter;

impl Splitter for LineSplitter {
    fn input(&mut self, data: &[u8]) -> Option<usize> {
        data.iter()
            .position(|b| *b == b'\n')
            .map(|position| position + 1)
    }

    fn clear(&mut self) {}
}

pub struct AppendSplitter {
    buffer: Vec<u8>,
    fragment: Range<usize>,
    fed: usize,
    splitter: Box<dyn Splitter>,
}

impl AppendSplitter {
    pub fn new(splitter: Box<dyn Splitter>) -> Self {
        Self {
            buffer: Vec::new(),
            fragment: 0..0,
            fed: 0,
            splitter,
        }
    }
    pub fn next(&mut self, eoi: bool) -> Option<&[u8]> {
        match self
            .splitter
            .input(&self.buffer[self.fed..self.fragment.end])
        {
            Some(n) => {
                let chunk = &self.buffer[self.fragment.start..self.fed + n];
                self.fed += n;
                self.fragment.start = self.fed;
                Some(chunk)
            }
            None => {
                self.fed = self.fragment.end;
                if eoi {
                    self.final_chunk()
                } else {
                    None
                }
            }
        }
    }
    pub fn final_chunk(&mut self) -> Option<&[u8]> {
        if !self.fragment.is_empty() {
            let chunk = &self.buffer[self.fragment.clone()];
            self.fragment.end = self.fragment.start;
            Some(chunk)
        } else {
            None
        }
    }
    pub fn append(&mut self, data: &[u8]) {
        let final_len = self.fragment.len() + data.len();
        if final_len > self.buffer.len() {
            self.buffer.reserve(final_len - self.buffer.len());
        }
        self.buffer.copy_within(self.fragment.clone(), 0);
        self.buffer.resize(self.fragment.len(), 0);
        self.buffer.extend(data);
        self.fed -= self.fragment.start;
        self.fragment = 0..self.buffer.len();
    }
}

pub struct StreamingSplitter {
    buffer: Vec<u8>,
    fragment: Range<usize>,
    fed: usize,
    splitter: Box<dyn Splitter>,
}

impl StreamingSplitter {
    pub fn new(splitter: Box<dyn Splitter>, buffer_size: Option<usize>) -> Self {
        let mut buffer = Vec::new();
        let buffer_size = buffer_size.unwrap_or_default();
        buffer.resize(if buffer_size == 0 { 8192 } else { buffer_size }, 0);
        Self {
            buffer,
            fragment: 0..0,
            fed: 0,
            splitter,
        }
    }
    pub fn next(&mut self, eoi: bool) -> Option<&[u8]> {
        match self
            .splitter
            .input(&self.buffer[self.fed..self.fragment.end])
        {
            Some(n) => {
                let chunk = &self.buffer[self.fragment.start..self.fed + n];
                self.fed += n;
                self.fragment.start = self.fed;
                Some(chunk)
            }
            None => {
                self.fed = self.fragment.end;
                if eoi {
                    self.final_chunk()
                } else {
                    None
                }
            }
        }
    }
    pub fn final_chunk(&mut self) -> Option<&[u8]> {
        if !self.fragment.is_empty() {
            let chunk = &self.buffer[self.fragment.clone()];
            self.fragment.end = self.fragment.start;
            Some(chunk)
        } else {
            None
        }
    }
    pub fn spare_capacity_mut(&mut self) -> &mut [u8] {
        self.buffer.copy_within(self.fragment.clone(), 0);
        self.fed -= self.fragment.start;
        self.fragment = 0..self.fragment.len();
        if self.fragment.len() == self.buffer.len() {
            self.buffer.resize(self.buffer.capacity() * 2, 0);
        }
        &mut self.buffer[self.fragment.len()..]
    }
    pub fn added_data(&mut self, n: usize) {
        self.fragment.end += n;
    }
    pub fn read(&mut self, file: &mut File, max: usize) -> Result<usize, IoError> {
        let mut space = self.spare_capacity_mut();
        if space.len() > max {
            space = &mut space[..max];
        }
        let result = file.read(space);
        if let Ok(n) = result {
            println!("read {n} bytes");
            self.added_data(n);
        }
        result
    }
}

pub trait OutputFormat: Send + Sync {
    /// Unique name of the data format.
    fn name(&self) -> Cow<'static, str>;

    /// Extract encoder configuration from an HTTP request.
    ///
    /// Returns the extracted configuration cast to the `ErasedSerialize` trait
    /// object (to keep this trait object-safe).
    fn config_from_http_request(
        &self,
        endpoint_name: &str,
        request: &HttpRequest,
    ) -> Result<Box<dyn ErasedSerialize>, ControllerError>;

    /// Create a new encoder for the format.
    ///
    /// # Arguments
    ///
    /// * `config` - Format-specific configuration.
    ///
    /// * `consumer` - Consumer to send encoded data batches to.
    fn new_encoder(
        &self,
        endpoint_name: &str,
        config: &ConnectorConfig,
        schema: &Relation,
        consumer: Box<dyn OutputConsumer>,
    ) -> Result<Box<dyn Encoder>, ControllerError>;
}

impl dyn OutputFormat {
    /// Lookup output format by name.
    pub fn get_format(name: &str) -> Option<&'static dyn OutputFormat> {
        OUTPUT_FORMATS.get(name).map(|f| &**f)
    }
}

pub trait Encoder: Send {
    /// Returns a reference to the consumer that the encoder is connected to.
    fn consumer(&mut self) -> &mut dyn OutputConsumer;

    /// Encode a batch of updates, push encoded buffers to the consumer
    /// using [`OutputConsumer::push_buffer`].
    fn encode(&mut self, batch: &dyn SerBatchReader) -> AnyResult<()>;
}

pub trait OutputConsumer: Send {
    /// Maximum buffer size that this transport can transmit.
    /// The encoder should not generate buffers exceeding this size.
    fn max_buffer_size_bytes(&self) -> usize;

    fn batch_start(&mut self, step: Step);
    fn push_buffer(&mut self, buffer: &[u8], num_records: usize);
    fn push_key(&mut self, key: &[u8], val: Option<&[u8]>, num_records: usize);
    fn batch_end(&mut self);
}
