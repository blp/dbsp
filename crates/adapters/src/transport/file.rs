use super::{
    InputConsumer, InputEndpoint, InputReader, InputReaderCommand, InputStep, OutputEndpoint,
    TransportInputEndpoint,
};
use crate::format::Splitter;
use crate::{InputBuffer, Parser};
use anyhow::{bail, Error as AnyError, Result as AnyResult};
use feldera_types::program_schema::Relation;
use feldera_types::transport::file::{FileInputConfig, FileOutputConfig};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::{self, Thread};
use std::{
    fs::File,
    io::{Error as IoError, Write},
    thread::spawn,
    time::Duration,
};

const SLEEP: Duration = Duration::from_millis(200);

pub(crate) struct FileInputEndpoint {
    config: FileInputConfig,
}

impl FileInputEndpoint {
    pub(crate) fn new(config: FileInputConfig) -> Self {
        Self { config }
    }
}

impl InputEndpoint for FileInputEndpoint {
    fn is_fault_tolerant(&self) -> bool {
        true
    }
}

impl TransportInputEndpoint for FileInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _start_step: Option<InputStep>,
        _schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(FileInputReader::new(
            &self.config,
            consumer,
            parser,
        )?))
    }
}

struct FileSplitter {
    buffer: Vec<u8>,
    start: u64,
    fragment: Range<usize>,
    fed: usize,
    splitter: Box<dyn Splitter>,
}

impl FileSplitter {
    fn new(splitter: Box<dyn Splitter>, buffer_size: Option<usize>) -> Self {
        let mut buffer = Vec::new();
        let buffer_size = buffer_size.unwrap_or_default();
        buffer.resize(if buffer_size == 0 { 8192 } else { buffer_size }, 0);
        Self {
            buffer,
            start: 0,
            fragment: 0..0,
            fed: 0,
            splitter,
        }
    }
    fn next(&mut self, eoi: bool) -> Option<&[u8]> {
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
    fn position(&self) -> u64 {
        self.start + self.fragment.start as u64
    }
    fn final_chunk(&mut self) -> Option<&[u8]> {
        if !self.fragment.is_empty() {
            let chunk = &self.buffer[self.fragment.clone()];
            self.fragment.end = self.fragment.start;
            Some(chunk)
        } else {
            None
        }
    }
    fn spare_capacity_mut(&mut self) -> &mut [u8] {
        self.buffer.copy_within(self.fragment.clone(), 0);
        self.start += self.fragment.start as u64;
        self.fed -= self.fragment.start;
        self.fragment = 0..self.fragment.len();
        if self.fragment.len() == self.buffer.len() {
            self.buffer.resize(self.buffer.capacity() * 2, 0);
        }
        &mut self.buffer[self.fragment.len()..]
    }
    fn added_data(&mut self, n: usize) {
        self.fragment.end += n;
    }
    fn read(&mut self, file: &mut File, max: usize) -> Result<usize, IoError> {
        let mut space = self.spare_capacity_mut();
        if space.len() > max {
            space = &mut space[..max];
        }
        let result = file.read(space);
        if let Ok(n) = result {
            self.added_data(n);
        }
        result
    }
    fn seek(&mut self, offset: u64) {
        self.start = offset;
        self.fragment = 0..0;
        self.fed = 0;
        self.splitter.clear();
    }
}

struct FileInputReader {
    sender: Sender<InputReaderCommand>,
    thread: Thread,
}

impl FileInputReader {
    fn new(
        config: &FileInputConfig,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
    ) -> AnyResult<Self> {
        let mut file = File::open(&config.path).map_err(|e| {
            AnyError::msg(format!("Failed to open input file '{}': {e}", config.path))
        })?;

        let (sender, receiver) = channel();
        let join_handle = spawn({
            let follow = config.follow;
            let buffer_size = config.buffer_size_bytes;
            move || {
                if let Err(error) = Self::worker_thread(
                    file,
                    buffer_size,
                    &consumer,
                    parser,
                    receiver,
                    follow,
                ) {
                    consumer.error(true, error);
                }
            }
        });

        Ok(Self { sender, thread: join_handle.thread().clone() })
    }

    fn worker_thread(
        mut file: File,
        buffer_size: Option<usize>,
        consumer: &Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
        receiver: Receiver<InputReaderCommand>,
        follow: bool,
    ) -> AnyResult<()> {
        let mut splitter = FileSplitter::new(parser.splitter(), buffer_size);

        let mut queue = VecDeque::<(Range<u64>, Box<dyn InputBuffer>)>::new();
        let mut n_queued = 0;
        let mut extending = false;
        let mut eof = false;
        loop {
            for command in receiver.try_iter() {
                match command {
                    InputReaderCommand::Extend => {
                        extending = true;
                    }
                    InputReaderCommand::Pause => {
                        extending = false;
                    }
                    InputReaderCommand::Queue => {
                        let mut total = 0;
                        let limit = consumer.max_batch_size();
                        let mut range: Option<Range<u64>> = None;
                        while let Some((offsets, mut buffer)) = queue.pop_front() {
                            range = match range {
                                Some(range) => Some(range.start..offsets.end),
                                None => Some(offsets),
                            };
                            total += buffer.len();
                            buffer.flush_all();
                            if total >= limit {
                                break;
                            }
                        }
                        n_queued -= total;
                        consumer.extended(
                            total,
                            serde_json::to_value(Metadata {
                                offsets: range.unwrap_or(0..0),
                            })?,
                        );
                    }
                    InputReaderCommand::Seek(metadata) => {
                        let Metadata { offsets } = serde_json::from_value(metadata)?;
                        let offset = offsets.end;
                        file.seek(SeekFrom::Start(offset))?;
                        splitter.seek(offset);
                    }
                    InputReaderCommand::Replay(metadata) => {
                        let Metadata { offsets } = serde_json::from_value(metadata)?;
                        file.seek(SeekFrom::Start(offsets.start))?;
                        splitter.seek(offsets.start);
                        let mut remainder = (offsets.end - offsets.start) as usize;
                        let mut num_records = 0;
                        while remainder > 0 {
                            let n = splitter.read(&mut file, remainder)?;
                            if n == 0 {
                                todo!();
                            }
                            remainder -= n;
                            while let Some(chunk) = splitter.next(remainder == 0) {
                                let (mut buffer, errors) = parser.parse(chunk);
                                consumer.parse_errors(errors);
                                consumer.buffered(buffer.len(), chunk.len());
                                num_records += buffer.flush_all();
                            }
                        }
                        consumer.replayed(num_records);
                    }
                    InputReaderCommand::Disconnect => return Ok(()),
                }
            }

            if !extending || eof {
                thread::park();
                continue;
            }

            let n = splitter.read(&mut file, usize::MAX)?;
            if n == 0 {
                if follow {
                    thread::park_timeout(SLEEP);
                    continue;
                }
                eof = true;
            }
            loop {
                let start = splitter.position();
                let Some(chunk) = splitter.next(eof) else {
                    break;
                };
                let (buffer, errors) = parser.parse(chunk);
                consumer.buffered(buffer.len(), chunk.len());
                consumer.parse_errors(errors);

                n_queued += buffer.len();
                if let Some(buffer) = buffer {
                    let end = splitter.position();
                    queue.push_back((start..end, buffer));
                }
            }
            if n == 0 {
                consumer.eoi();
            }
        }
    }
}

impl InputReader for FileInputReader {
    fn request(&self, command: super::InputReaderCommand) {
        let _ = self.sender.send(command);
        self.thread.unpark();
    }
}

impl Drop for FileInputReader {
    fn drop(&mut self) {
        self.disconnect();
    }
}

pub(crate) struct FileOutputEndpoint {
    file: File,
}

impl FileOutputEndpoint {
    pub(crate) fn new(config: FileOutputConfig) -> AnyResult<Self> {
        let file = File::create(&config.path).map_err(|e| {
            AnyError::msg(format!(
                "Failed to create output file '{}': {e}",
                config.path
            ))
        })?;
        Ok(Self { file })
    }
}

impl OutputEndpoint for FileOutputEndpoint {
    fn connect(
        &mut self,
        _async_error_callback: Box<dyn Fn(bool, AnyError) + Send + Sync>,
    ) -> AnyResult<()> {
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        self.file.write_all(buffer)?;
        self.file.sync_all()?;
        Ok(())
    }

    fn push_key(&mut self, _key: &[u8], _val: Option<&[u8]>) -> AnyResult<()> {
        bail!(
            "File output transport does not support key-value pairs. \
This output endpoint was configured with a data format that produces outputs as key-value pairs; \
however the File transport does not support this representation."
        );
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod test {
    use crate::test::{mock_input_pipeline, wait, DEFAULT_TIMEOUT_MS};
    use csv::WriterBuilder as CsvWriterBuilder;
    use feldera_types::deserialize_without_context;
    use feldera_types::program_schema::Relation;
    use serde::{Deserialize, Serialize};
    use std::{io::Write, thread::sleep, time::Duration};
    use tempfile::NamedTempFile;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
    pub struct TestStruct {
        s: String,
        b: bool,
        i: i64,
    }

    deserialize_without_context!(TestStruct);

    impl TestStruct {
        fn new(s: String, b: bool, i: i64) -> Self {
            Self { s, b, i }
        }
    }

    #[test]
    fn test_csv_file_nofollow() {
        let test_data = [
            TestStruct::new("foo".to_string(), true, 10),
            TestStruct::new("bar".to_string(), false, -10),
        ];
        let temp_file = NamedTempFile::new().unwrap();

        // Create a transport endpoint attached to the file.
        // Use a very small buffer size for testing.
        let config_str = format!(
            r#"
stream: test_input
transport:
    name: file_input
    config:
        path: {:?}
        buffer_size_bytes: 5
format:
    name: csv
"#,
            temp_file.path().to_str().unwrap()
        );

        println!("Config:\n{}", config_str);

        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(temp_file.as_file());
        for val in test_data.iter().cloned() {
            writer.serialize(val).unwrap();
        }
        writer.flush().unwrap();

        let (endpoint, consumer, _parser, zset) = mock_input_pipeline::<TestStruct, TestStruct>(
            serde_yaml::from_str(&config_str).unwrap(),
            Relation::empty(),
        )
        .unwrap();

        sleep(Duration::from_millis(10));

        // No outputs should be produced at this point.
        assert!(!consumer.state().eoi);

        // Unpause the endpoint, wait for the data to appear at the output.
        endpoint.extend();
        wait(
            || {
                endpoint.queue();
                zset.state().flushed.len() == test_data.len()
            },
            DEFAULT_TIMEOUT_MS,
        )
        .unwrap();
        for (i, upd) in zset.state().flushed.iter().enumerate() {
            assert_eq!(upd.unwrap_insert(), &test_data[i]);
        }
    }

    #[test]
    fn test_csv_file_follow() {
        let test_data = [
            TestStruct::new("foo".to_string(), true, 10),
            TestStruct::new("bar".to_string(), false, -10),
        ];
        let temp_file = NamedTempFile::new().unwrap();

        // Create a transport endpoint attached to the file.
        // Use a very small buffer size for testing.
        let config_str = format!(
            r#"
stream: test_input
transport:
    name: file_input
    config:
        path: {:?}
        buffer_size_bytes: 5
        follow: true
format:
    name: csv
"#,
            temp_file.path().to_str().unwrap()
        );

        println!("Config:\n{}", config_str);

        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(temp_file.as_file());

        let (endpoint, consumer, parser, zset) = mock_input_pipeline::<TestStruct, TestStruct>(
            serde_yaml::from_str(&config_str).unwrap(),
            Relation::empty(),
        )
        .unwrap();

        endpoint.extend();

        for _ in 0..10 {
            for val in test_data.iter().cloned() {
                writer.serialize(val).unwrap();
            }
            writer.flush().unwrap();

            sleep(Duration::from_millis(10));

            // No outputs should be produced at this point.
            assert!(!consumer.state().eoi);

            // Unpause the endpoint, wait for the data to appear at the output.
            wait(
                || {
                    endpoint.queue();
                    zset.state().flushed.len() == test_data.len()
                },
                DEFAULT_TIMEOUT_MS,
            )
            .unwrap();
            for (i, upd) in zset.state().flushed.iter().enumerate() {
                assert_eq!(upd.unwrap_insert(), &test_data[i]);
            }

            consumer.reset();
            zset.reset();
        }

        drop(writer);

        consumer.on_error(Some(Box::new(|_, _| {})));
        parser.on_error(Some(Box::new(|_, _| {})));
        temp_file.as_file().write_all(b"xxx\n").unwrap();
        temp_file.as_file().flush().unwrap();

        wait(
            || {
                endpoint.queue();
                let state = parser.state();
                // println!("result: {:?}", state.parser_result);
                state.parser_result.is_some() && !state.parser_result.as_ref().unwrap().is_empty()
            },
            DEFAULT_TIMEOUT_MS,
        )
        .unwrap();

        assert!(zset.state().flushed.is_empty());

        endpoint.disconnect();
    }
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    offsets: Range<u64>,
}
