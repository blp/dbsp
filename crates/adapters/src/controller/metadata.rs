use serde::{ser::SerializeStruct, Deserialize, Serialize, Serializer};
use serde_json::{Error as JsonError, Value as JsonValue};
use std::{
    backtrace::Backtrace,
    cmp::Ordering,
    collections::HashMap,
    fmt::{Display, Formatter, Result as FmtResult},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Error as IoError, Seek, SeekFrom, Write},
    mem,
    path::{Path, PathBuf},
    sync::mpsc::{channel, Receiver, Sender},
    thread::{self, JoinHandle},
};
use uuid::Uuid;

use crate::{transport::Step, ControllerError};

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum StepsError {
    /// I/O error.
    #[serde(serialize_with = "serialize_io_error")]
    IoError {
        path: PathBuf,
        io_error: IoError,
        backtrace: Backtrace,
    },

    ParseError {
        path: PathBuf,
        #[serde(serialize_with = "serialize_json_error")]
        error: JsonError,
        line_number: usize,
        offset: u64,
    },

    MissingStep {
        path: PathBuf,
        step: Step,
    },

    UnexpectedRead,
    UnexpectedWrite,
    UnexpectedWait,
}

fn serialize_io_error<S>(
    path: &PathBuf,
    io_error: &IoError,
    backtrace: &Backtrace,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_struct("IoError", 4)?;
    ser.serialize_field("path", path)?;
    ser.serialize_field("kind", &io_error.kind().to_string())?;
    ser.serialize_field("os_error", &io_error.raw_os_error())?;
    ser.serialize_field("backtrace", &backtrace.to_string())?;
    ser.end()
}

fn serialize_json_error<S>(error: &JsonError, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&error.to_string())
}

impl Display for StepsError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            StepsError::ParseError { path, error, line_number, offset } => write!(f, "error parsing step on line {line_number} starting at offset {offset} in {} ({error})", path.display()),
            StepsError::MissingStep { path, step } => write!(f, "{} should contain step {step} but it is not present", path.display()),
            StepsError::IoError { path, io_error, .. } => write!(f, "I/O error on {}: {io_error}", path.display()),
            StepsError::UnexpectedRead => write!(f, "Unexpected read while in write mode"),
            StepsError::UnexpectedWrite => write!(f, "Unexpected write while in read mode"),
            StepsError::UnexpectedWait => write!(f, "Unexpected wait while in read mode"),
        }
    }
}

impl StepsError {
    fn io_error(path: &Path, io_error: IoError) -> StepsError {
        StepsError::IoError {
            path: path.to_path_buf(),
            io_error,
            backtrace: Backtrace::capture(),
        }
    }
}

impl From<StepsError> for ControllerError {
    fn from(value: StepsError) -> Self {
        ControllerError::StepsError(value)
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct Checkpoint {
    pub circuit_uuid: Uuid,
    pub step: Step,
}

pub struct BackgroundSync {
    join_handle: Option<JoinHandle<()>>,
    request_sender: Option<Sender<()>>,
    reply_receiver: Receiver<()>,
    n_incomplete_requests: usize,
}

impl BackgroundSync {
    pub fn new(file: &File) -> Self {
        let file = file.try_clone().unwrap();
        let (request_sender, request_receiver) = channel();
        let (reply_sender, reply_receiver) = channel();
        let join_handle = thread::Builder::new()
            .name("dbsp-step-sync".into())
            .spawn({
                move || {
                    for () in request_receiver {
                        let _ = file.sync_data();
                        let _ = reply_sender.send(());
                    }
                }
            })
            .unwrap();
        Self {
            join_handle: Some(join_handle),
            request_sender: Some(request_sender),
            reply_receiver,
            n_incomplete_requests: 0,
        }
    }

    pub fn sync(&mut self) {
        let _ = self.request_sender.as_mut().unwrap().send(());
        self.n_incomplete_requests += 1;
    }

    pub fn wait(&mut self) {
        while self.n_incomplete_requests > 0 {
            let _ = self.reply_receiver.recv();
            self.n_incomplete_requests -= 1;
        }
    }
}

impl Drop for BackgroundSync {
    fn drop(&mut self) {
        let _ = self.request_sender.take();
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.join();
        }
    }
}

pub enum Steps {
    Reading {
        path: PathBuf,
        reader: BufReader<File>,
        line_number: usize,
    },
    Writing {
        path: PathBuf,
        writer: BufWriter<File>,
        offset: Option<u64>,
        sync: BackgroundSync,
    },
    Invalid,
}

impl Steps {
    pub fn open<P>(path: P) -> Result<Self, StepsError>
    where
        P: AsRef<Path>,
    {
        let path = PathBuf::from(path.as_ref());
        let reader = BufReader::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|io_error| StepsError::io_error(&path, io_error))?,
        );
        Ok(Self::Reading {
            path,
            reader,
            line_number: 0,
        })
    }

    pub fn create<P>(path: P) -> Result<Self, StepsError>
    where
        P: AsRef<Path>,
    {
        let path = PathBuf::from(path.as_ref());
        let file =
            File::create_new(&path).map_err(|io_error| StepsError::io_error(&path, io_error))?;
        Ok(Self::new_writer(path, file, None))
    }

    fn new_writer(path: PathBuf, file: File, offset: Option<u64>) -> Self {
        let sync = BackgroundSync::new(&file);
        Self::Writing {
            path,
            writer: BufWriter::new(file),
            offset,
            sync,
        }
    }

    pub fn read(&mut self) -> Result<Option<StepMetadata>, StepsError> {
        let Self::Reading {
            path,
            reader,
            line_number,
        } = self
        else {
            return Err(StepsError::UnexpectedRead);
        };

        let start_offset = reader.stream_position().unwrap();
        *line_number += 1;
        let mut line = String::new();
        if reader
            .read_line(&mut line)
            .map_err(|error| StepsError::io_error(path, error))?
            > 0
            && line.ends_with('\n')
        {
            let step = serde_json::from_str::<StepMetadata>(&line).map_err(|error| {
                StepsError::ParseError {
                    path: path.clone(),
                    error,
                    line_number: *line_number,
                    offset: start_offset,
                }
            })?;
            Ok(Some(step))
        } else {
            let Self::Reading { path, reader, .. } = mem::replace(self, Self::Invalid) else {
                unreachable!()
            };
            *self = Self::new_writer(path, reader.into_inner(), Some(start_offset));
            Ok(None)
        }
    }

    pub fn seek(&mut self, step: Step) -> Result<StepMetadata, StepsError> {
        while let Some(m) = self.read()? {
            match m.step.cmp(&step) {
                Ordering::Less => (),
                Ordering::Equal => return Ok(m),
                Ordering::Greater => break,
            }
        }
        Err(StepsError::MissingStep {
            path: self.path().clone(),
            step,
        })
    }

    pub fn path(&self) -> &PathBuf {
        match self {
            Steps::Reading {
                path,
                reader,
                line_number,
            } => path,
            Steps::Writing {
                path,
                writer,
                offset,
                sync,
            } => path,
            Steps::Invalid => unreachable!(),
        }
    }

    pub fn write(&mut self, step: &StepMetadata) -> Result<(), StepsError> {
        let Self::Writing {
            path,
            writer,
            offset,
            sync,
        } = self
        else {
            return Err(StepsError::UnexpectedWrite);
        };

        let io_error = |error| StepsError::io_error(path, error);

        if let Some(offset) = offset.take() {
            writer
                .get_mut()
                .set_len(offset)
                .and_then(|()| writer.seek(SeekFrom::Start(offset)))
                .map_err(io_error)?;
        }
        serde_json::to_writer(&mut *writer, step)
            .map_err(|error| IoError::from(error.io_error_kind().unwrap()))
            .and_then(|()| writeln!(writer))
            .and_then(|()| writer.flush())
            .map_err(io_error)?;
        writer.get_mut().sync_data().map_err(io_error)?;
        sync.sync();
        Ok(())
    }

    pub fn wait(&mut self) {
        match self {
            Self::Writing { sync, .. } => sync.wait(),
            _ => unreachable!("Should always be in write mode"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct StepMetadata {
    pub step: Step,
    pub input_endpoints: HashMap<String, JsonValue>,
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, iter};

    use tempfile::TempDir;

    use super::{StepMetadata, Steps};

    /// Create and write a steps file and then read it back.
    #[test]
    fn test_create() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join("steps.json");

        let written_data = (0..10)
            .map(|step| StepMetadata {
                step,
                input_endpoints: HashMap::new(),
            })
            .collect::<Vec<_>>();

        let mut steps = Steps::create(&path).unwrap();
        for step in written_data.iter() {
            steps.write(step).unwrap();
        }

        let mut steps = Steps::open(&path).unwrap();
        let read_data = iter::from_fn(|| steps.read().unwrap()).collect::<Vec<_>>();
        assert_eq!(written_data, read_data);
        println!("{}", tempdir.into_path().display());
    }

    /// Create and write a steps file and then read it back with seeking.
    #[test]
    fn test_seek() {
        let tempdir = TempDir::new().unwrap();
        let path = tempdir.path().join("steps.json");

        let written_data = (0..10)
            .map(|step| StepMetadata {
                step,
                input_endpoints: HashMap::new(),
            })
            .collect::<Vec<_>>();

        let mut steps = Steps::create(&path).unwrap();
        for step in written_data.iter() {
            steps.write(step).unwrap();
        }

        for start in 0..10 {
            let mut steps = Steps::open(&path).unwrap();
            let read_data = iter::once(steps.seek(start).unwrap())
                .chain(iter::from_fn(|| steps.read().unwrap()))
                .collect::<Vec<_>>();
            assert_eq!(written_data, read_data);
        }
    }
}
