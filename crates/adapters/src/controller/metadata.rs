use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Seek, SeekFrom},
    mem,
    path::{Path, PathBuf},
};
use uuid::Uuid;

use crate::{transport::Step, ControllerError};

#[derive(Serialize, Deserialize, Default)]
pub struct Checkpoint {
    pub circuit_uuid: Uuid,
    pub step: Step,
}

pub enum Steps {
    Reading {
        path: PathBuf,
        reader: BufReader<File>,
        current: StepMetadata,
    },
    Writing {
        path: PathBuf,
        writer: BufWriter<File>,
        offset: Option<u64>,
    },
    Invalid,
}

impl Steps {
    pub fn open<P>(path: P, start: Step) -> Result<Self, ControllerError>
    where
        P: AsRef<Path>,
    {
        let path = PathBuf::from(path.as_ref());

        let open_io_error =
            |error| ControllerError::io_error(format!("reading {}", path.display()), error);

        let mut file = BufReader::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(open_io_error)?,
        );
        let mut line = String::new();
        while file.read_line(&mut line).map_err(open_io_error)? > 0 && line.ends_with('\n') {
            let step_metadata = serde_json::from_str::<StepMetadata>(&line).map_err(|error| {
                ControllerError::StepsParseError {
                    error: format!("parsing {}: {error}", path.display()),
                }
            })?;
            if step_metadata.step == start {
                return Ok(Self::Reading {
                    path,
                    reader: file,
                    current: step_metadata,
                });
            }
            line.clear();
        }
        todo!()
    }

    pub fn create<P>(_path: P) -> Result<Self, ControllerError>
    where
        P: AsRef<Path>,
    {
        todo!()
    }

    pub fn get(&mut self, step: Step) -> Result<Option<StepMetadata>, ControllerError> {
        let Self::Reading {
            path,
            reader,
            current,
        } = self
        else {
            unreachable!("Should always be in read mode")
        };
        assert_eq!(step, current.step);

        let start_offset = reader.stream_position().unwrap();
        let mut line = String::new();
        if reader.read_line(&mut line).map_err(|error| {
            ControllerError::io_error(format!("reading {}", path.display()), error)
        })? > 0
            && !line.ends_with('\n')
        {
            let next = serde_json::from_str::<StepMetadata>(&line).map_err(|error| {
                ControllerError::StepsParseError {
                    error: format!("parsing {}: {error}", path.display()),
                }
            })?;
            if next.step != step + 1 {
                return Err(ControllerError::StepsParseError {
                    error: format!(
                        "parsing {} at offset {start_offset}: expected step {}, got step {}",
                        path.display(),
                        step + 1,
                        next.step
                    ),
                });
            }
            Ok(Some(mem::replace(current, next)))
        } else {
            let Self::Reading {
                path,
                reader,
                current: _,
            } = mem::replace(self, Self::Invalid)
            else {
                unreachable!()
            };

            *self = Self::Writing {
                path,
                writer: BufWriter::new(reader.into_inner()),
                offset: Some(start_offset),
            };
            Ok(None)
        }
    }

    pub fn put(&mut self, step: StepMetadata) -> Result<(), ControllerError> {
        let Self::Writing {
            path,
            writer,
            offset,
        } = self
        else {
            unreachable!("Should always be in write mode")
        };

        let write_io_error =
            |error| ControllerError::io_error(format!("writing {}", path.display()), error);

        if let Some(offset) = offset.take() {
            writer
                .get_mut()
                .set_len(offset)
                .and_then(|()| writer.seek(SeekFrom::Start(offset)))
                .map_err(write_io_error)?;
        }
        serde_json::to_writer(&mut *writer, &step).map_err(|error| {
            ControllerError::io_error(
                format!("writing {}", path.display()),
                error.io_error_kind().unwrap().into(),
            )
        })?;
        writer.get_mut().sync_data().map_err(write_io_error)?;
        Ok(())
    }

    pub fn wait(&mut self) -> Result<(), ControllerError> {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct StepMetadata {
    pub step: Step,
    pub input_endpoints: HashMap<String, JsonValue>,
}

impl Iterator for &mut Steps {
    type Item = Result<StepMetadata, ControllerError>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
