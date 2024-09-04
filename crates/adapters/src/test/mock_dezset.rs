use crate::catalog::ArrowStream;
use crate::format::InputBuffer;
use crate::{
    catalog::{DeCollectionStream, RecordFormat},
    static_compile::deinput::{
        CsvDeserializerFromBytes, DeserializerFromBytes, JsonDeserializerFromBytes,
    },
    ControllerError, DeCollectionHandle,
};
use anyhow::Result as AnyResult;
use dbsp::DBData;
use pipeline_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
use std::{
    cmp::min,
    fmt::Debug,
    mem::take,
    sync::{Arc, Mutex, MutexGuard},
};

use super::{wait, DEFAULT_TIMEOUT_MS};

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub enum MockUpdate<T, U> {
    Insert(T),
    Delete(T),
    Update(U),
}

impl<T: Debug, U: Debug> MockUpdate<T, U> {
    pub fn with_polarity(val: T, polarity: bool) -> Self {
        if polarity {
            Self::Insert(val)
        } else {
            Self::Delete(val)
        }
    }

    pub fn unwrap_insert(&self) -> &T {
        if let Self::Insert(val) = self {
            val
        } else {
            panic!("MockUpdate {self:?} is not an insert")
        }
    }

    pub fn unwrap_delete(&self) -> &T {
        if let Self::Delete(val) = self {
            val
        } else {
            panic!("MockUpdate {self:?} is not a delete")
        }
    }

    pub fn unwrap_update(&self) -> &U {
        if let Self::Update(val) = self {
            val
        } else {
            panic!("MockUpdate {self:?} is not an update")
        }
    }
}

/// Inner state of `MockDeZSet`.
pub struct MockDeZSetState<T, U> {
    /// Records flushed since the last `reset`.
    pub flushed: Vec<MockUpdate<T, U>>,
}

impl<T, U> Default for MockDeZSetState<T, U> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, U> MockDeZSetState<T, U> {
    pub fn new() -> Self {
        Self {
            flushed: Vec::new(),
        }
    }

    /// Clear internal state.
    pub fn reset(&mut self) {
        self.flushed.clear();
    }
}

pub struct MockDeZSet<T, U>(Arc<Mutex<MockDeZSetState<T, U>>>);

impl<T, U> Default for MockDeZSet<T, U> {
    fn default() -> Self {
        Self::new()
    }
}

/// Mock implementation of `DeCollectionHandle`.
impl<T, U> Clone for MockDeZSet<T, U> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T, U> MockDeZSet<T, U> {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(MockDeZSetState::new())))
    }

    pub fn reset(&self) {
        self.0.lock().unwrap().reset();
    }

    pub fn state(&self) -> MutexGuard<MockDeZSetState<T, U>> {
        self.0.lock().unwrap()
    }
}

impl<T, U> DeCollectionHandle for MockDeZSet<T, U>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
{
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeCollectionStream>, ControllerError> {
        match record_format {
            RecordFormat::Csv => Ok(Box::new(MockDeZSetStream::<
                CsvDeserializerFromBytes<_>,
                T,
                U,
            >::new(
                self.clone(), SqlSerdeConfig::default()
            ))),
            RecordFormat::Json(flavor) => Ok(Box::new(MockDeZSetStream::<
                JsonDeserializerFromBytes<SqlSerdeConfig>,
                T,
                U,
            >::new(
                self.clone(),
                SqlSerdeConfig::from(flavor),
            ))),
            RecordFormat::Parquet(_) => {
                todo!()
            }
            #[cfg(feature = "with-avro")]
            RecordFormat::Avro => {
                todo!()
            }
        }
    }

    fn configure_arrow_deserializer(
        &self,
        _config: SqlSerdeConfig,
    ) -> Result<Box<dyn ArrowStream>, ControllerError> {
        todo!()
    }
}

struct MockDeZSetStreamBuffer<T, U> {
    updates: Vec<MockUpdate<T, U>>,
    handle: MockDeZSet<T, U>,
}

impl<T, U> InputBuffer for MockDeZSetStreamBuffer<T, U>
where
    T: Send,
    U: Send,
{
    fn flush(&mut self, n: usize) -> usize {
        let n = min(n, self.len());
        let mut state = self.handle.0.lock().unwrap();
        state.flushed.extend(self.updates.drain(..n));
        n
    }

    fn len(&self) -> usize {
        self.updates.len()
    }
}

#[derive(Clone)]
pub struct MockDeZSetStream<De, T, U> {
    updates: Vec<MockUpdate<T, U>>,
    committed_len: usize,
    handle: MockDeZSet<T, U>,
    deserializer: De,
    config: SqlSerdeConfig,
}

impl<De, T, U> MockDeZSetStream<De, T, U>
where
    De: DeserializerFromBytes<SqlSerdeConfig>,
{
    pub fn new(handle: MockDeZSet<T, U>, config: SqlSerdeConfig) -> Self {
        Self {
            updates: Vec::new(),
            committed_len: 0,
            handle,
            deserializer: De::create(config.clone()),
            config,
        }
    }
}

impl<De, T, U> DeCollectionStream for MockDeZSetStream<De, T, U>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig> + Send + 'static,
    De: DeserializerFromBytes<SqlSerdeConfig> + Send + 'static,
{
    fn insert(&mut self, data: &[u8]) -> AnyResult<()> {
        let val = DeserializerFromBytes::deserialize::<T>(&mut self.deserializer, data)?;
        self.updates.push(MockUpdate::Insert(val));
        Ok(())
    }

    fn delete(&mut self, data: &[u8]) -> AnyResult<()> {
        let val = DeserializerFromBytes::deserialize::<T>(&mut self.deserializer, data)?;
        self.updates.push(MockUpdate::Delete(val));
        Ok(())
    }

    fn update(&mut self, data: &[u8]) -> AnyResult<()> {
        let val = DeserializerFromBytes::deserialize::<U>(&mut self.deserializer, data)?;
        self.updates.push(MockUpdate::Update(val));
        Ok(())
    }

    fn reserve(&mut self, _reservation: usize) {}

    fn save(&mut self) {
        self.committed_len = self.updates.len();
    }

    fn discard(&mut self) {
        self.updates.truncate(self.committed_len);
    }

    fn fork(&self) -> Box<dyn DeCollectionStream> {
        Box::new(Self::new(self.handle.clone(), self.config.clone()))
    }

    fn push(&mut self, n: usize) -> usize{
        self.save();
        let n = min(n, self.updates.len());
        self.committed_len -= n;

        let mut state = self.handle.0.lock().unwrap();
        state.flushed.extend(self.updates.drain(..n));
        n
    }

    fn take_buffer(&mut self) -> Box<dyn InputBuffer> {
        self.committed_len = 0;
        Box::new(MockDeZSetStreamBuffer {
            updates: take(&mut self.updates),
            handle: self.handle.clone(),
        })
    }
}

/// Wait to receive all records in `data` in the same order.
pub fn wait_for_output_ordered<T>(zset: &MockDeZSet<T, T>, data: &[Vec<T>])
where
    T: DBData,
{
    let num_records: usize = data.iter().map(Vec::len).sum();

    wait(
        || zset.state().flushed.len() == num_records,
        DEFAULT_TIMEOUT_MS,
    )
    .unwrap();

    let expected = zset
        .state()
        .flushed
        .iter()
        .map(|u| u.unwrap_insert())
        .cloned()
        .collect::<Vec<_>>();

    let flattened = data
        .iter()
        .flat_map(|data| data.iter())
        .cloned()
        .collect::<Vec<_>>();

    assert_eq!(&expected, &flattened);

    // for (i, val) in data.iter().flat_map(|data| data.iter()).enumerate() {
    //     assert_eq!(zset.state().flushed[i].unwrap_insert(), val);
    // }
}

/// Wait to receive all records in `data` in some order.
pub fn wait_for_output_unordered<T>(zset: &MockDeZSet<T, T>, data: &[Vec<T>])
where
    T: DBData,
{
    let num_records: usize = data.iter().map(Vec::len).sum();

    wait(
        || zset.state().flushed.len() == num_records,
        DEFAULT_TIMEOUT_MS,
    )
    .unwrap();

    let mut data_sorted = data
        .iter()
        .flat_map(|data| data.clone().into_iter())
        .collect::<Vec<_>>();
    data_sorted.sort();

    let mut zset_sorted = zset
        .state()
        .flushed
        .iter()
        .map(|upd| upd.unwrap_insert().clone())
        .collect::<Vec<_>>();
    zset_sorted.sort();

    assert_eq!(zset_sorted, data_sorted);
}
