use crate::transport::{InputEndpoint, InputStep};
use crate::Parser;
use crate::{
    transport::{
        kafka::{rdkafka_loglevel_from, refine_kafka_error, DeferredLogging},
        secret_resolver::MaybeSecret,
        InputReader,
    },
    InputConsumer, PipelineState, TransportInputEndpoint,
};
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use atomic::Atomic;
use crossbeam::queue::ArrayQueue;
use feldera_types::program_schema::Relation;
use feldera_types::{secret_ref::MaybeSecretRef, transport::kafka::KafkaInputConfig};
use log::debug;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::{
    config::FromClientConfigAndContext,
    consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, RebalanceProtocol},
    error::{KafkaError, KafkaResult},
    ClientConfig, ClientContext, Message,
};
use rdkafka::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Range;
use std::thread::JoinHandle;
use std::{
    sync::{atomic::Ordering as AtomicOrdering, Arc, Mutex, Weak},
    thread::spawn,
    time::{Duration, Instant},
};

/// Poll timeout must be low, as it bounds the amount of time it takes to resume the connector.
const POLL_TIMEOUT: Duration = Duration::from_millis(5);

// Size of the circular buffer used to pass errors from ClientContext
// to the worker thread.
const ERROR_BUFFER_SIZE: usize = 1000;

pub struct KafkaFtInputEndpoint {
    config: Arc<KafkaInputConfig>,
}

impl KafkaFtInputEndpoint {
    pub fn new(mut config: KafkaInputConfig) -> AnyResult<KafkaFtInputEndpoint> {
        config.validate()?;
        Ok(KafkaFtInputEndpoint {
            config: Arc::new(config),
        })
    }
}

struct KafkaInputReader(Arc<KafkaInputReaderInner>, JoinHandle<()>);

/// Client context used to intercept rebalancing events.
///
/// `rdkafka` allows consumers to register callbacks invoked on various
/// Kafka events.  We need to intercept rebalancing events, when the
/// consumer gets assigned new partitions, since these new partitions are
/// may not be in the paused/unpaused state required by the endpoint,
/// so we may need to pause or unpause them as appropriate.
///
/// See <https://github.com/edenhill/librdkafka/issues/1849> for a discussion
/// of the pause/unpause behavior.
struct KafkaInputContext {
    // We keep a weak reference to the endpoint to avoid a reference cycle:
    // endpoint->BaseConsumer->context->endpoint.
    endpoint: Mutex<Weak<KafkaInputReaderInner>>,

    deferred_logging: DeferredLogging,
}

impl KafkaInputContext {
    fn new() -> Self {
        Self {
            endpoint: Mutex::new(Weak::new()),
            deferred_logging: DeferredLogging::new(),
        }
    }
}

impl ClientContext for KafkaInputContext {
    fn error(&self, error: KafkaError, reason: &str) {
        // eprintln!("Kafka error: {error}");
        if let Some(endpoint) = self.endpoint.lock().unwrap().upgrade() {
            endpoint.push_error(error, reason);
        }
    }

    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.deferred_logging.log(level, fac, log_message);
    }
}

impl ConsumerContext for KafkaInputContext {
    fn post_rebalance(&self, rebalance: &Rebalance<'_>) {
        // println!("Rebalance: {rebalance:?}");
        if matches!(rebalance, Rebalance::Assign(_)) {
            if let Some(endpoint) = self.endpoint.lock().unwrap().upgrade() {
                if endpoint.pipeline_state() == PipelineState::Running {
                    let _ = endpoint.resume_partitions();
                } else {
                    let _ = endpoint.pause_partitions();
                }
            }
        }

        // println!("Rebalance complete");
    }
}

struct Tpo<'a> {
    topic: &'a str,
    partition: i32,
    offset: i64,
}

impl<'a> Tpo<'a> {
    /// Is `self` before or after or within the window for `metadata`?
    fn vs_metadata(&self, metadata: &Metadata) -> Option<Ordering> {
        let range = metadata
            .offsets
            .get(self.topic)
            .and_then(|partitions| partitions.get(self.partition as usize))?;
        if self.offset < range.start {
            Some(Ordering::Less)
        } else if self.offset >= range.end {
            Some(Ordering::Greater)
        } else {
            Some(Ordering::Equal)
        }
    }
}

enum State {
    New {
        previous: Option<Metadata>,
        new: Metadata,
    },
    Replaying(Metadata),
}

impl State {
    fn update_for_received_message(&mut self, tpo: Tpo) -> Ordering {
        let topic = tpo.topic;
        let partition: usize = tpo.partition.try_into()?;
        let offset = tpo.offset;
        match self {
            State::New {
                previous,
                new: metadata,
            } => {
                if let Some(previous) = previous {
                    // `tpo` must be after the previous step.
                    assert_eq!(
                        tpo.vs_metadata(previous).unwrap_or(Ordering::Greater),
                        Ordering::Greater
                    );
                }
                let partitions = match metadata.offsets.get_mut(topic) {
                    Some(partitions) => partitions,
                    None => metadata
                        .offsets
                        .entry(String::from(topic))
                        .or_insert(Vec::new()),
                };
                while partitions.len() <= partition {
                    partitions.push(0..0);
                }

                // Read the step's data.
                while p.next_offset < index_entry.data_offsets.end {
                    let data_message = consumer_eh
                        .read_partition_queue(&consumer, &data_queues[p.index as usize], || {
                            self.action()
                        })
                        .with_context(|| {
                            format!("Failed to read {} partition queue.", p.data_ctp)
                        })?;
                    if data_message.offset() < index_entry.data_offsets.start {
                        continue;
                    }
                    if let Some(payload) = data_message.payload() {
                        //self.receiver.queue(payload.len(), self.parser.parse(payload));
                    }
                    p.next_offset = data_message.offset() + 1;
                }
            }
        }

        if let Some(log_level) = config.log_level {
            client_config.set_log_level(rdkafka_loglevel_from(log_level));
        }

        // Context object to intercept rebalancing events and errors.
        let context = KafkaInputContext::new();

        debug!("Creating Kafka consumer");
        let kafka_consumer = BaseConsumer::from_config_and_context(&client_config, context)?;
        let topics = config.topics.iter().map(String::as_str).collect::<Vec<_>>();
        let state = match step.unwrap() {
            InputStep::New {
                step: 0,
                prev_metadata: None,
            } => {
                kafka_consumer.subscribe(&topics)?;
                State::New(Metadata::default())
            }
            InputStep::New {
                step: _,
                prev_metadata: Some(prev_metadata),
            } => {
                let mut metadata: Metadata = serde_json::from_slice(prev_metadata)?;
                // XXX make sure the metadata topics are the same as `topics`.
                let mut assignment = TopicPartitionList::new();
                for (topic, partitions) in &metadata.offsets {
                    for (partition, range) in partitions.iter().enumerate() {
                        assignment
                            .add_partition_offset(
                                topic.as_str(),
                                partition as i32,
                                Offset::Offset(range.end),
                            )
                            .unwrap();
                    }
                }
                kafka_consumer.assign(&assignment)?;
                metadata.new_step();
                State::New(metadata)
            }
            InputStep::New { .. } => unreachable!(),
            InputStep::Replay { step: _, metadata } => {
                let metadata: Metadata = serde_json::from_slice(metadata)?;
                let mut assignment = TopicPartitionList::new();
                for (topic, partitions) in &metadata.offsets {
                    for (partition, range) in partitions.iter().enumerate() {
                        assignment
                            .add_partition_offset(
                                topic.as_str(),
                                partition as i32,
                                Offset::Offset(range.start),
                            )
                            .unwrap();
                    }
                }
                kafka_consumer.assign(&assignment)?;
            }
        };

        let inner = Arc::new(KafkaInputReaderInner {
            config: config.clone(),
            pipeline_state: Atomic::new(PipelineState::Paused),
            kafka_consumer,
            errors: ArrayQueue::new(ERROR_BUFFER_SIZE),
            receiver: consumer.clone(),
            state,
        });
        *inner.kafka_consumer.context().endpoint.lock().unwrap() = Arc::downgrade(&inner);

        // Subscribe consumer to `topics`.

        let start = Instant::now();

        // Wait for the consumer to join the group by waiting for the group
        // rebalance protocol to be set.
        loop {
            // We must poll in order to receive connection failures; otherwise
            // we'd have to rely on timeouts only.
            match inner
                .kafka_consumer
                .context()
                .deferred_logging
                .with_deferred_logging(|| inner.kafka_consumer.poll(POLL_TIMEOUT))
            {
                Some(Err(e)) => {
                    // Topic-does-not-exist error will be reported here.
                    bail!(
                        "failed to subscribe to topics '{topics:?}' (consumer group id '{}'): {e}",
                        inner.config.kafka_options.get("group.id").unwrap(),
                    );
                }
                Some(Ok(message)) => {
                    // `KafkaInputContext` should instantly pause the topic upon connecting to it.
                    // Hopefully, this guarantees that we won't see any messages from it, but if
                    // that's not the case, there shouldn't be any harm in sending them downstream.
                    if let Some(payload) = message.payload() {
                        let errors = parser.input_chunk(payload);
                        //inner.receiver.queue(payload.len(), parser.take(), errors);
                    }
                }
                _ => (),
            }

            // Invalid broker address and other global errors are reported here.
            if let Some((_error, reason)) = inner.pop_error() {
                bail!("error subscribing to topics {topics:?}: {reason}");
            }

            if matches!(
                inner.kafka_consumer.rebalance_protocol(),
                RebalanceProtocol::None
            ) {
                if start.elapsed()
                    >= Duration::from_secs(inner.config.group_join_timeout_secs as u64)
                {
                    let p = &mut partitions[next_partition];

                    self.wait_for_pipeline_start(step)?;
                    check_fatal_errors(consumer.client())
                        .context("Consumer reported fatal error")?;
                    check_fatal_errors(producer.client())
                        .context("Producer reported fatal error")?;

                    // If there's a saved message, take it, otherwise try to
                    // fetch a message.  Because we resume where we left off,
                    // any saved message is for `next_partition`.
                    let data_message = if let Some(message) = saved_message.take() {
                        Ok(Some(message))
                    } else {
                        data_queues[next_partition]
                            .poll(Duration::ZERO)
                            .transpose()
                            .with_context(|| {
                                format!("Failed to read {} partition queue.", p.data_ctp)
                            })
                    };

                    match data_message? {
                        Some(data_message)
                            if n_messages == 0
                                || n_bytes + data_message.payload_len()
                                    <= self.config.max_step_bytes =>
                        {
                            // We got a message that fits within the byte
                            // limit (or there are no messages in the current
                            // step).

                            if let Some(payload) = data_message.payload() {
                                //self.receiver.queue(payload.len(), self.parser.parse(payload));
                            }
                            n_messages += 1;
                            n_bytes += data_message.payload_len();

                            p.start_offset.get_or_insert(data_message.offset());
                            p.next_offset = data_message.offset() + 1;
                            lack_of_progress = 0;
                        }
                        Some(data_message) => {
                            // We got a message but it would overflow the byte
                            // limit for the step.  Save it for the next step.
                            //
                            // We don't advance `next_partition` so that we'll
                            // resume in the same place.
                            saved_message = Some(data_message);
                            break 'assemble;
                        }
                        None => lack_of_progress += 1,
                    }

                    next_partition += 1;
                    if next_partition >= n_partitions {
                        next_partition = 0;
                    }

                    if lack_of_progress >= n_partitions {
                        // Wait for at least one of these to happen:
                        //   - A message to arrive in one of the data partitions.
                        //   - A completion request for this step.
                        //   - A request to exit.
                        self.parker.park();
                        lack_of_progress = 0;
                    }
                }
                // println!("waiting to join the group");
            } else {
                break;
            }
        }

        let endpoint_clone = inner.clone();
        let poller =
            spawn(move || KafkaInputReader::poller_thread(endpoint_clone, consumer, parser));
        Ok(KafkaInputReader(inner, poller))
    }

    /// The main poller thread for a Kafka input. Polls `endpoint` as long as
    /// the pipeline is running, and passes the data to `consumer`.
    fn poller_thread(
        endpoint: Arc<KafkaInputReaderInner>,
        consumer: Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
    ) {
        let mut state = PipelineState::Paused;

        loop {
            let new_state = endpoint.pipeline_state();
            if state != new_state {
                let result = match new_state {
                    PipelineState::Paused => endpoint.pause_partitions(),
                    PipelineState::Running => endpoint.resume_partitions(),
                    PipelineState::Terminated => return,
                };
                if let Err(e) = result {
                    let (_fatal, e) = endpoint.refine_error(e);
                    consumer.error(true, e);
                    return;
                };
                state = new_state;
            }

            // Keep polling even while the consumer is paused as `BaseConsumer`
            // processes control messages (including rebalancing and errors)
            // within the polling thread.
            match endpoint.kafka_consumer.poll(POLL_TIMEOUT) {
                None => (),
                Some(Err(e)) => {
                    // println!("poll returned error");
                    let (fatal, e) = endpoint.refine_error(e);
                    consumer.error(fatal, e);
                    return;
                }
                Some(Ok(message)) => {
                    // println!("received {} bytes", message.payload().unwrap().len());
                    // message.payload().map(|payload| consumer.input(payload));

                    if let Some(payload) = message.payload() {
                        // Leave it to the controller to handle errors.  There is noone we can
                        // forward the error to upstream.
                        let errors = parser.input_chunk(payload);
                        //consumer.queue(payload.len(), parser.take(), errors);
                    }
                }
            }

            while let Some((error, reason)) = endpoint.pop_error() {
                let (fatal, _e) = endpoint.refine_error(error);
                // `reason` contains a human-readable description of the
                // error.
                consumer.error(fatal, anyhow!(reason));
                if fatal {
                    return;
                }
            }
        }
    }
}

impl InputEndpoint for KafkaFtInputEndpoint {
    fn is_fault_tolerant(&self) -> bool {
        true
    }
}

impl TransportInputEndpoint for KafkaFtInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        start_step: Option<InputStep>,
        _schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(KafkaInputReader::new(
            &self.config,
            consumer,
            parser,
            start_step,
        )?))
    }
}

impl InputReader for KafkaInputReader {
    fn request(&self, command: super::InputReaderCommand) {
        todo!()
    }
}

impl Drop for KafkaInputReader {
    fn drop(&mut self) {
        self.disconnect();
    }
}

#[derive(Deserialize, Serialize, Default)]
struct Metadata {
    offsets: BTreeMap<String, Vec<Range<i64>>>,
}

impl Metadata {
    fn new_step(&mut self) {
        for (_topic, partitions) in self.offsets.iter_mut() {
            for (_partition, range) in partitions.iter_mut().enumerate() {
                *range = range.end..range.end;
            }
        }
    }
}
