use crate::transport::{InputEndpoint, InputReaderCommand, InputStep};
use crate::{
    transport::{
        kafka::{rdkafka_loglevel_from, refine_kafka_error, DeferredLogging},
        secret_resolver::MaybeSecret,
        InputReader,
    },
    InputConsumer, PipelineState, TransportInputEndpoint,
};
use crate::{InputBuffer, ParseError, Parser};
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
use serde_json::Value;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::{self, available_parallelism, JoinHandle, Thread};
use std::{
    collections::HashSet,
    sync::{atomic::Ordering, Arc, Mutex, Weak},
    thread::spawn,
    time::{Duration, Instant},
};

/// Poll timeout must be low, as it bounds the amount of time it takes to resume the connector.
const POLL_TIMEOUT: Duration = Duration::from_millis(5);

// Size of the circular buffer used to pass errors from ClientContext
// to the worker thread.
const ERROR_BUFFER_SIZE: usize = 1000;

pub struct KafkaInputEndpoint {
    config: Arc<KafkaInputConfig>,
}

impl KafkaInputEndpoint {
    pub fn new(mut config: KafkaInputConfig) -> AnyResult<KafkaInputEndpoint> {
        config.validate()?;
        Ok(KafkaInputEndpoint {
            config: Arc::new(config),
        })
    }
}

struct KafkaInputReader {
    _inner: Arc<KafkaInputReaderInner>,
    command_sender: Sender<InputReaderCommand>,
    poller_thread: Thread,
}

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
                if true
                /*XXX*/
                {
                    let _ = endpoint.resume_partitions();
                } else {
                    let _ = endpoint.pause_partitions();
                }
            }
        }

        // println!("Rebalance complete");
    }
}

/// A thread-safe queue for collecting and flushing input buffers.
///
/// Commonly used by `InputReader` implementations for staging buffers from
/// worker threads.
pub struct InputQueue {
    pub queue: Mutex<VecDeque<Box<dyn InputBuffer>>>,
    pub consumer: Box<dyn InputConsumer>,
}

impl InputQueue {
    pub fn new(consumer: Box<dyn InputConsumer>) -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            consumer,
        }
    }

    /// Appends `buffer`, if non-`None` to the queue.  Reports to the controller
    /// that `num_bytes` have been received and at least partially parsed, and
    /// that `errors` have occurred during parsing.
    ///
    /// Using this method automatically satisfies the requirements described for
    /// [InputConsumer::queued].
    pub fn push(
        &self,
        (buffer, errors): (Option<Box<dyn InputBuffer>>, Vec<ParseError>),
        num_bytes: usize,
    ) {
        self.consumer.parse_errors(errors);
        match buffer {
            Some(buffer) if !buffer.is_empty() => {
                let num_records = buffer.len();
                self.queue.lock().unwrap().push_back(buffer);
                self.consumer.buffered(num_bytes, num_records);
            }
            _ => self.consumer.buffered(num_bytes, 0),
        }
    }

    pub fn queue(&self) {
        let mut total = 0;
        let n = self.consumer.max_batch_size();
        while total < n {
            let Some(mut buffer) = self.queue.lock().unwrap().pop_front() else {
                break;
            };
            total += buffer.flush(n - total);
            if !buffer.is_empty() {
                self.queue.lock().unwrap().push_front(buffer);
                break;
            }
        }
        self.consumer.extended(total, Value::Null);
    }
}

struct KafkaInputReaderInner {
    config: Arc<KafkaInputConfig>,
    kafka_consumer: BaseConsumer<KafkaInputContext>,
    errors: ArrayQueue<(KafkaError, String)>,
}

impl KafkaInputReaderInner {
    /// Tries to read a message from this Kafka consumer. If successful, passes
    /// it to `consumer`. If there's a problem or an EOF, sends it to
    /// `feedback`.
    fn poll(
        self: &Arc<Self>,
        consumer: &Box<dyn InputConsumer>,
        parser: &mut Box<dyn Parser>,
        feedback: &Sender<HelperFeedback>,
        queue: &InputQueue,
    ) {
        match self.kafka_consumer.poll(POLL_TIMEOUT) {
            None => (),
            Some(Err(KafkaError::PartitionEOF(p))) => {
                feedback.send(HelperFeedback::PartitionEOF(p)).unwrap()
            }
            Some(Err(e)) => {
                // println!("poll returned error");
                let (fatal, e) = self.refine_error(e);
                consumer.error(fatal, e);
                if fatal {
                    feedback.send(HelperFeedback::FatalError).unwrap();
                }
            }
            Some(Ok(message)) => {
                if let Some(payload) = message.payload() {
                    queue.push(parser.parse(payload), payload.len());
                }
            }
        }
    }

    fn push_error(&self, error: KafkaError, reason: &str) {
        // `force_push` makes the queue operate as a circular buffer.
        self.errors.force_push((error, reason.to_string()));
    }

    fn pop_error(&self) -> Option<(KafkaError, String)> {
        self.errors.pop()
    }

    #[allow(dead_code)]
    fn debug_consumer(&self) {
        /*let topic_metadata = self.kafka_consumer.fetch_metadata(None, Duration::from_millis(1000)).unwrap();
        for topic in topic_metadata.topics() {
            println!("topic: {}", topic.name());
            for partition in topic.partitions() {
                println!("  partition: {}, leader: {}, error: {:?}, replicas: {:?}, isr: {:?}", partition.id(), partition.leader(), partition.error(), partition.replicas(), partition.isr());
            }
        }*/
        // println!("Subscription: {:?}", self.kafka_consumer.subscription());
        println!("Assignment: {:?}", self.kafka_consumer.assignment());
    }

    /// Pause all partitions assigned to the consumer.
    fn pause_partitions(&self) -> KafkaResult<()> {
        // println!("pause");
        // self.debug_consumer();

        self.kafka_consumer
            .pause(&self.kafka_consumer.assignment()?)?;
        Ok(())
    }

    /// Resume all partitions assigned to the consumer.
    fn resume_partitions(&self) -> KafkaResult<()> {
        self.kafka_consumer
            .resume(&self.kafka_consumer.assignment()?)?;
        Ok(())
    }

    fn refine_error(&self, e: KafkaError) -> (bool, AnyError) {
        refine_kafka_error(self.kafka_consumer.client(), e)
    }
}

impl KafkaInputReader {
    fn new(
        config: &Arc<KafkaInputConfig>,
        consumer: Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
    ) -> AnyResult<Self> {
        // Create Kafka consumer configuration.
        debug!("Starting Kafka input endpoint: {:?}", config);

        let mut client_config = ClientConfig::new();

        for (key, value) in config.kafka_options.iter() {
            // If it is a secret reference, resolve it to the actual secret string
            match MaybeSecret::new_using_default_directory(
                MaybeSecretRef::new_using_pattern_match(value.clone()),
            )? {
                MaybeSecret::String(simple_string) => {
                    client_config.set(key, simple_string);
                }
                MaybeSecret::Secret(secret_string) => {
                    client_config.set(key, secret_string);
                }
            }
        }

        if let Some(log_level) = config.log_level {
            client_config.set_log_level(rdkafka_loglevel_from(log_level));
        }

        // Context object to intercept rebalancing events and errors.
        let context = KafkaInputContext::new();

        debug!("Creating Kafka consumer");
        let inner = Arc::new(KafkaInputReaderInner {
            config: config.clone(),
            kafka_consumer: BaseConsumer::from_config_and_context(&client_config, context)?,
            errors: ArrayQueue::new(ERROR_BUFFER_SIZE),
        });

        *inner.kafka_consumer.context().endpoint.lock().unwrap() = Arc::downgrade(&inner);

        let topics = config.topics.iter().map(String::as_str).collect::<Vec<_>>();

        // Subscribe consumer to `topics`.
        inner.kafka_consumer.subscribe(&topics)?;

        let start = Instant::now();

        let queue = InputQueue::new(consumer.clone());
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
                        queue.push(parser.parse(payload), payload.len());
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
                    bail!(
                        "failed to subscribe to topics '{topics:?}' (consumer group id '{}'), giving up after {}s",
                        inner.config.kafka_options.get("group.id").unwrap(),
                        inner.config.group_join_timeout_secs
                    );
                }
                // println!("waiting to join the group");
            } else {
                break;
            }
        }

        let (command_sender, command_receiver) = channel();
        let poller_handle = spawn({
            let endpoint = inner.clone();
            move || {
                if let Err(e) = KafkaInputReader::poller_thread(
                    &endpoint,
                    &consumer,
                    parser,
                    command_receiver,
                    queue,
                ) {
                    let (_fatal, e) = endpoint.refine_error(e);
                    consumer.error(true, e);
                }
            }
        });
        let poller_thread = poller_handle.thread().clone();
        Ok(KafkaInputReader {
            _inner: inner,
            command_sender,
            poller_thread,
        })
    }

    /// The main poller thread for a Kafka input. Polls `endpoint` as long as
    /// the pipeline is running, and passes the data to `consumer`.
    fn poller_thread(
        endpoint: &Arc<KafkaInputReaderInner>,
        consumer: &Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
        command_receiver: Receiver<InputReaderCommand>,
        queue: InputQueue,
    ) -> Result<(), KafkaError> {
        // Figure out the number of threads based on configuration, defaults,
        // and system resources.
        let max_threads = available_parallelism().map_or(16, NonZeroUsize::get);
        let n_threads = endpoint
            .config
            .poller_threads
            .unwrap_or(3)
            .clamp(1, max_threads);

        let mut partition_eofs = HashSet::new();
        let (feedback_sender, feedback_receiver) = channel();
        let helper_state = Arc::new(Atomic::new(PipelineState::Paused));
        let mut threads: Vec<HelperThread> = Vec::with_capacity(n_threads - 1);
        let queue = Arc::new(queue);

        // Create the rest of threads (start from 1 instead of 0
        // because we're one of the threads).
        for _ in 1..n_threads {
            threads.push(HelperThread::new(
                Arc::clone(&endpoint),
                consumer.clone(),
                parser.fork(),
                Arc::clone(&helper_state),
                feedback_sender.clone(),
                queue.clone(),
            ));
        }

        let mut running = false;
        let mut kafka_paused = true;
        let mut when_paused = Instant::now();
        const PAUSE_TIMEOUT: Duration = Duration::from_millis(3000);
        loop {
            for command in command_receiver.try_iter() {
                match command {
                    InputReaderCommand::Seek(_) | InputReaderCommand::Replay(_) => {
                        unreachable!("Not a fault-tolerant connector.");
                    }
                    InputReaderCommand::Extend => {
                        println!("start");
                        running = true;
                    }
                    InputReaderCommand::Pause => {
                        println!("pause");
                        running = false;
                        when_paused = Instant::now();
                        helper_state.store(PipelineState::Paused, Ordering::Release);
                    }
                    InputReaderCommand::Queue => queue.queue(),
                    InputReaderCommand::Disconnect => return Ok(()),
                }
            }

            if !running && !kafka_paused && when_paused.elapsed() >= PAUSE_TIMEOUT {
                endpoint.pause_partitions()?;
                kafka_paused = true;
            } else if running && kafka_paused {
                endpoint.resume_partitions()?;
                helper_state.store(PipelineState::Running, Ordering::Release);
                for thread in threads.iter() {
                    thread.unpark();
                }
                kafka_paused = false;
            }

            // Keep polling even while the consumer is paused as `BaseConsumer`
            // processes control messages (including rebalancing and errors)
            // within the polling thread.
            endpoint.poll(consumer, &mut parser, &feedback_sender, &queue);

            for feedback in feedback_receiver.try_iter() {
                match feedback {
                    HelperFeedback::PartitionEOF(p) => {
                        // If all the partitions we're subscribed to have received
                        // an EOF, then we're done.
                        partition_eofs.insert(p);
                        if endpoint
                            .kafka_consumer
                            .assignment()
                            .unwrap_or_default()
                            .elements()
                            .iter()
                            .map(|tpl| tpl.partition())
                            .all(|p| partition_eofs.contains(&p))
                        {
                            consumer.eoi();
                            return Ok(());
                        }
                    }
                    HelperFeedback::FatalError => return Ok(()),
                }
            }

            while let Some((error, reason)) = endpoint.pop_error() {
                let (fatal, _e) = endpoint.refine_error(error);
                // `reason` contains a human-readable description of the
                // error.
                consumer.error(fatal, anyhow!(reason));
                if fatal {
                    return Ok(());
                }
            }
        }
    }
}

/// A thread that will help the main poller thread by processing messages
/// received from Kafka.
struct HelperThread {
    /// Used by the poller thread to tell us what to do.
    state: Arc<Atomic<PipelineState>>,

    /// Our own join handle so we can wait when dropped.
    join_handle: Option<JoinHandle<()>>,
}

impl HelperThread {
    fn new(
        endpoint: Arc<KafkaInputReaderInner>,
        mut consumer: Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
        state: Arc<Atomic<PipelineState>>,
        feedback_sender: Sender<HelperFeedback>,
        queue: Arc<InputQueue>,
    ) -> Self {
        Self {
            state: state.clone(),
            join_handle: {
                Some(spawn(move || loop {
                    match state.load(Ordering::Acquire) {
                        PipelineState::Paused => thread::park(),
                        PipelineState::Running => {
                            endpoint.poll(&mut consumer, &mut parser, &feedback_sender, &queue)
                        }
                        PipelineState::Terminated => break,
                    }
                }))
            },
        }
    }

    fn unpark(&self) {
        if let Some(h) = &self.join_handle {
            h.thread().unpark()
        }
    }
}

impl Drop for HelperThread {
    /// When we're dropped, make the thread exit.
    ///
    /// *Careful*: `state` is shared with all the helper threads, so if one gets
    /// dropped, the others will exit too. Currently this is OK because we
    /// always drop all of them together. It is in fact desirable because if
    /// they had separate `should_exit` flags then we'd have to block up to
    /// `POLL_TIMEOUT` per thread whereas since it is shared we will only block
    /// that long once.
    fn drop(&mut self) {
        self.state
            .store(PipelineState::Terminated, Ordering::Release);

        let _ = self.join_handle.take().map(|handle| {
            handle.thread().unpark();
            handle.join()
        });
    }
}

enum HelperFeedback {
    /// Helper delivered a fatal error to the consumer.
    FatalError,

    /// Helper received [`KafkaError::PartitionEOF`] on partition `p`.
    PartitionEOF(i32),
}

impl InputEndpoint for KafkaInputEndpoint {
    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

impl TransportInputEndpoint for KafkaInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _start_step: Option<InputStep>,
        _schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(KafkaInputReader::new(
            &self.config,
            consumer,
            parser,
        )?))
    }
}

impl InputReader for KafkaInputReader {
    fn request(&self, command: InputReaderCommand) {
        let _ = self.command_sender.send(command);
        self.poller_thread.unpark();
    }
}

impl Drop for KafkaInputReader {
    fn drop(&mut self) {
        self.request(InputReaderCommand::Disconnect);
    }
}
