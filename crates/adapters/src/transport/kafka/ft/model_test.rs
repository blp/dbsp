use anyhow::Error as AnyError;
use clap::{Parser, ValueEnum};
use log::error;
use pipeline_types::transport::kafka::default_redpanda_server;
use rdkafka::{mocking::MockCluster, producer::DefaultProducerContext};
use std::{
    cmp::max,
    env,
    fmt::{Debug, Formatter, Result as FmtResult},
    iter::once,
    sync::{
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc, Mutex, Weak,
    },
    thread::{sleep, spawn},
    time::Duration,
};
use uuid::Uuid;

use crate::{
    test::{
        kafka::KafkaResources,
        model_test::{test_model, ModelTestOptions, MutateError},
    },
    transport::{kafka::ft::test::init_test_logger, Step},
    InputConsumer, InputEndpoint, InputReader, InputTransport, OutputEndpoint, OutputTransport,
    ParseError,
};

/// This test accepts options through the environment.  The default options are
/// ones that make sense for running in CI.  Passing other options allows for
/// testing more targeted cases, or just more cases, or for using search
/// strategies other than the default.  Use `DBSP_ADAPTER_MODEL_TEST=--help` to
/// get help.
#[test]
fn test() {
    init_test_logger();
    println!("To quiet logging while running this test, invoke with `RUST_LOG=info` (quieter) or `RUST_LOG=error` (very quiet).");

    const VAR_NAME: &str = "DBSP_ADAPTER_MODEL_TEST";
    println!("Pass options to this test with `{VAR_NAME}`; use `{VAR_NAME}=--help` for help.");
    let args = shell_words::split(env::var(VAR_NAME).unwrap_or("".into()).as_str()).unwrap();

    let args = Args::parse_from(once("".into()).chain(args));

    let topic_factory = TopicFactory::new(args.broker, args.mock_parallel);
    test_model(&args.model_test_options, |name, actions, verbosity| {
        ft_test_model(name, actions, verbosity, &topic_factory)
    });
}

fn ft_test_model(
    name: &str,
    actions: &[usize],
    verbosity: usize,
    topic_factory: &TopicFactory,
) -> Result<usize, MutateError> {
    let uuid = Uuid::new_v4();
    let topic = format!("{name}_{uuid}");
    let index_topic = format!("{topic}_input-index");

    let (bootstrap_servers, _resources) =
        topic_factory.new_topics(&[(topic.as_str(), 1), (index_topic.as_str(), 1)]);
    let model = Model::new(bootstrap_servers, topic, index_topic, verbosity);

    for (i, &action) in actions.iter().enumerate() {
        Model::mutate(&model, action, i)?;
    }
    match Model::mutate(&model, usize::MAX, usize::MAX) {
        Err(MutateError::NoSuchStep(n)) => Ok(n),
        _ => unreachable!(),
    }
}

#[derive(Parser)]
struct Args {
    /// Model test options.
    #[command(flatten)]
    model_test_options: ModelTestOptions,

    /// Which Kafka broker to test against.
    #[arg(long, value_enum, default_value_t = Broker::Mock)]
    broker: Broker,

    /// For mock broker only, max number of tests run in parallel against a
    /// single mock broker before creating another.
    #[arg(long, default_value_t = 8)]
    mock_parallel: usize,
}

/// Which Kafka broker to test against.
#[derive(Clone, ValueEnum)]
enum Broker {
    /// Mock broker or brokers running in test process.
    Mock,

    /// `localhost:9092`, which must have already been started.
    Localhost,
}

type MockClusterId = usize;

enum MockClusterCommand {
    Acquire(Vec<(String, i32)>, SyncSender<(String, MockClusterId)>),
    Release(MockClusterId),
}

struct MockClusterResources {
    send_request: SyncSender<MockClusterCommand>,
    id: MockClusterId,
}

impl Drop for MockClusterResources {
    fn drop(&mut self) {
        self.send_request
            .send(MockClusterCommand::Release(self.id))
            .unwrap()
    }
}

/// Wraps [`MockCluster`] to support multiple threads.
///
/// Since [`MockCluster`] isn't `Send` or `Sync`, we create a separate thread to
/// manage it and then communicate with that thread through `mpsc` channels.
struct TestMockCluster {
    send_request: SyncSender<MockClusterCommand>,
}

impl TestMockCluster {
    fn new(max_parallel: usize) -> TestMockCluster {
        let (send_request, recv_request) = sync_channel(0);
        spawn(move || Self::worker(max_parallel, recv_request));
        Self { send_request }
    }

    fn worker(max_parallel: usize, recv_request: Receiver<MockClusterCommand>) {
        struct Cluster<'a> {
            mock_cluster: MockCluster<'a, DefaultProducerContext>,
            ref_count: usize,
        }
        let mut mock_clusters: Vec<Cluster> = Vec::new();
        while let Ok(command) = recv_request.recv() {
            match command {
                MockClusterCommand::Acquire(topics, send_reply) => {
                    let id = mock_clusters
                        .iter()
                        .position(|cluster| cluster.ref_count < max_parallel)
                        .unwrap_or_else(|| {
                            let mock_cluster = MockCluster::new(1).unwrap();
                            mock_clusters.push(Cluster {
                                mock_cluster,
                                ref_count: 0,
                            });
                            mock_clusters.len() - 1
                        });
                    let cluster = &mut mock_clusters[id];
                    for (topic, n) in topics {
                        cluster.mock_cluster.create_topic(&topic, n, 1).unwrap();
                    }
                    cluster.ref_count += 1;
                    send_reply
                        .send((cluster.mock_cluster.bootstrap_servers(), id))
                        .unwrap();
                }
                MockClusterCommand::Release(id) => {
                    mock_clusters[id].ref_count -= 1;
                }
            }
        }
    }

    fn create_topics(&self, topics: &[(&str, i32)]) -> (String, MockClusterResources) {
        let topics = topics
            .iter()
            .copied()
            .map(|(topic, n)| (topic.into(), n))
            .collect();
        let (send_reply, recv_reply) = sync_channel(0);
        self.send_request
            .send(MockClusterCommand::Acquire(topics, send_reply))
            .unwrap();
        let (bootstrap_servers, id) = recv_reply.recv().unwrap();
        (
            bootstrap_servers,
            MockClusterResources {
                send_request: self.send_request.clone(),
                id,
            },
        )
    }
}

enum TopicFactory {
    /// Mock cluster.
    ///
    /// `rdkafka` mock clusters are faster than using a real Kafka or Red Panda
    /// cluster, but they're not particularly robust: they tend to fall over
    /// after you use them intensively for a while.
    Mock(TestMockCluster),

    /// Real cluster.
    ///
    /// This uses the default cluster `localhost:9092`.  This is more reliable
    /// than mock clusters, but slower, and you have to have already started a
    /// broker on localhost.
    Real(String),
}

impl TopicFactory {
    /// Returns a new factory that can produce a broker for the model-based test
    /// to use.
    fn new(broker: Broker, mock_parallel: usize) -> Self {
        match broker {
            Broker::Localhost => TopicFactory::Real(default_redpanda_server()),
            Broker::Mock => TopicFactory::Mock(TestMockCluster::new(mock_parallel)),
        }
    }

    /// Creates a broker for a test to use.  Returns the `bootstrap.servers` to
    /// use for the broker and a tuple to drop when the broker is no longer
    /// needed.
    fn new_topics(
        &self,
        topics: &[(&str, i32)],
    ) -> (
        String,
        (Option<KafkaResources>, Option<MockClusterResources>),
    ) {
        match self {
            TopicFactory::Mock(ref mock_cluster) => {
                let (bootstrap_servers, mock_cluster_resources) =
                    mock_cluster.create_topics(topics);
                (bootstrap_servers, (None, Some(mock_cluster_resources)))
            }
            TopicFactory::Real(ref bootstrap_servers) => (
                bootstrap_servers.clone(),
                (Some(KafkaResources::create_topics(topics)), None),
            ),
        }
    }
}

struct Model {
    bootstrap_servers: String,
    topic: String,
    verbosity: usize,

    started_step: Option<Step>,
    committed_step: Option<Step>,
    received_chunks: Vec<usize>,

    endpoint: Box<dyn InputEndpoint>,
    reader: Box<dyn InputReader>,
    read_state: ReadState,

    writer: Box<dyn OutputEndpoint>,
    write_state: WriteState,

    fail: bool,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ReadAction {
    Start(Step),
    Complete(Step),
}

struct ReadState {
    start: Step,
    read: Option<Step>,
    complete: Option<Step>,
}

impl ReadState {
    fn new() -> Self {
        Self {
            start: 0,
            read: None,
            complete: None,
        }
    }

    fn valid_actions(&self) -> Vec<ReadAction> {
        let mut actions = Vec::new();

        let start = self.read.map(|x| x + 1).unwrap_or(0);
        if start < MAX_STEPS {
            actions.push(ReadAction::Start(start));
        }

        if let Some(read) = self.read {
            if let Some(complete) = self.complete {
                if complete < read {
                    actions.push(ReadAction::Complete(read));
                }
            }
        }

        actions
    }

    fn act(&mut self, action: ReadAction) {
        match action {
            ReadAction::Start(step) => self.read = Some(step),
            ReadAction::Complete(step) => self.complete = Some(step),
        }
    }
}

impl Debug for ReadState {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_struct("ReadState")
            .field("start", &self.start)
            .field("read", &self.read)
            .field("complete", &self.complete)
            .finish()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum WriteAction {
    BatchStart,
    PushBuffer,
    BatchEnd,
    RestartWriter,
}

impl WriteAction {
    fn cost(&self) -> usize {
        match self {
            WriteAction::BatchStart => 1,
            WriteAction::PushBuffer => 1,
            WriteAction::BatchEnd => 0,
            WriteAction::RestartWriter => 1,
        }
    }
}

#[derive(Clone, Debug)]
struct WriteState {
    actions: Vec<WriteAction>,
    position: usize,
    restarts: usize,
}

impl WriteState {
    fn new() -> WriteState {
        Self {
            actions: Vec::new(),
            position: 0,
            restarts: 0,
        }
    }

    fn n_finished_batches(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| **a == WriteAction::BatchEnd)
            .count()
    }

    fn n_batches(&self) -> usize {
        self.actions[0..self.position]
            .iter()
            .filter(|a| **a == WriteAction::BatchStart)
            .count()
    }

    fn is_batch_open(&self) -> bool {
        let actions = &self.actions[0..self.position];
        match actions.iter().rposition(|a| *a == WriteAction::BatchStart) {
            None => false,
            Some(last_batch_start) => !actions[last_batch_start..]
                .iter()
                .any(|a| *a == WriteAction::BatchEnd),
        }
    }

    fn n_finished_buffers(&self) -> usize {
        let last_batch_end = self
            .actions
            .iter()
            .rposition(|a| *a == WriteAction::BatchEnd)
            .unwrap_or(0);
        self.actions[0..last_batch_end]
            .iter()
            .filter(|a| **a == WriteAction::PushBuffer)
            .count()
    }

    fn act(&mut self, action: WriteAction) {
        if action == WriteAction::RestartWriter {
            self.position = 0;
            self.restarts += 1;
        } else {
            if self.position < self.actions.len() {
                assert_eq!(self.actions[self.position], action);
            } else {
                match action {
                    WriteAction::BatchStart => {
                        assert!(!self.is_batch_open());
                    }
                    WriteAction::PushBuffer | WriteAction::BatchEnd => {
                        assert!(self.is_batch_open());
                    }
                    WriteAction::RestartWriter => unreachable!(),
                }
                self.actions.push(action);
            }
            self.position += 1;
        }
    }
    fn cost(&self) -> usize {
        self.actions.iter().map(|a| a.cost()).sum::<usize>() + self.restarts
    }

    fn valid_actions(&self) -> Vec<WriteAction> {
        let mut actions = if self.position < self.actions.len() {
            vec![self.actions[self.position]]
        } else if self.is_batch_open() {
            vec![WriteAction::PushBuffer, WriteAction::BatchEnd]
        } else {
            vec![WriteAction::BatchStart]
        };
        if self.position > 0 {
            actions.push(WriteAction::RestartWriter);
        }
        if self.cost() >= MAX_STEPS as usize {
            actions.retain(|a| a.cost() == 0);
        }
        actions
    }
}

/// A limit on the number of steps and some other actions that the model can
/// take.  We limit these so that we don't end up doing, e.g., a depth-first
/// search where we do the same thing over and over and over.
const MAX_STEPS: Step = 5;

impl Model {
    fn log_action(&self, i: usize, action: impl AsRef<str>) {
        if self.verbosity > 1 {
            println!("    Action {i}: {}", action.as_ref());
        }
    }
    fn log(&self, s: impl AsRef<str>) {
        if self.verbosity > 1 {
            println!("    {}", s.as_ref());
        }
    }
    fn make_writer(bootstrap_servers: &str, topic: &str) -> Box<dyn OutputEndpoint> {
        let transport = <dyn OutputTransport>::get_transport("kafka").unwrap();
        let config_str = format!(
            r#"
stream: test_output1
transport:
    name: kafka
    config:
        bootstrap.servers: "{bootstrap_servers}"
        log_level: info
        topic: {topic}
        fault_tolerance: {{}}
format:
    name: csv
"#
        );
        let mut writer = transport
            .new_endpoint(&serde_yaml::from_str(&config_str).unwrap())
            .unwrap();
        writer.connect(Box::new(|_, _| ())).unwrap();
        writer
    }

    fn new(
        bootstrap_servers: String,
        topic: String,
        index_topic: String,
        verbosity: usize,
    ) -> Arc<Mutex<Self>> {
        let endpoint_config_str = format!(
            r#"
topics: [{topic}]
bootstrap.servers: "{bootstrap_servers}"
log_level: info
fault_tolerance: {{}}
"#
        );
        let transport = <dyn InputTransport>::get_transport("kafka").unwrap();
        let endpoint = transport
            .new_endpoint(&serde_yaml::from_str(&endpoint_config_str).unwrap())
            .unwrap();

        let writer = Self::make_writer(&bootstrap_servers, &topic);

        let mut action_log = Vec::new();
        action_log.push(format!("Data topic is {topic}"));
        action_log.push(format!("Index topic is {index_topic}"));
        action_log.push(
            "To preserve the topics for post-test inspection, run with `TEST_KEEP_KAFKA_TOPICS=1`."
                .into(),
        );

        Arc::new_cyclic(move |weak| {
            Mutex::new({
                let reader = endpoint.open(Box::new(Consumer(weak.clone())), 0).unwrap();
                Self {
                    bootstrap_servers,
                    topic,
                    verbosity,
                    started_step: None,
                    committed_step: None,
                    received_chunks: Vec::new(),
                    endpoint,
                    reader,
                    read_state: ReadState::new(),
                    writer,
                    write_state: WriteState::new(),
                    fail: false,
                }
            })
        })
    }

    fn mutate(arc: &Arc<Mutex<Self>>, action: usize, i: usize) -> Result<(), MutateError> {
        Self::do_mutate(arc, action, i)?;
        for _ in 0..100 {
            let model = arc.lock().unwrap();
            if model.fail {
                return Err(MutateError::Failure);
            }
            let expect_end_step = model.read_state.complete.map(|c| c + 1).unwrap_or(0);
            let steps = model.endpoint.steps().unwrap();
            assert_eq!(steps.start, 0);
            assert!(steps.end <= expect_end_step, "Reader has steps {steps:?} although the last step should be at most {expect_end_step} (read_state={:?})", model.read_state);
            if expect_end_step == steps.end {
                break;
            }
            drop(model);

            sleep(Duration::from_millis(100));
        }
        Ok(())
    }

    fn do_mutate(
        arc: &Arc<Mutex<Self>>,
        initial_action: usize,
        i: usize,
    ) -> Result<(), MutateError> {
        let mut action = initial_action;
        let mut model = arc.lock().unwrap();

        let write_actions = model.write_state.valid_actions();
        if let Some(&action) = write_actions.get(action) {
            match action {
                WriteAction::BatchStart => {
                    let batch = model.write_state.n_batches() as Step;
                    model.log_action(i, format!("OutputEndpoint.batch_start({batch})"));
                    model.writer.batch_start(batch).unwrap();
                }
                WriteAction::PushBuffer => {
                    model.log_action(i, "OutputEndpoint.push_buffer()");
                    model.writer.push_buffer(b"").unwrap();
                }
                WriteAction::BatchEnd => {
                    model.log_action(i, "OutputEndpoint.batch_end()");
                    model.writer.batch_end().unwrap();
                }
                WriteAction::RestartWriter => {
                    model.log_action(i, "restart writer");
                    model.writer = Self::make_writer(&model.bootstrap_servers, &model.topic);
                }
            }
            model.write_state.act(action);
            return Ok(());
        }
        action -= write_actions.len();

        let read_actions = model.read_state.valid_actions();
        if let Some(&action) = read_actions.get(action) {
            match action {
                ReadAction::Start(step) => {
                    model.reader.start(step).unwrap();
                    model.log_action(i, format!("InputReader.start({step})"));
                }
                ReadAction::Complete(step) => {
                    model.reader.complete(step);
                    model.log_action(i, format!("InputReader.complete({step})"))
                }
            }
            model.read_state.act(action);
            return Ok(());
        }
        action -= read_actions.len();

        Err(MutateError::NoSuchStep(initial_action - action))
    }

    fn fail(&mut self, msg: impl AsRef<str>) {
        self.fail = true;
        let msg = msg.as_ref();
        error!("Model check failed: {msg}");
    }
}

struct Consumer(Weak<Mutex<Model>>);
impl InputConsumer for Consumer {
    fn start_step(&mut self, step: Step) {
        let Some(arc) = self.0.upgrade() else { return };
        let mut model = arc.lock().unwrap();

        model.log(format!("start_step({step})"));

        // Check that `step` is no greater than the maximum step that should
        // have been started.
        let mut max_started_state = model.write_state.n_finished_batches() as Step;
        if let Some(complete) = model.read_state.complete {
            max_started_state = max(max_started_state, complete + 1);
        }
        if step > max_started_state {
            let msg = format!(
                "{step} in start_step({step}) is invalid for write state {:?} and complete {:?}",
                model.read_state.complete, model.write_state
            );
            model.fail(msg);
            return;
        }

        // Check that `step` is strictly greater than the previous value.
        if let Some(started_step) = model.started_step {
            if step <= started_step {
                model.fail(format!(
                    "start_step({step}) called after previous start_step({started_step})"
                ));
                return;
            }
        }
        model.started_step = Some(step);
    }

    fn input_fragment(&mut self, _data: &[u8]) -> Vec<ParseError> {
        // Fault-tolerant Kafka only submits chunks.
        unreachable!()
    }

    fn input_chunk(&mut self, _data: &[u8]) -> Vec<ParseError> {
        let Some(arc) = self.0.upgrade() else {
            return vec![];
        };
        let mut model = arc.lock().unwrap();

        let Some(step) = model.started_step else {
            model.fail("input_chunk called before a step was started");
            return vec![];
        };
        model.log(format!("input_chunk for step {step}"));

        while (model.received_chunks.len() as Step) <= step {
            model.received_chunks.push(0);
        }

        *model.received_chunks.last_mut().unwrap() += 1;
        let received_chunks = model.received_chunks.last().copied().unwrap();
        let max_received_chunks = model.write_state.n_finished_buffers();
        if received_chunks > max_received_chunks {
            let msg = format!(
                "{} chunks were received in step {step} but at most {} should have been",
                received_chunks, max_received_chunks,
            );
            model.fail(msg);
            return vec![];
        }

        vec![]
    }

    fn committed(&mut self, step: Step) {
        let Some(arc) = self.0.upgrade() else {
            return;
        };
        let mut model = arc.lock().unwrap();

        model.log(format!("committed({step})"));
        let Some(started_step) = model.started_step else {
            model.fail(format!("Step {step} cannot commit before any steps start"));
            return;
        };
        if started_step <= step {
            model.fail(format!("Step {step} cannot commit before step {} starts (and only step {started_step} has started)", step + 1));
            return;
        }

        if let Some(committed_step) = model.committed_step {
            if step <= committed_step {
                model.fail(format!("Committed steps must commit strictly monotonically, so {committed_step} then {step} is invalid."));
            }
        }
    }

    fn error(&mut self, _fatal: bool, _error: AnyError) {
        //todo!()
    }

    fn eoi(&mut self) -> Vec<ParseError> {
        //todo!()
        vec![]
    }

    fn fork(&self) -> Box<dyn InputConsumer> {
        unreachable!()
    }
}
