use clap::{App, Arg};

use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::get_rdkafka_version;

use std::sync::Arc;
use std::thread::spawn;
use std::time::Duration;

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance<'a>(&self, _rebalance: &Rebalance<'a>) {
        println!("pre_rebalance");
    }

    fn post_rebalance<'a>(&self, _rebalance: &Rebalance<'a>) {
        println!("post_rebalance");
    }

    fn commit_callback(&self, _result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        println!("commit");
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = BaseConsumer<CustomContext>;

fn consume_and_print(brokers: &str, group_id: &str, topics: &[&str], n_partitions: usize) {
    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "true")
        .set("session.timeout.ms", "6000")
        //.set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "smallest")
        //.set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&topics.to_vec())
        .expect("Can't subscribe to specified topics");

    let mut n_records = 0;
    let mut n_bytes = 0;
    let mut n_eofs = 0;
    let mut n_messages = 0;
    for result in &consumer {
        match result {
            Err(KafkaError::PartitionEOF(_p)) if n_partitions > 0 => {
                n_eofs += 1;
                if n_eofs == n_partitions {
                    println!("read {n_records} records ({n_messages} messages, {n_bytes} bytes) across {n_partitions} partitions");
                    return;
                }
            }
            Err(e) => panic!("Kafka error after {n_messages} messages: {}", e),
            Ok(m) => {
                let payload = m.payload().unwrap_or(&[]);
                n_bytes += payload.len();
                n_records += payload
                    .split(|c| *c == b'\n')
                    .filter(|line| !line.is_empty())
                    .count();
                n_messages += 1;
            }
        };
    }
}

fn threaded_consume_and_print(brokers: &str, group_id: &str, topics: &[&str], n_partitions: usize) {
    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "true")
        .set("session.timeout.ms", "6000")
        //.set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "smallest")
        //.set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    let mut assignment = TopicPartitionList::new();
    for topic in topics {
        assignment.add_partition_range(topic, 0, n_partitions as i32);
    }
    consumer.assign(&assignment).unwrap();

    let consumer = Arc::new(consumer);

    let mut threads = Vec::new();
    for partition in 0..n_partitions as i32 {
        let queue = consumer
            .split_partition_queue(topics[0], partition)
            .unwrap();
        threads.push(spawn(move || {
            let mut n_messages = 0;
            let mut n_records = 0;
            let mut n_bytes = 0;
            loop {
                match queue.poll(None).unwrap() {
                    Err(KafkaError::PartitionEOF(_p)) => break,
                    Err(e) => panic!("Kafka error after {n_messages} messages: {}", e),
                    Ok(m) => {
                        let payload = m.payload().unwrap_or(&[]);
                        n_bytes += payload.len();
                        n_records += payload
                            .split(|c| *c == b'\n')
                            .filter(|line| !line.is_empty())
                            .count();
                        n_messages += 1;
                    }
                };
            }
            (n_messages, n_records, n_bytes)
        }));
    }

    let mut n_messages = 0;
    let mut n_records = 0;
    let mut n_bytes = 0;
    for thread in threads {
        while !thread.is_finished() {
            consumer.poll(Duration::from_millis(1));
        }
        let (thread_messages, thread_records, thread_bytes) = thread.join().unwrap();
        n_messages += thread_messages;
        n_records += thread_records;
        n_bytes += thread_bytes;
    }
    println!("read {n_records} records ({n_messages} messages, {n_bytes} bytes) across {n_partitions} partitions");
}

fn main() {
    let matches = App::new("consumer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(
            Arg::with_name("brokers")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("group-id")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("example_consumer_group_id"),
        )
        .arg(
            Arg::with_name("topics")
                .long("topics")
                .help("Topic list")
                .takes_value(true)
                .multiple(true)
                .required(true),
        )
        .arg(
            Arg::with_name("partitions")
                .long("partitions")
                .help("Number of partitions")
                .takes_value(true)
                .default_value("0"),
        )
        .arg(
            Arg::with_name("threaded")
                .long("threaded")
                .help("Use one thread per partition"),
        )
        .get_matches();

    let n_partitions: usize = matches.value_of("partitions").unwrap().parse().unwrap();
    let threaded = matches.contains_id("threaded");

    let (version_n, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topics = matches.values_of("topics").unwrap().collect::<Vec<&str>>();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();

    if threaded {
        threaded_consume_and_print(brokers, group_id, &topics, n_partitions);
    } else {
        consume_and_print(brokers, group_id, &topics, n_partitions);
    }
}
