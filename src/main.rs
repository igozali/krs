use clap::{App, Arg, ArgMatches, SubCommand};
use rdkafka::{ClientConfig};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::metadata::{Metadata, MetadataTopic};
use serde::{Serialize, Deserialize};
use serde_json;

use std::time::Duration;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Serialize, Deserialize, Default)]
struct PartitionInfo {
    id: i32,
    watermarks: (i64, i64),
    //leader: Option<i32>,
    //replicas: Option<Vec<i32>>,
    //isr: Option<Vec<i32>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TopicInfo {
    name: String,
    partitions: Vec<PartitionInfo>
}

#[derive(Debug, Serialize, Deserialize)]
struct ShortTopicInfo {
    name: String,
    num_partitions: usize,

    // TODO: Get from zookeeper.
    //replication_factor: usize
    //ctime: Instant,
    //btime
}

impl From<&MetadataTopic> for ShortTopicInfo {
    fn from(mt: &MetadataTopic) -> Self {
        Self {
            name: mt.name().to_owned(),
            num_partitions: mt.partitions().len()
        }
    }
}

impl From<&MetadataTopic> for TopicInfo {
    fn from(mt: &MetadataTopic) -> Self {
        Self {
            name: mt.name().to_owned(),
            partitions: mt.partitions().iter().map(|p| PartitionInfo {
                id: p.id(),
                watermarks: (-1, -1),
                //leader: Some(p.leader()),
                //replicas: Some(Vec::from(p.replicas())),
                //isr: Some(Vec::from(p.isr()))
            }).collect()
        }
    }
}

fn list_topics(brokers: String) {
    let consumer: BaseConsumer<_> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap();

    let md = consumer.fetch_metadata(None, Some(DEFAULT_TIMEOUT)).unwrap();
    let topics = md.topics();
    let infos: Vec<ShortTopicInfo> = topics.iter().map(|t| t.into()).collect();

    println!("{}", serde_json::to_string(&infos).unwrap());
}

fn describe_topic(brokers: String, topic: String) {
    let consumer: BaseConsumer<_> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap();

    let md = consumer.fetch_metadata(Some(&topic), Some(DEFAULT_TIMEOUT)).unwrap();
    let topics = md.topics();
    assert!(topics.len() == 1);

    let mut infos: Vec<TopicInfo> = topics.iter().map(|t| t.into()).collect();
    let mut info: TopicInfo = infos.pop().unwrap();
    for t in info.partitions.iter_mut() {
        let watermarks = consumer.fetch_watermarks(&topic, t.id, Some(DEFAULT_TIMEOUT)).unwrap();
        t.watermarks = watermarks;
    }

    println!("{}", serde_json::to_string(&info).unwrap());
}

fn dispatch<'a>(m: ArgMatches<'a>) {
    if let Some(s) = m.subcommand_matches("topics") {
        if let Some(ss) = s.subcommand_matches("list") {
            let brokers = ss.value_of("brokers").unwrap().to_owned();
            list_topics(brokers);
        } else if let Some(ss) = s.subcommand_matches("describe") {
            let brokers = ss.value_of("brokers").unwrap().to_owned();
            let topic = ss.value_of("topic").unwrap().to_owned();
            describe_topic(brokers, topic);
        }
    } else {
        println!("Please specify a subcommand! Use -h for more information.")
    }
}

// krs topics list|create|delete|describe --brokers
fn main() {
    fn brokers<'a, 'n>() -> Arg<'a, 'n> {
        Arg::with_name("brokers")
            .help("Comma-delimited list of brokers")
            .short("b")
            .takes_value(true)
            .required(true)
    }

    fn topic<'a, 'n>() -> Arg<'a, 'n> {
        Arg::with_name("topic")
            .help("Topic name")
            .short("t")
            .takes_value(true)
            .required(true)
    }

    let matches = App::new("krs")
        .about("Better Kafka CLI tool.")
        .author("Ivan Gozali <gozaliivan@gmail.com>")
        .subcommand(SubCommand::with_name("topics")
            .about("Topic commands")
            .subcommand(SubCommand::with_name("list")
                .about("List topics")
                .arg(brokers())
            )
            .subcommand(SubCommand::with_name("describe")
                .about("Describe topic")
                .arg(brokers())
                .arg(topic())
            )
        )
        .get_matches();

    dispatch(matches);
}
