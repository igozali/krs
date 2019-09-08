use chrono::{DateTime, TimeZone, Utc};
use clap::{App, Arg, ArgMatches, SubCommand};
use rdkafka::{ClientConfig};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::metadata::{MetadataTopic};
use serde::{Serialize, Deserialize};
use serde_json;
use zookeeper::{ZooKeeper, Watcher, WatchedEvent, };

use std::time::Duration;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

struct _Watcher;
impl Watcher for _Watcher {
    fn handle(&self, e: WatchedEvent) {
        // Do nothing
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct PartitionInfo {
    id: i32,
    low_watermark: i64,
    high_watermark: i64,
    //leader: Option<i32>,
    //replicas: Option<Vec<i32>>,
    //isr: Option<Vec<i32>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TopicInfo {
    name: String,
    partitions: Vec<PartitionInfo>,
    // replication_factor: usize,
    // TODO: Get from zookeeper.
    ctime: Option<String>,
    mtime: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ShortTopicInfo {
    name: String,
    num_partitions: usize,
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
                low_watermark: -1,
                high_watermark: -1,
                //leader: Some(p.leader()),
                //replicas: Some(Vec::from(p.replicas())),
                //isr: Some(Vec::from(p.isr()))
            }).collect(),
            ctime: None,
            mtime: None,
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

fn describe_topic(brokers: String, zookeeper: Option<String>, topic: String) {
    let consumer: BaseConsumer<_> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .unwrap();

    let zk: Option<ZooKeeper> = zookeeper.and_then(|z| ZooKeeper::connect(&z, DEFAULT_TIMEOUT, _Watcher).ok());

    let md = consumer.fetch_metadata(Some(&topic), Some(DEFAULT_TIMEOUT)).unwrap();
    let topics = md.topics();
    assert!(topics.len() == 1);

    let mut info: TopicInfo = (&topics[0]).into();
    for p in info.partitions.iter_mut() {
        let watermarks = consumer.fetch_watermarks(&topic, p.id, Some(DEFAULT_TIMEOUT)).unwrap();
        p.low_watermark = watermarks.0;
        p.high_watermark = watermarks.1;
    }

    if let Some(zk) = &zk {
        let (_, stat) = zk.get_data(&format!("/brokers/topics/{}", topic), false).unwrap();
        // println!("from_utf8(bytes) -> {:?}", String::from_utf8(bytes));
        info.ctime = Some(Utc.timestamp(stat.ctime / 1000, 0).to_string());
        info.mtime = Some(Utc.timestamp(stat.mtime / 1000, 0).to_string());
    }

    println!("{}", serde_json::to_string(&info).unwrap());
}

fn dispatch(m: ArgMatches) {
    if let Some(s) = m.subcommand_matches("topics") {
        if let Some(ss) = s.subcommand_matches("list") {
            let brokers = ss.value_of("brokers").unwrap().to_owned();
            list_topics(brokers);
        } else if let Some(ss) = s.subcommand_matches("describe") {
            let brokers = ss.value_of("brokers").unwrap().to_owned();
            let topic = ss.value_of("topic").unwrap().to_owned();
            let zookeeper = ss.value_of("zookeeper").map(|x| x.to_owned());
            describe_topic(brokers, zookeeper, topic);
        }
    } else {
        println!("Please specify a subcommand! Use -h for more information.")
    }
}

// krs topics list|create|delete|describe --brokers
fn main() {
    fn brokers<'a, 'b>() -> Arg<'a, 'b> {
        Arg::with_name("brokers")
            .help("Comma-delimited list of brokers")
            .short("b")
            .takes_value(true)
            .required(true)
    }

    fn zookeeper<'a, 'b>() -> Arg<'a, 'b> {
        Arg::with_name("zookeeper")
            .help("Zookeeper address")
            .short("z")
            .takes_value(true)
    }

    fn topic<'a, 'b>() -> Arg<'a, 'b> {
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
                .about("Show more info about topic name.")
                .arg(brokers())
                .arg(topic())
                .arg(zookeeper())
            )
        )
        .get_matches();

    dispatch(matches);
}
