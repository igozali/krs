use chrono::{TimeZone, Utc};
use clap::{App, SubCommand};
use rdkafka::consumer::Consumer;
use rdkafka::metadata::MetadataTopic;
use serde::{Deserialize, Serialize};
use serde_json;
use zookeeper::{WatchedEvent, Watcher, ZooKeeper};

use crate::args;
use crate::{CommandBase, Config, Error, DEFAULT_TIMEOUT};

#[derive(Debug, Serialize, Deserialize, Default)]
struct PartitionInfo {
    id: i32,
    watermarks: (i64, i64),
    //leader: Option<i32>,
    //replicas: Option<Vec<i32>>,
    //isr: Option<Vec<i32>>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct TopicInfo {
    name: String,
    partitions: Vec<PartitionInfo>,
    // Both the fields below are only retrieved in DescribeCommand
    ctime: Option<String>,
    mtime: Option<String>,
}

impl From<&MetadataTopic> for TopicInfo {
    fn from(mt: &MetadataTopic) -> Self {
        Self {
            name: mt.name().to_owned(),
            partitions: mt
                .partitions()
                .iter()
                .map(|p| PartitionInfo {
                    id: p.id(),
                    watermarks: (-1, -1),
                    //leader: Some(p.leader()),
                    //replicas: Some(Vec::from(p.replicas())),
                    //isr: Some(Vec::from(p.isr()))
                })
                .collect(),
            ..Default::default()
        }
    }
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
            num_partitions: mt.partitions().len(),
        }
    }
}

pub struct ListCommand {
    base: CommandBase,
}

impl From<Config> for ListCommand {
    fn from(args: Config) -> Self {
        let brokers = args
            .brokers
            .as_ref()
            .expect("brokers is required for `topics list`");
        Self {
            base: CommandBase::new(&brokers),
        }
    }
}

impl ListCommand {
    pub fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("list")
            .about("List topics")
            .arg(args::brokers())
    }

    pub fn run(&self) -> crate::Result<()> {
        let res = self
            .base
            .consumer
            .fetch_metadata(None, Some(DEFAULT_TIMEOUT));

        let md = match res {
            Ok(v) => v,
            Err(e) => {
                return Err(Error::Generic(format!(
                    "Error while fetching metadata. {:?}",
                    e
                )))
            }
        };

        let topics = md.topics();
        let infos: Vec<ShortTopicInfo> = topics.iter().map(|t| t.into()).collect();

        println!("{}", serde_json::to_string(&infos).unwrap());
        Ok(())
    }
}

struct _Watcher;
impl Watcher for _Watcher {
    fn handle(&self, _e: WatchedEvent) {
        // Do nothing
    }
}

pub struct DescribeCommand {
    base: CommandBase,
    zk: ZooKeeper,
    topic: String,
}

impl From<Config> for DescribeCommand {
    fn from(conf: Config) -> Self {
        let brokers = conf.brokers.as_ref().unwrap();
        let zookeeper = conf.zookeeper.as_ref().unwrap();

        let zk = ZooKeeper::connect(&zookeeper, DEFAULT_TIMEOUT, _Watcher).unwrap();

        Self {
            base: CommandBase::new(&brokers),
            zk: zk,
            topic: conf.topic.unwrap(),
        }
    }
}

impl DescribeCommand {
    pub fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("describe")
            .about("Show more info about topic name.")
            .arg(args::brokers())
            .arg(args::topic())
            .arg(args::zookeeper())
    }

    pub fn run(&self) -> crate::Result<()> {
        let topic_name = &self.topic;

        let md = self
            .base
            .consumer
            .fetch_metadata(Some(&topic_name), Some(DEFAULT_TIMEOUT))
            .unwrap();
        let topics = md.topics();
        assert!(topics.len() == 1, "DescribeCommand takes only 1 topic");

        let mut info = TopicInfo::from(&topics[0]);
        for p in info.partitions.iter_mut() {
            let watermarks = self
                .base
                .consumer
                .fetch_watermarks(&topic_name, p.id, Some(DEFAULT_TIMEOUT))
                .unwrap();
            p.watermarks = watermarks;
        }

        let (_, stat) = self
            .zk
            .get_data(&format!("/brokers/topics/{}", topic_name), false)
            .unwrap();
        info.ctime = Some(Utc.timestamp(stat.ctime / 1000, 0).to_string());
        info.mtime = Some(Utc.timestamp(stat.mtime / 1000, 0).to_string());

        println!("{}", serde_json::to_string(&info).unwrap());
        Ok(())
    }
}
