use chrono::{TimeZone, Utc};
use clap::{App, SubCommand};
use futures::future::Future;
// TODO: Need this line after fixing AdminClient hack
// use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
// TODO: Remove this line after fixing AdminClient hack
use crate::hack::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::metadata::MetadataTopic;
use serde::{Deserialize, Serialize};
use serde_json;
use zookeeper::{WatchedEvent, Watcher, ZooKeeper};

use crate::args;
use crate::{new_admin_client, new_consumer, Config, Error, DEFAULT_TIMEOUT};

#[derive(Debug, Serialize, Deserialize, Default)]
struct PartitionInfo {
    id: i32,
    watermarks: (i64, i64),
    //leader: Option<i32>,
    //replicas: Option<Vec<i32>>,
    //isr: Option<Vec<i32>>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct TopicInfo {
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
pub(crate) struct ShortTopicInfo {
    pub name: String,
    pub num_partitions: usize,
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
    consumer: BaseConsumer,
}

impl ListCommand {
    pub fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("list").about("List topics")
    }

    pub fn run(&self) -> crate::Result<()> {
        let md = self.consumer.fetch_metadata(None, Some(DEFAULT_TIMEOUT))?;

        let topics = md.topics();
        let infos: Vec<ShortTopicInfo> = topics.iter().map(|t| t.into()).collect();

        println!("{}", serde_json::to_string(&infos).unwrap());
        Ok(())
    }
}

impl From<Config> for ListCommand {
    fn from(conf: Config) -> Self {
        let brokers = conf
            .brokers
            .as_ref()
            .expect("brokers is required for `topics list`");
        Self {
            consumer: new_consumer(&brokers, None),
        }
    }
}

struct DoNothingWatcher;
impl Watcher for DoNothingWatcher {
    fn handle(&self, _e: WatchedEvent) {
        // Do nothing
    }
}

pub struct DescribeCommand {
    consumer: BaseConsumer,
    zk: ZooKeeper,
}

impl DescribeCommand {
    pub fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("describe")
            .about("Show more info about the specified topic.")
            .arg(args::topic().required(true))
    }

    pub fn run(&self, topic_name: &str) -> crate::Result<()> {
        let md = self
            .consumer
            .fetch_metadata(Some(&topic_name), Some(DEFAULT_TIMEOUT))
            .unwrap();
        let topics = md.topics();
        assert!(topics.len() == 1, "DescribeCommand takes only 1 topic");

        let mut info = TopicInfo::from(&topics[0]);
        for p in info.partitions.iter_mut() {
            let watermarks = self
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

impl From<Config> for DescribeCommand {
    fn from(conf: Config) -> Self {
        let brokers = conf
            .brokers
            .as_ref()
            .expect("brokers is required for `topics describe`");
        let zookeeper = conf
            .zookeeper
            .as_ref()
            .expect("zookeeper is required for `topics describe`");

        Self {
            consumer: new_consumer(&brokers, None),
            zk: ZooKeeper::connect(&zookeeper, DEFAULT_TIMEOUT, DoNothingWatcher).unwrap(),
        }
    }
}

pub struct CreateCommand {
    admin: AdminClient<DefaultClientContext>,
}

impl CreateCommand {
    pub fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("create")
            .about("Creates a new Kafka topic.")
            .arg(args::topic().required(true))
            .arg(args::num_partitions())
            .arg(args::num_replicas())
    }

    pub fn run(
        &self,
        topic_name: &str,
        num_partitions: i32,
        num_replicas: i32,
    ) -> crate::Result<()> {
        let new_topics = &[NewTopic::new(
            topic_name,
            num_partitions,
            TopicReplication::Fixed(num_replicas),
        )];
        let admin_options = &AdminOptions::new();
        let rx = self
            .admin
            .create_topics(new_topics, admin_options)
            .wait()?;
            // TODO: Need this line after fixing AdminClient hack
            // .map_err(Error::Kafka)?;

        rx[0]
            .as_ref()
            // Print topic name only if successful
            .map(|t| println!("{}", t))
            .map_err(|(n, e)| {
                Error::Generic(format!("Failed to create topic `{}`. Reason: `{:?}`", n, e))
            })
    }
}

impl From<Config> for CreateCommand {
    fn from(conf: Config) -> Self {
        let brokers = conf.brokers.as_ref().unwrap();

        Self {
            admin: new_admin_client(&brokers),
        }
    }
}

pub struct DeleteCommand {
    admin: AdminClient<DefaultClientContext>,
}

impl DeleteCommand {
    pub fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("delete")
            .about("Deletes the specified Kafka topic.")
            .arg(args::topic().required(true))
    }

    pub fn run(&self, topic_name: &str) -> crate::Result<()> {
        let rx = self
            .admin
            .delete_topics(&[topic_name], &AdminOptions::new())
            .wait()?;
            // TODO: Need this line after fixing AdminClient hack
            // .map_err(Error::Kafka)?;

        rx[0]
            .as_ref()
            // Print topic name only if successful
            .map(|t| println!("{}", t))
            .map_err(|(n, e)| {
                Error::Generic(format!("Failed to delete topic `{}`. Reason: `{:?}`", n, e))
            })
    }
}

impl From<Config> for DeleteCommand {
    fn from(conf: Config) -> Self {
        let brokers = conf.brokers.as_ref().unwrap();

        Self {
            admin: new_admin_client(&brokers),
        }
    }
}
