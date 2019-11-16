use clap::{App, Arg, ArgMatches, SubCommand};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::metadata::MetadataTopic;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::{CommandBase, Config};

use serde_json;

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
    partitions: Vec<PartitionInfo>,
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
    base: CommandBase
}

impl From<Config> for ListCommand {
    fn from(args: Config) -> Self {
        let brokers = args.brokers.as_ref().unwrap();
        Self {
            base: CommandBase::new(&brokers)
        }
    }
}

impl ListCommand {
    pub fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("list").about("List topics").arg(
            Arg::with_name("brokers")
                .help("Comma-delimited list of brokers")
                .short("b")
                .takes_value(true)
        )
    }

    pub fn run(&self) -> crate::Result<()> {
        let md = match self.base.consumer.fetch_metadata(None, Some(DEFAULT_TIMEOUT)) {
            Ok(v) => v,
            Err(e) => {
                return Err(crate::Error::Generic(format!(
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
