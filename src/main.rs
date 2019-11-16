use chrono::{DateTime, TimeZone, Utc};
use clap::{App, Arg, ArgMatches, SubCommand};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::metadata::MetadataTopic;
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use serde_json;
use zookeeper::{WatchedEvent, Watcher, ZooKeeper};

use std::env;
use std::time::Duration;

use krs::Config;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

struct _Watcher;
impl Watcher for _Watcher {
    fn handle(&self, e: WatchedEvent) {
        // Do nothing
    }
}

// fn describe_topic(brokers: String, zookeeper: Option<String>, topic: String) {
//     let consumer: BaseConsumer<_> = ClientConfig::new()
//         .set("bootstrap.servers", &brokers)
//         .create()
//         .unwrap();

//     let zk: Option<ZooKeeper> = zookeeper.and_then(|z| ZooKeeper::connect(&z, DEFAULT_TIMEOUT, _Watcher).ok());

//     let md = consumer.fetch_metadata(Some(&topic), Some(DEFAULT_TIMEOUT)).unwrap();
//     let topics = md.topics();
//     assert!(topics.len() == 1);

//     let mut info: TopicInfo = (&topics[0]).into();
//     for p in info.partitions.iter_mut() {
//         let watermarks = consumer.fetch_watermarks(&topic, p.id, Some(DEFAULT_TIMEOUT)).unwrap();
//         p.low_watermark = watermarks.0;
//         p.high_watermark = watermarks.1;
//     }

//     if let Some(zk) = &zk {
//         let (_, stat) = zk.get_data(&format!("/brokers/topics/{}", topic), false).unwrap();
//         // println!("from_utf8(bytes) -> {:?}", String::from_utf8(bytes));
//         info.ctime = Some(Utc.timestamp(stat.ctime / 1000, 0).to_string());
//         info.mtime = Some(Utc.timestamp(stat.mtime / 1000, 0).to_string());
//     }

//     println!("{}", serde_json::to_string(&info).unwrap());
// }

// fn dispatch(m: ArgMatches) {
//     if let Some(s) = m.subcommand_matches("topics") {
//         if let Some(ss) = s.subcommand_matches("list") {
//             let brokers = ss.value_of("brokers").unwrap().to_owned();
//             list_topics(brokers);
//         } else if let Some(ss) = s.subcommand_matches("describe") {
//             let brokers = ss.value_of("brokers").unwrap().to_owned();
//             let topic = ss.value_of("topic").unwrap().to_owned();
//             let zookeeper = ss.value_of("zookeeper").map(|x| x.to_owned());
//             describe_topic(brokers, zookeeper, topic);

fn dispatch(m: ArgMatches<'_>) -> krs::Result<()> {
    let config: Config = (&m).into();
    if let Some(s) = m.subcommand_matches("topics") {
        if let Some(ss) = s.subcommand_matches("list") {
            let cmd: krs::topics::ListCommand = config.into();
            cmd.run()
        }
        // else if let Some(ss) = s.subcommand_matches("describe") {
        //     let brokers = ss.value_of("brokers").unwrap().to_owned();
        //     let topic = ss.value_of("topic").unwrap().to_owned();
        //     // describe_topic(brokers, topic);
        //     Ok(())
        // }
        else {
            Err(krs::Error::Generic(
                "Please specify a subcommand! Use -h for more information.".into(),
            ))
        }
    } else if let Some(s) = m.subcommand_matches("env") {
        if let Some(ss) = s.subcommand_matches("show") {
            let cmd: krs::env::ShowCommand = config.into();
            cmd.run()
        } else {
            Err(krs::Error::Generic(
                "Please specify a subcommand! Use -h for more information.".into(),
            ))
        }
    } else {
        Err(krs::Error::Generic(
            "Please specify a subcommand! Use -h for more information.".into(),
        ))
    }
}

fn make_parser<'a, 'b>() -> App<'a, 'b> {
    fn brokers<'a, 'n>() -> Arg<'a, 'n> {
        Arg::with_name("brokers")
            .help("Comma-delimited list of brokers")
            .short("b")
            .takes_value(true)
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
    }

    App::new("krs")
        .about("Decent Kafka CLI tool.")
        .arg(brokers())
        .arg(zookeeper())
        .arg(
            Arg::with_name("output-type")
                .short("o")
                .help("Output type (can be table, csv, json)")
                .default_value("json")
        )
        .subcommand(
            SubCommand::with_name("env")
                .about("Environment commands")
                .subcommand(krs::env::ShowCommand::subcommand())
        )
        .subcommand(
            SubCommand::with_name("topics")
                .about("Topic commands")
                .subcommand(krs::topics::ListCommand::subcommand())
                .subcommand(
                    SubCommand::with_name("describe")
                        .about("Show more info about topic name.")
                        .arg(brokers())
                        .arg(topic())
                        .arg(zookeeper()),
                ),
        )
}

// krs env show|set
// krs topics list|create|delete|describe --brokers
fn main() -> krs::Result<()> {
    let app = make_parser();
    let matches = app.get_matches_from(env::args());

    dispatch(matches)
}
