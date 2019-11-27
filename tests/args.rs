#[cfg(test)]
use chrono::Utc;

use krs::{make_parser, Config, Sourced};
use krs::commands::topics::{ListCommand, CreateCommand, DescribeCommand};

macro_rules! assert_ok {
    ($x:expr) => {
        if let Err(v) = $x {
            panic!(format!("Expected Ok value, but got {:?}", v));
        }
    };
}

#[test]
fn test_global_args() {
    let parser = make_parser();
    assert_ok!(parser.get_matches_from_safe(&["./binary", "env", "show"]));

    let parser = make_parser();
    let matches = parser
        .get_matches_from_safe(&["./binary", "-b", "localhost:9092", "env", "show"])
        .unwrap();
    let config = Config::from(&matches);

    assert_eq!(config.brokers.value, Some("localhost:9092".to_owned()));

    let parser = make_parser();
    let matches = parser
        .get_matches_from_safe(&["./binary", "env", "show", "-b", "localhost:9092"])
        .unwrap();
    let config = Config::from(&matches);

    assert_eq!(config.brokers.value, Some("localhost:9092".to_owned()));
}

#[test]
fn test_list_topics_runs() {
    // Assumes that a Kafka broker is running at localhost:9092
    let cmd = ListCommand::from(Config {
        brokers: Sourced {
            source: "test".to_owned(),
            value: Some("localhost:9092".to_owned())
        },
        ..Default::default()
    });

    assert_ok!(cmd.run());
}

#[test]
fn test_create_and_describe_topics() {
    // Assumes that a Kafka broker is running at localhost:9092
    let cmd = CreateCommand::from(Config {
        brokers: Sourced {
            source: "test".to_owned(),
            value: Some("localhost:9092".to_owned())
        },
        ..Default::default()
    });

    let topic_name = format!("krs-topic-{}", Utc::now().timestamp_millis());

    assert_ok!(cmd.run(&topic_name, 1, 1));

    let cmd = DescribeCommand::from(Config {
        brokers: Sourced {
            source: "test".to_owned(),
            value: Some("localhost:9092".to_owned())
        },
        zookeeper: Sourced {
            source: "test".to_owned(),
            value: Some("localhost:2181".to_owned())
        },
        ..Default::default()
    });
    assert_ok!(cmd.run(&topic_name));
}

// #[test]
// fn test_run_twice() {
//     let cmd = ListCommand::from(Config {
//         brokers: Sourced {
//             source: "test".to_owned(),
//             value: Some("localhost:9092".to_owned())
//         },
//         ..Default::default()
//     });

//     cmd.run().unwrap();
// }
// test that running krs prints usage
// test that running `krs topics` prints `Incomplete subcommand` error
// test that running `krs topics invalid-subcommand` prints `Invalid subcommand` error
