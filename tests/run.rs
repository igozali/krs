#[cfg(test)]
mod util;

use chrono::Utc;

use krs::{Config, Sourced};
use krs::commands::topics::{CreateCommand, DeleteCommand, DescribeCommand, ListCommand};

#[test]
fn test_list_topics_runs() {
    // Assumes that a Kafka broker is running at localhost:9092
    let cmd = ListCommand::from(Config {
        brokers: Sourced {
            source: "test".to_owned(),
            value: Some("localhost:9092".to_owned()),
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
            value: Some("localhost:9092".to_owned()),
        },
        ..Default::default()
    });

    let topic_name = format!("krs-topic-{}", Utc::now().timestamp_millis());

    assert_ok!(cmd.run(&topic_name, 1, 1));

    let cmd = DescribeCommand::from(Config {
        brokers: Sourced {
            source: "test".to_owned(),
            value: Some("localhost:9092".to_owned()),
        },
        zookeeper: Sourced {
            source: "test".to_owned(),
            value: Some("localhost:2181".to_owned()),
        },
        ..Default::default()
    });
    assert_ok!(cmd.run(&topic_name));
}

#[test]
fn test_create_and_delete_topics() {
    // Assumes that a Kafka broker is running at localhost:9092
    let cmd = CreateCommand::from(Config {
        brokers: Sourced {
            source: "test".to_owned(),
            value: Some("localhost:9092".to_owned()),
        },
        ..Default::default()
    });

    let topic_name = format!("krs-topic-{}", Utc::now().timestamp_millis());

    assert_ok!(cmd.run(&topic_name, 1, 1));

    let cmd = DeleteCommand::from(Config {
        brokers: Sourced {
            source: "test".to_owned(),
            value: Some("localhost:9092".to_owned()),
        },
        ..Default::default()
    });
    assert_ok!(cmd.run(&topic_name));
}
