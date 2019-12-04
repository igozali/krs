#[cfg(test)]
// TODO: https://github.com/rust-lang/rust/issues/46379
mod util;
use std::time::Duration;
pub use util::*;

use rand::random;

use krs::commands::topics::{CreateCommand, DeleteCommand, DescribeCommand, ListCommand};

#[test]
fn test_list_topics_ok() {
    // Assumes that a Kafka broker is running at localhost:9092
    let cmd = ListCommand::from(test_config());
    assert_ok!(cmd.run());
}

#[test]
fn test_create_and_describe_topics() {
    // Assumes that a Kafka broker is running at localhost:9092
    let cmd = CreateCommand::from(test_config());

    let topic_name = format!("krs-topic-{}", random::<u64>());

    assert_ok!(cmd.run(&topic_name, 1, 1));

    let cmd = DescribeCommand::from(test_config());
    assert_ok!(cmd.run(&topic_name));
}

#[test]
fn test_create_and_delete_topics() {
    // Assumes that a Kafka broker is running at localhost:9092
    let cmd = CreateCommand::from(test_config());

    let topic_name = format!("krs-topic-{}", random::<u64>());

    assert_ok!(cmd.run(&topic_name, 1, 1));

    // Apparently topics aren't immediately available after creation.
    std::thread::sleep(Duration::from_secs(1));

    let cmd = DeleteCommand::from(test_config());
    assert_ok!(cmd.run(&topic_name));
}
