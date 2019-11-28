#[cfg(test)]
mod util;

use std::env;
use std::fs::File;
use std::io::Write;

use krs::{make_parser, Config};

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
fn test_sourced_configs_precedence() {
    // Since current directory might have .env
    let dir = env::temp_dir();
    env::set_current_dir(dir).unwrap();
    {
        // Truncate and close the file immediately
        File::create("./.env").unwrap();
    }

    env::set_var("KRS_BROKERS", "kafka_from_env");

    let parser = make_parser();
    let config = Config::init(
        &parser
            .get_matches_from_safe(&["./binary", "env", "show"])
            .unwrap(),
    );

    assert_eq!(config.brokers.source, "env var (KRS_BROKERS)".to_owned());
    assert_eq!(config.brokers.value, Some("kafka_from_env".to_owned()));

    let parser = make_parser();
    let config = Config::init(
        &parser
            .get_matches_from_safe(&["./binary", "env", "show", "-b", "kafka_from_cli"])
            .unwrap(),
    );

    assert_eq!(config.brokers.source, "-b/--brokers".to_owned());
    assert_eq!(config.brokers.value, Some("kafka_from_cli".to_owned()));

    let mut f = File::create("./.env").unwrap();
    f.write_all("KRS_BROKERS=kafka_from_dotenv\n".as_bytes())
        .unwrap();

    let parser = make_parser();
    let config = Config::init(
        &parser
            .get_matches_from_safe(&["./binary", "env", "show"])
            .unwrap(),
    );

    assert_eq!(config.brokers.source, ".env file (KRS_BROKERS)".to_owned());
    assert_eq!(config.brokers.value, Some("kafka_from_dotenv".to_owned()));

    let parser = make_parser();
    let config = Config::init(
        &parser
            .get_matches_from_safe(&["./binary", "env", "show", "-b", "kafka_from_cli"])
            .unwrap(),
    );

    assert_eq!(config.brokers.source, "-b/--brokers".to_owned());
    assert_eq!(config.brokers.value, Some("kafka_from_cli".to_owned()));
}

// test that running krs prints usage
// test that running `krs topics` prints `Incomplete subcommand` error
// test that running `krs topics invalid-subcommand` prints `Invalid subcommand` error
