#[cfg(test)]
use krs::{make_parser, Config};

macro_rules! assert_ok {
    ($x:expr) => {
        if let Err(v) = $x {
            panic!(format!("Expected Ok value, but got {:?}", v));
        }
    }
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

// test that running krs prints usage
// test that running `krs topics` prints `Incomplete subcommand` error
// test that running `krs topics invalid-subcommand` prints `Invalid subcommand` error
