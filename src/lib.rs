use std::fmt::{Debug, Display};
use std::convert::{TryFrom, TryInto};
use std::result::Result as _Result;

use clap::ArgMatches;
use rdkafka::consumer::BaseConsumer;
use rdkafka::ClientConfig;

pub mod env;
pub mod topics;
pub mod util;

pub type Result<T> = _Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    InvalidArgument(String, String),
    Generic(String),
}

#[derive(Debug)]
enum OutputType {
    Table,
    Csv,
    Json
}

// Can't do <T: AsRef<str>>
// https://github.com/rust-lang/rust/issues/50133
impl TryFrom<&str> for OutputType {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self> {
        match s.as_ref() {
            "table" => Ok(OutputType::Table),
            "csv" => Ok(OutputType::Csv),
            "json" => Ok(OutputType::Json),
            other => Err(Error::InvalidArgument("output_type".into(), other.into()))
        }
    }
}

// Indicates that a value may come from environment variables,
// .env file, or CLI options.
#[derive(Debug)]
struct Sourced<T> {
    source: String,
    value: T
}

impl<T> std::ops::Deref for Sourced<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: Display> Display for Sourced<Option<T>> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self.value {
            Some(v) => write!(f, "{} (from {})", v, self.source),
            None => write!(f ,"None")
        }
    }
}

#[derive(Debug)]
pub struct Config {
    output_type: OutputType,
    brokers: Sourced<Option<String>>,
    zookeeper: Sourced<Option<String>>,
    topic: Option<String>
}

impl From<&ArgMatches<'_>> for Config {
    fn from(args: &ArgMatches<'_>) -> Self {
        let args = args;
        Self {
            output_type: args.value_of("output-type").unwrap().try_into().unwrap(),
            brokers: Sourced { source: "-b/--brokers".into(), value: args.value_of("brokers").map(|x| x.into()) },
            zookeeper: Sourced { source: "-z/--zookeeper".into(), value: args.value_of("zookeeper").map(|x| x.into()) },
            topic: args.value_of("topic").map(|x| x.into())
        }
    }
}

struct CommandBase {
    // All commands need a Kafka consumer.
    consumer: BaseConsumer
}

impl CommandBase {
    fn new(brokers: &String) -> Self {
        Self {
            consumer: ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .create()
                .unwrap()
        }
    }
}
