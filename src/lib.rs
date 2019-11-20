use std::convert::TryFrom;
use std::fmt::{Debug, Display};
use std::result::Result as _Result;
use std::time::Duration;

use clap::{App, ArgMatches, SubCommand};
use dotenv;
use rdkafka::consumer::BaseConsumer;
use rdkafka::ClientConfig;

mod args;
pub mod env;
pub mod topics;
pub mod util;

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

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
    Json,
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
            other => Err(Error::InvalidArgument("output_type".into(), other.into())),
        }
    }
}

// Indicates that a value may come from environment variables,
// .env file, or CLI options.
#[derive(Debug)]
struct Sourced<T> {
    source: String,
    value: T,
}

impl<T> Default for Sourced<Option<T>> {
    fn default() -> Self {
        Self {
            source: "unknown".to_owned(),
            value: None,
        }
    }
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
            None => write!(f, "None"),
        }
    }
}

// All fields are optional since I'd like to collect them from various
// places: env variables, .env file, CLI arguments from various levels
#[derive(Debug, Default)]
pub struct Config {
    output_type: Option<OutputType>,
    brokers: Sourced<Option<String>>,
    zookeeper: Sourced<Option<String>>,
    topic: Option<String>,
}

impl Config {
    fn from_env() -> Self {
        Self {
            brokers: Sourced {
                source: "envvar (KRS_BROKERS)".to_owned(),
                value: std::env::var("KRS_BROKERS").ok(),
            },
            zookeeper: Sourced {
                source: "envvar (KRS_ZOOKEEPER)".to_owned(),
                value: std::env::var("KRS_ZOOKEEPER").ok(),
            },
            ..Default::default()
        }
    }

    fn from_dotenv() -> Self {
        Self {
            brokers: Sourced {
                source: ".env file (KRS_BROKERS)".to_owned(),
                value: dotenv::var("KRS_BROKERS").ok(),
            },
            zookeeper: Sourced {
                source: ".env file (KRS_ZOOKEEPER)".to_owned(),
                value: dotenv::var("KRS_ZOOKEEPER").ok(),
            },
            ..Default::default()
        }
    }

    // Merge two configs together, giving preference to `rhs` only if
    // the fields in `rhs` are not None.
    fn merge(self, rhs: Self) -> Self {
        Self {
            output_type: rhs.output_type.or(self.output_type),
            brokers: match rhs.brokers.value {
                Some(_) => rhs.brokers,
                None => self.brokers,
            },
            zookeeper: match rhs.zookeeper.value {
                Some(_) => rhs.zookeeper,
                None => self.zookeeper,
            },
            topic: rhs.topic.or(self.topic),
        }
    }
}

impl From<&ArgMatches<'_>> for Config {
    fn from(args: &ArgMatches<'_>) -> Self {
        let args = args;
        Self {
            output_type: args
                .value_of("output-type")
                .map(|x| OutputType::try_from(x).expect(&format!("Invalid output type {}", x))),
            brokers: Sourced {
                source: "-b/--brokers".into(),
                value: args.value_of("brokers").map(|x| x.to_owned()),
            },
            zookeeper: Sourced {
                source: "-z/--zookeeper".into(),
                value: args.value_of("zookeeper").map(|x| x.to_owned()),
            },
            topic: args.value_of("topic").map(|x| x.to_owned()),
        }
    }
}

// This is me experimenting with "superclasses" in Rust. Maybe I'll realize eventually
// that it's not really needed.
struct CommandBase {
    // All commands need a Kafka consumer.
    consumer: BaseConsumer,
}

impl CommandBase {
    fn new(brokers: &str) -> Self {
        Self {
            consumer: ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .create()
                .unwrap(),
        }
    }
}

// TODO: This function still looks really ugly. I wonder if I could macro this.
pub fn dispatch(m: ArgMatches<'_>) -> Result<()> {
    fn fail(name: &str) -> Result<()> {
        Err(Error::Generic(format!(
            "Unhandled subcommand `{}`! Use -h for more information.",
            name
        )))
    }

    let config = Config::from_env();
    let config = config.merge(Config::from_dotenv());
    let config = config.merge(Config::from(&m));
    // FIXME: Commands should implement TryFrom, not From.
    match m.subcommand() {
        ("topics", Some(s)) => match s.subcommand() {
            ("list", Some(ss)) => topics::ListCommand::from(config.merge(Config::from(ss))).run(),
            ("describe", Some(ss)) => {
                topics::DescribeCommand::from(config.merge(Config::from(ss))).run()
            }
            (unhandled, _) => fail(unhandled),
        },
        ("env", Some(s)) => match s.subcommand() {
            ("show", Some(ss)) => env::ShowCommand::from(config.merge(Config::from(ss))).run(),
            (unhandled, _) => fail(unhandled),
        },
        (unhandled, _) => fail(unhandled),
    }
}

pub fn make_parser<'a, 'b>() -> App<'a, 'b> {
    App::new("krs")
        .about("Decent Kafka CLI tool.")
        .arg(args::brokers())
        .arg(args::zookeeper())
        .arg(args::output_type())
        .subcommand(
            SubCommand::with_name("env")
                .about("Environment commands")
                .subcommand(env::ShowCommand::subcommand()),
        )
        .subcommand(
            SubCommand::with_name("topics")
                .about("Topic commands")
                .subcommand(topics::ListCommand::subcommand())
                .subcommand(topics::DescribeCommand::subcommand()),
        )
}
