use std::convert::TryFrom;
use std::fmt::{Debug, Display};
use std::time::Duration;

use clap::{App, ArgMatches, SubCommand};
use dotenv;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::BaseConsumer;
use rdkafka::ClientConfig;

mod args;
pub mod env;
pub mod topics;
pub mod util;

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub enum Error {
    InvalidUsage(String),
    Generic(String),
    Kafka(rdkafka::error::KafkaError),
    Clap(clap::Error),
    Other(Box<dyn std::error::Error>),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // Just use Debug representation
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum OutputType {
    Table,
    Csv,
    Json,
}

// Can't do <T: AsRef<str>>
// https://github.com/rust-lang/rust/issues/50133
impl TryFrom<&str> for OutputType {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self> {
        match s {
            "table" => Ok(OutputType::Table),
            "csv" => Ok(OutputType::Csv),
            "json" => Ok(OutputType::Json),
            other => Err(Error::InvalidUsage(format!("Invalid argument for output_type: {}", other))),
        }
    }
}

// Indicates that a value may come from environment variables,
// .env file, or CLI options.
#[derive(Debug)]
pub struct Sourced<T> {
    pub source: String,
    pub value: T,
}

impl<T> Sourced<Option<T>> {
    fn or(self, other: Sourced<Option<T>>) -> Sourced<Option<T>> {
        match self.value {
            Some(_) => self,
            None => other,
        }
    }
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
    pub output_type: Option<OutputType>,
    pub brokers: Sourced<Option<String>>,
    pub zookeeper: Sourced<Option<String>>,
    pub topic: Option<String>,
}

impl Config {
    fn from_env() -> Self {
        Self {
            brokers: Sourced {
                source: "env var (KRS_BROKERS)".to_owned(),
                value: std::env::var("KRS_BROKERS").ok(),
            },
            zookeeper: Sourced {
                source: "env var (KRS_ZOOKEEPER)".to_owned(),
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
            brokers: rhs.brokers.or(self.brokers),
            zookeeper: rhs.zookeeper.or(self.zookeeper),
            topic: rhs.topic.or(self.topic),
        }
    }
}

impl From<&ArgMatches<'_>> for Config {
    fn from(args: &ArgMatches<'_>) -> Self {
        let args = args;
        Self {
            output_type: args.value_of("output-type").map(|x| {
                OutputType::try_from(x).unwrap_or_else(|_| panic!("Invalid output type {}", x))
            }),
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

// Creates a new Kafka consumer with only the para
fn new_consumer(brokers: &str) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap()
}

fn new_admin_client(brokers: &str) -> AdminClient<DefaultClientContext> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap()
}

// Just having fun with extension methods here. Not really necessary,
// but looks cool.
trait StringExt: ToString {
    fn to_i32(&self) -> i32 {
        self.to_string().parse().unwrap()
    }
}

impl StringExt for &str {}

// TODO: This function still looks really ugly. I wonder if I could macro this.
pub fn dispatch(m: ArgMatches<'_>) -> Result<()> {
    fn fail(base: &str, subcmd: &str) -> Result<()> {
        Err(Error::Generic(
            if subcmd.len() == 0 {
                format!("Incomplete subcommand: '{}'! Use -h for more information.", base)
            } else {
                format!("Invalid subcommand: '{} {}'! Use -h for more information.", base, subcmd)
            }
        ))
    }

    let config = Config::from_env();
    let config = config.merge(Config::from_dotenv());
    let config = config.merge(Config::from(&m));
    // FIXME: Commands should implement TryFrom, not From.
    match m.subcommand() {
        ("topics", Some(s)) => match s.subcommand() {
            ("list", _) => topics::ListCommand::from(config).run(),
            ("describe", Some(ss)) => {
                let topic_name = ss.value_of("topic").unwrap();
                topics::DescribeCommand::from(config).run(topic_name)
            }
            ("create", Some(ss)) => {
                let topic_name = ss.value_of("topic").expect("topic name is required for `topics create`");
                let num_partitions = ss.value_of("num_partitions").map(|x| x.to_i32()).unwrap();
                let num_replicas = ss.value_of("num_replicas").map(|x| x.to_i32()).unwrap();
                topics::CreateCommand::from(config).run(topic_name, num_partitions, num_replicas)
            }
            (unhandled, _) => fail("topics", unhandled),
        },
        ("env", Some(s)) => match s.subcommand() {
            ("show", _) => env::ShowCommand::from(config).run(),
            ("set", _) => env::SetCommand::from(config).run(),
            (unhandled, _) => fail("env", unhandled),
        },
        (unhandled, _) => fail("", unhandled),
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
                .subcommand(env::ShowCommand::subcommand())
                .subcommand(env::SetCommand::subcommand()),
        )
        .subcommand(
            SubCommand::with_name("topics")
                .about("Topic commands")
                .subcommand(topics::ListCommand::subcommand())
                .subcommand(topics::DescribeCommand::subcommand())
                .subcommand(topics::CreateCommand::subcommand()),
        )
}
