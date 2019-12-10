use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{Debug, Display};
use std::time::Duration;

use chrono::offset::Utc;
use clap::{App, ArgMatches, SubCommand, crate_version, crate_authors};
use dotenv;

use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;

mod args;
pub mod commands;

// TODO: Move to global var as well.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

pub const VERSION: &str = env!("GIT_DESCRIPTION");

pub const BROKERS_ENV_KEY: &str = "KRS_BROKERS";
pub const ZOOKEEPER_ENV_KEY: &str = "KRS_ZOOKEEPER";

#[derive(Debug)]
pub enum Error {
    InvalidUsage(String),
    Generic(String),
    Kafka(rdkafka::error::KafkaError),
    Clap(clap::Error),
    Io(std::io::Error),
    Other(Box<dyn std::error::Error>),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl From<rdkafka::error::KafkaError> for Error {
    fn from(e: rdkafka::error::KafkaError) -> Self {
        Error::Kafka(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<clap::Error> for Error {
    fn from(e: clap::Error) -> Self {
        Error::Clap(e)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

// Indicates that a value may come from environment variables,
// .env file, or CLI options.
#[derive(Debug)]
pub struct Sourced<T> {
    pub source: String,
    pub value: T,
}

impl<T> Sourced<T> {
    pub fn new(source: &str, value: T) -> Self {
        Self {
            source: source.to_owned(),
            value,
        }
    }
}

impl<T> std::ops::Deref for Sourced<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: Display> Display for Sourced<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} (from {})", self.value, self.source)
    }
}

// All fields are optional since I'd like to collect them from various
// places: env variables, .env file, CLI arguments from various levels
#[derive(Debug, Default)]
pub struct Config {
    pub brokers: Option<Sourced<String>>,
    pub group_id: Option<String>,

    pub zookeeper: Option<Sourced<String>>,
}

impl Config {
    pub fn init(args: &ArgMatches<'_>) -> Self {
        Config::from_env()
            .merge(Config::from_dotenv())
            .merge(Config::from(args))
    }

    fn from_env() -> Self {
        Self {
            brokers: std::env::var(BROKERS_ENV_KEY)
                .ok()
                .map(|value| Sourced::new(&format!("env var ({})", BROKERS_ENV_KEY), value)),
            zookeeper: std::env::var(ZOOKEEPER_ENV_KEY)
                .ok()
                .map(|value| Sourced::new(&format!(".env file ({})", ZOOKEEPER_ENV_KEY), value)),
            ..Default::default()
        }
    }

    fn from_dotenv() -> Self {
        // Will be undeprecated at some point.
        // https://github.com/dotenv-rs/dotenv/issues/13
        #[allow(deprecated)]
        let vars: HashMap<String, String> = dotenv::from_path_iter("./.env")
            .map(|itr| itr.map(|x| x.ok()).flatten().collect())
            .unwrap_or_default();

        Self {
            brokers: vars.get(BROKERS_ENV_KEY).map(|value| {
                Sourced::new(
                    &format!(".env file ({})", BROKERS_ENV_KEY),
                    value.to_owned(),
                )
            }),
            zookeeper: vars.get(ZOOKEEPER_ENV_KEY).map(|value| {
                Sourced::new(
                    &format!(".env file ({})", ZOOKEEPER_ENV_KEY),
                    value.to_owned(),
                )
            }),
            ..Default::default()
        }
    }

    // Merge two configs together, giving preference to `rhs` only if
    // the fields in `rhs` are not None.
    fn merge(self, rhs: Self) -> Self {
        Self {
            brokers: rhs.brokers.or(self.brokers),
            zookeeper: rhs.zookeeper.or(self.zookeeper),
            group_id: rhs.group_id.or(self.group_id),
        }
    }
}

impl From<&ArgMatches<'_>> for Config {
    fn from(args: &ArgMatches<'_>) -> Self {
        let default_group_id = format!("krs-{}", Utc::now().timestamp_millis());
        Self {
            brokers: args
                .value_of("brokers")
                .map(|value| Sourced::new("-b/--brokers", value.to_owned())),
            zookeeper: args
                .value_of("zookeeper")
                .map(|value| Sourced::new("-z/--zookeeper", value.to_owned())),
            group_id: args
                .value_of("group-id")
                .map(|x| x.to_owned())
                .or(Some(default_group_id)),
        }
    }
}

/// Creates a new Kafka consumer with only the parameters I care about.
fn new_consumer<T>(brokers: &str, group_id: Option<&str>) -> T
where
    T: Consumer + FromClientConfig,
{
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers);

    if let Some(v) = group_id {
        config.set("group.id", v);
    }

    eprintln!(
        "Created Consumer(brokers={}, group_id={:?})",
        brokers, group_id
    );

    config.create().unwrap()
}

fn new_producer(brokers: &str) -> FutureProducer {
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
        let msg = if base.is_empty() {
            "No subcommand specified. Use -h for more info.".to_string()
        } else if subcmd.is_empty() {
            format!("Incomplete subcommand: '{}'! Use -h for more info.", base)
        } else {
            format!(
                "Invalid subcommand: '{} {}'! Use -h for more info.",
                base, subcmd
            )
        };
        Err(Error::Generic(msg))
    }

    let config = Config::init(&m);
    // FIXME: Commands should implement TryFrom(config), not From.
    match m.subcommand() {
        ("topics", Some(s)) => match s.subcommand() {
            ("list", _) => commands::topics::ListCommand::from(config).run(),
            ("describe", Some(ss)) => {
                let topic_name = ss.value_of("topic").unwrap();
                commands::topics::DescribeCommand::from(config).run(topic_name)
            }
            ("create", Some(ss)) => {
                let topic_name = ss
                    .value_of("topic")
                    .expect("topic name is required for `topics create`");
                let num_partitions = ss.value_of("num_partitions").map(|x| x.to_i32()).unwrap();
                let num_replicas = ss.value_of("num_replicas").map(|x| x.to_i32()).unwrap();
                commands::topics::CreateCommand::from(config).run(
                    topic_name,
                    num_partitions,
                    num_replicas,
                )
            }
            ("delete", Some(ss)) => {
                let topic_name = ss
                    .value_of("topic")
                    .expect("topic name is required for `topics delete`");
                commands::topics::DeleteCommand::from(config).run(topic_name)
            }
            // `krs topics` defaults to `krs topics show`
            (_, _) => commands::topics::ListCommand::from(config).run(),
        },
        ("env", Some(s)) => match s.subcommand() {
            ("show", _) => commands::env::ShowCommand::from(config).run(),
            ("set", _) => commands::env::SetCommand::from(config).run(),
            // If only `krs env` is specified, default to `krs env show`
            (_, _) => commands::env::ShowCommand::from(config).run(),
        },
        ("consumer", Some(s)) => {
            let topic_name = s
                .value_of("topic")
                .expect("topic name is required for `consumer`");
            commands::consumer::ConsumerCommand::from(config).run(topic_name)
        }
        ("producer", Some(s)) => {
            let topic_name = s
                .value_of("topic")
                .expect("topic name is required for `consumer`");
            commands::producer::ProducerCommand::from(config).run(topic_name)
        }
        ("wait", Some(_)) => commands::wait::WaitCommand::from(config).run(),
        (unhandled, _) => fail("", unhandled),
    }
}

// TODO: Probably use lazy_static! for this.
pub fn make_parser<'a, 'b>() -> App<'a, 'b> {
    App::new("krs")
        .author(crate_authors!())
        .version(crate_version!())
        .about("Simple Kafka CLI tool.")
        .arg(args::brokers())
        .arg(args::group_id())
        .arg(args::zookeeper())
        .subcommand(
            SubCommand::with_name("env")
                .about("Environment commands (defaults to `env show`).")
                .long_about("Environment commands.\n\nIf no subcommand to `env` is specified, defaults to `env show`.")
                .subcommand(commands::env::ShowCommand::subcommand())
                .subcommand(commands::env::SetCommand::subcommand()),
            )
        .subcommand(
            SubCommand::with_name("topics")
                .about("Topic commands (defaults to `topics list`).")
                .long_about("Topic commands.\n\nIf no subcommand to `topics` is specified, will default to `topics list`.")
                .subcommand(commands::topics::ListCommand::subcommand())
                .subcommand(commands::topics::DescribeCommand::subcommand())
                .subcommand(commands::topics::CreateCommand::subcommand())
                .subcommand(commands::topics::DeleteCommand::subcommand()),
        )
        .subcommand(commands::consumer::ConsumerCommand::subcommand())
        .subcommand(commands::producer::ProducerCommand::subcommand())
        .subcommand(commands::wait::WaitCommand::subcommand())
}
