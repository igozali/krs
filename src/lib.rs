use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{Debug, Display};
use std::time::Duration;

use chrono::offset::Utc;
use clap::{crate_authors, crate_version, App, ArgMatches, SubCommand};
use dotenv;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;

mod args;
pub mod commands;
pub mod errors;

pub use errors::Error;

// TODO: Move to global CLI arg as well.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

pub const VERSION: &str = env!("GIT_DESCRIPTION");

pub const BROKERS_ENV_KEY: &str = "KRS_BROKERS";
pub const ZOOKEEPER_ENV_KEY: &str = "KRS_ZOOKEEPER";

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

fn required<'a>(m: &'a ArgMatches<'a>, x: &str) -> Result<&'a str> {
    m.value_of(x)
        .ok_or_else(|| Error::InvalidUsage(format!("Argument is required for {}", x)))
}

fn required_i32(m: &ArgMatches<'_>, x: &str) -> Result<i32> {
    required(m, x)?
        .parse::<i32>()
        .map_err(|e| Error::InvalidUsage(format!("Expected integer argument for {}, but {}", x, e)))
}

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
            ("list", _) => commands::topics::ListCommand::try_from(config)?.run(),
            ("describe", Some(ss)) => {
                let topic_name = required(ss, "topic")?;
                commands::topics::DescribeCommand::try_from(config)?.run(topic_name)
            }
            ("create", Some(ss)) => {
                let topic_name = required(ss, "topic")?;
                let num_partitions = required_i32(ss, "num_partitions")?;
                let num_replicas = required_i32(ss, "num_replicas")?;
                commands::topics::CreateCommand::try_from(config)?.run(
                    topic_name,
                    num_partitions,
                    num_replicas,
                )
            }
            ("delete", Some(ss)) => {
                let topic_name = required(ss, "topic")?;
                commands::topics::DeleteCommand::try_from(config)?.run(topic_name)
            }
            // `krs topics` defaults to `krs topics show`
            (_, _) => commands::topics::ListCommand::try_from(config)?.run(),
        },
        ("env", Some(s)) => match s.subcommand() {
            ("show", _) => commands::env::ShowCommand::try_from(config)?.run(),
            ("set", _) => commands::env::SetCommand::try_from(config)?.run(),
            // If only `krs env` is specified, default to `krs env show`
            (_, _) => commands::env::ShowCommand::try_from(config)?.run(),
        },
        ("consumer", Some(s)) => {
            let topic_name = required(s, "topic")?;
            commands::consumer::ConsumerCommand::try_from(config)?.run(topic_name)
        }
        ("producer", Some(s)) => {
            let topic_name = required(s, "topic")?;
            commands::producer::ProducerCommand::try_from(config)?.run(topic_name)
        }
        ("wait", Some(_)) => commands::wait::WaitCommand::try_from(config)?.run(),
        (unhandled, _) => fail("", unhandled),
    }
}

// TODO: Probably use lazy_static! for this.
pub fn make_parser<'a, 'b>() -> App<'a, 'b> {
    // TODO: Can't embed extended Git information in version since it's not
    // available when running `cargo install` on some other machine.
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
