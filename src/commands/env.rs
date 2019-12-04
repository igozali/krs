use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};

use clap::{App, SubCommand};

use crate::{Config, Sourced, BROKERS_ENV_KEY, ZOOKEEPER_ENV_KEY};

pub struct ShowCommand {
    config: Config,
}

impl From<Config> for ShowCommand {
    fn from(args: Config) -> Self {
        Self { config: args }
    }
}

impl ShowCommand {
    pub fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("show").about("Shows the current Kafka environment")
    }

    pub fn run(&self) -> crate::Result<()> {
        println!("Environment: ");
        println!("  brokers: {:?}", self.config.brokers);
        println!("  zookeeper: {:?}", self.config.zookeeper);

        Ok(())
    }
}

// TODO: Not sure what this should contain.
pub struct SetCommand {
    config: Config,
}

impl From<Config> for SetCommand {
    fn from(args: Config) -> Self {
        Self { config: args }
    }
}

// If there's some value in the option, "consume" the option by replacing
// the contents with None and writing it to the file. Else, just pass.
fn maybe_write(
    f: &mut File,
    value: &mut Option<Sourced<String>>,
    key: &str,
) -> std::io::Result<()> {
    if let Some(sourced) = value.take() {
        println!("Writing {}={} to .env", key, sourced.value);
        f.write_all(format!("{}={}\n", key, sourced.value).as_bytes())
    } else {
        Ok(())
    }
}

impl SetCommand {
    pub fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("set")
            .about(
                "Sets default values for some arguments (by writing to the `.env` file in current directory)"
            )
    }

    pub fn run(self) -> crate::Result<()> {
        let mut brokers = self.config.brokers;
        let mut zookeeper = self.config.zookeeper;
        if brokers.is_none() && zookeeper.is_none() {
            return Err(crate::Error::InvalidUsage(
                "At least one of brokers/zookeeper need to be specified.".to_owned(),
            ));
        }

        let mut f = File::create("./.env.new")?;

        // If lines for the args already exist in .env, replace them.
        if let Ok(orig) = File::open("./.env") {
            // TODO: Why does `lines().map(|x| x?)` not work?
            for line in BufReader::new(orig).lines().map(|x| x.unwrap()) {
                if line.starts_with(BROKERS_ENV_KEY) {
                    // TODO: Should not write if self.config.brokers.source is already ".env file"
                    maybe_write(&mut f, &mut brokers, BROKERS_ENV_KEY)?;
                } else if line.starts_with(ZOOKEEPER_ENV_KEY) {
                    maybe_write(&mut f, &mut zookeeper, ZOOKEEPER_ENV_KEY)?;
                } else {
                    f.write_all(format!("{}\n", line).as_bytes())?;
                }
            }
        }

        // If lines weren't found, then try writing them again at the end.
        maybe_write(&mut f, &mut brokers, BROKERS_ENV_KEY)?;
        maybe_write(&mut f, &mut zookeeper, ZOOKEEPER_ENV_KEY)?;

        fs::rename("./.env.new", "./.env")?;

        Ok(())
    }
}
