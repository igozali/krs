use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};

use clap::{App, SubCommand};

use crate::Config;

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
        println!("  brokers: {}", self.config.brokers);
        println!("  zookeeper: {}", self.config.zookeeper);

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

impl SetCommand {
    pub fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("set")
            .about(
                "Sets default values for some arguments (by writing to the `.env` file in current directory)"
            )
    }

    pub fn run(self) -> crate::Result<()> {
        let mut brokers = self.config.brokers.value;
        let mut zookeeper = self.config.zookeeper.value;
        if brokers.is_none() && zookeeper.is_none() {
            return Err(crate::Error::InvalidUsage(
                "At least one of brokers/zookeeper need to be specified.".to_owned(),
            ));
        }

        let mut f = File::create("./.env.new").unwrap();

        // If lines for the args already exist in .env, replace them.
        if let Ok(orig) = File::open("./.env") {
            for line in BufReader::new(orig).lines().map(|x| x.unwrap()) {
                if line.starts_with("KRS_BROKERS=") {
                    if let Some(value) = brokers.take() {
                        // TODO: Should not write if self.config.brokers.source is already ".env file"
                        f.write_all(format!("KRS_BROKERS={}\n", value).as_bytes())
                            .unwrap();
                        println!("Written KRS_BROKERS={} to .env", value);
                    }
                } else if line.starts_with("KRS_ZOOKEEPER") {
                    if let Some(value) = zookeeper.take() {
                        f.write_all(format!("KRS_ZOOKEEPER={}\n", value).as_bytes())
                            .unwrap();
                        println!("Written KRS_ZOOKEEPER={} to .env", value);
                    }
                }
            }
        }

        // If lines weren't found, then try writing them again at the end.
        if let Some(value) = brokers.take() {
            f.write_all(format!("KRS_BROKERS={}\n", value).as_bytes())
                .unwrap();
            println!("Written KRS_BROKERS={} to .env", value);
        }
        if let Some(value) = zookeeper.take() {
            f.write_all(format!("KRS_ZOOKEEPER={}\n", value).as_bytes())
                .unwrap();
            println!("Written KRS_ZOOKEEPER={} to .env", value);
        }

        fs::rename("./.env.new", "./.env").unwrap();

        Ok(())
    }
}
