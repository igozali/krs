use crate::Config;
use clap::{App, SubCommand};

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
        SubCommand::with_name("show").help("Shows the current Kafka environment")
    }

    pub fn run(&self) -> crate::Result<()> {
        println!("Environment: ");
        println!("  brokers: {}", self.config.brokers);
        println!("  zookeeper: {}", self.config.zookeeper);

        Ok(())
    }
}

