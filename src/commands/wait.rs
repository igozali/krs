use std::convert::TryFrom;

use clap::{App, SubCommand};
use rdkafka::consumer::{BaseConsumer, Consumer};

use crate::{new_consumer, Config, Error, DEFAULT_TIMEOUT};

pub struct WaitCommand {
    consumer: BaseConsumer,
}

impl WaitCommand {
    pub fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("wait").about("Waits until the specified Kafka broker is ready.")
    }

    pub fn run(&self) -> crate::Result<()> {
        eprintln!("Waiting for Kafka broker to be ready (press Ctrl+C to interrupt).");
        loop {
            let md = match self.consumer.fetch_metadata(None, Some(DEFAULT_TIMEOUT)) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("Error connecting to Kafka broker - either Kafka is not up or not ready: {:?}", e);
                    continue;
                }
            };
            let topic_count = md
                .topics()
                .iter()
                .filter(|&t| t.name() == "__confluent.support.metrics")
                .count();

            if topic_count == 1 {
                break;
            } else {
                eprintln!("Did not find __confluent.support.metrics in the list of topics.");
            }
        }
        Ok(())
    }
}

impl TryFrom<Config> for WaitCommand {
    type Error = Error;

    fn try_from(conf: Config) -> crate::Result<Self> {
        let brokers = conf
            .brokers
            .as_ref()
            .ok_or_else(|| Error::InvalidUsage("brokers is required for `wait`".into()))?;

        Ok(Self {
            consumer: new_consumer(&brokers, None),
        })
    }
}
