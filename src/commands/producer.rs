use std::convert::TryFrom;

use clap::{App, SubCommand};
use futures::future::Future;
use futures::stream::Stream;
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::args;
use crate::{new_producer, Config, Error};

pub struct ProducerCommand {
    producer: FutureProducer,
}

fn prompt() -> impl Stream<Item = String, Error = std::io::Error> {
    use tokio::io::{lines, stdin};
    let prompter = futures::stream::repeat::<_, std::io::Error>(()).map(|_| eprint!("> "));
    let user_input = lines(std::io::BufReader::new(stdin()));
    prompter.zip(user_input).map(|(_, line)| line)
}

impl ProducerCommand {
    pub fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("producer")
            .about("Takes records from the specified input and produces them to a topic.")
            .arg(args::topic().required(true))
    }

    pub fn run(&self, topic_name: &str) -> crate::Result<()> {
        let producer = self.producer.clone();
        let topic_name = topic_name.to_owned();

        let fut = prompt()
            .fuse()
            .map_err(|e| eprintln!("Error reading line from stdin: {:?}", e))
            .for_each(move |line| {
                producer
                    .send(FutureRecord::to(&topic_name).key("").payload(&line), 0)
                    .and_then(|r| {
                        match r {
                            Ok(delivery) => eprintln!("Sent: {:?}", delivery),
                            Err((e, _)) => eprintln!("Error: {:?}", e),
                        };
                        Ok(())
                    })
                    .map_err(|_| ())
            });

        eprintln!("Starting console producer. Press Ctrl+C to exit.");
        // Can't use current_thread::Runtime here because otherwise no one's
        // handling stdin.
        // https://stackoverflow.com/questions/57590175
        tokio::run(fut);
        Ok(())
    }
}

impl TryFrom<Config> for ProducerCommand {
    type Error = Error;

    fn try_from(conf: Config) -> crate::Result<Self> {
        let brokers = conf
            .brokers
            .as_ref()
            .ok_or_else(|| Error::InvalidUsage("brokers is required for `producer`".into()))?;

        Ok(Self {
            producer: new_producer(&brokers),
        })
    }
}
