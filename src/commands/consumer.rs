use clap::{App, SubCommand};
use futures::stream::Stream;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio::runtime::current_thread::Runtime;

use crate::args;
use crate::{new_consumer, Config};

pub struct ConsumerCommand {
    consumer: StreamConsumer,
}

impl ConsumerCommand {
    pub fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("consumer")
            .about("Consumes records from multiple topics and prints them to stdout.")
            .arg(args::topic().required(true))
    }

    pub fn run(&self, topic_name: &str) -> crate::Result<()> {
        self.consumer
            .subscribe(&[topic_name])
            .map_err(|_| crate::Error::Generic("Failed to subscribe to topic".to_owned()))?;

        let pipeline = self
            .consumer
            .start()
            .filter_map(|r| match r {
                Ok(msg) => Some(msg),
                Err(e) => {
                    println!("Error while receiving from Kafka: {:?}", e);
                    None
                }
            })
            .for_each(|msg| {
                let msg = msg.detach();
                match msg.payload_view::<str>() {
                    Some(Ok(v)) => println!("{}", v),
                    Some(Err(_)) => eprintln!("Message payload is not a string."),
                    None => eprintln!("No message."),
                };
                Ok(())
            });

        let mut rt = Runtime::new().unwrap();
        rt.block_on(pipeline)
            .expect("Failed to start consumer pipeline");
        Ok(())
    }
}

impl From<Config> for ConsumerCommand {
    fn from(conf: Config) -> Self {
        let brokers = conf
            .brokers
            .value
            .expect("brokers is required for `consumer`");
        let group_id = conf.group_id;

        Self {
            // TODO: Can do group_id.as_deref() in Rust 1.40
            consumer: new_consumer(&brokers, group_id.as_ref().map(String::as_str)),
        }
    }
}
