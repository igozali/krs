use rdkafka::consumer::BaseConsumer;
use rdkafka::ClientConfig;

pub fn make_consumer(brokers: &str) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap()
}
