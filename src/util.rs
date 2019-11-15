use rdkafka::ClientConfig;
use rdkafka::consumer::BaseConsumer;

pub fn make_consumer(brokers: &String) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap()
}
