use std::fmt::{Debug, Display};

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

impl From<std::num::ParseIntError> for Error {
    fn from(e: std::num::ParseIntError) -> Self {
        Error::InvalidUsage(format!("Expected integer as argument, but {}", e))
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
