#[cfg(test)]
use krs::{Config, Sourced};

#[macro_export]
macro_rules! assert_ok {
    ($x:expr) => {
        if let Err(v) = $x {
            panic!(format!(
                "In '{}', expected Ok value, but got {:?}",
                stringify!($x),
                v
            ));
        }
    };
}

#[macro_export]
macro_rules! assert_some {
    ($x:expr) => {
        if $x.is_none() {
            panic!(format!(
                "In '{}', expected Some value, but got None",
                stringify!($x)
            ));
        }
    };
}

pub fn test_config() -> Config {
    Config {
        brokers: Some(Sourced::new("unit_test", "localhost:9092".to_owned())),
        zookeeper: Some(Sourced::new("unit_test", "localhost:2181".to_owned())),
        ..Default::default()
    }
}

pub fn test_config_brokers_only() -> Config {
    Config {
        brokers: Some(Sourced::new("unit_test", "localhost:9092".to_owned())),
        ..Default::default()
    }
}

pub fn test_config_zk_only() -> Config {
    Config {
        zookeeper: Some(Sourced::new("unit_test", "localhost:2181".to_owned())),
        ..Default::default()
    }
}
