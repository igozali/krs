use clap::Arg;

pub fn brokers<'a, 'n>() -> Arg<'a, 'n> {
    // TODO: brokers should allow multiple values
    Arg::with_name("brokers")
        .short("b")
        .long("--brokers")
        .help("Kafka brokers. Specify multiple times for multiple brokers.")
        .global(true)
        .takes_value(true)
}

pub fn zookeeper<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("zookeeper")
        .short("z")
        .long("--zookeeper")
        .help("Zookeeper address")
        .global(true)
        .takes_value(true)
}

pub fn topic<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("topic")
        .short("t")
        .long("--topic")
        .help("Kafka topic name")
        .takes_value(true)
}

pub fn group_id<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("group-id")
        .short("g")
        .long("--group-id")
        .help("Kafka consumer group ID")
        .global(true)
        .takes_value(true)
}

pub fn num_partitions<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("num_partitions")
        .long("--num-partitions")
        .help("Number of partitions")
        .default_value("1")
        .takes_value(true)
}

pub fn num_replicas<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("num_replicas")
        .long("--num-replicas")
        .help("Number of replicas")
        .default_value("1")
        .takes_value(true)
}

pub fn output_type<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("output-type")
        .short("o")
        .help("Output type (can be table, csv, json)")
        .default_value("json")
        .takes_value(true)
}
