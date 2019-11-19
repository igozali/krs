use clap::Arg;

pub fn brokers<'a, 'n>() -> Arg<'a, 'n> {
    Arg::with_name("brokers")
        .help("Comma-delimited list of brokers")
        .short("b")
        .takes_value(true)
}

pub fn zookeeper<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("zookeeper")
        .help("Zookeeper address")
        .short("z")
        .takes_value(true)
}

pub fn topic<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("topic")
        .help("Topic name")
        .short("t")
        .takes_value(true)
}

pub fn output_type<'a, 'b>() -> Arg<'a, 'b> {
    Arg::with_name("output-type")
        .short("o")
        .help("Output type (can be table, csv, json)")
        .default_value("json")
}
