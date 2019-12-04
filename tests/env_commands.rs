#[cfg(test)]
// TODO: https://github.com/rust-lang/rust/issues/46379
mod util;
pub use util::*;

use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;

use krs::{BROKERS_ENV_KEY, ZOOKEEPER_ENV_KEY};

fn readlines(f: File) -> Vec<String> {
    BufReader::new(f).lines().map(|x| x.unwrap()).collect()
}

#[test]
fn test_set_env_does_not_destroy_existing_dotenv_file() -> std::io::Result<()> {
    use krs::commands::env::SetCommand;

    let dir = env::temp_dir();
    env::set_current_dir(dir).unwrap();
    {
        let mut f = File::create(".env").unwrap();
        f.write_all(b"TEST_KEY=test_value")
    }?;

    assert_ok!(SetCommand::from(test_config_brokers_only()).run());
    let lines: Vec<String> = readlines(File::open(".env").unwrap());

    assert!(
        lines.iter().any(|x| x.starts_with("TEST_KEY")),
        "Cannot find TEST_KEY in temp .env file"
    );
    assert_some!(lines.iter().find(|x| x.starts_with(BROKERS_ENV_KEY)));

    assert_ok!(SetCommand::from(test_config()).run());
    let lines: Vec<String> = readlines(File::open(".env").unwrap());

    assert!(
        lines.iter().any(|x| x.starts_with("TEST_KEY")),
        "Cannot find TEST_KEY in temp .env file"
    );
    assert_some!(lines.iter().find(|x| x.starts_with(BROKERS_ENV_KEY)));
    assert_some!(lines.iter().find(|x| x.starts_with(ZOOKEEPER_ENV_KEY)));

    Ok(())
}
