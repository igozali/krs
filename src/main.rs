use std::env;

use krs::{make_parser, dispatch};

// krs env show|set
// krs topics list|create|delete|describe --brokers
fn main() -> krs::Result<()> {
    let app = make_parser();
    let matches = app.get_matches_from(env::args());
    dispatch(matches)
}
