use std::env;
use std::error::Error;

use krs::{dispatch, make_parser};

// krs env show|set
// krs topics list|create|delete|describe --brokers
fn main() -> Result<(), Box<dyn Error>> {
    let mut app = make_parser();
    let result = app
        .get_matches_from_safe_borrow(env::args())
        .map(|matches| dispatch(matches));

    if let Err(e) = result {
        println!("{}", e);
        app.print_help()?;
    }

    Ok(())
}
