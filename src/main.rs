use krs::{dispatch, make_parser};

// krs env show|set
// krs topics list|create|delete|describe --brokers
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = make_parser();
    let matches = app.get_matches();
    let result = dispatch(matches);

    if let Err(e) = result {
        println!("error: {}", e);
    }

    Ok(())
}
