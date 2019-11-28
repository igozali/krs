use std::process::Command;

fn main() {
    let output = Command::new("git").args(&["describe", "--long", "--dirty", "--tags"]).output().unwrap();
    let commit = String::from_utf8(output.stdout).unwrap();
    println!("cargo:rustc-env=GIT_DESCRIPTION={}", commit);
}
