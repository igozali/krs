use std::sync::Once;
use std::thread;
use std::time::Duration;
use std::process::{Command, Child};

static mut SETUP: Option<Setup> = None;
static mut SETUP_ONCE: Once = Once::new();
static mut TEARDOWN_ONCE: Once = Once::new();

#[derive(Debug)]
pub struct Setup {
    process: Child,
}

pub fn setup() {
    unsafe {
        SETUP_ONCE.call_once(|| {
            let p = Command::new("docker-compose")
                .args(&["up"])
                .spawn()
                .expect("Failed to spawn docker-compose");

            // Wait until Kafka and Zookeeper has been initialized
            // TODO: Figure out a better way to determine if a Kafka broker is ready to
            // accept commands.
            thread::sleep(Duration::from_secs(15));

            SETUP = Some(Setup { process: p });

            // Without this, the process will become a zombie.
            thread::spawn(|| {
                SETUP.as_mut().unwrap().process.wait().unwrap();
            });
        });
    }
}

pub fn teardown() {
    unsafe {
        TEARDOWN_ONCE.call_once(|| {
            let p = &SETUP.as_ref().unwrap().process;
            let pid = p.id();

            Command::new("kill")
                .arg(format!("{}", pid))
                .output()
                .unwrap();

            loop {
                println!("Waiting for command to die");
                let sc = Command::new("kill")
                    .arg("-0")
                    .arg(pid.to_string())
                    .status()
                    .unwrap();
                if sc.code().unwrap() != 0 {
                    break;
                }
                thread::sleep(Duration::from_secs(1));
            }
        });
    }
}
