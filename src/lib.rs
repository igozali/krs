use std::fmt::Debug;
use std::result::Result as _Result;

pub mod topics;
pub mod util;

pub type Result<T> = _Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Generic(String),
}

struct CommandBase {}
