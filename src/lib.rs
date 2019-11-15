use std::fmt::Debug;
use std::result::Result as _Result;

pub mod util;
pub mod topics;

pub type Result<T> = _Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Generic(String)
}
