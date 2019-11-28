#[cfg(test)]

#[macro_export]
macro_rules! assert_ok {
    ($x:expr) => {
        if let Err(v) = $x {
            panic!(format!("Expected Ok value, but got {:?}", v));
        }
    };
}
