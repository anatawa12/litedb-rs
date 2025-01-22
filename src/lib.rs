/*!
 *! # LiteDB in Rust
 *! This is a reimplementation of [LiteDB] in Rust.
 *!
 *! This implementation (currently?) only supports single-threaded operation.
 *!
 *! [LiteDB]: https://www.litedb.org/
 */

mod engine;

pub type Result<T> = std::result::Result<T, Error>;

pub struct Error {
    message: String,
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error {
            message: err.to_string(),
        }
    }
}
