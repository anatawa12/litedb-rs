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
