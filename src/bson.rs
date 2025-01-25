//! The bson module
//!
//! Bson used in litedb is a subset of that of mongodb, which is implemented in bson crate.
//! And internal representation can be different some portions.
//! To avoid any problem with those differences, we use custom bson structure instead of bson crate.

mod utils;

mod array;
mod binary;
mod date_time;
mod decimal128;
mod document;
mod guid;
mod object_id;

pub use array::Array;
pub use binary::Binary;
pub use date_time::DateTime;
pub use decimal128::Decimal128;
pub use document::Document;
pub use guid::Guid;
pub use object_id::ObjectId;

/// The type of bson [`Value`]
///
/// The number representation of [`BsonType`] is used internally in litedb
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BsonType {
    MinValue = 0,

    Null = 1,

    Int32 = 2,
    Int64 = 3,
    Double = 4,
    Decimal = 5,

    String = 6,

    Document = 7,
    Array = 8,

    Binary = 9,
    ObjectId = 10,
    Guid = 11,

    Boolean = 12,
    DateTime = 13,

    MaxValue = 14,
}

/// The num represents one bson value.
///
/// Any instance of this value can be expressed in this enum can be serialized to binary representation without any error
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// The MinValue. The smallest value of bson value
    MinValue,
    /// The null value.
    Null,
    /// The signed 32bit integer.
    Int32(i32),
    /// The signed 64bit integer.
    Int64(i64),
    /// The IEEE 754 binary64 floating point value.
    Double(f64),
    /// The IEEE 754 decimal128 floating point value.
    Decimal(Decimal128),
    /// The UTF-8 encoded string value
    String(String),
    /// The string key mapping
    Document(Document),
    /// The array of bson value
    Array(Array),
    /// The byte array
    Binary(Binary),
    /// ObjectId
    ObjectId(ObjectId),
    /// Guid
    /// In bson representation this will be expressed as UUID binary
    Guid(Guid),
    /// Boolean
    Boolean(bool),
    /// DateTime
    /// Time can be represented with this is 0001-01-01 to 9999-12-31 since date time in C# is so
    DateTime(DateTime),
    /// The MaxValue. The biggest value of bson value
    MaxValue,
}

impl Value {
    pub fn ty(&self) -> BsonType {
        match self {
            Value::MinValue => BsonType::MinValue,
            Value::Null => BsonType::Null,
            Value::Int32(_) => BsonType::Int32,
            Value::Int64(_) => BsonType::Int64,
            Value::Double(_) => BsonType::Double,
            Value::Decimal(_) => BsonType::Decimal,
            Value::String(_) => BsonType::String,
            Value::Document(_) => BsonType::Document,
            Value::Array(_) => BsonType::Array,
            Value::Binary(_) => BsonType::Binary,
            Value::ObjectId(_) => BsonType::ObjectId,
            Value::Guid(_) => BsonType::Guid,
            Value::Boolean(_) => BsonType::Boolean,
            Value::DateTime(_) => BsonType::DateTime,
            Value::MaxValue => BsonType::MaxValue,
        }
    }
}
