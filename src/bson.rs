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

impl BsonType {
    pub(crate) fn bson_tag(self) -> BsonTag {
        match self {
            BsonType::Double => BsonTag::Double,
            BsonType::String => BsonTag::String,
            BsonType::Document => BsonTag::Document,
            BsonType::Array => BsonTag::Array,
            BsonType::Binary => BsonTag::Binary,
            BsonType::Guid => BsonTag::Boolean, // GUID is a kind of binary in bson
            BsonType::ObjectId => BsonTag::ObjectId,
            BsonType::Boolean => BsonTag::Boolean,
            BsonType::DateTime => BsonTag::DateTime,
            BsonType::Null => BsonTag::Null,
            BsonType::Int32 => BsonTag::Int32,
            BsonType::Int64 => BsonTag::Int64,
            BsonType::Decimal => BsonTag::Decimal,
            BsonType::MinValue => BsonTag::MinValue,
            BsonType::MaxValue => BsonTag::MaxValue,
        }
    }
}

#[repr(i8)]
enum BsonTag {
    Double = 1,
    String = 2,
    Document = 3,
    Array = 4,
    Binary = 5,
    // 6: undefined
    ObjectId = 7,
    Boolean = 8,
    DateTime = 9,
    Null = 10,
    // 11: regex
    // 12: DBPointer
    // 13: JavaScript code
    // 14: Symbol
    // 15: JavaScript code with scope
    Int32 = 16,
    // 17: timestamp
    Int64 = 18,
    Decimal = 19,

    MinValue = -1,
    MaxValue = 127,
}

impl BsonTag {
    fn from_i8(i: i8) -> Option<Self> {
        match i {
            1 => Some(Self::Double),
            2 => Some(Self::String),
            3 => Some(Self::Document),
            4 => Some(Self::Array),
            5 => Some(Self::Binary),
            7 => Some(Self::ObjectId),
            8 => Some(Self::Boolean),
            9 => Some(Self::DateTime),
            10 => Some(Self::Null),
            16 => Some(Self::Int32),
            18 => Some(Self::Int64),
            19 => Some(Self::Decimal),
            -1 => Some(Self::MinValue),
            127 => Some(Self::MaxValue),
            _ => None,
        }
    }
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

impl Value {
    /// Returns the size of serialized value.
    ///
    /// This doesn't include tag or name of key.
    pub fn get_serialized_value_len(&self) -> usize {
        match self {
            // tag only types
            Value::MinValue => 0,
            Value::Null => 0,
            Value::MaxValue => 0,

            // constant length types
            Value::Int32(_) => 4,
            Value::Int64(_) => 8,
            Value::Double(_) => 8,
            Value::Decimal(_) => 16,
            Value::ObjectId(_) => 12,
            Value::Boolean(_) => 1,
            Value::DateTime(_) => 8, // DateTime in bson is 64bit unix milliseconds time

            // binary type (len, subtype, data)
            Value::Binary(b) => b.get_serialized_value_len(),
            &Value::Guid(g) => g.get_serialized_value_len(),

            // string type (len, data, trailing null)
            Value::String(s) => 4 + s.len() + 1,

            // complex (embedded) types
            Value::Document(d) => d.get_serialized_value_len(),
            Value::Array(a) => a.get_serialized_value_len(),
        }
    }

    /// Writes the value to the BsonWriter
    pub fn write_value<W: BsonWriter>(&self, w: &mut W) -> Result<(), W::Error> {
        match self {
            Value::MinValue => Ok(()),
            Value::Null => Ok(()),
            Value::MaxValue => Ok(()),

            // constant length types
            Value::Int32(v) => w.write_bytes(&v.to_le_bytes()),
            Value::Int64(v) => w.write_bytes(&v.to_le_bytes()),
            Value::Double(v) => w.write_bytes(&v.to_le_bytes()),
            Value::Decimal(v) => w.write_bytes(&v.bytes()),
            Value::ObjectId(v) => w.write_bytes(v.as_bytes()),
            &Value::Boolean(v) => w.write_bytes(&[v as u8]),
            &Value::DateTime(v) => {
                // DateTime in bson is 64bit unix milliseconds time
                w.write_bytes(&v.as_unix_milliseconds().to_le_bytes())
            }

            // binary type (len, subtype, data)
            Value::Binary(b) => b.write_value(w),
            Value::Guid(g) => g.write_value(w),

            // string type (len, data, trailing null)
            Value::String(s) => {
                let len = s.len() + 1;
                let len = i32::try_from(len).map_err(|_| W::when_too_large(len))?;
                w.write_bytes(&len.to_le_bytes())?;
                w.write_bytes(s.as_bytes())?;
                w.write_bytes(&[0])
            }

            // complex (embedded) types
            Value::Document(d) => d.write_value(w),
            Value::Array(a) => a.write_value(w),
        }
    }
}

pub trait BsonWriter {
    type Error;
    /// Returns the error for the data exceeds size limit
    fn when_too_large(size: usize) -> Self::Error;
    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), Self::Error>;
}

mod from_impls {
    use super::*;

    impl From<i32> for Value {
        fn from(v: i32) -> Value {
            Value::Int32(v)
        }
    }

    impl From<i64> for Value {
        fn from(v: i64) -> Value {
            Value::Int64(v)
        }
    }

    impl From<f64> for Value {
        fn from(v: f64) -> Value {
            Value::Double(v)
        }
    }

    impl From<Decimal128> for Value {
        fn from(v: Decimal128) -> Value {
            Value::Decimal(v)
        }
    }

    impl From<String> for Value {
        fn from(v: String) -> Value {
            Value::String(v)
        }
    }

    impl From<Document> for Value {
        fn from(v: Document) -> Value {
            Value::Document(v)
        }
    }

    impl From<Array> for Value {
        fn from(v: Array) -> Value {
            Value::Array(v)
        }
    }

    impl From<Binary> for Value {
        fn from(v: Binary) -> Value {
            Value::Binary(v)
        }
    }

    impl From<ObjectId> for Value {
        fn from(v: ObjectId) -> Value {
            Value::ObjectId(v)
        }
    }

    impl From<Guid> for Value {
        fn from(v: Guid) -> Value {
            Value::Guid(v)
        }
    }

    impl From<bool> for Value {
        fn from(v: bool) -> Value {
            Value::Boolean(v)
        }
    }

    impl From<DateTime> for Value {
        fn from(v: DateTime) -> Value {
            Value::DateTime(v)
        }
    }

    impl From<Vec<Value>> for Value {
        fn from(v: Vec<Value>) -> Value {
            Value::Array(v.into())
        }
    }

    impl<const L: usize> From<[Value; L]> for Value {
        fn from(v: [Value; L]) -> Value {
            Value::Array(v.into())
        }
    }

    impl From<&[Value]> for Value {
        fn from(v: &[Value]) -> Value {
            Value::Array(v.into())
        }
    }

    impl<'a, T> From<&'a [T]> for Value
    where
        Value: From<&'a T>,
    {
        fn from(data: &'a [T]) -> Value {
            Value::Array(Array::from(data))
        }
    }
}
