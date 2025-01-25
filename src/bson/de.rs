use super::*;
use std::fmt::{Display, Formatter};
use std::io::Cursor;
use std::string::FromUtf8Error;

pub trait BsonReader {
    type Error: From<ParseError>;
    fn read_fully(&mut self, bytes: &mut [u8]) -> Result<(), Self::Error>;

    fn is_end(&self) -> bool;

    fn read_fully_fixed<const SIZE: usize>(&mut self) -> Result<[u8; SIZE], Self::Error> {
        let mut buffer = [0u8; SIZE];
        self.read_fully(&mut buffer)?;
        Ok(buffer)
    }
}

#[derive(Debug)]
pub enum ParseError {
    BadLength,
    SizeExceeded,
    RemainingDataInDocument,
    BadTag(u8),
    BadUtf8(FromUtf8Error),
    NoTrailingZero,
    DateTimeRange,
    BadGuidLength,
    BadBinarySubType(u8),
    DuplicatedKey(String),
    BadIndexKey { expected: usize, actual: String },
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::BadLength => f.write_str("bad length in bson document"),
            ParseError::SizeExceeded => {
                f.write_str("size limit exceeded in bson document or inner document")
            }
            ParseError::RemainingDataInDocument => {
                f.write_str("The bson document is shorter than expected")
            }
            ParseError::BadTag(tag) => write!(f, "bad tag: {tag:02x}"),
            ParseError::BadUtf8(e) => Display::fmt(e, f),
            ParseError::NoTrailingZero => f.write_str("no trailing zero byte in string"),
            ParseError::DateTimeRange => f.write_str("invalid date time range"),
            ParseError::BadGuidLength => f.write_str("bad GUID bytes length"),
            ParseError::BadBinarySubType(t) => write!(f, "bad binary subtype: {t}"),
            ParseError::DuplicatedKey(key) => write!(f, "duplicate key: {key}"),
            ParseError::BadIndexKey { .. } => write!(f, "bad index key in bson array"),
        }
    }
}

impl<T> BsonReader for Cursor<T>
where
    T: AsRef<[u8]>,
{
    type Error = ParseError;

    fn read_fully(&mut self, bytes: &mut [u8]) -> Result<(), Self::Error> {
        let current_pos = self.position();
        let new_pos = self.position() + bytes.len() as u64;
        if new_pos > self.get_ref().as_ref().len() as u64 {
            return Err(ParseError::SizeExceeded);
        }

        self.set_position(new_pos);

        let slice = &self.get_ref().as_ref()[current_pos as usize..new_pos as usize];
        bytes.copy_from_slice(slice);

        Ok(())
    }

    fn is_end(&self) -> bool {
        self.position() == self.get_ref().as_ref().len() as u64
    }
}

pub(super) fn len_to_usize(v: i32) -> Result<usize, ParseError> {
    v.try_into().map_err(|_| ParseError::BadLength)
}

pub(super) fn read_len<R: BsonReader>(reader: &mut R) -> Result<usize, R::Error> {
    let len = i32::from_le_bytes(reader.read_fully_fixed()?);
    Ok(len_to_usize(len)?)
}

pub(super) fn limit_reader<R: BsonReader>(reader: &mut R) -> Result<LimitReader<R>, R::Error> {
    let offset = 4;
    let len = i32::from_le_bytes(reader.read_fully_fixed()?);
    let len = len_to_usize(len)?;
    if len < offset {
        return Err(ParseError::SizeExceeded.into());
    }
    let remaining = len - offset;
    Ok(LimitReader { reader, remaining })
}

pub(super) struct LimitReader<'a, R: BsonReader> {
    reader: &'a R,
    remaining: usize,
}

impl<R: BsonReader> BsonReader for LimitReader<'_, R> {
    type Error = R::Error;

    fn read_fully(&mut self, bytes: &mut [u8]) -> Result<(), Self::Error> {
        if self.remaining < bytes.len() {
            return Err(Self::Error::from(ParseError::SizeExceeded));
        }
        self.read_fully(bytes)?;
        self.remaining -= bytes.len();
        Ok(())
    }

    fn is_end(&self) -> bool {
        self.remaining == 0
    }
}

pub(super) fn parse_element<R: BsonReader>(
    r: &mut LimitReader<R>,
) -> Result<Option<(String, Value)>, R::Error> {
    let tag = r.read_fully_fixed::<1>()?[0];
    if tag == 0 {
        return Ok(None);
    }
    let tag = BsonTag::from_i8(tag as i8).ok_or(ParseError::BadTag(tag))?;

    let key = parse_c_string(r)?;

    let value = match tag {
        BsonTag::Double => Value::Double(f64::from_le_bytes(r.read_fully_fixed()?)),
        BsonTag::Int32 => Value::Int32(i32::from_le_bytes(r.read_fully_fixed()?)),
        BsonTag::Int64 => Value::Int64(i64::from_le_bytes(r.read_fully_fixed()?)),
        BsonTag::MinValue => Value::MinValue,
        BsonTag::MaxValue => Value::MaxValue,
        BsonTag::Null => Value::Null,
        BsonTag::ObjectId => Value::ObjectId(ObjectId::from_bytes(r.read_fully_fixed()?)),
        BsonTag::Boolean => Value::Boolean(r.read_fully_fixed::<1>()?[0] != 0),
        BsonTag::Decimal => Value::Decimal(Decimal128::from_bytes(r.read_fully_fixed()?)),
        BsonTag::DateTime => Value::DateTime(
            DateTime::from_unix_milliseconds(i64::from_le_bytes(r.read_fully_fixed()?))
                .ok_or(ParseError::DateTimeRange)?,
        ),

        BsonTag::String => {
            let len = read_len(r)?;
            if len == 0 {
                return Err(ParseError::BadLength.into());
            }
            let mut buffer = vec![0; len];

            r.read_fully(&mut buffer)?;

            if buffer[buffer.len() - 1] != 0 {
                return Err(ParseError::NoTrailingZero.into());
            }

            Value::String(String::from_utf8(buffer).map_err(ParseError::BadUtf8)?)
        }
        BsonTag::Binary => {
            let len = read_len(r)?;
            let sub_type = r.read_fully_fixed::<1>()?[0];

            match sub_type {
                0 => {
                    // Generic
                    let mut buffer = vec![0; len];
                    r.read_fully(&mut buffer)?;
                    Value::Binary(Binary::new(buffer))
                }
                4 => {
                    // UUID
                    if len != 16 {
                        return Err(ParseError::BadGuidLength.into());
                    }
                    Value::Guid(Guid::from_bytes(r.read_fully_fixed()?))
                }
                sub_type => return Err(ParseError::BadBinarySubType(sub_type).into()),
            }
        }
        BsonTag::Document => Value::Document(Document::parse_document_inner(r)?),
        BsonTag::Array => Value::Array(Array::parse_array(r)?),
    };

    Ok(Some((key, value)))
}

fn parse_c_string<R: BsonReader>(p: &mut LimitReader<R>) -> Result<String, R::Error> {
    let mut buffer = vec![];

    loop {
        let data = p.read_fully_fixed::<1>()?[0];
        if data == 0 {
            break;
        }
        buffer.push(data);
    }

    String::from_utf8(buffer)
        .map_err(ParseError::BadUtf8)
        .map_err(R::Error::from)
}
