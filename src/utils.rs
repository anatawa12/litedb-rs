use crate::Error;
use crate::engine::{BufferReader, BufferWriter, PageAddress};
use std::time::SystemTime;

// TODO: Implement the CompareOptions struct
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompareOptions(pub i32);

impl CompareOptions {
    pub const IGNORE_CASE: CompareOptions = CompareOptions(1);
    pub const IGNORE_KANA_TYPE: CompareOptions = CompareOptions(8);
    pub const IGNORE_NON_SPACE: CompareOptions = CompareOptions(2);
    pub const IGNORE_SYMBOLS: CompareOptions = CompareOptions(4);
    pub const IGNORE_WIDTH: CompareOptions = CompareOptions(16);
    pub const ORDINAL: CompareOptions = CompareOptions(1073741824);
    pub const STRING_SORT: CompareOptions = CompareOptions(536870912);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Collation {
    pub lcid: i32,
    pub sort_options: CompareOptions,
}

impl Default for Collation {
    fn default() -> Self {
        Collation {
            lcid: 127, // invariant
            sort_options: CompareOptions::IGNORE_CASE,
        }
    }
}

impl Collation {
    pub fn new(lcid: i32, sort_options: CompareOptions) -> Self {
        Collation { lcid, sort_options }
    }
}

// TODO: move to somewhere better
#[repr(u8)]
enum BsonType {
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
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::MinValue),
            1 => Some(Self::Null),
            2 => Some(Self::Int32),
            3 => Some(Self::Int64),
            4 => Some(Self::Double),
            5 => Some(Self::Decimal),
            6 => Some(Self::String),
            7 => Some(Self::Document),
            8 => Some(Self::Array),
            9 => Some(Self::Binary),
            10 => Some(Self::ObjectId),
            11 => Some(Self::Guid),
            12 => Some(Self::Boolean),
            13 => Some(Self::DateTime),
            14 => Some(Self::MaxValue),
            _ => None,
        }
    }
}

#[repr(transparent)]
pub struct BufferSlice {
    buffer: [u8],
}

impl BufferSlice {
    pub fn new(buffer: &[u8]) -> &Self {
        unsafe { &*(buffer as *const _ as *const Self) }
    }

    pub fn new_mut(buffer: &mut [u8]) -> &mut Self {
        unsafe { &mut *(buffer as *mut _ as *mut Self) }
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }
}

impl BufferSlice {
    pub fn read_bool(&self, offset: usize) -> bool {
        self.buffer[offset] != 0
    }

    pub fn read_byte(&self, offset: usize) -> u8 {
        self.buffer[offset]
    }

    pub fn read_u8(&self, offset: usize) -> u8 {
        self.buffer[offset]
    }

    pub fn read_i16(&self, offset: usize) -> i16 {
        i16::from_le_bytes(self.buffer[offset..][..2].try_into().unwrap())
    }

    pub fn read_i32(&self, offset: usize) -> i32 {
        i32::from_le_bytes(self.buffer[offset..][..4].try_into().unwrap())
    }

    pub fn read_i64(&self, offset: usize) -> i64 {
        i64::from_le_bytes(self.buffer[offset..][..8].try_into().unwrap())
    }

    pub fn read_u16(&self, offset: usize) -> u16 {
        u16::from_le_bytes(self.buffer[offset..][..2].try_into().unwrap())
    }

    pub fn read_u32(&self, offset: usize) -> u32 {
        u32::from_le_bytes(self.buffer[offset..][..4].try_into().unwrap())
    }

    pub fn read_u64(&self, offset: usize) -> u64 {
        u64::from_le_bytes(self.buffer[offset..][..8].try_into().unwrap())
    }

    pub fn read_f64(&self, offset: usize) -> f64 {
        f64::from_le_bytes(self.buffer[offset..][..8].try_into().unwrap())
    }

    // TODO: Decimal

    pub fn read_bytes(&self, offset: usize, length: usize) -> &[u8] {
        &self.buffer[offset..][..length]
    }

    pub fn read_string(&self, offset: usize, length: usize) -> crate::Result<&str> {
        std::str::from_utf8(self.read_bytes(offset, length)).map_err(Error::err)
    }

    pub fn read_date_time(&self, offset: usize) -> crate::Result<CsDateTime> {
        CsDateTime::from_ticks(self.read_u64(offset)).ok_or_else(Error::datetime_overflow)
    }

    pub fn read_page_address(&self, offset: usize) -> PageAddress {
        PageAddress::new(self.read_u32(offset), self.read_byte(offset + 4))
    }

    pub fn read_index_key(&self, offset: usize) -> crate::Result<bson::Bson> {
        // extended length: use two bytes for type and length pair
        let type_byte = self.read_byte(offset);
        let length_byte = self.read_byte(offset + 1);

        let type_ = BsonType::from_u8(type_byte & 0b0011_1111).ok_or_else(Error::invalid_bson)?;
        let length = ((length_byte as u16 & 0b1100_0000) << 2) | (length_byte as u16);
        let offset = offset + 1; // length byte might not be used

        let value = match type_ {
            BsonType::MinValue => bson::Bson::MinKey,
            BsonType::Null => bson::Bson::Null,
            BsonType::Int32 => bson::Bson::Int32(self.read_i32(offset)),
            BsonType::Int64 => bson::Bson::Int64(self.read_i64(offset)),
            BsonType::Double => bson::Bson::Double(self.read_f64(offset)),
            BsonType::Decimal => bson::Bson::Decimal128(bson::Decimal128::from_bytes(
                self.read_bytes(offset, 16).try_into().unwrap(),
            )), // known to be 16 bytes
            BsonType::String => {
                let offset = offset + 1; // using length byte
                bson::Bson::String(self.read_string(offset, length as usize)?.to_owned())
            }
            BsonType::Document => bson::Bson::Document(
                BufferReader::new(self.slice(offset, self.len() - offset)).read_document()?,
            ),
            BsonType::Array => bson::Bson::Array(
                BufferReader::new(self.slice(offset, self.len() - offset)).read_array()?,
            ),
            BsonType::Binary => {
                let length = length + 1; // using length byte
                bson::Bson::Binary(bson::Binary {
                    subtype: bson::spec::BinarySubtype::Generic,
                    bytes: self.read_bytes(offset, length as usize).to_vec(),
                })
            }
            BsonType::ObjectId => bson::Bson::ObjectId(bson::oid::ObjectId::from_bytes(
                self.read_bytes(offset, 16).try_into().unwrap(),
            )),
            BsonType::Guid => bson::Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Uuid,
                bytes: self.read_bytes(offset, 16).to_vec(),
            }),
            BsonType::Boolean => bson::Bson::Boolean(self.read_bool(offset)),
            BsonType::DateTime => {
                todo!("CsDateTime in BSON")
                //bson::Bson::DateTime(self.read_date_time(offset)?.ticks())
            }
            BsonType::MaxValue => bson::Bson::MaxKey,
        };

        Ok(value)
    }

    pub(crate) fn slice(&self, offset: usize, count: usize) -> &Self {
        Self::new(&self.buffer[offset..][..count])
    }

    pub fn clear(&mut self, offset: usize, count: usize) {
        self.buffer[offset..][..count].fill(0);
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }
}

// writers
impl BufferSlice {
    pub fn write_bool(&mut self, offset: usize, value: bool) {
        self.buffer[offset] = value as u8;
    }

    pub fn write_byte(&mut self, offset: usize, value: u8) {
        self.buffer[offset] = value;
    }

    pub fn write_i16(&mut self, offset: usize, value: i16) {
        self.buffer[offset..][..2].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_i32(&mut self, offset: usize, value: i32) {
        self.buffer[offset..][..4].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_i64(&mut self, offset: usize, value: i64) {
        self.buffer[offset..][..8].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_u8(&mut self, offset: usize, value: u8) {
        self.buffer[offset] = value;
    }

    pub fn write_u16(&mut self, offset: usize, value: u16) {
        self.buffer[offset..][..2].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_u32(&mut self, offset: usize, value: u32) {
        self.buffer[offset..][..4].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_u64(&mut self, offset: usize, value: u64) {
        self.buffer[offset..][..8].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_f64(&mut self, offset: usize, value: f64) {
        self.buffer[offset..][..8].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_bytes(&mut self, offset: usize, value: &[u8]) {
        self.buffer[offset..][..value.len()].copy_from_slice(value);
    }

    pub fn write_string(&mut self, offset: usize, value: &str) {
        self.write_bytes(offset, value.as_bytes());
    }

    pub fn write_date_time(&mut self, offset: usize, value: CsDateTime) {
        self.write_u64(offset, value.ticks());
    }

    pub fn write_page_address(&mut self, offset: usize, value: PageAddress) {
        self.write_u32(offset, value.page_id());
        self.write_u8(offset + 4, value.index());
    }

    pub fn write_index_key(&mut self, offset: usize, value: &bson::Bson) {
        // TODO: check for key length
        fn make_extended_length(tag: BsonType, length: usize) -> [u8; 2] {
            assert!(length <= 1024);

            let type_ = tag as u8;
            let length_lsb = (length & 0xFF) as u8;
            let length_msb = ((length & 0x300) >> 2) as u8;
            [type_ | length_msb, length_lsb]
        }

        match value {
            // variable length values
            bson::Bson::Binary(bin) => {
                self.write_bytes(
                    offset,
                    &make_extended_length(BsonType::Binary, bin.bytes.len()),
                );
                self.write_bytes(offset + 2, &bin.bytes);
            }
            bson::Bson::String(str) => {
                self.write_bytes(offset, &make_extended_length(BsonType::String, str.len()));
                self.write_bytes(offset + 2, str.as_bytes());
            }

            // single tag values
            bson::Bson::Null => self.write_u8(offset, BsonType::Null as u8),
            bson::Bson::MaxKey => self.write_u8(offset, BsonType::MaxValue as u8),
            bson::Bson::MinKey => self.write_u8(offset, BsonType::MinValue as u8),

            // simple values
            bson::Bson::Int32(v) => {
                self.write_u8(offset, BsonType::Int32 as u8);
                self.write_i32(offset + 1, *v);
            }
            bson::Bson::Int64(v) => {
                self.write_u8(offset, BsonType::Int64 as u8);
                self.write_i64(offset + 1, *v);
            }
            bson::Bson::Double(d) => {
                self.write_u8(offset, BsonType::Double as u8);
                self.write_f64(offset + 1, *d);
            }
            bson::Bson::Decimal128(d) => {
                self.write_u8(offset, BsonType::Decimal as u8);
                self.write_bytes(offset + 1, &d.bytes());
            }
            bson::Bson::Boolean(b) => {
                self.write_u8(offset, BsonType::Boolean as u8);
                self.write_bool(offset + 1, *b);
            }
            bson::Bson::DateTime(_) => {
                todo!("CsDateTime")
                //self.write_u8(offset, BsonType::DateTime as u8);
                //self.write_date_time(offset + 1, v);
            }

            bson::Bson::Document(d) => {
                self.write_u8(offset, BsonType::Document as u8);
                BufferWriter::new(self.slice_mut(offset + 1, self.len() - offset - 1))
                    .write_document(d)
                    .unwrap()
            }
            bson::Bson::Array(a) => {
                self.write_u8(offset, BsonType::Array as u8);
                BufferWriter::new(self.slice_mut(offset + 1, self.len() - offset - 1))
                    .write_array(a)
                //.unwrap()
            }

            _ => panic!("Unsupported BSON type"),
        }
    }

    pub(crate) fn slice_mut(&mut self, offset: usize, count: usize) -> &mut Self {
        Self::new_mut(&mut self.buffer[offset..][..count])
    }
}

/// C# DateTime in Rust
///
/// This represents number of 100 nano seconds since 0001-01-01 00:00:00 UTC
/// This can represent 0001-01-01 00:00:00 ~ 9999-12-31 23:59:59.99999999
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct CsDateTime(pub u64);

impl CsDateTime {
    const MAX_TICKS: u64 = 3155378975999999999;
    const UNIX_EPOC_TICKS: u64 = 621355968000000000;

    pub const MIN: CsDateTime = CsDateTime(0);
    pub const MAX: CsDateTime = CsDateTime(Self::MAX_TICKS);

    pub fn now() -> Self {
        // now must not exceed MAX_TICKS / MIN_TICKS
        Self::from_system(SystemTime::now()).unwrap()
    }

    pub fn from_system(system: SystemTime) -> Option<Self> {
        let ticks = match system.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(duration) => {
                let nanos = duration.as_nanos();

                let after_epoc_ticks = Self::MAX_TICKS - Self::UNIX_EPOC_TICKS;
                let after_epoc_nanos = after_epoc_ticks as u128 * 100;
                if nanos > after_epoc_nanos {
                    return None;
                }

                Self::UNIX_EPOC_TICKS + (nanos / 100) as u64
            }
            Err(e) => {
                let duration = e.duration();
                let nanos = duration.as_nanos();

                let unix_epoc_nanos = Self::UNIX_EPOC_TICKS as u128 * 100;

                if nanos > unix_epoc_nanos {
                    return None;
                }

                Self::UNIX_EPOC_TICKS - (nanos / 100) as u64
            }
        };
        Some(CsDateTime(ticks))
    }

    pub fn from_ticks(ticks: u64) -> Option<CsDateTime> {
        if ticks > Self::MAX_TICKS {
            None
        } else {
            Some(CsDateTime(ticks))
        }
    }
}

impl CsDateTime {
    pub fn ticks(&self) -> u64 {
        self.0
    }
}

pub(crate) enum Order {
    Ascending = 1,
    Descending = 2,
}
