use super::utils::ToHex;
use crate::bson::BsonWriter;
use std::fmt::{Debug, Formatter};

/// Represents GUID (or UUID)
#[derive(Copy, Clone, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct Guid {
    bytes: [u8; 16],
}

impl Guid {
    pub fn new() -> Guid {
        let mut bytes = rand::random::<[u8; 16]>();
        bytes[6] = bytes[6] & 0x0F | 0x40;
        bytes[8] = bytes[8] & 0x3F | 0x80;
        Guid::from_bytes(bytes)
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Guid {
        Guid { bytes }
    }

    pub fn to_bytes(&self) -> [u8; 16] {
        self.bytes
    }
}

impl Guid {
    const SIZE: usize = 16 + 4 + 1;

    /// Returns the size of serialized value.
    ///
    /// This doesn't include tag or name of key.
    pub fn get_serialized_value_len(&self) -> usize {
        Self::SIZE
    }

    /// Writes the value to the BsonWriter
    pub fn write_value<W: BsonWriter>(&self, w: &mut W) -> Result<(), W::Error> {
        let len =
            i32::try_from(self.bytes.len()).map_err(|_| W::when_too_large(self.bytes.len()))?;

        let mut buffer = [0u8; Self::SIZE];

        *<&mut [u8; 4]>::try_from(&mut buffer[0..][..4]).unwrap() = len.to_le_bytes();
        buffer[4] = 4;
        *<&mut [u8; 16]>::try_from(&mut buffer[5..][..16]).unwrap() = self.bytes;

        w.write_bytes(buffer.as_ref())
    }
}

impl Debug for Guid {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Guid").field(&ToHex(self.bytes)).finish()
    }
}
