use super::utils::ToHex;
use std::fmt::{Debug, Formatter};

/// The IEEE 754 decimal128
///
/// This struct is only for storing / passing data, so no mathematical operations are implemented (yet)
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal128 {
    pub(super) bytes: [u8; 16],
}

impl Decimal128 {
    /// Construct a new decimal128 from raw representation
    pub fn from_bytes(bytes: [u8; 16]) -> Decimal128 {
        Self { bytes }
    }

    pub fn bytes(&self) -> [u8; 16] {
        self.bytes
    }
}

impl Debug for Decimal128 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Decimal128")
            .field(&ToHex(self.bytes))
            .finish()
    }
}

// TODO: implement display for better visibility
