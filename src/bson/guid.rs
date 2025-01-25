use super::utils::ToHex;
use std::fmt::{Debug, Formatter};

/// Represents GUID (or UUID)
#[derive(Copy, Clone, Eq, Hash, PartialEq)]
pub struct Guid {
    bytes: [u8; 16],
}

impl Guid {
    pub fn from_bytes(bytes: [u8; 16]) -> Guid {
        Guid { bytes }
    }

    pub fn to_bytes(&self) -> [u8; 16] {
        self.bytes
    }
}

impl Debug for Guid {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Guid").field(&ToHex(self.bytes)).finish()
    }
}
