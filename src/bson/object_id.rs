use std::fmt::{Debug, Formatter};
use super::utils::ToHex;

/// Represents ObjectId
#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectId {
    bytes: [u8; 12],
}

impl ObjectId {
    pub fn from_bytes(bytes: [u8; 12]) -> Self {
        ObjectId { bytes }
    }

    pub fn as_bytes(&self) -> &[u8; 12] {
        &self.bytes
    }
}

impl Debug for ObjectId {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_tuple("ObjectId")
            .field(&ToHex(self.bytes))
            .finish()
    }
}
