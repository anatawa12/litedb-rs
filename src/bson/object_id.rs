use super::utils::ToHex;
use std::fmt::{Debug, Formatter};
use std::sync::LazyLock;
use std::sync::atomic::AtomicUsize;

/// Represents ObjectId
#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectId {
    bytes: [u8; 12],
}

impl ObjectId {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        static INCREMENT: AtomicUsize = AtomicUsize::new(0);
        static MACHINE: LazyLock<u32> = LazyLock::new(|| rand::random::<u32>() & 0xFFFFFF);

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let increment = INCREMENT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let machine = *MACHINE;
        let pid = std::process::id();

        let mut bytes = [0; 12];
        bytes[0..4].clone_from_slice(&((timestamp & 0xFFFFFFFF) as u32).to_be_bytes());
        bytes[4..7].clone_from_slice(&machine.to_be_bytes()[..3]);
        bytes[7..9].clone_from_slice(&pid.to_be_bytes()[..2]);
        bytes[9..12].clone_from_slice(&increment.to_be_bytes()[..3]);

        Self::from_bytes(bytes)
    }

    pub fn from_bytes(bytes: [u8; 12]) -> Self {
        ObjectId { bytes }
    }

    pub fn as_bytes(&self) -> &[u8; 12] {
        &self.bytes
    }

    pub fn unix_timestamp(&self) -> u32 {
        u32::from_be_bytes(self.bytes[0..4].try_into().unwrap())
    }
}

impl Debug for ObjectId {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_tuple("ObjectId").field(&ToHex(self.bytes)).finish()
    }
}
