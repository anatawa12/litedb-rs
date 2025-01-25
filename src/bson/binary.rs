use std::fmt::Debug;

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct Binary {
    bytes: Vec<u8>,
}

impl Binary {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}

impl From<Vec<u8>> for Binary {
    fn from(bytes: Vec<u8>) -> Self {
        Binary::new(bytes)
    }
}
