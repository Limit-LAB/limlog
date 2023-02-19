use serde::{Deserialize, Serialize};
use uuid7::Uuid;

use crate::consts::{INDEX_MAGIC, LOG_MAGIC};

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Log {
    pub uuid: Uuid,

    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Log {
    pub fn new(uuid: Uuid, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { uuid, key, value }
    }
}

#[repr(C, align(8))]
#[derive(Debug, Copy, Clone, Default, PartialEq)]
pub struct Header {
    pub magic_number: [u8; 8],
    pub attributes: [u8; 8],
}

impl Header {
    pub const INDEX_DEFAULT: Header = Header {
        magic_number: *INDEX_MAGIC,
        attributes: [0u8; 8],
    };
    pub const LOG_DEFAULT: Header = Header {
        magic_number: *LOG_MAGIC,
        attributes: [0u8; 8],
    };

    pub fn as_bytes(&self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        unsafe {
            std::ptr::copy_nonoverlapping(self as *const Self as *const u8, bytes.as_mut_ptr(), 16);
        }
        bytes
    }

    pub fn from_bytes(chunk: &[u8; 16]) -> Self {
        let mut header = Header::default();
        unsafe {
            std::ptr::copy_nonoverlapping(chunk.as_ptr(), &mut header as *mut Self as *mut u8, 16);
        }
        header
    }
}

/// Index of UUID
#[derive(Debug, Default, Copy, Clone, PartialEq)]
pub(crate) struct UuidIndex {
    pub uuid: Uuid,  // UUID
    pub offset: u64, // OFFSET
}

impl UuidIndex {
    pub fn new(uuid: Uuid, offset: u64) -> Self {
        Self { uuid, offset }
    }

    pub fn as_bytes(&self) -> [u8; 24] {
        let mut bytes = [0u8; 24];
        unsafe {
            // use big-endian encoding for UUID
            std::ptr::copy_nonoverlapping(
                <Uuid as Into<u128>>::into(self.uuid).to_le_bytes().as_ptr(),
                bytes.as_mut_ptr(),
                16,
            );
            std::ptr::copy_nonoverlapping(
                self.offset.to_le_bytes().as_ptr(),
                bytes.as_mut_ptr().add(16),
                8,
            );
        }
        bytes
    }

    pub fn from_bytes(chunk: &[u8; 24]) -> Self {
        // use big-endian decoding for UUID
        let uuid =
            <Uuid as From<u128>>::from(u128::from_le_bytes(chunk[0..16].try_into().unwrap()));
        let offset = u64::from_le_bytes(chunk[16..24].try_into().unwrap());

        Self { uuid, offset }
    }
}
