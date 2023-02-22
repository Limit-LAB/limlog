use serde::{Deserialize, Serialize};
use uuid7::{uuid7, Uuid};

use crate::consts::{HEADER_SIZE, INDEX_MAGIC, LOG_MAGIC};

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Log {
    pub uuid: Uuid,

    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[repr(C, align(8))]
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Header {
    pub magic_number: [u8; 8],
    pub attributes: [u8; 8],
}

impl Header {
    pub const INDEX: Header = Header {
        magic_number: *INDEX_MAGIC,
        attributes: [0u8; 8],
    };
    pub const LOG: Header = Header {
        magic_number: *LOG_MAGIC,
        attributes: [0u8; 8],
    };

    pub fn as_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut bytes = [0u8; HEADER_SIZE];
        self.write_to(&mut bytes);
        bytes
    }

    pub fn write_to(&self, buf: &mut [u8]) {
        assert!(buf.len() >= 16);
        unsafe {
            std::ptr::copy_nonoverlapping(
                self as *const Self as *const u8,
                buf.as_mut_ptr(),
                HEADER_SIZE,
            );
        }
    }
}

/// Index of UUID
#[derive(Serialize, Deserialize, Debug, Default, Copy, Clone, PartialEq)]
pub(crate) struct UuidIndex {
    pub uuid: Uuid,  // UUID
    pub offset: u64, // OFFSET
}

impl UuidIndex {
    pub fn new(offset: u64) -> Self {
        Self {
            uuid: uuid7(),
            offset,
        }
    }

    pub fn as_bytes(&self) -> [u8; 24] {
        let mut bytes = [0u8; 24];
        self.write_to(&mut bytes);
        bytes
    }

    pub fn write_to(&self, slice: &mut [u8; 24]) {
        unsafe {
            std::ptr::copy_nonoverlapping(self.uuid.as_bytes().as_ptr(), slice.as_mut_ptr(), 16);
            std::ptr::copy_nonoverlapping(
                self.offset.to_le_bytes().as_ptr(),
                slice.as_mut_ptr().add(16),
                8,
            );
        }
    }

    pub fn from_bytes(&self, chunk: &[u8; 24]) -> Self {
        let uuid = <Uuid as From<[u8; 16]>>::from(chunk[0..16].try_into().unwrap());
        let offset = u64::from_le_bytes(chunk[16..24].try_into().unwrap());

        Self { uuid, offset }
    }
}

#[test]
fn test_min_log_size() {
    use bincode::Options;

    use crate::util::bincode_option;

    let min = Log {
        uuid: Uuid::default(),
        key: vec![],
        value: vec![],
    };

    let min_size = bincode_option().serialized_size(&min).unwrap();
    println!("min_size: {min_size}");
}
