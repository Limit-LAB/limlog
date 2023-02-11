use serde::{Deserialize, Serialize};

// log file format:
// MAGIC_NUMBER 8 bytes
// ATTRIBUTES 8 bytes
// ENTRY_COUNT 8 bytes
// logs👇

// log format: OFFSET point to here
// TS 8 bytes
// ID 8 bytes
// key N bytes
// value N bytes

// index file format:
// MAGIC_NUMBER 8 bytes
// indexes👇

// index format:
// ID 8 bytes
// OFFSET 8 bytes

// timestamp file format:
// MAGIC_NUMBER 8 bytes
// timestamps👇

// timestamp format:
// TS 8 bytes
// OFFSET 8 bytes

macro_rules! impl_from_bytes {
    ($class:ty) => {
        impl TryFrom<&[u8]> for $class {
            type Error = bincode::Error;

            fn try_from(bytes: &[u8]) -> std::result::Result<Self, Self::Error> {
                bincode::deserialize(bytes)
            }
        }
    };
}

macro_rules! impl_key_ord {
    ($class:ty) => {
        impl PartialOrd for $class {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                self.0.partial_cmp(&other.0)
            }
        }
    };
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub(crate) struct LogFileHeader {
    pub magic_number: u64,
    pub attributes: u64,
    pub entry_count: u64,
    // LOGS
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Log {
    pub ts: u64,
    pub id: u64,

    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl_from_bytes!(Log);
impl_from_bytes!(LogFileHeader);

pub(crate) const INDEX_HEADER: IndexFileHeader = IndexFileHeader { magic_number: 1 };
pub(crate) const TS_INDEX_HEADER: IndexFileHeader = IndexFileHeader { magic_number: 2 };

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub(crate) struct IndexFileHeader {
    pub magic_number: u64,
    // INDEXES
}

#[derive(Serialize, Deserialize, Debug, Default, Copy, Clone, PartialEq)]
pub(crate) struct Index(pub u64, pub u64);

#[derive(Serialize, Deserialize, Debug, Default, Copy, Clone, PartialEq)]
pub(crate) struct Timestamp(pub u64, pub u64);

impl_from_bytes!(Index);
impl_from_bytes!(Timestamp);
impl_from_bytes!(IndexFileHeader);

impl_key_ord!(Index);
impl_key_ord!(Timestamp);
