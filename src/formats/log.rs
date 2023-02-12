use serde::{Deserialize, Serialize};

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
    ($class:ty, $key:ident) => {
        impl PartialOrd for $class {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                self.$key.partial_cmp(&other.$key)
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

/// Index of ID
#[repr(C)]
#[derive(Serialize, Deserialize, Debug, Default, Copy, Clone, PartialEq)]
pub(crate) struct IdIndex {
    pub id: u64,     // ID
    pub offset: u64, // OFFSET
}

/// Index of timestamp
#[repr(C)]
#[derive(Serialize, Deserialize, Debug, Default, Copy, Clone, PartialEq)]
pub(crate) struct TsIndex {
    pub ts: u64,     // TS
    pub offset: u64, // OFFSET
}

impl_from_bytes!(IdIndex);
impl_from_bytes!(TsIndex);
impl_from_bytes!(IndexFileHeader);

impl_key_ord!(IdIndex, id);
impl_key_ord!(TsIndex, ts);
