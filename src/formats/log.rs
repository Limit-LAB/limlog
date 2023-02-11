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

/// Index type. Notice that the size of this type is an invariant so that data
/// can be correctly indexed.
#[repr(C)]
#[derive(Serialize, Deserialize, Debug, Default, Copy, Clone, PartialEq)]
pub(crate) struct Index(
    pub u64, // ID
    pub u64, // OFFSET
);

#[repr(C)]
#[derive(Serialize, Deserialize, Debug, Default, Copy, Clone, PartialEq)]
pub(crate) struct Timestamp(
    pub u64, // TS
    pub u64, // OFFSET
);

impl_from_bytes!(Index);
impl_from_bytes!(Timestamp);
impl_from_bytes!(IndexFileHeader);

impl_key_ord!(Index);
impl_key_ord!(Timestamp);
