use serde::{Deserialize, Serialize};
use uuid7::Uuid;

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

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub(crate) struct LogFileHeader {
    pub magic_number: u64,
    pub attributes: u64,
    pub entry_count: u64,
    // LOGS
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Log {
    pub uuid: Uuid,

    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl_from_bytes!(Log);
impl_from_bytes!(LogFileHeader);

pub(crate) const INDEX_HEADER: IndexFileHeader = IndexFileHeader { magic_number: 1 };

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub(crate) struct IndexFileHeader {
    pub magic_number: u64,
    // INDEXES
}

/// Index of UUID
#[derive(Serialize, Deserialize, Debug, Default, Copy, Clone, PartialEq)]
pub(crate) struct UuidIndex {
    pub uuid: Uuid,  // UUID
    pub offset: u64, // OFFSET
}

impl_from_bytes!(UuidIndex);
impl_from_bytes!(IndexFileHeader);

#[test]
fn test_min_log_size() {
    let min = Log {
        uuid: Uuid::default(),
        key: vec![],
        value: vec![],
    };

    let min_size = bincode::serialized_size(&min).unwrap();
    println!("min_size: {}", min_size);
}
