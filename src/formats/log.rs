use std::collections::BTreeMap;

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

macro_rules! impl_into_bytes {
    ($class:ty) => {
        impl $class {
            pub fn into_bytes(self) -> std::result::Result<Vec<u8>, bincode::Error> {
                bincode::serialize(&self)
            }
        }
    };
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LogFile {
    pub header: LogFileHeader,
    pub logs: Vec<Log>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LogFileHeader {
    pub magic_number: u64,
    pub attributes: u64,
    pub entry_count: u64,
    // LOGS
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Log {
    pub ts: u64,
    pub id: u64,

    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl_from_bytes!(Log);
impl_from_bytes!(LogFile);
impl_from_bytes!(LogFileHeader);

impl_into_bytes!(Log);
impl_into_bytes!(LogFile);
impl_into_bytes!(LogFileHeader);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) struct Index(pub u64, pub u64);
pub type Indexes = BTreeMap<
    u64, // ID
    u64, // OFFSET
>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct IndexFile {
    pub magic_number: u64,
    pub indexes: Indexes,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) struct Timestamp(pub u64, pub u64);
pub type Timestamps = BTreeMap<
    u64, // TS
    u64, // OFFSET
>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TimestampFile {
    pub magic_number: u64,
    pub timestamps: Timestamps,
}

impl_from_bytes!(Index);
impl_from_bytes!(IndexFile);
impl_from_bytes!(Timestamp);
impl_from_bytes!(TimestampFile);

impl_into_bytes!(Index);
impl_into_bytes!(IndexFile);
impl_into_bytes!(Timestamp);
impl_into_bytes!(TimestampFile);
