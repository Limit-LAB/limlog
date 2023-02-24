use serde::{Deserialize, Serialize};
use uuid7::{uuid7, Uuid};

use crate::{
    consts::{HEADER_SIZE, INDEX_MAGIC, INDEX_SIZE, LOG_MAGIC},
    util::SubArray,
};

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct Log {
    #[serde(with = "uuid_u128_little_endian")]
    pub uuid: Uuid,
    pub body: Vec<u8>,
}

impl Log {
    #[inline]
    pub fn new(body: impl Into<Vec<u8>>) -> Self {
        Self {
            uuid: uuid7(),
            body: body.into(),
        }
    }

    /// Specialized short cut of `bincode::Options::serialized_size()`
    #[inline]
    pub fn byte_len(&self) -> usize {
        24 + self.body.len()
    }
}

#[repr(C, align(8))]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub struct Header {
    pub magic_number: [u8; 8],
    pub attributes: [u8; 8],
}

impl Header {
    pub const INDEX: Self = Self {
        magic_number: *INDEX_MAGIC,
        attributes: [0u8; 8],
    };
    pub const LOG: Self = Self {
        magic_number: *LOG_MAGIC,
        attributes: [0u8; 8],
    };

    pub fn as_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut bytes = [0u8; HEADER_SIZE];
        self.write_to(&mut bytes);
        bytes
    }

    /// # Panics
    ///
    /// Panics if `buf.len() < 16`.
    pub fn write_to(&self, buf: &mut [u8]) {
        assert!(buf.len() >= 16);
        unsafe {
            std::ptr::copy_nonoverlapping(
                (self as *const Self).cast(),
                buf.as_mut_ptr(),
                HEADER_SIZE,
            );
        }
    }

    pub fn from_bytes(chunk: &[u8; 16]) -> Self {
        let mut header = Self::default();
        unsafe {
            std::ptr::copy_nonoverlapping(
                chunk.as_ptr(),
                std::ptr::addr_of_mut!(header).cast(),
                16,
            );
        }
        header
    }
}

/// Index of UUID
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
pub struct UuidIndex {
    pub uuid: Uuid,
    pub offset: u64,
}

impl UuidIndex {
    pub fn as_bytes(&self) -> [u8; INDEX_SIZE] {
        let mut bytes = [0u8; INDEX_SIZE];
        self.write_to(&mut bytes);
        bytes
    }

    pub fn write_to(&self, slice: &mut [u8; INDEX_SIZE]) {
        unsafe {
            std::ptr::copy_nonoverlapping(self.uuid.as_bytes().as_ptr(), slice.as_mut_ptr(), 16);
            std::ptr::copy_nonoverlapping(
                self.offset.to_le_bytes().as_ptr(),
                slice.as_mut_ptr().add(16),
                8,
            );
        }
    }

    pub fn from_bytes(chunk: &[u8; INDEX_SIZE]) -> Self {
        let uuid = Uuid::from(*chunk.sub::<0, 16>());
        let offset = u64::from_le_bytes(*chunk.sub::<16, INDEX_SIZE>());

        Self { uuid, offset }
    }
}

mod uuid_u128_little_endian {
    use std::fmt;

    use serde::{de, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &uuid7::Uuid, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u128(u128::from_le_bytes(*v.as_bytes()))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<uuid7::Uuid, D::Error> {
        d.deserialize_u128(U128LittleEndianVisitor)
    }

    struct U128LittleEndianVisitor;

    impl<'de> de::Visitor<'de> for U128LittleEndianVisitor {
        type Value = uuid7::Uuid;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "a UUID representation")
        }

        fn visit_u128<E: de::Error>(self, value: u128) -> Result<Self::Value, E> {
            Ok(Self::Value::from(value.to_le_bytes()))
        }
    }
}

#[test]
fn test_log_size() {
    use bincode::Options;

    let log = Log::default();
    let len = crate::bincode_option().serialized_size(&log).unwrap();

    assert_eq!(len, log.byte_len() as _);

    let log = Log::new(vec![1, 1, 4, 5, 1, 4]);
    let len = crate::bincode_option().serialized_size(&log).unwrap();

    assert_eq!(len, log.byte_len() as _);
}
