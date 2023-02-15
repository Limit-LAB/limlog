use bincode::Options;
use uuid7::Uuid;

use crate::{
    formats::log::{IndexFileHeader, Log, LogFileHeader, UuidIndex, INDEX_HEADER},
    util::{bincode_option, to_uuid}, consts::MIN_LOG_SIZE,
};

pub(crate) const LOG1: [u8; 34] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
    0x00, // uuid
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // key length
    0x01, // key
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // value length
    0x0A, // value
];
pub(crate) const LOG2: [u8; 34] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
    0x00, // uuid
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // key length
    0x02, // key
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // value length
    0x0B, // value
];
pub(crate) const LOG3: [u8; 34] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00,
    0x00, // uuid
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // key length
    0x03, // key
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // value length
    0x0C, // value
];

pub(crate) const LOG_FILE_HEADER: [u8; 24] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // magic
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // attributes
    0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* entry_count
           * logs */
];

pub(crate) const INDEX_FILE_HEADER: [u8; 8] = [
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // magic
];
pub(crate) const INDEX1: [u8; 24] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
    0x00, // uuid
    0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset
];
pub(crate) const INDEX2: [u8; 24] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
    0x00, // uuid
    0x3A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset
];
pub(crate) const INDEX3: [u8; 24] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00,
    0x00, // uuid
    0x5C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset
];

#[test]
fn test_log_format() {
    let l1 = Log::try_from(&LOG1[..]).unwrap();
    let l2 = Log::try_from(&LOG2[..]).unwrap();
    let l3 = Log::try_from(&LOG3[..]).unwrap();
    let lh = LogFileHeader::try_from(&LOG_FILE_HEADER[..]).unwrap();

    let idx1 = UuidIndex::try_from(&INDEX1[..]).unwrap();
    let idx2 = UuidIndex::try_from(&INDEX2[..]).unwrap();
    let idx3 = UuidIndex::try_from(&INDEX3[..]).unwrap();
    let idx_h = IndexFileHeader::try_from(&INDEX_FILE_HEADER[..]).unwrap();

    assert_eq!(
        Log {
            uuid: to_uuid(1, 0),
            key: vec![1],
            value: vec![10]
        },
        l1
    );
    assert_eq!(
        Log {
            uuid: to_uuid(2, 0),
            key: vec![2],
            value: vec![11]
        },
        l2
    );
    assert_eq!(
        Log {
            uuid: to_uuid(3, 0),
            key: vec![3],
            value: vec![12]
        },
        l3
    );

    assert_eq!(
        LogFileHeader {
            magic_number: 0,
            attributes: 0,
            entry_count: 3
        },
        lh
    );

    assert_eq!(
        UuidIndex {
            uuid: to_uuid(1, 0),
            offset: 24
        },
        idx1
    );
    assert_eq!(
        UuidIndex {
            uuid: to_uuid(2, 0),
            offset: 58
        },
        idx2
    );
    assert_eq!(
        UuidIndex {
            uuid: to_uuid(3, 0),
            offset: 92
        },
        idx3
    );
    assert_eq!(INDEX_HEADER, idx_h);
}

#[test]
fn test_ser() {
    let l1 = Log {
        uuid: Uuid::MAX,
        key: vec![1, 1, 4, 5, 1, 4],
        value: vec![1, 1, 1, 1, 1, 1, 1, 1],
    };
    let opt = bincode_option();
    let len = opt.serialized_size(&l1).unwrap() as usize;
    let mut vec = vec![0u8; len];
    opt.serialize_into(&mut vec[..], &l1).unwrap();

    eprintln!("Log bytes: {vec:?}");

    let l2 = opt.deserialize(&vec).unwrap();

    assert_eq!(l1, l2);

    let min_size = opt.serialized_size(&Log::default()).unwrap();
    eprintln!("Min log size: {min_size}");
    assert_eq!(min_size, MIN_LOG_SIZE as _);
}
