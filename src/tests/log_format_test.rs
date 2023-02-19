use bincode::Options;

use crate::{
    consts::MIN_LOG_SIZE,
    formats::log::{Header, Log, UuidIndex},
    util::{bincode_option, to_uuid},
};

pub(crate) const LOG_FILE_HEADER: [u8; 16] = [
    b'L', b'I', b'M', b'_', b'L', b'O', b'G', 0x00, // magic
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // attributes
];

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

pub(crate) const INDEX_FILE_HEADER: [u8; 16] = [
    b'L', b'I', b'M', b'_', b'I', b'D', b'X', 0x00, // magic
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // attributes
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
fn test_log() {
    let opt = bincode_option();

    // log file header serialization
    assert_eq!(LOG_FILE_HEADER, Header::LOG_DEFAULT.as_bytes());
    // log file header deserialization
    assert_eq!(
        Header::LOG_DEFAULT,
        Header::from_bytes(&LOG_FILE_HEADER.try_into().unwrap())
    );

    let expected_l1 = Log::new(to_uuid(1, 0), vec![1], vec![10]);
    let expected_l2 = Log::new(to_uuid(2, 0), vec![2], vec![11]);
    let expected_l3 = Log::new(to_uuid(3, 0), vec![3], vec![12]);

    println!("{expected_l1:?}");

    // log serialization
    assert_eq!(LOG1, opt.serialize(&expected_l1).unwrap().as_slice());
    assert_eq!(LOG2, opt.serialize(&expected_l2).unwrap().as_slice());
    assert_eq!(LOG3, opt.serialize(&expected_l3).unwrap().as_slice());

    // log deserialization
    assert_eq!(expected_l1, opt.deserialize(&LOG1[..]).unwrap());
    assert_eq!(expected_l2, opt.deserialize(&LOG2[..]).unwrap());
    assert_eq!(expected_l3, opt.deserialize(&LOG3[..]).unwrap());

    // log min size
    assert_eq!(
        MIN_LOG_SIZE as u64,
        opt.serialized_size(&Log::default()).unwrap()
    );
}

#[test]
fn test_index() {
    // index file header serialization
    assert_eq!(INDEX_FILE_HEADER, Header::INDEX_DEFAULT.as_bytes());
    // index file header deserialization
    assert_eq!(
        Header::INDEX_DEFAULT,
        Header::from_bytes(&INDEX_FILE_HEADER.try_into().unwrap())
    );

    let expected_i1 = UuidIndex::new(to_uuid(1, 0), 24);
    let expected_i2 = UuidIndex::new(to_uuid(2, 0), 58);
    let expected_i3 = UuidIndex::new(to_uuid(3, 0), 92);

    // index serialization
    assert_eq!(INDEX1, expected_i1.as_bytes());
    assert_eq!(INDEX2, expected_i2.as_bytes());
    assert_eq!(INDEX3, expected_i3.as_bytes());

    // index deserialization
    assert_eq!(
        expected_i1,
        UuidIndex::from_bytes(&INDEX1.try_into().unwrap())
    );
    assert_eq!(
        expected_i2,
        UuidIndex::from_bytes(&INDEX2.try_into().unwrap())
    );
    assert_eq!(
        expected_i3,
        UuidIndex::from_bytes(&INDEX3.try_into().unwrap())
    );
}
