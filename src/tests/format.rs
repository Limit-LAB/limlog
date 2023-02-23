use bincode::Options;
use uuid7::Uuid;

use crate::{
    formats::{Log, UuidIndex},
    util::{bincode_option, to_uuid, try_decode},
};

pub(crate) const LOG1: [u8; 34] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, // uuid
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // key length
    0x01, // key
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // value length
    0x0A, // value
];
pub(crate) const LOG2: [u8; 34] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, // uuid
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // key length
    0x02, // key
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // value length
    0x0B, // value
];
pub(crate) const LOG3: [u8; 34] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, // uuid
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // key length
    0x03, // key
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // value length
    0x0C, // value
];

pub(crate) const INDEX1: [u8; 24] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, // uuid
    0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset
];
pub(crate) const INDEX2: [u8; 24] = [
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, // uuid
    114, 5, 14, 0x00, 0x00, 0x00, 0x00, 0x00, // offset
];

#[test]
fn test_log_format() {
    let (l1, 34) = try_decode(&LOG1[..]).unwrap().unwrap() else { panic!("Missmatched parsed length") };
    let (l2, 34) = try_decode(&LOG2[..]).unwrap().unwrap() else { panic!("Missmatched parsed length") };
    let (l3, 34) = try_decode(&LOG3[..]).unwrap().unwrap() else { panic!("Missmatched parsed length") };

    let idx1 = UuidIndex::from_bytes(&INDEX1);
    let idx2 = UuidIndex::from_bytes(&INDEX2);

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
        UuidIndex {
            uuid: to_uuid(1, 0),
            offset: 24
        },
        idx1
    );
    assert_eq!(
        UuidIndex {
            uuid: to_uuid(0x0000FFFFFFFFFFFF, 0),
            offset: 918898
        },
        idx2
    );
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

    eprintln!("{vec:?}");

    let l2 = opt.deserialize(&vec).unwrap();

    assert_eq!(l1, l2)
}
