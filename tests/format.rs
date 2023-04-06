use bincode::Options;
use limlog::{
    bincode_option,
    consts::SmallBytes,
    formats::{Log, UuidIndex},
    try_decode,
};
use smallvec::smallvec;
use uuid7::Uuid;

mod_use::mod_use!(common);

pub(crate) const LOG1: [u8; 25] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, // uuid
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // body length
    0x0A, // body
];
pub(crate) const LOG2: [u8; 25] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, // uuid
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // body length
    0x0B, // body
];
pub(crate) const LOG3: [u8; 25] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, // uuid
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // body length
    0x0C, // body
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
    let (l1, 25) = try_decode(&LOG1).unwrap().unwrap() else { panic!("Missmatched parsed length") };
    let (l2, 25) = try_decode(&LOG2).unwrap().unwrap() else { panic!("Missmatched parsed length") };
    let (l3, 25) = try_decode(&LOG3).unwrap().unwrap() else { panic!("Missmatched parsed length") };

    let idx1 = UuidIndex::from_bytes(&INDEX1);
    let idx2 = UuidIndex::from_bytes(&INDEX2);

    assert_eq!(
        Log {
            uuid: to_uuid(1, 0),
            body: smallvec![10]
        },
        l1
    );
    assert_eq!(
        Log {
            uuid: to_uuid(2, 0),
            body: smallvec![11]
        },
        l2
    );
    assert_eq!(
        Log {
            uuid: to_uuid(3, 0),
            body: smallvec![12]
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
        body: smallvec![1, 1, 1, 1, 1, 1, 1, 1],
    };
    let opt = bincode_option();
    let len = opt.serialized_size(&l1).unwrap() as usize;
    let mut vec: SmallBytes = smallvec![0u8; len];
    opt.serialize_into(&mut vec[..], &l1).unwrap();

    eprintln!("{vec:?}");

    let l2 = opt.deserialize(&vec).unwrap();

    assert_eq!(l1, l2)
}
