use super::{
    log_format_test::{
        INDEX1, INDEX2, INDEX3, INDEX_FILE_HEADER, LOG1, LOG2, LOG3, LOG_FILE_HEADER,
    },
    TestFile,
};
use crate::{
    formats::log::UuidIndex,
    selector::{index_reader::IndexReader, log_reader::LogReader},
    util::ts_to_uuid,
    Log,
};

#[test]
fn test_reader() {
    let log_file = TestFile::new(
        LOG_FILE_HEADER
            .iter()
            .chain(LOG1.iter().chain(LOG2.iter().chain(LOG3.iter())))
            .copied()
            .collect::<Vec<_>>(),
    );

    let expected_logs = vec![
        Log {
            uuid: ts_to_uuid(1, 0),
            key: vec![1],
            value: vec![10],
        },
        Log {
            uuid: ts_to_uuid(2, 0),
            key: vec![2],
            value: vec![11],
        },
        Log {
            uuid: ts_to_uuid(3, 0),
            key: vec![3],
            value: vec![12],
        },
    ];

    let logs = LogReader::new(log_file)
        .unwrap()
        .select_logs(24, 3)
        .unwrap();
    assert_eq!(logs, expected_logs);

    let idx_file = TestFile::new(
        INDEX_FILE_HEADER
            .iter()
            .chain(INDEX1.iter().chain(INDEX2.iter().chain(INDEX3.iter())))
            .copied()
            .collect::<Vec<_>>(),
    );

    let idx_reader = IndexReader::new(idx_file).unwrap();

    let res = idx_reader
        .select_range(&ts_to_uuid(0, 0), &ts_to_uuid(2, 0xFF))
        .unwrap()
        .unwrap();
    assert_eq!(
        res,
        (
            UuidIndex {
                uuid: ts_to_uuid(1, 0),
                offset: 24
            },
            1
        )
    );
    let res = idx_reader
        .select_range(&ts_to_uuid(2, 0), &ts_to_uuid(4, 0xFF))
        .unwrap()
        .unwrap();
    assert_eq!(
        res,
        (
            UuidIndex {
                uuid: ts_to_uuid(1, 0),
                offset: 24
            },
            3
        )
    );
}
