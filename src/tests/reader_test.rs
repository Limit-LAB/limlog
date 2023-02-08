use crate::{
    formats::log::{Index, Timestamp, INDEX_HEADER, TS_INDEX_HEADER},
    selector::{index_reader::IndexReader, log_reader::LogReader},
    Log,
};

use super::{
    log_format_test::{
        INDEX1, INDEX2, INDEX3, INDEX_FILE_HEADER, LOG1, LOG2, LOG3, LOG_FILE_HEADER, TIMESTAMP1,
        TIMESTAMP2, TIMESTAMP3, TS_INDEX_FILE_HEADER,
    },
    TestFile,
};

#[test]
fn test_reader() {
    let log_file = TestFile::new(
        LOG_FILE_HEADER
            .iter()
            .chain(LOG1.iter().chain(LOG2.iter().chain(LOG3.iter())))
            .map(|b| *b)
            .collect::<Vec<_>>(),
    );

    let expected_logs = vec![
        Log {
            ts: 1,
            id: 1,
            key: vec![1],
            value: vec![10],
        },
        Log {
            ts: 2,
            id: 2,
            key: vec![2],
            value: vec![11],
        },
        Log {
            ts: 3,
            id: 3,
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
            .map(|b| *b)
            .collect::<Vec<_>>(),
    );

    let idx_reader = IndexReader::new::<Index>(idx_file, INDEX_HEADER).unwrap();

    let res = idx_reader
        .select_range(&Index(0, 0), &Index(2, 0))
        .unwrap()
        .unwrap();
    assert_eq!(res, (Index(1, 24), 2));
    let res = idx_reader
        .select_range(&Index(2, 0), &Index(4, 0))
        .unwrap()
        .unwrap();
    assert_eq!(res, (Index(1, 24), 2));

    let ts_idx_file = TestFile::new(
        TS_INDEX_FILE_HEADER
            .iter()
            .chain(
                TIMESTAMP1
                    .iter()
                    .chain(TIMESTAMP2.iter().chain(TIMESTAMP3.iter())),
            )
            .map(|b| *b)
            .collect::<Vec<_>>(),
    );

    let idx_reader = IndexReader::new::<Timestamp>(ts_idx_file, TS_INDEX_HEADER).unwrap();

    let res = idx_reader
        .select_range(&Timestamp(0, 0), &Timestamp(2, 0))
        .unwrap()
        .unwrap();
    assert_eq!(res, (Timestamp(1, 24), 2));
    let res = idx_reader
        .select_range(&Timestamp(1, 0), &Timestamp(4, 0))
        .unwrap()
        .unwrap();
    assert_eq!(res, (Timestamp(1, 24), 2));
}
