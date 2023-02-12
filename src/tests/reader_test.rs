use super::{
    log_format_test::{
        INDEX1, INDEX2, INDEX3, INDEX_FILE_HEADER, LOG1, LOG2, LOG3, LOG_FILE_HEADER, TIMESTAMP1,
        TIMESTAMP2, TIMESTAMP3, TS_INDEX_FILE_HEADER,
    },
    TestFile,
};
use crate::{
    formats::log::{IdIndex, TsIndex, INDEX_HEADER, TS_INDEX_HEADER},
    selector::{index_reader::IndexReader, log_reader::LogReader},
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
            .copied()
            .collect::<Vec<_>>(),
    );

    let idx_reader = IndexReader::new(idx_file, INDEX_HEADER).unwrap();

    let res = idx_reader
        .select_range(&IdIndex { id: 0, offset: 0 }, &IdIndex { id: 2, offset: 0 })
        .unwrap()
        .unwrap();
    assert_eq!(res, (IdIndex { id: 1, offset: 24 }, 2));
    let res = idx_reader
        .select_range(&IdIndex { id: 2, offset: 0 }, &IdIndex { id: 4, offset: 0 })
        .unwrap()
        .unwrap();
    assert_eq!(res, (IdIndex { id: 1, offset: 24 }, 2));

    let ts_idx_file = TestFile::new(
        TS_INDEX_FILE_HEADER
            .iter()
            .chain(
                TIMESTAMP1
                    .iter()
                    .chain(TIMESTAMP2.iter().chain(TIMESTAMP3.iter())),
            )
            .copied()
            .collect::<Vec<_>>(),
    );

    let idx_reader = IndexReader::new(ts_idx_file, TS_INDEX_HEADER).unwrap();

    let res = idx_reader
        .select_range(&TsIndex { ts: 0, offset: 0 }, &TsIndex { ts: 2, offset: 0 })
        .unwrap()
        .unwrap();
    assert_eq!(res, (TsIndex { ts: 1, offset: 24 }, 2));
    let res = idx_reader
        .select_range(&TsIndex { ts: 1, offset: 0 }, &TsIndex { ts: 4, offset: 0 })
        .unwrap()
        .unwrap();
    assert_eq!(res, (TsIndex { ts: 1, offset: 24 }, 2));
}
