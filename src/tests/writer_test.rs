use std::{thread, time::Duration};

use crate::{
    appender::log_writer::LogWriter,
    tests::log_format_test::{
        INDEX1, INDEX2, INDEX3, INDEX_FILE_HEADER, TIMESTAMP1, TIMESTAMP2, TIMESTAMP3,
        TS_INDEX_FILE_HEADER,
    },
    Log,
};

use super::{
    log_format_test::{LOG1, LOG2, LOG3, LOG_FILE_HEADER},
    TestFile,
};

#[test]
fn test_writer() {
    let log_file = TestFile::new(Vec::new());
    let idx_file = TestFile::new(Vec::new());
    let ts_idx_file = TestFile::new(Vec::new());

    let logs = vec![
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

    let writer = LogWriter::new(log_file.clone(), idx_file.clone(), ts_idx_file.clone()).unwrap();
    writer.append_logs(logs).unwrap();

    // wait for appender finished
    thread::sleep(Duration::from_millis(500));

    let expected_log_file = LOG_FILE_HEADER
        .iter()
        .chain(LOG1.iter().chain(LOG2.iter().chain(LOG3.iter()))).copied()
        .collect::<Vec<_>>();

    let expected_idx_file = INDEX_FILE_HEADER
        .iter()
        .chain(INDEX1.iter().chain(INDEX2.iter().chain(INDEX3.iter()))).copied()
        .collect::<Vec<_>>();

    let expected_ts_idx_file = TS_INDEX_FILE_HEADER
        .iter()
        .chain(
            TIMESTAMP1
                .iter()
                .chain(TIMESTAMP2.iter().chain(TIMESTAMP3.iter())),
        ).copied()
        .collect::<Vec<_>>();

    assert_eq!(log_file.get_buf(), expected_log_file);
    assert_eq!(idx_file.get_buf(), expected_idx_file);
    assert_eq!(ts_idx_file.get_buf(), expected_ts_idx_file);
}
