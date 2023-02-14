use std::{thread, time::Duration};

use super::{
    log_format_test::{LOG1, LOG2, LOG3, LOG_FILE_HEADER},
    TestFile,
};
use crate::{
    appender::log_writer::LogWriter,
    tests::log_format_test::{INDEX1, INDEX2, INDEX3, INDEX_FILE_HEADER},
    util::ts_to_uuid,
    Log,
};

#[test]
fn test_writer() {
    let log_file = TestFile::new(Vec::new());
    let idx_file = TestFile::new(Vec::new());

    let logs = vec![
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

    let writer = LogWriter::new(log_file.clone(), idx_file.clone()).unwrap();
    writer.append_logs(logs).unwrap();

    // wait for appender finished
    thread::sleep(Duration::from_millis(500));

    let expected_log_file = LOG_FILE_HEADER
        .iter()
        .chain(LOG1.iter().chain(LOG2.iter().chain(LOG3.iter())))
        .copied()
        .collect::<Vec<_>>();

    let expected_idx_file = INDEX_FILE_HEADER
        .iter()
        .chain(INDEX1.iter().chain(INDEX2.iter().chain(INDEX3.iter())))
        .copied()
        .collect::<Vec<_>>();

    assert_eq!(log_file.get_buf(), expected_log_file);
    assert_eq!(idx_file.get_buf(), expected_idx_file);
}
