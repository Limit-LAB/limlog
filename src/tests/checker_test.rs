use std::mem::size_of;

use crate::{
    checker::{IndexChecker, LogChecker},
    formats::log::{Index, IndexFileHeader, LogFileHeader, INDEX_HEADER, TS_INDEX_HEADER},
    tests::log_format_test::{
        INDEX1, INDEX2, INDEX3, INDEX_FILE_HEADER, LOG1, LOG2, LOG3, LOG_FILE_HEADER,
    },
    util::BlockIODevice,
};

use super::TestFile;

#[test]
fn test_checker() {
    log_file_check();
    idx_file_check();
}

fn log_file_check() {
    // empty log file
    let mut log_file = TestFile::new(Vec::new());
    let mut log_len = log_file.len().unwrap();
    let header = LogChecker::check(&mut log_file, &mut log_len)
        .or_init()
        .unwrap();
    assert_eq!(header, LogFileHeader::default());
    assert_eq!(log_len, size_of::<LogFileHeader>() as u64);
    assert_eq!(
        log_file.get_buf(),
        bincode::serialize(&LogFileHeader::default()).unwrap()
    );

    // valid log file
    let mut log_file = TestFile::new(
        LOG_FILE_HEADER
            .iter()
            .chain(LOG1.iter().chain(LOG2.iter().chain(LOG3.iter()))).copied()
            .collect::<Vec<_>>(),
    );
    let mut log_len = log_file.len().unwrap();
    let header = LogChecker::check(&mut log_file, &mut log_len)
        .header()
        .unwrap();
    assert_eq!(
        header,
        LogFileHeader {
            magic_number: 0,
            attributes: 0,
            entry_count: 3
        }
    );

    // invalid log file
    let mut log_file = TestFile::new(log_file.get_buf()[0..11].into());
    let mut log_len = log_file.len().unwrap();
    assert!(LogChecker::check(&mut log_file, &mut log_len)
        .header()
        .is_err());
}

fn idx_file_check() {
    // empty index file
    let mut idx_file = TestFile::new(Vec::new());
    let mut idx_len = idx_file.len().unwrap();
    IndexChecker::check::<Index>(&mut idx_file, &mut idx_len, INDEX_HEADER)
        .or_init()
        .unwrap();
    assert_eq!(idx_len, size_of::<IndexFileHeader>() as u64);
    assert_eq!(idx_file.get_buf(), INDEX_FILE_HEADER);

    // valid index file
    let mut idx_file = TestFile::new(
        INDEX_FILE_HEADER
            .iter()
            .chain(INDEX1.iter().chain(INDEX2.iter().chain(INDEX3.iter()))).copied()
            .collect::<Vec<_>>(),
    );
    let mut idx_len = idx_file.len().unwrap();
    IndexChecker::check::<Index>(&mut idx_file, &mut idx_len, INDEX_HEADER)
        .header()
        .unwrap();

    // invalid index file
    let mut idx_file = TestFile::new(idx_file.get_buf()[0..9].into());
    let mut idx_len = idx_file.len().unwrap();
    assert!(
        IndexChecker::check::<Index>(&mut idx_file, &mut idx_len, INDEX_HEADER)
            .header()
            .is_err()
    );

    let mut idx_file = TestFile::new(idx_file.get_buf()[0..5].into());
    let mut idx_len = idx_file.len().unwrap();
    assert!(
        IndexChecker::check::<Index>(&mut idx_file, &mut idx_len, INDEX_HEADER)
            .header()
            .is_err()
    );

    let mut idx_file = TestFile::new(idx_file.get_buf());
    let mut idx_len = idx_file.len().unwrap();
    assert!(
        IndexChecker::check::<Index>(&mut idx_file, &mut idx_len, TS_INDEX_HEADER)
            .header()
            .is_err()
    );
}
