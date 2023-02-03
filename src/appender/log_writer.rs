use anyhow::{ensure, Result};
use kanal::{bounded, Receiver, Sender};
use positioned_io::{ReadAt, WriteAt};
use std::{
    fs::File,
    io::Write,
    mem::size_of,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
};

use crate::{
    appender::index_writer::IndexWriter,
    formats::log::{Index, Log, LogFileHeader, Timestamp, INDEX_HEADER, TS_INDEX_HEADER},
};

type Counter = Arc<AtomicU64>;

#[derive(Debug)]
pub(crate) struct LogWriter {
    sender: Sender<Vec<Log>>,
    file_size_view: Counter,
}

impl LogWriter {
    pub(crate) fn new(path: impl AsRef<Path>, file_name: String) -> Result<LogWriter> {
        let file = File::options()
            .append(true)
            .create(true)
            .read(true)
            .open(path.as_ref().join(format!("{file_name}.limlog")))?;

        let file_size = file.metadata()?.len();
        ensure!(
            file_size == 0 || file_size > size_of::<LogFileHeader>() as u64,
            "Invalid log file: broken header"
        );

        let idx_writer =
            IndexWriter::new(path.as_ref().join(format!("{file_name}.idx")), INDEX_HEADER)?;
        let ts_idx_writer = IndexWriter::new(
            path.as_ref().join(format!("{file_name}.ts.idx")),
            TS_INDEX_HEADER,
        )?;
        let (sender, receiver) = bounded(8);

        let file_size = Arc::new(AtomicU64::new(file_size));
        let file_size_view = file_size.clone();
        thread::spawn(move || Self::exec(file, file_size, receiver, idx_writer, ts_idx_writer));

        Ok(Self {
            sender,
            file_size_view,
        })
    }

    pub(crate) fn append_logs(&self, logs: Vec<Log>) -> Result<()> {
        self.sender.send(logs)?;
        Ok(())
    }

    pub(crate) fn file_size(&self) -> u64 {
        self.file_size_view.load(Ordering::Acquire)
    }

    fn exec(
        mut log_file: File,
        file_size: Counter,
        receiver: Receiver<Vec<Log>>,
        idx_writer: IndexWriter<Index>,
        ts_idx_writer: IndexWriter<Timestamp>,
    ) -> Result<()> {
        let mut header = Self::get_or_init_header(&mut log_file, &file_size)?;
        Self::check_header(&header)?;

        while let Ok(logs) = receiver.recv() {
            let mut buf = Vec::with_capacity(1024);
            let mut idx = Vec::with_capacity(logs.len());
            let mut ts_idx = Vec::with_capacity(logs.len());

            for log in logs {
                let size = file_size.load(Ordering::Acquire);
                let bytes = bincode::serialize(&log)?;

                idx.push(Index(log.id, size));
                ts_idx.push(Timestamp(log.ts, size));

                buf.write_all(&bytes).unwrap();
                file_size.fetch_add(bytes.len() as u64, Ordering::AcqRel);
                header.entry_count += 1;
            }

            idx_writer.append_log_indexes(idx)?;
            ts_idx_writer.append_log_indexes(ts_idx)?;

            log_file.write_all(&buf)?;
            log_file.write_at(0, &bincode::serialize(&header)?)?;

            log_file.sync_data()?;
        }

        Ok(())
    }

    fn get_or_init_header(log_file: &mut File, file_size: &Counter) -> Result<LogFileHeader> {
        let mut header = LogFileHeader::default();

        if file_size.load(Ordering::Acquire) == 0 {
            log_file.write_all(&bincode::serialize(&header)?)?;
            log_file.sync_data()?;

            file_size.store(size_of::<LogFileHeader>() as _, Ordering::Release);
        } else {
            let mut buf = Box::new([0u8; size_of::<LogFileHeader>()]);
            log_file.read_at(0, buf.as_mut_slice())?;

            header = LogFileHeader::try_from(buf.as_slice())?;
        }

        Ok(header)
    }

    #[inline]
    fn check_header(_header: &LogFileHeader) -> Result<()> {
        // TODO
        Ok(())
    }
}
