use anyhow::Result;
use kanal::{unbounded, Receiver, Sender};
use std::{
    io::Write,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
};

use crate::{
    appender::index_writer::IndexWriter,
    checker::LogChecker,
    formats::log::{Index, Log, LogFileHeader, Timestamp, INDEX_HEADER, TS_INDEX_HEADER},
    util::BlockIODevice,
};

#[derive(Debug)]
pub(crate) struct LogWriter {
    sender: Sender<Vec<Log>>,
    file_size_view: Arc<AtomicU64>,
}

impl LogWriter {
    pub(crate) fn new<F: BlockIODevice>(
        mut file: F,
        idx_file: F,
        ts_idx_file: F,
    ) -> Result<LogWriter> {
        let mut file_size = file.len()?;
        let header = LogChecker::check(&mut file, &mut file_size).or_init()?;

        let (sender, receiver) = unbounded();
        let inner = LogWriterInner {
            file,
            file_size: Arc::new(AtomicU64::new(file_size)),
            receiver,
            idx_writer: IndexWriter::new(idx_file, INDEX_HEADER)?,
            ts_idx_writer: IndexWriter::new(ts_idx_file, TS_INDEX_HEADER)?,
        };

        let file_size_view = inner.file_size.clone();
        thread::spawn(move || inner.exec(header));

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
}

struct LogWriterInner<F> {
    file: F,
    file_size: Arc<AtomicU64>,
    receiver: Receiver<Vec<Log>>,
    idx_writer: IndexWriter<F, Index>,
    ts_idx_writer: IndexWriter<F, Timestamp>,
}

impl<F: BlockIODevice> LogWriterInner<F> {
    fn exec(mut self, mut header: LogFileHeader) -> Result<()> {
        let mut buf = Vec::with_capacity(1024);

        while let Ok(logs) = self.receiver.recv() {
            buf.clear();

            let mut idx = Vec::with_capacity(logs.len());
            let mut ts_idx = Vec::with_capacity(logs.len());

            for log in logs {
                let size = self.file_size.load(Ordering::Acquire);
                let bytes = bincode::serialize(&log)?;

                idx.push(Index(log.id, size));
                ts_idx.push(Timestamp(log.ts, size));

                buf.write_all(&bytes)?;
                header.entry_count += 1;
                self.file_size
                    .fetch_add(bytes.len() as u64, Ordering::AcqRel);
            }

            self.idx_writer.append_log_indexes(idx)?;
            self.ts_idx_writer.append_log_indexes(ts_idx)?;

            self.file.write_all(&buf)?;
            self.file.write_at(0, &bincode::serialize(&header)?)?;
            self.file.sync_data()?;
        }

        Ok(())
    }
}
