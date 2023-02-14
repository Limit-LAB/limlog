use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
};

use bytes::{BufMut, BytesMut};
use kanal::{unbounded, Receiver, Sender};
use smallvec::SmallVec;

use crate::{
    appender::index_writer::IndexWriter,
    checker::LogChecker,
    formats::log::{IdIndex, Log, LogFileHeader, TsIndex, INDEX_HEADER, TS_INDEX_HEADER},
    util::BlockIODevice,
    Result,
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
        Ok(self.sender.send(logs)?)
    }

    pub(crate) fn file_size(&self) -> u64 {
        self.file_size_view.load(Ordering::Acquire)
    }
}

struct LogWriterInner<F> {
    file: F,
    file_size: Arc<AtomicU64>,
    receiver: Receiver<Vec<Log>>,
    idx_writer: IndexWriter<F, IdIndex>,
    ts_idx_writer: IndexWriter<F, TsIndex>,
}

impl<F: BlockIODevice> LogWriterInner<F> {
    // LARGE FUNCTION
    // write logs to file and drive IndexWriter to write indexes
    fn exec(mut self, mut header: LogFileHeader) -> Result<()> {
        let mut buf = BytesMut::with_capacity(1024).writer();

        while let Ok(logs) = self.receiver.recv() {
            buf.get_mut().clear();

            let mut idx = SmallVec::with_capacity(logs.len());
            let mut ts_idx = SmallVec::with_capacity(logs.len());

            for log in logs {
                let file_size = self.file_size.load(Ordering::Acquire);

                idx.push(IdIndex {
                    id: log.id,
                    offset: file_size,
                });
                ts_idx.push(TsIndex {
                    ts: log.ts,
                    offset: file_size,
                });

                let old_len = buf.get_ref().len();
                bincode::serialize_into(&mut buf, &log)?;
                self.file_size
                    .fetch_add((buf.get_ref().len() - old_len) as u64, Ordering::AcqRel);

                header.entry_count += 1;
            }

            // write indexes
            self.idx_writer.append_log_indexes(idx)?;
            self.ts_idx_writer.append_log_indexes(ts_idx)?;

            // write logs
            self.file.write_all(buf.get_ref())?;
            buf.get_mut().clear();

            // write header
            bincode::serialize_into(&mut buf, &header)?;
            self.file.write_at(0, buf.get_ref())?;
            self.file.sync_data()?;
        }

        Ok(())
    }
}
