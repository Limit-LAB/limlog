use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
};

use anyhow::Result;
use bytes::{BufMut, BytesMut};
use kanal::{unbounded, Receiver, Sender};
use smallvec::SmallVec;

use crate::{
    appender::index_writer::IndexWriter,
    checker::LogChecker,
    formats::log::{Log, LogFileHeader, UuidIndex},
    util::BlockIODevice,
};

// Write log and index
#[derive(Debug)]
pub(crate) struct LogWriter {
    sender: Sender<Vec<Log>>,
    file_size_view: Arc<AtomicU64>,
}

impl LogWriter {
    pub(crate) fn new<F: BlockIODevice>(mut file: F, idx_file: F) -> Result<LogWriter> {
        let mut file_size = file.len()?;
        let header = LogChecker::check(&mut file, &mut file_size).or_init()?;

        let (sender, receiver) = unbounded();
        let inner = LogWriterInner {
            file,
            file_size: Arc::new(AtomicU64::new(file_size)),
            receiver,
            idx_writer: IndexWriter::new(idx_file)?,
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
    idx_writer: IndexWriter<F>,
}

impl<F: BlockIODevice> LogWriterInner<F> {
    // LARGE FUNCTION
    // write logs to file and drive IndexWriter to write indexes
    fn exec(mut self, mut header: LogFileHeader) -> Result<()> {
        let mut buf = BytesMut::with_capacity(1024).writer();

        while let Ok(logs) = self.receiver.recv() {
            buf.get_mut().clear();

            let mut idx = SmallVec::with_capacity(logs.len());

            for log in logs {
                idx.push(UuidIndex {
                    uuid: log.uuid,
                    offset: self.file_size.load(Ordering::Acquire),
                });

                bincode::serialize_into(&mut buf, &log)?;
                self.file_size
                    .fetch_add(bincode::serialized_size(&log)?, Ordering::AcqRel);

                header.entry_count += 1;
            }

            // write indexes
            self.idx_writer.append_log_indexes(idx)?;

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
